use aws_sdk_cloudformation::Client as CloudFormationClient;
use aws_sdk_cloudformation::types::{
    ChangeSetType, OnFailure, Parameter, Stack, StackEvent, StackResource, StackStatus, Tag,
};
use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, CreateBucketConfiguration,
    ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
    NoncurrentVersionExpiration, ObjectIdentifier, PublicAccessBlockConfiguration, Tag as BucketTag,
};
use aws_sdk_sts::Client as StsClient;
use aws_types::region::Region;
use clap::{Arg, ArgAction, Command, ArgMatches};
use colored::*;
use itertools::Itertools;
use std::fs;
use std::time::Duration;
use std::io::{Write, stdin, stdout};
use serde_json::Value;
use chrono::{DateTime, Local, NaiveDateTime, Duration as ChronoDuration};
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use async_recursion::async_recursion;
use std::process::{Command as StdCommand, Stdio};
use string_morph::Morph;
use walkdir::WalkDir;
use json_structural_diff::JsonDiff;

pub mod utils;
pub use utils::*;

fn default_region() -> Region {
  Region::new("us-east-1")
}

fn region_name(region: &Region) -> String {
  region.to_string()
}

fn region_eq(region: &Region, other: &Region) -> bool {
  region.as_ref() == other.as_ref()
}

async fn build_cfn_client(region: Region) -> CloudFormationClient {
  let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .region(region)
    .load()
    .await;
  CloudFormationClient::new(&config)
}

async fn build_s3_client(region: Region) -> S3Client {
  let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .region(region)
    .load()
    .await;
  S3Client::new(&config)
}

async fn build_ec2_client(region: Region) -> Ec2Client {
  let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .region(region)
    .load()
    .await;
  Ec2Client::new(&config)
}

async fn build_sts_client(region: Region) -> StsClient {
  let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .region(region)
    .load()
    .await;
  StsClient::new(&config)
}

fn make_parameter(key: Option<String>, value: Option<String>, resolved_value: Option<String>, use_previous_value: Option<bool>) -> Parameter {
  Parameter::builder()
    .set_parameter_key(key)
    .set_parameter_value(value)
    .set_resolved_value(resolved_value)
    .set_use_previous_value(use_previous_value)
    .build()
}

fn make_tag(key: String, value: String) -> Tag {
  Tag::builder()
    .key(key)
    .value(value)
    .build()
}

async fn lookup_stack_outputs(stack_name: String, client: CloudFormationClient) -> Vec<Parameter> {
  lookup_stack_outputs_rek(stack_name, client, 0).await
}

#[async_recursion]
async fn lookup_stack_outputs_rek(stack_name: String, client: CloudFormationClient, i: u64) -> Vec<Parameter> {
  let describe_input = client.describe_stacks().stack_name(stack_name.clone());
  match describe_input.send().await {
    Ok(result) => {
      result.stacks()[0].outputs().iter().map(|output| {
        make_parameter(
          output.output_key().map(|k| k.to_string()),
          output.output_value().map(|v| v.to_string()),
          None,
          None,
        )
      }).collect::<Vec<Parameter>>()

    },
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in describe stack: {}", e);
      } else {
        println!("Something went wrong describing stack (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      lookup_stack_outputs_rek(stack_name, client, i+1).await
    }
  }
}

#[async_recursion]
async fn generate_completion_test_rek(stack_name: Option<String>, client: CloudFormationClient, i: u64) -> Vec<Stack> {
  let describe_input = client.describe_stacks().set_stack_name(stack_name.clone());
  match describe_input.send().await {
    Ok(result) => {
      result.stacks().to_vec()
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in describing Stack: {}", e);
      } else {
        println!("Something went wrong in describing Stack (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      generate_completion_test_rek(stack_name, client, i+1).await
    }
  }
}

#[async_recursion]
async fn wait_for_bucket_creation(client: S3Client, name: String, i: u64) {
  match client.list_buckets().send().await {
    Ok(result) => {
      let buckets = result.buckets();
      match buckets.binary_search_by(|bucket| bucket.name().unwrap_or("-").cmp(&name)) {
        Ok(_) => {}
        Err(e) => {
          let wait_time = 2000 + 1000 * i * i;
          if i > 20 {
            panic!("Retry limit reached in waiting for template bucket to create: {}", e);
          }
          tokio::time::sleep(Duration::from_millis(wait_time)).await;
          wait_for_bucket_creation(client, name, i+1).await
        }
      }
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in waiting for template bucket to create: {}", e);
      } else {
        println!("Something went wrong in waiting for template bucket (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      wait_for_bucket_creation(client, name, i+1).await
    }
  }
}

#[async_recursion]
async fn wait_for_changeset_creation(client: CloudFormationClient, change_set_name: String, stack_name: Option<String>, i: u64) {
  let describe_input = client.describe_change_set().change_set_name(change_set_name.clone()).set_stack_name(stack_name.clone());
  match describe_input.send().await {
    Ok(result) => {
      match result.status() {
        Some(status) => match status.as_str() {
          "CREATE_COMPLETE" => {}
          "CREATE_IN_PROGRESS" | "CREATE_PENDING" => {
            let wait_time = 2000 + 1000 * i * i;
            if i > 20 {
              panic!("Retry limit reached in waiting for changeset to complete");
            }
            tokio::time::sleep(Duration::from_millis(wait_time)).await;
            wait_for_changeset_creation(client, change_set_name, stack_name, i+1).await
          }
          "FAILED" => {pretty_panic(format!("Failed state in describe change set: {}", result.status_reason().unwrap_or("Empty status_reason")))}
          x => {panic!("Unknown state in describe change set: {}", x)}
        },
        None => {
          let wait_time = 2000 + 1000 * i * i;
          if i > 20 {
            panic!("Retry limit reached in waiting for changeset to complete");
          }
          tokio::time::sleep(Duration::from_millis(wait_time)).await;
          wait_for_changeset_creation(client, change_set_name, stack_name, i+1).await
        }
      }
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in describing changeset: {}", e);
      } else {
        println!("Something went wrong in describing changeset (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      wait_for_changeset_creation(client, change_set_name, stack_name, i+1).await
    }
  }
}

#[async_recursion]
async fn generate_events_output_rek(stack_name: Option<String>, client: CloudFormationClient, i: u64) -> Vec<StackEvent> {
  let events_input = client.describe_stack_events().set_stack_name(stack_name.clone());
  match events_input.send().await {
    Ok(result) => result.stack_events().to_vec(),
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in getting stack events: {}", e);
      } else {
        println!("Something went wrong in getting stack events (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      generate_events_output_rek(stack_name, client, i+1).await
    }
  }
}

#[async_recursion]
async fn poll_stack_status(stack_id: Option<String>, client: CloudFormationClient, region: Region, start_time: DateTime<Local>) {
  println!("DEBUG start poll");
  let mut last_printed = start_time;
  println!("{:25.25} {:70.70} {:50.50} {:}", "Time".bold(), "Resource Logical Id".bold(), "Resource Status".bold(), "Resource Status Reason".bold());
  // sleep(Duration::from_millis(1000));
  loop {
    let completion_test = generate_completion_test_rek(stack_id.clone(), client.clone(), 0).await;
    let events = generate_events_output_rek(stack_id.clone(), client.clone(), 0).await;
    pretty_print_stack_events(events.clone(), last_printed);
    last_printed = NaiveDateTime::parse_from_str(events.iter().max_by_key(|event| event.timestamp()).unwrap().timestamp().unwrap().to_string().as_str(), "%Y-%m-%dT%H:%M:%S%.3fZ").unwrap().and_utc().with_timezone(&Local);
    if [
      "CREATE_COMPLETE",
      "UPDATE_COMPLETE",
      "IMPORT_COMPLETE",
      "DELETE_COMPLETE",
      "CREATE_FAILED",
      "ROLLBACK_FAILED",
      "DELETE_FAILED",
      "UPDATE_ROLLBACK_FAILED",
      "UPDATE_ROLLBACK_COMPLETE",
      "IMPORT_ROLLBACK_FAILED"
    ].contains(&completion_test[0].stack_status().unwrap().as_str()) {
      break;
    }
    tokio::time::sleep(Duration::from_millis(2000)).await;
  }

  // TODO: Final printout? Status?, Exit-Code!
  let stack_result = client.describe_stacks().set_stack_name(stack_id.clone()).send().await.expect("Something went wrong describing stack").stacks().to_vec();

  if [
    "CREATE_FAILED"
  ].contains(&stack_result[0].stack_status().unwrap().as_str()) {
    let delete_stack_input = client.delete_stack().stack_name(stack_id.clone().expect("Something went wrong deleting failed stack"));
    if always_yes_or_ask(false, "destroy stack") {
      cleanup_resources(stack_id.clone().expect("Something went wrong deleting failed stack"), region.clone()).await;
      delete_stack_rek(client.clone(), delete_stack_input, 0).await;
      poll_stack_status(Some(lookup_stackid_to_name(stack_id.clone().expect("Something went wrong deleting failed stack"), client.clone()).await), client.clone(), region.clone(), start_time).await;
    } else {
      println!("Canceling destroy stack");
    }
  }

  if [
    "CREATE_COMPLETE",
    "UPDATE_COMPLETE"
  ].contains(&stack_result[0].stack_status().unwrap().as_str()) {
    let stack_output = client.describe_stacks().set_stack_name(stack_id.clone()).send().await.expect("Something went wrong describing stack");
    let outputs = stack_output.stacks()[0].outputs();
    if !outputs.is_empty() {
      println!("Outputs:");
      for output in outputs.iter().sorted_by_key(|output| output.output_key().map(|k| k.to_string())) {
        println!("{:50.50}: {}", output.output_key().unwrap_or("-").to_string().bold(), output.output_value().unwrap_or("-"));
      }
    }
  }
}

fn get_template_params(json: Value, is_update: bool) -> Vec<Parameter> {
  if let Some(params) = json.get("Parameters").and_then(Value::as_object) {
    params.iter().map(|(key, value)| {
      let optional_default = value.get("Default");

      let val = optional_default.and_then(value_to_string);
      make_parameter(
        Some(key.to_string()),
        val,
        None,
        if is_update { Some(optional_default.is_some()) } else { None },
      )
    }).collect()
  } else {
    vec![]
  }
}

#[derive(Clone)]
struct StackParameterFile {
  apply_stacks: Option<Vec<String>>,
  parameters: Option<HashMap<String, String>>,
  mappings: Option<HashMap<String, String>>,
  apply_mappings: Option<Vec<MappingValue>>,
  tags: Option<HashMap<String, String>>,
  template: Option<String>,
  region: Region
}

fn get_stack_parameter_file(stack_name: String) -> Option<StackParameterFile> {
  // stack-parameters
  let mut rb_filename: String = "".to_string();
  let mut json_filename: String = "".to_string();

  if Path::new("stack-parameters").exists() {
    for entry in WalkDir::new("stack-parameters") {
      let entry = entry.unwrap();
      let file_name = entry.file_name().to_str().unwrap();
      if file_name == format!("{}.rb", stack_name) {
        println!("Using parameter file: {}", entry.path().display());
        if !rb_filename.is_empty() || !json_filename.is_empty() {
          println!("{}", "Warning: Overriding stack parameter file to be used with new finding".yellow());
        }
        rb_filename = entry.path().display().to_string();
      } else if file_name == format!("{}.json", stack_name) {
        println!("Using parameter file: {}", entry.path().display());
        if !rb_filename.is_empty() || !json_filename.is_empty() {
          println!("{}", "Warning: Overriding stack parameter file to be used with new finding".yellow());
        }
        json_filename = entry.path().display().to_string();
      }
    }
  }
  if rb_filename.is_empty() && json_filename.is_empty() {
    println!("{}", "Warning: no stack parameter file found".yellow());
  }

  let body: String = if !rb_filename.is_empty() {
    ruby_stack_parameters(rb_filename)
  } else if !json_filename.is_empty() {
    fs::read_to_string(json_filename).expect("Something went wrong reading json stack params")
  } else {
    return None;
  };
  let content: Value = serde_json::from_str(&body).unwrap();

  let template = content.get("template").map(|template| value_to_string(template).expect("Template path is not a string"));

  let mut region = default_region();
  if let Some(parsed_region) = content.get("region") {
    region = map_region(&value_to_string(parsed_region).expect("Region parsing failed"));
  }

  let tags = content.get("tags").map(|tags_raw| {
    tags_raw.as_object().expect("Tags malformed in stack parameter file").iter().map(|(key, value)| {
      (key.clone(), value_to_string(value).expect("Tag value isn't string convertible"))
    }).collect()
  });
  let mappings = content.get("mappings").map(|mappings_raw| {
    mappings_raw.as_object().expect("Mappings malformed in stack parameter file").iter().map(|(key, value)| {
      (string_morph::to_pascal_case(key), value_to_string(value).expect("Mappings value isn't string convertible").to_pascal_case())
    }).collect()
  });
  let apply_mappings = content.get("apply_mappings").map(|raw_content| {
    raw_content.as_object().expect("Apply Mappings malformed in stack parameter file").iter().map(|(key, value)| {
      let obj = value.as_object().expect("Apply Mappings malformed in stack parameter file");
      MappingValue {
        stack_name: obj.get("stack_name").and_then(value_to_string),
        input_name: key.to_string().to_pascal_case(),
        output_name: value_to_string(obj.get("output_name").expect("Apply Mappings must contain the name of an output")).expect("Apply Mappings Output is not string convertible").to_pascal_case(),
        region: obj.get("region").map(|region| map_region(&value_to_string(region).expect("Region in apply mappings not string covertible")))
      }
    }).collect()
  });

  let parameters = content.get("parameters").map(|parameters_raw| {
    parameters_raw.as_object().expect("Parameters malformed in stack parameter file").iter().map(|(key, value)| {
      (key.clone(), value_to_string(value).expect("Parameter value isn't string convertible"))
    }).collect()
  });
  let apply_stacks = content.get("apply_stacks").map(|apply_stacks_raw| {
    apply_stacks_raw.as_array().expect("Apply stacks malformed in stack parameter file").iter().map(|v| value_to_string(v).expect("Stack name not stringifiable")).collect()
  });

  Some(StackParameterFile {
    apply_stacks,
    parameters,
    mappings,
    apply_mappings,
    tags,
    template,
    region
  })
}

#[async_recursion]
async fn list_stacks_prep(ec2: Ec2Client, list_opts: &ArgMatches, i: u64) {
  let all_regions_input = ec2.describe_regions().all_regions(false);
  match all_regions_input.send().await {
    Ok(output) => {
      let regions = output.regions();
      if regions.is_empty() {
        panic!("No regions returned from all regions");
      }
      for ec2_region in regions {
        let region = map_region(ec2_region.region_name().expect("No region name in all regions"));
        let client = build_cfn_client(region.clone()).await;
        list_stacks_main(client, region, list_opts).await;
      }
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in list stacks prep: {}", e);
      } else {
        println!("Something went wrong in list stacks prep (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      list_stacks_prep(ec2, list_opts, i + 1).await
    }
  }
}

async fn list_stacks_main(client: CloudFormationClient, region: Region, list_opts: &ArgMatches) {
    println!("Listing stacks for region {}", region_name(&region).bright_white().bold());
    let mut list_stacks_input = client.list_stacks();
    if list_opts.contains_id("status") {
        list_stacks_input = list_stacks_input.stack_status_filter(StackStatus::from(list_opts.get_one::<String>("status").unwrap().as_str()));
    } else if !list_opts.contains_id("deleted") {
    let list_of_types = [
      "CREATE_IN_PROGRESS",
      "CREATE_FAILED",
      "CREATE_COMPLETE",
      "ROLLBACK_IN_PROGRESS",
      "ROLLBACK_FAILED",
      "ROLLBACK_COMPLETE",
      "DELETE_IN_PROGRESS",
      "DELETE_FAILED",
      // "DELETE_COMPLETE",
      "UPDATE_IN_PROGRESS",
      "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
      "UPDATE_COMPLETE",
      "UPDATE_ROLLBACK_IN_PROGRESS",
      "UPDATE_ROLLBACK_FAILED",
      "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
      "UPDATE_ROLLBACK_COMPLETE",
      "REVIEW_IN_PROGRESS",
      "IMPORT_IN_PROGRESS",
      "IMPORT_COMPLETE",
      "IMPORT_ROLLBACK_IN_PROGRESS",
      "IMPORT_ROLLBACK_FAILED",
      "IMPORT_ROLLBACK_COMPLETE"
    ];
    for status in list_of_types.iter() {
      list_stacks_input = list_stacks_input.stack_status_filter(StackStatus::from(*status));
    }
  }
  list_stacks_rek(client, list_stacks_input, 0).await
}

async fn list_stacks(matches: ArgMatches) {
    let region = default_region();

    let list_opts = matches.subcommand_matches("list").unwrap();
    if list_opts.contains_id("all-regions") {
    let ec2 = build_ec2_client(region.clone()).await;
    list_stacks_prep(ec2, list_opts, 0).await
  } else {
    let client = build_cfn_client(region.clone()).await;
    list_stacks_main(client, region, list_opts).await
  }
}

#[async_recursion]
async fn list_stacks_rek(_client: CloudFormationClient, list_stacks_input: aws_sdk_cloudformation::operation::list_stacks::builders::ListStacksFluentBuilder, i: u64) {
  match list_stacks_input.clone().send().await {
    Ok(output) => {
      let stack_list = output.stack_summaries();
      if stack_list.is_empty() {
        println!("No stacks");
      } else {
        for (status, grouped_stack_list) in stack_list.iter().map(|stack| (stack.stack_status().map(|s| s.as_str().to_string()).unwrap_or("UNKNOWN".to_string()), stack.clone())).into_group_map().iter().sorted_by_key(|(status, _)| *status) {
          println!("{}", match_status_color(status, status).bold());
          for stack in grouped_stack_list {
            println!("{:120.120} {}", match_status_color(status, stack.stack_name().unwrap_or("-")), match_status_color(status, status));
          }
          println!();
        }
      }
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in list stacks: {}", e);
      } else {
        println!("Something went wrong listing stacks (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      list_stacks_rek(_client, list_stacks_input, i+1).await
    }
  }
}

fn generate_matches() -> ArgMatches {
    Command::new("sfn-ng")
        .version("0.2.30")
        .author("Patrick Robinson <patrick.robinson@bertelsmann.de>")
        .about("Does sparkleformation command stuff")
        .subcommand(
            Command::new("list")
                .about("Lists stacks")
                .arg(
                    Arg::new("status")
                        .short('s')
                        .long("status")
                        .action(ArgAction::Set)
                        .help("Match stacks with given status")
                )
                .arg(
                    Arg::new("deleted")
                        .short('d')
                        .long("deleted")
                        .action(ArgAction::SetTrue)
                        .help("Include deleted stacks")
                )
                .arg(
                    Arg::new("all-regions")
                        .short('a')
                        .long("all-regions")
                        .action(ArgAction::SetTrue)
                        .help("List stacks for each regions")
                )
        )
        .subcommand(
            Command::new("attach-event-stream")
                .about("Attaches to a running CloudFormation operations event stream")
                .arg(
                    Arg::new("STACKNAME")
                        .help("Name of the stack to follow")
                        .required(true)
                        .index(1)
                )
                .arg(
                    Arg::new("time-backwards")
                        .short('t')
                        .long("time-backwards")
                        .action(ArgAction::Set)
                        .help("How long backwards (in minutes) to start printing events from")
                )
        )
        .subcommand(
            Command::new("destroy")
                .about("Destroys a stack")
                .arg(
                    Arg::new("STACKNAME")
                        .help("Sets the StackName")
                        .required(true)
                        .index(1)
                )
                .arg(
                    Arg::new("yes")
                        .short('y')
                        .long("yes")
                        .action(ArgAction::SetTrue)
                        .help("Automatically accept any requests for confirmation")
                )
                .arg(
                    Arg::new("poll")
                        .short('p')
                        .long("poll")
                        .action(ArgAction::Set)
                        .help("Poll stack events on modification actions (default: true)")
                )
        )
        .subcommand(
            Command::new("convert-parameter-file")
                .about("Converts a ruby parameter file to json")
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .action(ArgAction::Set)
                        .required(true)
                        .help("Which stack parameter file to use")
                )
        )
        .subcommand(
            Command::new("create")
                .about("Create a new stack")
                .arg(
                    Arg::new("STACKNAME")
                        .help("Sets the StackName")
                        .required(true)
                        .index(1)
                )
                .arg(
                    Arg::new("apply-mapping")
                        .long("apply-mapping")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Customize apply stack mapping (OutputName=ParameterName[,OutputName=ParameterName,...])")
                )
                .arg(
                    Arg::new("apply-stack")
                        .short('A')
                        .long("apply-stack")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Apply outputs from stack to input parameters")
                )
                .arg(
                    Arg::new("defaults")
                        .short('d')
                        .long("defaults")
                        .action(ArgAction::SetTrue)
                        .help("Automatically accept default values")
                )
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .value_name("FILE")
                        .action(ArgAction::Set)
                        .help("Path to template file")
                )
                .arg(
                    Arg::new("parameters")
                        .short('m')
                        .long("parameters")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Pass template parameters directly (Key=Value[,Key=Value,...])")
                )
                .arg(
                    Arg::new("poll")
                        .short('p')
                        .long("poll")
                        .action(ArgAction::Set)
                        .help("Poll stack events on modification actions (default: true)")
                )
                .arg(
                    Arg::new("tags")
                        .short('t')
                        .long("tags")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Tags of the resulting Stack (Key=Value[,Key=Value,...])")
                )
                .arg(
                    Arg::new("yes")
                        .short('y')
                        .long("yes")
                        .action(ArgAction::SetTrue)
                        .help("Automatically accept any requests for confirmation")
                )
        )
        .subcommand(
            Command::new("update")
                .about("Updates a stack")
                .arg(
                    Arg::new("STACKNAME")
                        .help("Sets the StackName")
                        .required(true)
                        .index(1)
                )
                .arg(
                    Arg::new("apply-mapping")
                        .long("apply-mapping")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Customize apply stack mapping (OutputName=ParameterName[,OutputName=ParameterName,...])")
                )
                .arg(
                    Arg::new("apply-stack")
                        .short('A')
                        .long("apply-stack")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Apply outputs from stack to input parameters")
                )
                .arg(
                    Arg::new("defaults")
                        .short('d')
                        .long("defaults")
                        .action(ArgAction::SetTrue)
                        .help("Automatically accept default values")
                )
                .arg(
                    Arg::new("changed-params")
                        .short('D')
                        .long("changed-params")
                        .action(ArgAction::SetTrue)
                        .help("Only show the parameters that differ from the currently deployed stack")
                )
                .arg(
                    Arg::new("diff")
                        .short('j')
                        .long("diff")
                        .action(ArgAction::Set)
                        .help("Display JSON diff of templates (default: true)")
                )
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .value_name("FILE")
                        .action(ArgAction::Set)
                        .help("Path to template file")
                )
                .arg(
                    Arg::new("parameters")
                        .short('m')
                        .long("parameters")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Pass template parameters directly (Key=Value[,Key=Value,...])")
                )
                .arg(
                    Arg::new("poll")
                        .short('p')
                        .long("poll")
                        .action(ArgAction::Set)
                        .help("Poll stack events on modification actions (default: true)")
                )
                .arg(
                    Arg::new("tags")
                        .short('t')
                        .long("tags")
                        .action(ArgAction::Append)
                        .value_delimiter(',')
                        .help("Tags of the resulting Stack (Key=Value[,Key=Value,...])")
                )
                .arg(
                    Arg::new("yes")
                        .short('y')
                        .long("yes")
                        .action(ArgAction::SetTrue)
                        .help("Automatically accept any requests for confirmation")
                )
        )
        .get_matches()
}

#[async_recursion]
async fn create_stack_rek(poll: bool, client: CloudFormationClient, region: Region, create_stack_input: aws_sdk_cloudformation::operation::create_stack::builders::CreateStackFluentBuilder, start_time: DateTime<Local>, i: u64) {
  match create_stack_input.clone().send().await {
    Ok(output) => {
      if poll {
        poll_stack_status(output.stack_id().map(|s| s.to_string()), client, region.clone(), start_time).await;
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in create stacks: {}", e);
      } else {
        println!("Something went wrong creating stacks (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      create_stack_rek(poll, client, region.clone(), create_stack_input, start_time, i+1).await
    }
  }
}

#[async_recursion]
async fn delete_stack_rek(_client: CloudFormationClient, delete_stack_input: aws_sdk_cloudformation::operation::delete_stack::builders::DeleteStackFluentBuilder, i: u64) {
  match delete_stack_input.clone().send().await {
    Ok(_) => {},
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in delete stack: {}", e);
      } else {
        println!("Something went wrong deleting stack (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      delete_stack_rek(_client, delete_stack_input, i+1).await
    }
  }
}

fn map_region(s: &str) -> Region {
  Region::new(s.to_string())
}

#[derive(Clone)]
struct StackInput {
  stack_name: String,
  region: Region,
  used_parameters: Vec<Parameter>,
  tags: Option<Vec<Tag>>,
  client: CloudFormationClient,
  bucket: String,
  path: String
}

#[async_recursion]
async fn get_old_stack_parameters_rek(stack_name: String, region: Region, i: u64) -> Vec<Parameter> {
  let client = build_cfn_client(region.clone()).await;
  let input = client.describe_stacks().stack_name(stack_name.clone());
  match input.send().await {
    Ok(output) => {
      let stacks = output.stacks();
      if stacks.len() == 1 {
        stacks[0].parameters().to_vec()
      } else {
        panic!("No existing stacks with that name found");
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in get_old_stack_parameters_rek: {}", e);
      } else {
        println!("Something went wrong in get_old_stack_parameters_rek (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      get_old_stack_parameters_rek(stack_name, region, i+1).await
    }
  }
}

#[async_recursion]
async fn get_old_template_body_rek(stack_name: String, region: Region, i: u64) -> String {
  let client = build_cfn_client(region.clone()).await;
  let input = client.get_template().stack_name(stack_name.clone());
  match input.send().await {
    Ok(output) => {
      output.template_body().expect("No template body returned from existing stack").to_string()
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in get_old_template_body_rek: {}", e);
      } else {
        println!("Something went wrong in get_old_template_body_rek (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      get_old_template_body_rek(stack_name, region, i+1).await
    }
  }
}

#[derive(Clone)]
struct MappingValue {
  stack_name: Option<String>,
  region: Option<Region>,
  output_name: String,
  input_name: String
}

#[derive(Clone)]
struct ApplyStackParameter {
  stack_name: String,
  region: Region,
  outputs: Vec<Parameter>
}

// if upgrade check for previous values of params & tags as well.
async fn prepare_stack_input(opts: &ArgMatches, start_time: DateTime<Local>, is_upgrade: bool) -> StackInput {
    let stack_name = opts.get_one::<String>("STACKNAME").expect("No Stack named").to_string();
    println!("Value for StackName: {}", stack_name);

    let stack_parameter_file = get_stack_parameter_file(stack_name.clone());

    let mut region = default_region();
    if let Some(stack_parameter_file) = stack_parameter_file.as_ref() {
        region = stack_parameter_file.region.clone();
    }
    let client = build_cfn_client(region.clone()).await;

    println!("Region: {}", region_name(&region));

    let explicit_parameters: Vec<Parameter> = match opts.get_many::<String>("parameters") {
        Some(parameters_list) => parameters_list.map(|input| {
            let pair = input.split("=").collect::<Vec<_>>();
            make_parameter(
              Some(pair[0].to_string()),
              Some(pair[1].to_string()),
              None,
              None,
            )
        }).collect::<Vec<Parameter>>(),
        None => Vec::new()
    };

  /*

    paramaters do
      input 0
    end
    mappings do
      output 'Input'
    end
    apply_mappings do
      input do
        region 'us-east-1'
        stack_name 'stack'
        output_name 'Output'
      end
     end
    apply_stacks [
      us_east_1__stack
    ]
   */

  let mut mappings: Vec<MappingValue> = match opts.get_many::<String>("apply-mapping") {
    Some(list) => list.map(|input| {
      let pair = input.split("=").collect::<Vec<&str>>();
      // TODO: allow for stack & region let enc_output = pair[0].to_string().clone().split("__");
      MappingValue {
        region: None,
        stack_name: None,
        output_name: string_morph::to_pascal_case(pair[0]),
        input_name: string_morph::to_pascal_case(pair[1])
      }
    }).collect(),
    None => vec![]
  };
  if let Some(stack_parameter_file) = stack_parameter_file.as_ref() {
    mappings.extend(stack_parameter_file.mappings.as_ref().unwrap_or(&HashMap::new()).iter().map(|(key, value)| {
      MappingValue {
        region: None,
        stack_name: None,
        output_name: string_morph::to_pascal_case(key),
        input_name: string_morph::to_pascal_case(value)
      }
    }).collect::<Vec<MappingValue>>());
    if let Some(apply_mappings) = stack_parameter_file.apply_mappings.as_ref() {
      mappings.extend(apply_mappings.clone());
    }
  }

  // TODO: Set default Tags by env, if not set by --tags manually
  let mut tags_vec: Vec<Tag> = vec![];
  if let Some(stack_parameter_file) = stack_parameter_file.as_ref()
    && let Some(tags) = stack_parameter_file.tags.as_ref() {
    for (key, value) in tags {
      tags_vec.push(make_tag(key.clone(), value.clone()));
    }
  }

  if let Some(mytags) = opts.get_many::<String>("tags") {
    for input in mytags {
      let pair = input.split("=").collect::<Vec<&str>>();
      let tag = make_tag(pair[0].to_string(), pair[1].to_string());
      let pos = tags_vec.iter().position(|ex_tag| ex_tag.key() == tag.key());
      if let Some(pos) = pos {
        tags_vec.push(tag);
        tags_vec.swap_remove(pos);
      } else {
        tags_vec.push(tag);
      }
    }
  }
  if !tags_vec.iter().any(|tag| tag.key() == Some("Projekt")) {
    let mut input = String::new();
    print!("Projekt?: ");
    stdout().flush().unwrap();
    stdin().read_line(&mut input).expect("Cancel Stack creation");
    input.pop();
    if input.is_empty() {
      panic!("No project tag set by any means");
    } else {
      tags_vec.push(make_tag("Projekt".to_string(), input));
    }
  }

  let search_for_creator = tags_vec.iter().position(|tag| tag.key() == Some("creator"));
  let mut me = whoami::username().expect("Could not determine username");
  if me.contains("\\") {
    let split = me.split("\\").collect::<Vec<&str>>();
    me = split[1].to_string();
  }
  tags_vec.push(make_tag("creator".to_string(), me));
  if let Some(pos) = search_for_creator {
    tags_vec.swap_remove(pos);
  }
  let tags = Some(tags_vec);
  let mut template_file: Option<String> = opts.get_one::<String>("file").map(|s| s.to_string());
  if let Some(stack_parameter_file) = stack_parameter_file.as_ref()
    && let Some(template) = stack_parameter_file.template.as_ref() {
    template_file = Some(template.clone());
  }
  let s3 = build_s3_client(region.clone()).await;
  let bucket = find_template_bucket_or_create_it_rek(region.clone(), 0).await;
  let template_parameters: Vec<Parameter>;
  let path: String;
  let template_body: Option<String>;
  let mut old_params: Vec<Parameter> = vec![];
  let mut old_params_map: HashMap<String, ColoredString> = HashMap::new();
  if is_upgrade {
    old_params = get_old_stack_parameters_rek(stack_name.clone(), region.clone(), 0).await;
    old_params_map = old_params.iter().map(|param| (param.parameter_key().map(|k| k.to_string()).expect("Old parameter key not set"), param.parameter_value().map(|v| v.to_string()).expect("Old parameter value not set").dimmed())).collect();
  }
  if let Some(template_file) = template_file.as_ref() {
    template_body = Some(fs::read_to_string(template_file).expect("Something went wrong reading the file"));
    let template_content: Value = serde_json::from_str(&template_body.clone().unwrap()).unwrap();

    let diff = match opts.get_one::<String>("diff") {
      Some(diff_value) => diff_value.as_str() == "true",
      None => true
    };

    if is_upgrade && diff {
      let old_template = serde_json::from_str(&get_old_template_body_rek(stack_name.clone(), region.clone(), 0).await).expect("Issue in parsing old template body as json");
      let json_diffs = JsonDiff::diff_string(&old_template, &template_content, false);
      match json_diffs {
        Some(json_diff) => {
          println!("Changes in template:");
          for line in json_diff.split('\n') {
            match line.chars().next() {
              Some('+') => {
                println!("{}", line.green());
              },
              Some('-') => {
                println!("{}", line.red());
              },
              Some('~') => {
                println!("{}", line.yellow());
              },
              _ => {
                println!("{}", line.white().dimmed());
              }
            };
          }
        },
        None => { println!("No changes in template"); }
      }
      println!("\n");
    }
    template_parameters = get_template_params(template_content, false); // TODO: yaml support
    path = format!("{}/{}", template_file, start_time.timestamp());
  } else {
    if is_upgrade {
      template_parameters = old_params;
      template_body = Some(get_old_template_body_rek(stack_name.clone(), region.clone(), 0).await);
      path = format!("without-new-template/{}/{}", stack_name.clone(), start_time.timestamp());
    } else {
      panic!("No template specified for create stack");
    }
  }

  let upload_template_input = s3.put_object()
    .body(aws_smithy_types::byte_stream::ByteStream::from(template_body.clone().unwrap().as_bytes().to_vec()))
    .bucket(bucket.clone())
    .key(path.clone());
  upload_template_input.send().await.expect("Template couldn't be uploaded to S3");

  let mut apply_stack_parameters: Vec<ApplyStackParameter> = vec![];
  let mut stacks: Vec<String> = vec![];
  if let Some(stack_parameter_file) = stack_parameter_file.as_ref()
    && let Some(apply_stacks) = stack_parameter_file.apply_stacks.as_ref() {
    stacks.extend(apply_stacks.iter().cloned());
  }
  if let Some(applystack) = opts.get_many::<String>("apply-stack") {
    stacks.extend(applystack.map(|s| s.to_string()).collect::<Vec<String>>());
  }

  for stack in stacks.iter().dedup() {
    let stack_parts: Vec<&str> = stack.split("__").collect();
    if stack_parts.len() > 1 {
      // Camel Cased: let region = stack_parts[0].split("_").collect::<Vec<&str>>().iter().map(upcast).collect::<Vec<String>>().join("");
      let region = stack_parts[0].split("_").collect::<Vec<&str>>().join("-");
      let aws_region = map_region(&region);
      let client = build_cfn_client(aws_region.clone()).await;
      apply_stack_parameters.push( ApplyStackParameter {
        outputs: lookup_stack_outputs(stack_parts[1].to_string(), client.clone()).await,
        region: aws_region,
        stack_name: stack_parts[1].to_string()
      });
    } else {
      apply_stack_parameters.push(ApplyStackParameter {
        outputs: lookup_stack_outputs(stack.to_string(), client.clone()).await,
        region: region.clone(),
        stack_name: stack.to_string()
      });
    }
  }

  apply_stack_parameters.reverse();

  let stack_params: Option<Vec<Parameter>> = if let Some(stack_parameter_file) = stack_parameter_file.as_ref() {
    stack_parameter_file.parameters.as_ref().map(|parameters| parameters.iter().map(|(key, value)| make_parameter(
      Some(key.to_string()),
      Some(value.to_string()),
      None,
      None
    )).collect())
  } else {
    None
  };

  let merged_parameters = template_parameters.iter().map(|default_param| {
    for explicit_param in explicit_parameters.iter() {
      if explicit_param.parameter_key() == default_param.parameter_key() {
        return explicit_param.clone();
      }
    }
    if let Some(stack_params) = stack_params.as_ref() {
      for stack_param in stack_params.iter() {
        if stack_param.parameter_key() == default_param.parameter_key() {
          return stack_param.clone();
        }
      }
    }
    for apply_param_stack in apply_stack_parameters.iter() {
      for apply_param in apply_param_stack.outputs.iter() {
        let matching_mapping = mappings.iter().find(|value| {
          let result1 = value.input_name == default_param.parameter_key().unwrap();
          let result2 = value.region.is_none() || (region_eq(value.region.as_ref().unwrap(), &apply_param_stack.region));
          let result3 = value.stack_name.is_none() || (*value.stack_name.as_ref().unwrap() == apply_param_stack.stack_name);
          result1 && result2 && result3
        });
        if let Some(mapping_value) = matching_mapping {
          if apply_param.parameter_key().unwrap() == mapping_value.output_name {
            println!("Mapping Matched input:{} output:{}", mapping_value.input_name, mapping_value.output_name);
            return make_parameter(
              Some(default_param.parameter_key().unwrap().to_string()),
              apply_param.parameter_value().map(|v| v.to_string()),
              apply_param.resolved_value().map(|v| v.to_string()),
              apply_param.use_previous_value()
            );
          }
        } else {
          if apply_param.parameter_key() == default_param.parameter_key() {
            return apply_param.clone();
          }
        }
      }
    }
    default_param.clone()
  }).collect::<Vec<Parameter>>();

  let mut dirty_flag_parameter_header = false;
  let used_parameters = merged_parameters.iter().map(|param| {
    if opts.contains_id("defaults")
      && param.parameter_value().is_some() {
      return make_parameter(
        param.parameter_key().map(|k| k.to_string()),
        param.parameter_value().map(|v| v.to_string()),
        param.resolved_value().map(|v| v.to_string()),
        param.use_previous_value()
      );
    }
    if is_upgrade {
      let new_word = "new".italic();
      let old = old_params_map.get(param.parameter_key().unwrap()).unwrap_or(&new_word);
      let new = param.parameter_value().unwrap_or("").italic();
      let not_changed = new.clone().normal().clear().eq(&old.clone().normal().clear());
      if opts.contains_id("changed-params") && not_changed {
        return make_parameter(
          param.parameter_key().map(|k| k.to_string()),
          param.parameter_value().map(|v| v.to_string()),
          param.resolved_value().map(|v| v.to_string()),
          param.use_previous_value()
        );
      }
      if !dirty_flag_parameter_header {
        println!("Parameters for StackName");
        dirty_flag_parameter_header = true;
      }
      let divider = if not_changed { "=" } else { "→" };
      print!("{}?:[{}{}{}] ", param.parameter_key().unwrap().bold(), old, divider, new);
    } else {
      if !dirty_flag_parameter_header {
        println!("Parameters for StackName");
        dirty_flag_parameter_header = true;
      }
      print!("{}?:[{}] ", param.parameter_key().unwrap().bold(), param.parameter_value().unwrap_or("").italic());
    }
    let mut input = String::new();
    stdout().flush().unwrap();
    stdin().read_line(&mut input).expect("Cancel Stack creation");
    input.pop();
    if !input.is_empty() {
      make_parameter(
        param.parameter_key().map(|k| k.to_string()),
        Some(input),
        None,
        None
      )
    } else {
      make_parameter(
        param.parameter_key().map(|k| k.to_string()),
        param.parameter_value().map(|v| v.to_string()),
        param.resolved_value().map(|v| v.to_string()),
        param.use_previous_value()
      )
    }
  }).collect::<Vec<Parameter>>();

  StackInput {
    stack_name,
    region,
    used_parameters,
    tags,
    client,
    bucket,
    path
  }
}

#[async_recursion]
async fn create_changeset_diff_display(client: CloudFormationClient, change_set_name: String, stack_name: Option<String>, next_token: Option<String>, _start_time: DateTime<Local>, i: u64) {
  if next_token.is_none() {
    wait_for_changeset_creation(client.clone(), change_set_name.clone(), stack_name.clone(), 0).await;
  }
  let mut describe_change_set_input = client.describe_change_set().change_set_name(change_set_name.clone()).set_stack_name(stack_name.clone());
  if let Some(token) = next_token.clone() {
    describe_change_set_input = describe_change_set_input.next_token(token);
  }
  match describe_change_set_input.send().await {
    Ok(output) => {
      let changes = output.changes();
      if changes.is_empty() {
        println!("No further changes found");
      } else {
        // TODO: Title Headers
        println!("{:6.6} {:7.7} {:50.50} {:50.50} {:70.70} {:}", "Action".bold(), "Replace".bold(), "Type".bold(), "Logical ID".bold(), "Physical ID".bold(), "Scope".bold());
        for change in changes {
          pretty_print_resource_change(change);
        }
      }
      if let Some(token) = output.next_token() {
        create_changeset_diff_display(client, change_set_name, stack_name, Some(token.to_string()), _start_time, 0).await;
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in update stacks: {}", e);
      } else {
        println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      create_changeset_diff_display(client, change_set_name, stack_name, next_token, _start_time, i+1).await
    }
  }
}

#[async_recursion]
async fn execute_change_set_rek(poll: bool, client: CloudFormationClient, region: Region, stack_id: Option<String>, execute_changeset_input: aws_sdk_cloudformation::operation::execute_change_set::builders::ExecuteChangeSetFluentBuilder, start_time: DateTime<Local>, i: u64) {
  match execute_changeset_input.clone().send().await {
    Ok(_output) => {
      if poll {
        poll_stack_status(stack_id.clone(), client, region.clone(), start_time).await;
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in update stacks: {}", e);
      } else {
        println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      execute_change_set_rek(poll, client, region.clone(), stack_id, execute_changeset_input, start_time, i+1).await
    }
  }
}

#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn update_stack_rek(poll: bool, client: CloudFormationClient, region: Region, create_changeset_input: aws_sdk_cloudformation::operation::create_change_set::builders::CreateChangeSetFluentBuilder, change_set_name: String, stack_name: String, always_yes: bool, start_time: DateTime<Local>, i: u64) {
  match create_changeset_input.clone().send().await {
    Ok(output) => {
      create_changeset_diff_display(client.clone(), change_set_name.clone(), Some(stack_name.clone()), None, start_time, 0).await;
      // Ask user for permission, unless --yes
      if always_yes_or_ask(always_yes, "update stack") {
        // execute change set & poll status, unless --no-poll
        let execute_change_set_input = client.execute_change_set()
          .change_set_name(change_set_name)
          .stack_name(stack_name);
        execute_change_set_rek(poll, client, region.clone(), output.stack_id().map(|s| s.to_string()), execute_change_set_input, start_time, 0).await;
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in update stacks: {}", e);
      } else {
        println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      update_stack_rek(poll, client, region.clone(), create_changeset_input, change_set_name, stack_name, always_yes, start_time, i+1).await
    }
  }
}

async fn bucket_settings(client: S3Client, name: String) {
  println!("Putting bucket lifecycle rule");
  let lifecycle_input = client.put_bucket_lifecycle_configuration()
    .bucket(name.clone())
    .lifecycle_configuration(BucketLifecycleConfiguration::builder()
      .rules(
        LifecycleRule::builder()
          .expiration(LifecycleExpiration::builder()
            .days(32)
            .build())
          .filter(LifecycleRuleFilter::builder().build())
          .id("Lifecycle".to_string())
          .noncurrent_version_expiration(NoncurrentVersionExpiration::builder()
            .noncurrent_days(32)
            .build())
          .status(ExpirationStatus::Enabled)
          .build()
          .expect("Failed to build LifecycleRule")
      )
      .rules(
        LifecycleRule::builder()
          .abort_incomplete_multipart_upload(AbortIncompleteMultipartUpload::builder()
            .days_after_initiation(3)
            .build())
          .expiration(LifecycleExpiration::builder()
            .expired_object_delete_marker(true)
            .build())
          .filter(LifecycleRuleFilter::builder().build())
          .id("Cleanup".to_string())
          .status(ExpirationStatus::Enabled)
          .build()
          .expect("Failed to build LifecycleRule")
      )
      .build()
      .expect("Failed to build BucketLifecycleConfiguration"));
  match lifecycle_input.send().await {
    Ok(_) => {
      // println!("DEBUG: Lifecycle worked");
    }
    Err(e) => {
      println!("Error putting bucket lifecycle rule: {}", e);
    }
  };
  println!("Putting public access block config");
  let public_access_block_input = client.put_public_access_block()
    .bucket(name.clone())
    .public_access_block_configuration(PublicAccessBlockConfiguration::builder()
      .block_public_acls(true)
      .block_public_policy(true)
      .ignore_public_acls(true)
      .restrict_public_buckets(true)
      .build());
  match public_access_block_input.send().await {
    Ok(_) => {
      println!("Put public access block config");
    }
    Err(e) => {
      println!("Error putting public access block config: {}", e);
    }
  }
  let get_tags = client.get_bucket_tagging().bucket(name.clone());
  println!("Tagging bucket {}", name);
  let mut tag_set: Vec<BucketTag>;
  match get_tags.send().await {
    Ok(tags) => {
      tag_set = tags.tag_set().to_vec();
      match tag_set.iter().position(|x| x.key() == "BackupPlan") {
        Some(i) => {
          if tag_set[i].value() == "none" {
            return;
          } else {
            tag_set[i] = BucketTag::builder()
              .key(tag_set[i].key().to_string())
              .value("none".to_string())
              .build()
              .expect("Failed to build BucketTag");
          }
        }
        None => {
          tag_set.push(BucketTag::builder()
            .key("BackupPlan".to_string())
            .value("none".to_string())
            .build()
            .expect("Failed to build BucketTag"));
        }
      }
    }
    Err(_) => {
      tag_set = vec![
        BucketTag::builder()
          .key("BackupPlan".to_string())
          .value("none".to_string())
          .build()
          .expect("Failed to build BucketTag")
      ];
    }
  }
  let mut tagging_builder = aws_sdk_s3::types::Tagging::builder();
  for tag in tag_set {
    tagging_builder = tagging_builder.tag_set(tag);
  }
  let tag_input = client.put_bucket_tagging()
    .bucket(name.clone())
    .tagging(tagging_builder.build().expect("Failed to build Tagging"));
  match tag_input.send().await {
    Ok(_) => {
      println!("Tagged bucket {}", name);
    }
    Err(e) => {
      println!("Error tagging bucket {}: {}", name, e);
    }
  }
}

#[async_recursion]
async fn create_bucket_rek(client: S3Client, region: Region, name: String, i: u64) -> String {
  let mut create_input = client.create_bucket()
    .bucket(name.clone());
  if !region_eq(&region, &Region::new("us-east-1")) {
    create_input = create_input.create_bucket_configuration(CreateBucketConfiguration::builder()
      .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(region_name(&region).as_str()))
      .build());
  }
  match create_input.send().await {
    Ok(_) => {
      wait_for_bucket_creation(client, name.clone(), 0).await;
      name
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in create bucket: {}", e);
      } else {
        println!("Something went wrong creating bucket (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      create_bucket_rek(client, region, name, i+1).await
    }
  }
}

#[async_recursion]
async fn find_template_bucket_or_create_it_rek(region: Region, i: u64) -> String {
  let client = build_s3_client(region.clone()).await;
  let sts = build_sts_client(region.clone()).await;

  let caller_identity_input = sts.get_caller_identity();

  match caller_identity_input.send().await {
    Ok(identity) => {
      let name = format!("sfn-ng-{}-{}", region_name(&region), identity.account().unwrap());
      let result: String;
      match client.list_buckets().send().await {
        Ok(bucket_output) => {
          let buckets = bucket_output.buckets();
          match buckets.iter().find(|bucket| bucket.name().unwrap() == name) {
            Some(_bucket) => {
              result = name.clone();
            }
            None => {
              result = create_bucket_rek(client.clone(), region, name.clone(), 0).await;
            }
          }
          tokio::spawn(bucket_settings(client.clone(), name.clone()));
          result
        }
        Err(e) => {
          let wait_time = 2000 + 1000 * i * i;
          if i > 20 {
            panic!("Retry limit reached in update stacks: {}", e);
          } else {
            println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
          }
          tokio::time::sleep(Duration::from_millis(wait_time)).await;
          find_template_bucket_or_create_it_rek(region, i+1).await
        }
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in find template bucket: {}", e);
      } else {
        println!("Something went wrong finding template bucket (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      find_template_bucket_or_create_it_rek(region, i+1).await
    }
  }
}

fn execute_ruby(input: String) -> String {
  let mut child = StdCommand::new("ruby")
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .spawn()
    .expect("Failed to spawn child process");

  {
    let stdin = child.stdin.as_mut().expect("Failed to open stdin");
    stdin.write_all(input.as_bytes()).expect("Failed to write to stdin");
  }

  let output = child.wait_with_output().expect("Failed to read stdout");
  String::from_utf8_lossy(&output.stdout).to_string()
}

fn ruby_stack_parameters(rb_filename: String) -> String {
  if Path::new(&rb_filename).exists() {
    let body = fs::read_to_string(rb_filename).expect("Something went wrong reading ruby stack params");
    // Insert require 'json', puts, Brackets, dump! & to_json
    let mut body_arr = VecDeque::new();
    body.split('\n')
      .filter(|x| !x.trim().is_empty())
      .for_each(|x| body_arr.push_back(x.to_string()));
    body_arr.insert(0, "require 'json'".to_string());
    // find
    let (pos, _) = body_arr.iter().find_position(|x| x.trim() == "AttributeStruct.new do").expect("No attribute struct defintion found");
    body_arr.remove(pos);
    body_arr.insert(pos, "puts (AttributeStruct.new do".to_string());
    let line = body_arr.pop_back().expect("No lines in body");
    body_arr.push_back(format!("{}.dump!.to_json)", line.trim()));

    let mod_body = body_arr.iter().join("\n");
    let output = execute_ruby(mod_body);
    let mut json: Value = serde_json::from_str(&output).expect("Invalid JSON converted from ruby");
    if json.get("parameters").is_some() {
      let params = json.get("parameters").unwrap().as_object().unwrap();
      let new_params: serde_json::Map<String, Value> = params.iter()
        .map(|(key, value)| (string_morph::to_pascal_case(key), value.clone()))
        .collect();
      json.as_object_mut().unwrap().insert("parameters".to_string(), serde_json::Value::Object(new_params));
    }
    if json.get("mappings").is_some() {
      let mappings = json.get("mappings").unwrap().as_object().unwrap();
      let new_mappings: serde_json::Map<String, Value> = mappings.iter()
        .map(|(key, value)| (string_morph::to_pascal_case(key), value.clone()))
        .collect();
      json.as_object_mut().unwrap().insert("mappings".to_string(), serde_json::Value::Object(new_mappings));
    }
    json.to_string()
  } else {
    panic!("Provided path invalid");
  }
}

#[tokio::main]
async fn main() {
  let matches = generate_matches();
  match matches.subcommand_name() {
    Some("attach-event-stream") => {
      let attach_opts = matches.subcommand_matches("attach-event-stream").unwrap();
      let start_time = Local::now() - ChronoDuration::minutes(attach_opts.get_one::<String>("time-backwards").map(|v| v.as_str()).unwrap_or("5").parse::<i64>().expect("Time backwards is not an integer"));
      let stack_name = attach_opts.get_one::<String>("STACKNAME").expect("No Stack named").to_string();
      let stack_parameter_file = get_stack_parameter_file(stack_name.clone());
      let mut region = default_region();
      if let Some(stack_parameter_file) = stack_parameter_file.as_ref() {
        region = stack_parameter_file.region.clone();
      }
      let client = build_cfn_client(region.clone()).await;

      poll_stack_status(Some(lookup_stackid_to_name(stack_name, client.clone()).await), client, region, start_time).await;
    }
    Some("convert-parameter-file") => {
      let convert_opts = matches.subcommand_matches("convert-parameter-file").unwrap();
      let rb_filename = convert_opts.get_one::<String>("file").expect("No file provided").to_string();
      let json_string = ruby_stack_parameters(rb_filename);
      println!("{}", json_string);
    }
    Some("list") => {
      list_stacks(matches).await;
    }
    Some("update") => {
      let update_opts = matches.subcommand_matches("update").unwrap();

      let start_time = Local::now();

      let stack_input = prepare_stack_input(update_opts, start_time, true).await;

      let change_set_name = format!("sfn-ng-{}", start_time.timestamp());
      let stack_name = stack_input.stack_name.clone();

      let create_changeset_input = stack_input.client.create_change_set()
        .capabilities(aws_sdk_cloudformation::types::Capability::CapabilityIam)
        .capabilities(aws_sdk_cloudformation::types::Capability::CapabilityNamedIam)
        .change_set_name(change_set_name.clone())
        .change_set_type(ChangeSetType::Update)
        .client_token(format!("sfn-ng-{}", start_time.timestamp()))
        .description("sfn-ng upgrade request")
        .set_parameters(Some(stack_input.used_parameters))
        .stack_name(stack_input.stack_name)
        .set_tags(stack_input.tags)
        .template_url(format!("https://{}.s3.{}.amazonaws.com/{}", stack_input.bucket, region_name(&stack_input.region), stack_input.path));

      let always_yes = update_opts.get_flag("yes");
      let poll = update_opts.get_one::<String>("poll").map(|v| v == "true").unwrap_or(true);

      println!("Polling: {}", poll);

      update_stack_rek(poll, stack_input.client, stack_input.region, create_changeset_input, change_set_name, stack_name, always_yes, start_time, 0).await;
    }
    Some("create") => {
      let create_opts = matches.subcommand_matches("create").unwrap();

      let start_time = Local::now();
      let stack_input = prepare_stack_input(create_opts, start_time, false).await;

      let create_stack_input = stack_input.client.create_stack()
        .capabilities(aws_sdk_cloudformation::types::Capability::CapabilityIam)
        .capabilities(aws_sdk_cloudformation::types::Capability::CapabilityNamedIam)
        .on_failure(OnFailure::DoNothing) // TODO: Optional DELETE
        .set_parameters(Some(stack_input.used_parameters))
        .stack_name(stack_input.stack_name)
        .set_tags(stack_input.tags)
        .template_url(format!("https://{}.s3.{}.amazonaws.com/{}", stack_input.bucket, region_name(&stack_input.region), stack_input.path));
      let start_time = Local::now();

      let poll = create_opts.get_one::<String>("poll").map(|v| v == "true").unwrap_or(true);
      create_stack_rek(poll, stack_input.client, stack_input.region, create_stack_input, start_time, 0).await;
    }
    Some("destroy") => {
      let destroy_opts = matches.subcommand_matches("destroy").unwrap();
      let stack_name = destroy_opts.get_one::<String>("STACKNAME").expect("No Stack named").to_string();
      let stack_parameter_file = get_stack_parameter_file(stack_name.clone());
      let mut region = default_region();
      if let Some(stack_parameter_file) = stack_parameter_file.as_ref() {
        region = stack_parameter_file.region.clone();
      }
      let client = build_cfn_client(region.clone()).await;
      let delete_stack_input = client.delete_stack().stack_name(stack_name.clone());
      let start_time = Local::now();
      let always_yes = destroy_opts.get_flag("yes");
      let poll = destroy_opts.get_one::<String>("poll").map(|v| v == "true").unwrap_or(true);
      if always_yes_or_ask(always_yes, "destroy stack") {
        cleanup_resources(stack_name.clone(), region.clone()).await;
        delete_stack_rek(client.clone(), delete_stack_input, 0).await;
        if poll {
          poll_stack_status(Some(lookup_stackid_to_name(stack_name, client.clone()).await), client.clone(), region.clone(), start_time).await;
        }
      } else {
        println!("Canceling destroy stack");
      }
    }
    Some(&_) | None => println!("No valid command specified")
  }
}

fn always_yes_or_ask(always_yes: bool, msg: &str) -> bool {
  let mut input = String::new();

  if !always_yes {
    print!("Do you want to execute {}?: ", msg);
    stdout().flush().unwrap();
    stdin().read_line(&mut input).unwrap_or_else(|_| panic!("Canceling {}", msg));
    input.pop();
  }
  always_yes || ["y", "j", "yes", "ja", "si"].contains(&input.to_lowercase().as_str())
}

#[async_recursion]
async fn describe_stack_resources_rek(client: CloudFormationClient, stack_name: String, i: u64) -> Vec<StackResource> {
  let resource_input = client.describe_stack_resources().stack_name(stack_name.clone());
  match resource_input.send().await {
    Ok(result) => result.stack_resources().to_vec(),
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in describe stack resources: {}", e);
      } else {
        println!("Something went wrong in describe stack resources (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      describe_stack_resources_rek(client, stack_name, i+1).await
    }
  }
}

#[async_recursion]
async fn get_bucket_versioning_rek(s3: S3Client, bucket: String, i: u64) -> bool {
  let version_input = s3.get_bucket_versioning().bucket(bucket.clone());
  match version_input.send().await {
    Ok(result) => result.status() == Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled),
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in get_bucket_versioning: {}", e);
      } else {
        println!("Something went wrong in get_bucket_versioning (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      get_bucket_versioning_rek(s3, bucket, i+1).await
    }
  }
}

#[async_recursion]
async fn list_object_versions_rek(s3: S3Client, bucket: String, key_marker: Option<String>, version_id_marker: Option<String>, i: u64) -> aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput {
  let mut list_version_input = s3.list_object_versions().bucket(bucket.clone());
  if let Some(marker) = key_marker.clone() {
    list_version_input = list_version_input.key_marker(marker);
  }
  if let Some(marker) = version_id_marker.clone() {
    list_version_input = list_version_input.version_id_marker(marker);
  }
  match list_version_input.send().await {
    Ok(result) => result,
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in list_object_versions: {}", e);
      } else {
        println!("Something went wrong in list_object_versions (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      list_object_versions_rek(s3, bucket, key_marker, version_id_marker, i+1).await
    }
  }
}

#[async_recursion]
async fn delete_objects_rek(s3: S3Client, bucket: String, objects: Vec<ObjectIdentifier>, i: u64) {
  let mut delete_builder = aws_sdk_s3::types::Delete::builder();
  for obj in objects.clone() {
    delete_builder = delete_builder.objects(obj);
  }
  let object_delete_input = s3.delete_objects()
    .bucket(bucket.clone())
    .delete(delete_builder.build().expect("Failed to build Delete"));
  match object_delete_input.send().await {
    Ok(_) => {},
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in delete_objects: {}", e);
      } else {
        println!("Something went wrong in delete_objects (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      delete_objects_rek(s3, bucket, objects, i+1).await
    }
  }
}

#[async_recursion]
async fn list_objects_rek(s3: S3Client, bucket: String, continuation_token: Option<String>, i: u64) -> aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output {
  let mut list_objects_input = s3.list_objects_v2().bucket(bucket.clone());
  if let Some(token) = continuation_token.clone() {
    list_objects_input = list_objects_input.continuation_token(token);
  }
  match list_objects_input.send().await {
    Ok(result) => result,
    Err(e) => {
      let wait_time = 2000 + 1000 * i * i;
      if i > 20 {
        panic!("Retry limit reached in list_objects: {}", e);
      } else {
        println!("Something went wrong in list_objects (retrying in {} ms): {}", wait_time, e);
      }
      tokio::time::sleep(Duration::from_millis(wait_time)).await;
      list_objects_rek(s3, bucket, continuation_token, i+1).await
    }
  }
}

async fn cleanup_resources(stack_name: String, region: Region) {
  let client = build_cfn_client(region.clone()).await;
  //TODO Cleanup ECR
  //     Cleanup manual edited AWS::IAM::Group
  //                           AWS::IAM::Role
  //                           AWS::Route53::HostedZone
  let s3 = build_s3_client(region.clone()).await;
  for resource in describe_stack_resources_rek(client, stack_name, 0).await.iter() {
    if let Some("AWS::S3::Bucket") = resource.resource_type() {
        let bucket = resource.physical_resource_id().expect("No physical resource id provided").to_string();
        println!("Deleting content from bucket {}", bucket.bold());
        if get_bucket_versioning_rek(s3.clone(), bucket.clone(), 0).await {
          let mut key_token = None;
          let mut version_id_marker = None;
          loop {
            let result = list_object_versions_rek(s3.clone(), bucket.clone(), key_token.clone(), version_id_marker.clone(), 0).await;
            if !result.versions().is_empty() || !result.delete_markers().is_empty() {
              let mut to_be_deleted: Vec<ObjectIdentifier> = vec![];
              to_be_deleted.extend(result.versions().iter().map(|version| ObjectIdentifier::builder()
                .key(version.key().expect("No key in version").to_string())
                .set_version_id(version.version_id().map(|v| v.to_string()))
                .build().expect("Failed to build ObjectIdentifier")));
              to_be_deleted.extend(result.delete_markers().iter().map(|delete_marker| ObjectIdentifier::builder()
                .key(delete_marker.key().expect("No key in version").to_string())
                .set_version_id(delete_marker.version_id().map(|v| v.to_string()))
                .build().expect("Failed to build ObjectIdentifier")));
              delete_objects_rek(s3.clone(), bucket.clone(), to_be_deleted, 0).await;
            }
            key_token = result.next_key_marker().map(|v| v.to_string());
            version_id_marker = result.next_version_id_marker().map(|v| v.to_string());
            if !result.is_truncated().unwrap_or(false) {
              break;
            }
          }
        } else {
          let mut token = None;
          loop {
            let result = list_objects_rek(s3.clone(), bucket.clone(), token.clone(), 0).await;
            if !result.contents().is_empty() {
              let objects = result.contents().iter().map(|object| ObjectIdentifier::builder()
                .key(object.key().expect("No object key received").to_string())
                .set_version_id(None)
                .build().expect("Failed to build ObjectIdentifier")).collect();
              delete_objects_rek(s3.clone(), bucket.clone(), objects, 0).await;
            }
            token = result.continuation_token().map(|v| v.to_string());
            if !result.is_truncated().unwrap_or(false) {
              break;
            }
          }
        }
    }
  }
}
