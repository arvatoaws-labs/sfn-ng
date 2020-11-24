use rusoto_core::{Region, ByteStream};
use rusoto_cloudformation::*;
use rusoto_s3::*;
use rusoto_sts::*;
use clap::{Arg, App, ArgMatches};
use colored::*;
use itertools::Itertools;
use std::fs;
use std::time::Duration;
use std::thread::sleep;
use std::io::{Write, stdin, stdout};
use serde_json::{Value};
use chrono::*;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use async_recursion::async_recursion;
use std::str::FromStr;
use std::process::{Command, Stdio};
use string_morph;

fn match_change_color(change_type: String, replacement: bool, msg: String) -> ColoredString {
  return match &change_type[..] {
    "Remove" => msg.red(),
    "Modify" => if replacement { msg.black().on_yellow() } else { msg.yellow() },
    "Add" => msg.green(),
    _ => msg.magenta()
  };
}

fn match_status_color(status: &str, msg: &str) -> ColoredString {
  return match status {
    "CREATE_IN_PROGRESS" | "DELETE_IN_PROGRESS" | "UPDATE_IN_PROGRESS" | "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS" | "REVIEW_IN_PROGRESS" | "IMPORT_IN_PROGRESS" => msg.bright_white().italic(),
    "CREATE_FAILED" | "ROLLBACK_FAILED" | "DELETE_FAILED" | "UPDATE_ROLLBACK_FAILED" | "IMPORT_ROLLBACK_FAILED" => msg.red(),
    "ROLLBACK_IN_PROGRESS" | "ROLLBACK_COMPLETE" | "UPDATE_ROLLBACK_IN_PROGRESS" | "UPDATE_ROLLBACK_COMPLETE" | "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS" | "IMPORT_ROLLBACK_IN_PROGRESS" | "IMPORT_ROLLBACK_COMPLETE" => msg.yellow(),
    "CREATE_COMPLETE" | "UPDATE_COMPLETE" | "IMPORT_COMPLETE" => msg.green(),
    "DELETE_COMPLETE" => msg.white().dimmed(),
    _ => msg.magenta()
  };
}

// TODO: improve and include more details and scope info
fn pretty_print_resource_change(change: Change) {
  match change.resource_change {
    Some(resource) => {
      let action = resource.action.unwrap_or("-".to_string());
      // let details = resource.details;
      let logical_resource_id = resource.logical_resource_id.unwrap_or("-".to_string());
      let physical_resource_id = resource.physical_resource_id.unwrap_or("-".to_string());
      let replacement = resource.replacement.unwrap_or("-".to_string());
      let resource_type = resource.resource_type.unwrap_or("-".to_string());
      let scope = resource.scope.unwrap_or(vec!["unknown".to_string()]).join(",");

      println!("{}", match_change_color(action.clone(), replacement == "True", format!("{:6.6} {:7.7} {:50.50} {:50.50} {:70.70} {:}", action, replacement, resource_type, logical_resource_id, physical_resource_id, scope)));
    }
    None => {}
  }
}

fn pretty_print_stack_events(mut events: Vec<StackEvent>, start_time: DateTime<Local>) {
  events.sort_by(|x, y| x.timestamp.cmp(&y.timestamp));
  for i in 0..events.len() {

    let line = &events[i];
    let event_time = Utc.datetime_from_str(&line.timestamp.as_ref(), "%Y-%m-%dT%H:%M:%S%.3fZ").unwrap();
    if start_time.lt(&event_time) {
      println!("{:25.25} {:70.70} {:50.50} {:}",
             match_status_color(line.resource_status.as_ref().unwrap(), line.timestamp.as_ref()),
             match_status_color(line.resource_status.as_ref().unwrap(), line.logical_resource_id.as_ref().unwrap()),
             match_status_color(line.resource_status.as_ref().unwrap(), line.resource_status.as_ref().unwrap()),
             match_status_color(line.resource_status.as_ref().unwrap(), line.resource_status_reason.as_ref().unwrap_or(&"".to_string()))
      );
    }
  }
}

async fn lookup_stackid_to_name(stack_name: String, client: CloudFormationClient) -> String {
  return lookup_stackid_to_name_rek(stack_name, client, 0).await;
}

#[async_recursion]
async fn lookup_stackid_to_name_rek(stack_name: String, client: CloudFormationClient, i: u64) -> String {
  let describe_input = DescribeStacksInput {
    next_token: None,
    stack_name: Some(stack_name.clone()),
  };
  return match client.describe_stacks(describe_input.clone()).await {
    Ok(result) => {
      result.stacks.expect("Something went wrong describing stack").iter().max_by_key(|s| s.creation_time.clone()).expect("Max failed in stack describe").stack_id.clone().expect("Something went wrong describing stack")
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in lookup stackid to name: {}", e);
      } else {
        println!("Something went wrong in lookup stackid to name (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      lookup_stackid_to_name_rek(stack_name, client, i+1).await
    }
  }
}

async fn lookup_stack_outputs(stack_name: String, client: CloudFormationClient) -> Vec<Parameter> {
  return lookup_stack_outputs_rek(stack_name, client, 0).await;
}

#[async_recursion]
async fn lookup_stack_outputs_rek(stack_name: String, client: CloudFormationClient, i: u64) -> Vec<Parameter> {
  let describe_input = DescribeStacksInput {
    next_token: None,
    stack_name: Some(stack_name.clone()),
  };
  return match client.describe_stacks(describe_input.clone()).await {
    Ok(result) => {
      let outputs = result.stacks.expect("Something went wrong describing stack")[0].outputs.clone().expect("Something went wrong describing stack");
      outputs.iter().map(|output| {
        return Parameter {
          parameter_key: Some(output.output_key.as_ref().unwrap().to_string()),
          parameter_value: Some(output.output_value.as_ref().unwrap().to_string()),
          resolved_value: None,
          use_previous_value: None,
        };
      }).collect::<Vec<Parameter>>()
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in describe stack: {}", e);
      } else {
        println!("Something went wrong describing stack (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      lookup_stack_outputs_rek(stack_name, client, i+1).await
    }
  }
}

#[async_recursion]
async fn generate_completion_test_rek(describe_input: DescribeStacksInput, client: CloudFormationClient, i: u64) -> Vec<Stack> {
  return match client.describe_stacks(describe_input.clone()).await {
    Ok(result) => {
      result.stacks.expect("Something went wrong describing stack")
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in describing Stack: {}", e);
      } else {
        println!("Something went wrong in describing Stack (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      generate_completion_test_rek(describe_input, client, i+1).await
    }
  }
}

#[async_recursion]
async fn wait_for_changeset_creation(client: CloudFormationClient, describe_input: DescribeChangeSetInput, i: u64) {
  match client.describe_change_set(describe_input.clone()).await {
    Ok(result) => {
      match &result.status.unwrap_or("-".to_string())[..] {
        "CREATE_COMPLETE" => {}
        "CREATE_IN_PROGRESS" | "CREATE_PENDING" => {
          let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
          if i > 20 {
            panic!("Retry limit reached in waiting for changeset to complete");
          }
          sleep(Duration::from_millis(wait_time));
          wait_for_changeset_creation(client, describe_input, i+1).await
        }
        "FAILED" => {panic!("Failed state in describe change set")}
        x => {panic!("Unknown state in describe change set: {}", x)}
      }
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in describing changeset: {}", e);
      } else {
        println!("Something went wrong in describing changeset (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      wait_for_changeset_creation(client, describe_input, i+1).await
    }
  }
}

#[async_recursion]
async fn generate_events_output_rek(events_input: DescribeStackEventsInput, client: CloudFormationClient, i: u64) -> DescribeStackEventsOutput {
  return match client.describe_stack_events(events_input.clone()).await {
    Ok(result) => result,
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in getting stack events: {}", e);
      } else {
        println!("Something went wrong in getting stack events (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      generate_events_output_rek(events_input, client, i+1).await
    }
  }
}

async fn poll_stack_status(stack_id: Option<String>, client: CloudFormationClient, start_time: DateTime<Local>) {
  let events_input = DescribeStackEventsInput {
    next_token: None,
    stack_name: stack_id.clone(),
  };
  let describe_input = DescribeStacksInput {
    next_token: None,
    stack_name: stack_id.clone(),
  };
  let mut last_printed = start_time;
  println!("{:25.25} {:70.70} {:50.50} {:}", "Time".bold(), "Resource Logical Id".bold(), "Resource Status".bold(), "Resource Status Reason".bold());
  // sleep(Duration::from_millis(1000));
  loop {
    let completion_test = generate_completion_test_rek(describe_input.clone(), client.clone(), 0).await;
    let events_output = generate_events_output_rek(events_input.clone(), client.clone(), 0).await;
    let events = events_output.stack_events.unwrap();
    pretty_print_stack_events(events.clone(), last_printed);
    last_printed = DateTime::from(Utc.datetime_from_str(events.iter().max_by_key(|event| event.timestamp.clone()).unwrap().timestamp.as_ref(), "%Y-%m-%dT%H:%M:%S%.3fZ").unwrap());
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
    ].contains(&completion_test[0].stack_status.as_str()) {
      break;
    }
    sleep(Duration::from_millis(2000));
  }

  // TODO: Final printout? Status?, Exit-Code!
  let stack_result = client.describe_stacks(describe_input.clone()).await.expect("Something went wrong describing stack").stacks.expect("Something went wrong describing stack");
  if [
    "CREATE_COMPLETE",
    "UPDATE_COMPLETE"
  ].contains(&stack_result[0].stack_status.as_str()) {
    let outputs = client.describe_stacks(describe_input.clone()).await.expect("Something went wrong describing stack").stacks.expect("Something went wrong describing stack")[0].outputs.clone();
    if outputs.is_some() {
      println!("Outputs:");
      for output in outputs.unwrap().iter().sorted_by_key(|output| output.output_key.clone()) {
        println!("{:50.50}: {}", output.output_key.as_ref().unwrap().to_string().bold(), output.output_value.as_ref().unwrap().to_string());
      }
    }
  }
}

fn get_template_params(json: Value, is_update: bool) -> Vec<Parameter> {
  if json.get("Parameters").is_some() {
    let params = json.get("Parameters").unwrap().as_object().unwrap();
    return params.iter().map(|(key, value)| {
      let optional_default = value.get("Default");

      let val = if optional_default.is_some() { value_to_string(&optional_default.unwrap().clone()) } else { None };
      return Parameter {
        parameter_key: Some(key.to_string()),
        parameter_value: val,
        resolved_value: None,
        use_previous_value: if is_update { Some(optional_default.is_some()) } else { None },
      };
    }).collect();
  } else {
    return vec![];
  }
}

#[derive(Clone)]
struct StackParameterFile {
  apply_stacks: Option<Vec<String>>,
  parameters: Option<HashMap<String, String>>,
  mappings: Option<HashMap<String, String>>,
  tags: Option<HashMap<String, String>>,
  template: String,
  region: Region
}

fn value_to_string(v: &Value) -> Option<String> {
  let mut val = None;
  match v {
    e @ Value::Number(_) | e @ Value::Bool(_) => val = Some(e.to_string()),
    Value::String(s) => val = Some(s.to_string()),
    _ => {}
  }
  return val;
}

fn get_stack_parameter_file(stack_name: String) -> Option<StackParameterFile> {
  // stack-parameters
  let rb_filename = format!("stack-parameters/{}.rb", stack_name);
  let body: Option<String>;
  if Path::new(&rb_filename.clone()).exists() {
    body = Some(ruby_stack_parameters(rb_filename));
  } else {
    let json_filename = format!("stack-parameters/{}.json", stack_name);
    if Path::new(&json_filename.clone()).exists() {
      body = Some(fs::read_to_string(json_filename).expect("Something went wrong reading json stack params"));
    } else {
      return None;
    }
  }
  let content: Value = serde_json::from_str(&&*(body.clone().unwrap())).unwrap();

  let template = value_to_string(&content.get("template").expect("No template specified in stack parameter file").clone()).expect("Template path is not a string");

  let mut region= Region::default();
  let parsed_region = content.get("region");
  if parsed_region.is_some() {
    region = map_region(&*value_to_string(parsed_region.unwrap()).expect("Region parsing failed"));
  }

  let tags_raw = content.get("tags");
  let mut tags: Option<HashMap<String, String>> = None;
  if tags_raw.is_some() {
    tags = Some(tags_raw.unwrap().as_object().expect("Tags malformed in stack parameter file").iter().map(|(key, value)| {
      return (key.clone(), value_to_string(&value.clone()).expect("Tag value isn't string convertible"));
    }).collect());
  }
  let mappings_raw = content.get("mappings");
  let mut mappings: Option<HashMap<String, String>> = None;
  if mappings_raw.is_some() {
    mappings = Some(mappings_raw.unwrap().as_object().expect("Mappings malformed in stack parameter file").iter().map(|(key, value)| {
      return (key.clone(), value_to_string(&value.clone()).expect("Mappings value isn't string convertible"));
    }).collect());
  }
  let parameters_raw = content.get("parameters");
  let mut parameters: Option<HashMap<String, String>> = None;
  if parameters_raw.is_some() {
    parameters = Some(parameters_raw.unwrap().as_object().expect("Parameters malformed in stack parameter file").iter().map(|(key, value)| {
      return (key.clone(), value_to_string(&value.clone()).expect("Parameter value isn't string convertible"));
    }).collect());
  }
  let apply_stacks_raw = content.get("apply_stacks");
  let mut apply_stacks = None;
  if apply_stacks_raw.is_some() {
    apply_stacks = Some(apply_stacks_raw.unwrap().as_array().expect("Apply stacks malformed in stack parameter file").iter().map(|v| value_to_string(&v.clone()).expect("Stack name not stringifiable")).collect());
  }

  return Some(StackParameterFile {
    apply_stacks,
    parameters,
    mappings,
    tags,
    template,
    region
  })
}

fn string_to_static_str(s: String) -> &'static str {
  Box::leak(s.into_boxed_str())
}

async fn list_stacks(client: CloudFormationClient, matches: ArgMatches) {
  let list_opts = matches.subcommand_matches("list").unwrap();
  let mut list_stacks_input: ListStacksInput = Default::default();
  if list_opts.is_present("status") {
    list_stacks_input.stack_status_filter = Some(vec![list_opts.value_of("status").unwrap().to_string()]);
  }
  list_stacks_rek(client, list_stacks_input, 0).await
}

#[async_recursion]
async fn list_stacks_rek(client: CloudFormationClient, list_stacks_input: ListStacksInput, i: u64) {
  match client.list_stacks(list_stacks_input.clone()).await {
    Ok(output) => match output.stack_summaries {
      Some(stack_list) => {
        println!("{}", "Stacks:".bold());
        for (status, grouped_stack_list) in stack_list.iter().map(|stack| (stack.stack_status.clone(), stack.clone())).into_group_map().iter().sorted_by_key(|(status, _)| status.clone()) {
          println!("{}", match_status_color(status, status).bold());
          for stack in grouped_stack_list {
            println!("{:120.120} {}", match_status_color(status, &stack.stack_name), match_status_color(status, &stack.stack_status));
          }
          println!();
        }
      }
      None => println!("No stacks"),
    },
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in list stacks: {}", e);
      } else {
        println!("Something went wrong listing stacks (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      list_stacks_rek(client, list_stacks_input, i+1).await
    }
  }
}

fn generate_matches() -> ArgMatches {
  return App::new("sfn-ng")
    .version("0.2.3")
    .author("Patrick Robinson <patrick.robinson@bertelsmann.de>")
    .about("Does sparkleformation command stuff")
    .subcommand(App::new("list")
      .about("Lists stacks")
      .arg(Arg::new("status")
        .short('s')
        .long("status")
        .takes_value(true)
        .about("Match stacks with given status")
      )
    )
    .subcommand(App::new("destroy")
      .about("Destroys a stack")
      .arg(Arg::new("STACKNAME")
        .about("Sets the StackName")
        .required(true)
        .index(1)
      )
      .arg(Arg::new("yes")
        .short('y')
        .long("yes")
        .takes_value(false)
        .about("Automatically accept any requests for confirmation")
      )
      .arg(Arg::new("poll")
        .short('p')
        .long("poll")
        .takes_value(true)
        .about("Poll stack events on modification actions (default: true)")
      )
    )
    .subcommand(App::new("convert-parameter-file")
      .about("Converts a ruby parameter file to json")
      .arg(Arg::new("file")
        .short('f')
        .long("file")
        .takes_value(true)
        .required(true)
        .about("Which stack parameter file to use")
      )
    )
    .subcommand(App::new("create")
      .about("Create a new stack")
      .arg(Arg::new("STACKNAME")
        .about("Sets the StackName")
        .required(true)
        .index(1)
      )
      .arg(Arg::new("apply-mapping")
        .long("apply-mapping")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Customize apply stack mapping (OutputName=ParameterName[,OutputName=ParameterName,...])")
      )
      .arg(Arg::new("apply-stack")
        .short('A')
        .long("apply-stack")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Apply outputs from stack to input parameters")
      )
      .arg(Arg::new("defaults")
        .short('d')
        .long("defaults")
        .takes_value(false)
        .about("Automatically accept default values")
      )
      .arg(Arg::new("file")
        .short('f')
        .long("file")
        .value_name("FILE")
        .takes_value(true)
        .about("Path to template file")
      )
      .arg(Arg::new("parameters")
        .short('m')
        .long("parameters")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Pass template parameters directly (Key=Value[,Key=Value,...])")
      )
      .arg(Arg::new("poll")
        .short('p')
        .long("poll")
        .takes_value(true)
        .about("Poll stack events on modification actions (default: true)")
      )
      .arg(Arg::new("tags")
        .short('t')
        .long("tags")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Tags of the resulting Stack (Key=Value[,Key=Value,...])")
      )
      .arg(Arg::new("yes")
        .short('y')
        .long("yes")
        .takes_value(false)
        .about("Automatically accept any requests for confirmation")
      )
    )
    .subcommand(App::new("update")
      .about("Updates a stack")
      .arg(Arg::new("STACKNAME")
        .about("Sets the StackName")
        .required(true)
        .index(1)
      )
      .arg(Arg::new("apply-mapping")
        .long("apply-mapping")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Customize apply stack mapping (OutputName=ParameterName[,OutputName=ParameterName,...])")
      )
      .arg(Arg::new("apply-stack")
        .short('A')
        .long("apply-stack")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Apply outputs from stack to input parameters")
      )
      .arg(Arg::new("defaults")
        .short('d')
        .long("defaults")
        .takes_value(false)
        .about("Automatically accept default values")
      )
      .arg(Arg::new("file")
        .short('f')
        .long("file")
        .value_name("FILE")
        .takes_value(true)
        .about("Path to template file")
      )
      .arg(Arg::new("parameters")
        .short('m')
        .long("parameters")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Pass template parameters directly (Key=Value[,Key=Value,...])")
      )
      .arg(Arg::new("poll")
        .short('p')
        .long("poll")
        .takes_value(true)
        .about("Poll stack events on modification actions (default: true)")
      )
      .arg(Arg::new("tags")
        .short('t')
        .long("tags")
        .takes_value(true)
        .multiple(true)
        .use_delimiter(true)
        .about("Tags of the resulting Stack (Key=Value[,Key=Value,...])")
      )
      .arg(Arg::new("yes")
        .short('y')
        .long("yes")
        .takes_value(false)
        .about("Automatically accept any requests for confirmation")
      )
    )
    .get_matches();
}

#[async_recursion]
async fn create_stack_rek(client: CloudFormationClient, create_stack_input: CreateStackInput, start_time: DateTime<Local>, i: u64) {
  match client.create_stack(create_stack_input.clone()).await {
    Ok(output) => {
      poll_stack_status(output.stack_id.clone(), client, start_time).await;
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in create stacks: {}", e);
      } else {
        println!("Something went wrong creating stacks (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      create_stack_rek(client, create_stack_input, start_time, i+1).await
    }
  }
}

#[async_recursion]
async fn delete_stack_rek(client: CloudFormationClient, delete_stack_input: DeleteStackInput, i: u64) {
  match client.delete_stack(delete_stack_input.clone()).await {
    Ok(_) => {},
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in delete stack: {}", e);
      } else {
        println!("Something went wrong deleting stack (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      delete_stack_rek(client, delete_stack_input, i+1).await
    }
  }
}

fn map_region(s: &str) -> Region {
  match Region::from_str(s) {
    Ok(region) => region,
    Err(e) => panic!("Unparseable region error: {}", e)
  }
}

#[derive(Clone)]
struct StackInput {
  stack_name: String,
  region: Region,
  used_parameters: Vec<Parameter>,
  tags: Option<Vec<rusoto_cloudformation::Tag>>,
  template_body: Option<String>,
  client: CloudFormationClient,
  bucket: String,
  path: String
}

// if upgrade check for previous values of params & tags as well.
async fn prepare_stack_input(opts: &ArgMatches, start_time: DateTime<Local>, _is_upgrade: bool) -> StackInput {
  let stack_name = opts.value_of("STACKNAME").expect("No Stack named").to_string();
  println!("Value for StackName: {}", stack_name);

  let stack_parameter_file = get_stack_parameter_file(stack_name.clone());

  let mut region = Region::default();
  if stack_parameter_file.clone().is_some() {
    region = stack_parameter_file.clone().unwrap().region;
  }
  let client = CloudFormationClient::new(region.clone());

  let explicit_parameters: Vec<Parameter> = match opts.values_of("parameters") {
    Some(parameters_list) => parameters_list.collect::<Vec<_>>().iter().map(|input| {
      let pair = input.split("=").collect::<Vec<&str>>();
      return Parameter {
        parameter_key: Some(pair[0].to_string()),
        parameter_value: Some(pair[1].to_string()),
        resolved_value: None,
        use_previous_value: None,
      };
    }).collect::<Vec<Parameter>>(),
    None => Vec::new()
  };

  let mut mappings: HashMap<String, String> = match opts.values_of("apply-mapping") {
    Some(list) => list.collect::<Vec<_>>().iter().map(|input| {
      let pair = input.split("=").collect::<Vec<&str>>();
      return (pair[0].to_string().clone(), pair[1].to_string().clone());
    }).collect(),
    None => HashMap::new()
  };
  if stack_parameter_file.clone().is_some() {
    let stack_parameter_file = stack_parameter_file.clone().unwrap();
    if stack_parameter_file.mappings.is_some() {
      for (key, value) in stack_parameter_file.mappings.unwrap() {
        if !mappings.contains_key(&*key.clone()) {
          mappings.insert(key, value);
        }
      }
    }
  }

  // TODO: Set default Tags by env, if not set by --tags manually
  let mut tags_vec: Vec<rusoto_cloudformation::Tag> = vec![];
  if stack_parameter_file.clone().is_some() {
    let stack_parameter_file = stack_parameter_file.clone().unwrap();
    if stack_parameter_file.tags.is_some() {
      for (key, value) in stack_parameter_file.tags.unwrap() {
        tags_vec.push(rusoto_cloudformation::Tag {
          key: key.clone(),
          value: value.clone()
        });
      }
    }
  }

  match opts.values_of("tags") {
    Some(mytags) => {
      for input in mytags.collect::<Vec<_>>().iter() {
        let pair = input.split("=").collect::<Vec<&str>>();
        let tag = rusoto_cloudformation::Tag {
          key: pair[0].to_string(),
          value: pair[1].to_string(),
        };
        let pos = tags_vec.iter().position(|ex_tag| ex_tag.key == tag.key);
        if pos.is_some() {
          tags_vec.push(tag);
          tags_vec.swap_remove(pos.unwrap());
        } else {
          tags_vec.push(tag);
        }
      }
    },
    None => {}
  };
  if !tags_vec.iter().any(|tag| tag.key == "Projekt") {
    let mut input = String::new();
    print!("Projekt?: ");
    stdout().flush().unwrap();
    stdin().read_line(&mut input).expect("Cancel Stack creation");
    input.pop();
    if input.clone().is_empty() {
      panic!("No project tag set by any means");
    } else {
      tags_vec.push(rusoto_cloudformation::Tag {
        key: "Projekt".to_string(),
        value: input.clone()
      });
    }
  }

  let search_for_creator = tags_vec.iter().position(|tag| tag.key =="creator");
  tags_vec.push(rusoto_cloudformation::Tag {
    key: "creator".to_string(),
    value: whoami::username()
  });
  match search_for_creator {
    Some(pos) => {tags_vec.swap_remove(pos);},
    None => {}
  }
  let tags = Some(tags_vec);
  let mut template_file = opts.value_of("file");
  if stack_parameter_file.clone().is_some() {
    template_file = Some(string_to_static_str(stack_parameter_file.clone().unwrap().template));
  }
  let template_body = Some(fs::read_to_string(template_file.expect("No template file specified")).expect("Something went wrong reading the file"));
  let template_content: Value = serde_json::from_str(&&*(template_body.clone().unwrap())).unwrap();
  let template_parameters = get_template_params(template_content, false); // TODO: yaml support

  let bucket = find_template_bucket_or_create_it_rek(region.clone(), 0).await;
  let s3 = S3Client::new(region.clone());

  let path = format!("{}/{}", template_file.expect("No template file specified").to_string(), start_time.timestamp());

  let upload_template_input = PutObjectRequest {
    acl: None,
    body: Some(ByteStream::from(template_body.clone().unwrap().as_bytes().to_vec())),
    bucket: bucket.clone(),
    cache_control: None,
    content_disposition: None,
    content_encoding: None,
    content_language: None,
    content_length: None,
    content_md5: None,
    content_type: None,
    expires: None,
    grant_full_control: None,
    grant_read: None,
    grant_read_acp: None,
    grant_write_acp: None,
    key: path.clone(),
    metadata: None,
    object_lock_legal_hold_status: None,
    object_lock_mode: None,
    object_lock_retain_until_date: None,
    request_payer: None,
    sse_customer_algorithm: None,
    sse_customer_key: None,
    sse_customer_key_md5: None,
    ssekms_encryption_context: None,
    ssekms_key_id: None,
    server_side_encryption: None,
    storage_class: None,
    tagging: None,
    website_redirect_location: None
  };
  s3.put_object(upload_template_input).await.expect("Template couldn't be uploaded to S3");

  let mut apply_stack_parameters: Vec<Parameter> = vec![];
  let mut stacks: Vec<&str> = vec![];
  if stack_parameter_file.clone().is_some() {
    let stack_parameter_file = stack_parameter_file.clone().unwrap();
    if stack_parameter_file.apply_stacks.is_some() {
      stacks.append(&mut stack_parameter_file.apply_stacks.unwrap().iter().map(|string| string_to_static_str(string.to_string())).collect());
    }
  }
  match opts.values_of("apply-stack") {
    Some(applystack) => {
      let mut cloneapply = applystack.clone().collect::<Vec<_>>();
      stacks.append(&mut cloneapply);
    },
    None => {}
  };

  for stack in stacks.iter().dedup() {
    let stack_parts: Vec<&str> = stack.split("__").collect();
    if stack_parts.len() > 1 {
      // Camel Cased: let region = stack_parts[0].split("_").collect::<Vec<&str>>().iter().map(upcast).collect::<Vec<String>>().join("");
      let region = stack_parts[0].split("_").collect::<Vec<&str>>().join("-");
      let client = CloudFormationClient::new(map_region(&region));
      apply_stack_parameters.append(&mut lookup_stack_outputs(stack_parts[1].to_string(), client.clone()).await);
    } else {
      apply_stack_parameters.append(&mut lookup_stack_outputs(stack.to_string(), client.clone()).await);
    }
  }

  apply_stack_parameters.reverse();

  let mut stack_params: Option<Vec<Parameter>> = None;
  if stack_parameter_file.clone().is_some() {
    let stack_parameter_file = stack_parameter_file.clone().unwrap();
    if stack_parameter_file.parameters.is_some() {
      stack_params = Some(stack_parameter_file.parameters.unwrap().iter().map(|(key, value)| Parameter {
        parameter_key: Some(key.to_string()),
        parameter_value: Some(value.to_string()),
        use_previous_value: None,
        resolved_value: None
      }).collect());
    }
  }

  let merged_parameters = template_parameters.iter().map(|default_param| {
    for explicit_param in explicit_parameters.clone() {
      if explicit_param.parameter_key == default_param.parameter_key {
        return explicit_param;
      }
    }
    if stack_params.is_some() {
      for stack_param in stack_params.clone().unwrap().clone() {
        if stack_param.parameter_key == default_param.parameter_key {
          return stack_param;
        }
      }
    }
    for apply_param in apply_stack_parameters.clone() {
      let matching_mapping = mappings.iter().find(|(_key, value)| value.to_string() == default_param.clone().parameter_key.unwrap());
      if matching_mapping.is_some() {
        let (mapped_output_key, mapped_output_value) = matching_mapping.unwrap();
        if apply_param.clone().parameter_key.unwrap() == mapped_output_key.to_string() {
          return Parameter {
            parameter_key: Some(mapped_output_value.to_string()),
            parameter_value: apply_param.parameter_value,
            resolved_value: apply_param.resolved_value,
            use_previous_value: apply_param.use_previous_value
          };
        }
      } else {
        if apply_param.parameter_key == default_param.parameter_key {
          return apply_param;
        }
      }
    }
    return default_param.clone();
  }).collect::<Vec<Parameter>>();

  println!("Parameters for StackName");
  let used_parameters = merged_parameters.iter().map(|param| {
    if opts.is_present("defaults") {
      if param.clone().parameter_value.is_some() {
        return Parameter {
          parameter_key: param.clone().parameter_key,
          parameter_value: param.clone().parameter_value,
          resolved_value: param.clone().resolved_value,
          use_previous_value: param.clone().use_previous_value
        };
      }
    }
    let mut input = String::new();
    print!("{}?:[{}] ", param.clone().parameter_key.unwrap().bold(), param.clone().parameter_value.unwrap_or(String::from("")).italic());
    stdout().flush().unwrap();
    stdin().read_line(&mut input).expect("Cancel Stack creation");
    input.pop();
    if !input.clone().is_empty() {
      // println!("Input: {}, Characters: {}",input.clone(), input.clone().chars().count());
      return Parameter {
        parameter_key: param.clone().parameter_key,
        parameter_value: Some(input.clone()),
        resolved_value: None,
        use_previous_value: None
      }
    } else {
      return Parameter {
        parameter_key: param.clone().parameter_key,
        parameter_value: param.clone().parameter_value,
        resolved_value: param.clone().resolved_value,
        use_previous_value: param.clone().use_previous_value
      };
    }
    // Pretty Print: param // no_echo?
    // Tippen -> Input
    // if Input == Einfach Enter return param
    // else return new  Parameter ( key = param.key, value = input.value )
  }).collect::<Vec<Parameter>>();
  
  return StackInput {
    stack_name,
    region,
    used_parameters,
    tags,
    template_body,
    client,
    bucket,
    path
  };
}

#[async_recursion]
async fn create_changeset_diff_display(client: CloudFormationClient, describe_change_set_input: DescribeChangeSetInput, start_time: DateTime<Local>, i: u64) {
  wait_for_changeset_creation(client.clone(), describe_change_set_input.clone(), 0).await;
  match client.describe_change_set(describe_change_set_input.clone()).await {
    Ok(output) => {
      match output.changes {
        Some(changes) => {
          // TODO: Title Headers
          println!("{:6.6} {:7.7} {:50.50} {:50.50} {:70.70} {:}", "Action".bold(), "Replace".bold(), "Type".bold(), "Logical ID".bold(), "Physical ID".bold(), "Scope".bold());
          for change in changes {
            pretty_print_resource_change(change);
          }
        }
        None => {
          println!("No further changes found");
        }
      }
      match output.next_token {
        Some(token) => {
          let mut new_input = describe_change_set_input.clone();
          new_input.next_token = Some(token);
          create_changeset_diff_display(client, new_input, start_time, 0).await;
        },
        None => {}
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in update stacks: {}", e);
      } else {
        println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      create_changeset_diff_display(client, describe_change_set_input, start_time, i+1).await
    }
  }
}

#[async_recursion]
async fn execute_change_set_rek(client: CloudFormationClient, stack_id: Option<String>, execute_changeset_input: ExecuteChangeSetInput, start_time: DateTime<Local>, i: u64) {
  match client.execute_change_set(execute_changeset_input.clone()).await {
    Ok(_output) => {
      poll_stack_status(stack_id.clone(), client, start_time).await;
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in update stacks: {}", e);
      } else {
        println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      execute_change_set_rek(client, stack_id, execute_changeset_input, start_time, i+1).await
    }
  }
}

#[async_recursion]
async fn update_stack_rek(client: CloudFormationClient, create_changeset_input: CreateChangeSetInput, always_yes: bool, start_time: DateTime<Local>, i: u64) {
  match client.create_change_set(create_changeset_input.clone()).await {
    Ok(output) => {
      let describe_change_set_input = DescribeChangeSetInput {
        change_set_name: create_changeset_input.change_set_name.clone(),
        next_token: None,
        stack_name: Some(create_changeset_input.stack_name.clone())
      };
      create_changeset_diff_display(client.clone(), describe_change_set_input, start_time, 0).await;
      // Ask user for permission, unless --yes
      if always_yes_or_ask(always_yes, "update stack") {
        // execute change set & poll status, unless --no-poll
        let execute_change_set_input = ExecuteChangeSetInput {
          change_set_name: create_changeset_input.change_set_name,
          client_request_token: None,
          stack_name: Some(create_changeset_input.stack_name)
        };
        execute_change_set_rek(client, output.stack_id, execute_change_set_input, start_time, 0).await;
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in update stacks: {}", e);
      } else {
        println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      update_stack_rek(client, create_changeset_input, always_yes, start_time, i+1).await
    }
  }
}

#[async_recursion]
async fn create_bucket_rek(client: S3Client, region: Region, name: String, i: u64) -> String {
  let bucket_configuration: Option<CreateBucketConfiguration>;
  if region == Region::UsEast1 {
    bucket_configuration = None;
  } else {
    bucket_configuration = Some(CreateBucketConfiguration {
      location_constraint: Some(region.name().to_string())
    });
  }
  let create_input = CreateBucketRequest {
    acl: None,
    bucket: name.clone(),
    create_bucket_configuration: bucket_configuration,
    grant_full_control: None,
    grant_read: None,
    grant_read_acp: None,
    grant_write: None,
    grant_write_acp: None,
    object_lock_enabled_for_bucket: None
  };
  match client.create_bucket(create_input).await {
    Ok(_) => {
      // TODO: wait for bucket creation completion
      return name;
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in create bucket: {}", e);
      } else {
        println!("Something went wrong creating bucket (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      create_bucket_rek(client, region, name, i+1).await
    }
  }
}

#[async_recursion]
async fn find_template_bucket_or_create_it_rek(region: Region, i: u64) -> String {
  let client = S3Client::new(region.clone());
  let sts = StsClient::new(region.clone());

  let caller_identity_input = GetCallerIdentityRequest {};

  match sts.get_caller_identity(caller_identity_input).await {
    Ok(identity) => {
      let name = format!("sfn-ng-{}-{}", region.name(), identity.account.unwrap());

      match client.list_buckets().await {
        Ok(bucket_output) => {
          match bucket_output.buckets {
            Some(buckets) => {
              match buckets.iter().find(|bucket| bucket.name.as_ref().unwrap().to_string() == name) {
                Some(_bucket) => {
                  return name.clone();
                }
                None => {
                  return create_bucket_rek(client, region, name, 0).await;
                }
              }
            }
            None => {
              return create_bucket_rek(client, region, name, 0).await;
            }
          }
        }
        Err(e) => {
          let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
          if i > 20 {
            panic!("Retry limit reached in update stacks: {}", e);
          } else {
            println!("Something went wrong updating stacks (retrying in {} ms): {}", wait_time, e);
          }
          sleep(Duration::from_millis(wait_time));
          find_template_bucket_or_create_it_rek(region, i+1).await
        }
      }
    }
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in find template bucket: {}", e);
      } else {
        println!("Something went wrong finding template bucket (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      find_template_bucket_or_create_it_rek(region, i+1).await
    }
  }
}

fn execute_ruby(input: String) -> String {
  let mut child = Command::new("ruby")
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .spawn()
    .expect("Failed to spawn child process");

  {
    let stdin = child.stdin.as_mut().expect("Failed to open stdin");
    stdin.write_all(input.as_bytes()).expect("Failed to write to stdin");
  }

  let output = child.wait_with_output().expect("Failed to read stdout");
  return format!("{}", String::from_utf8_lossy(&output.stdout));
}

fn ruby_stack_parameters(rb_filename: String) -> String {
  if Path::new(&rb_filename.clone()).exists() {
    let body = fs::read_to_string(rb_filename).expect("Something went wrong reading ruby stack params");
    // Insert require 'json', puts, Brackets, dump! & to_json
    let mut body_arr = VecDeque::new();
    body.split("\n")
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
    let mut json: Value = serde_json::from_str(&*output).expect("Invalid JSON converted from ruby");
    if json.get("parameters").is_some() {
      let params = json.get("parameters").unwrap().as_object().unwrap();
      let mut new_params: serde_json::Map<String, Value> = serde_json::Map::new();
      for (key, value) in params {
        new_params.insert(string_morph::to_pascal_case(key), value.clone());
      }
      let new_params_value = serde_json::Value::Object(new_params);
      json.as_object_mut().unwrap().remove("parameters");
      json.as_object_mut().unwrap().insert("parameters".to_string(), new_params_value);
    }
    if json.get("mappings").is_some() {
      let mappings = json.get("mappings").unwrap().as_object().unwrap();
      let mut new_mappings: serde_json::Map<String, Value> = serde_json::Map::new();
      for (key, value) in mappings {
        new_mappings.insert(string_morph::to_pascal_case(key), value.clone());
      }
      let new_mappings_value = serde_json::Value::Object(new_mappings);
      json.as_object_mut().unwrap().remove("mappings");
      json.as_object_mut().unwrap().insert("mappings".to_string(), new_mappings_value);
    }
    return json.to_string();
  } else {
    panic!("Provided path invalid");
  }
}

#[tokio::main]
async fn main() {
  let matches = generate_matches();
  match matches.subcommand_name() {
    Some("convert-parameter-file") => {
      let convert_opts = matches.subcommand_matches("convert-parameter-file").unwrap();
      let rb_filename = convert_opts.value_of("file").expect("No file provided").to_string();
      let json_string = ruby_stack_parameters(rb_filename);
      println!("{}", json_string);
    }
    Some("list") => {
      let region = Region::default();
      let client = CloudFormationClient::new(region.clone());
      list_stacks(client.clone(), matches.clone()).await;
    }
    Some("update") => {
      let update_opts = matches.subcommand_matches("update").unwrap();

      let start_time = chrono::offset::Local::now();
      let stack_input = prepare_stack_input(update_opts, start_time.clone(), true).await;

      let create_changeset_input = CreateChangeSetInput {
        capabilities: Some(["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"].iter().map(|i| String::from(*i)).collect::<Vec<String>>()),
        change_set_name: format!("sfn-ng-{}", start_time.timestamp()),
        change_set_type: Some("UPDATE".to_string()),
        client_token: Some(format!("sfn-ng-{}", start_time.timestamp())),
        description: Some("sfn-ng upgrade request".to_string()),
        notification_ar_ns: None,
        parameters: Some(stack_input.used_parameters),
        resource_types: None,
        resources_to_import: None,
        role_arn: None,
        rollback_configuration: None,
        stack_name: stack_input.stack_name,
        tags: stack_input.tags,
        template_body: None,
        template_url: Some(format!("https://{}.s3.{}.amazonaws.com/{}", stack_input.bucket, stack_input.region.name(), stack_input.path)),
        use_previous_template: None
      };

      let always_yes = update_opts.is_present("yes");

      update_stack_rek(stack_input.client, create_changeset_input, always_yes, start_time, 0).await;
    }
    Some("create") => {
      let create_opts = matches.subcommand_matches("create").unwrap();

      let start_time = chrono::offset::Local::now();
      let stack_input = prepare_stack_input(create_opts, start_time.clone(), false).await;

      let create_stack_input = CreateStackInput {
        capabilities: Some(["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"].iter().map(|i| String::from(*i)).collect::<Vec<String>>()),
        client_request_token: None,
        disable_rollback: None,
        enable_termination_protection: None,
        notification_ar_ns: None,
        on_failure: Some(String::from("DO_NOTHING")), // TODO: Optional DELETE
        parameters: Some(stack_input.used_parameters),
        resource_types: None,
        role_arn: None,
        rollback_configuration: None,
        stack_name: stack_input.stack_name,
        stack_policy_body: None,
        stack_policy_url: None,
        tags: stack_input.tags,
        template_body: None,
        template_url: Some(format!("https://{}.s3.{}.amazonaws.com/{}", stack_input.bucket, stack_input.region.name(), stack_input.path)),
        timeout_in_minutes: None,
      };
      let start_time = chrono::offset::Local::now();
      create_stack_rek(stack_input.client, create_stack_input, start_time, 0).await;
    }
    Some("destroy") => {
      let destroy_opts = matches.subcommand_matches("destroy").unwrap();
      let stack_name = destroy_opts.value_of("STACKNAME").expect("No Stack named").to_string();
      let stack_parameter_file = get_stack_parameter_file(stack_name.clone());
      let mut region = Region::default();
      if stack_parameter_file.clone().is_some() {
        region = stack_parameter_file.clone().unwrap().region;
      }
      let client = CloudFormationClient::new(region.clone());
      let delete_stack_input = DeleteStackInput {
        client_request_token: None,
        retain_resources: None,
        role_arn: None,
        stack_name: stack_name.clone(),
      };
      let start_time = chrono::offset::Local::now();
      let always_yes = destroy_opts.is_present("yes");
      if always_yes_or_ask(always_yes, "destroy stack") {
        cleanup_resources(stack_name.clone(), region.clone()).await;
        delete_stack_rek(client.clone(), delete_stack_input, 0).await;
        poll_stack_status(Some(lookup_stackid_to_name(stack_name, client.clone()).await), client.clone(), start_time).await;
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
    print!("Do you want to execute {}?: ", msg.clone());
    stdout().flush().unwrap();
    stdin().read_line(&mut input).expect(&format!("Canceling {}", msg)[..]);
    input.pop();
  }
  return always_yes || ["y", "j", "yes", "ja", "si"].contains(&&*input.to_lowercase());
}

#[async_recursion]
async fn describe_stack_resources_rek(client: CloudFormationClient, resource_input: DescribeStackResourcesInput, i: u64) -> Vec<StackResource> {
  match client.describe_stack_resources(resource_input.clone()).await {
    Ok(result) => result.stack_resources.expect("No stack resources"),
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in describe stack resources: {}", e);
      } else {
        println!("Something went wrong in describe stack resources (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      describe_stack_resources_rek(client, resource_input, i+1).await
    }
  }
}

#[async_recursion]
async fn get_bucket_versioning_rek(s3: S3Client, version_input: GetBucketVersioningRequest, i: u64) -> bool {
  match s3.get_bucket_versioning(version_input.clone()).await {
    Ok(result) => result.status.unwrap_or("Disabled".to_string()) == "Enabled".to_string(),
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in get_bucket_versioning: {}", e);
      } else {
        println!("Something went wrong in get_bucket_versioning (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      get_bucket_versioning_rek(s3, version_input, i+1).await
    }
  }
}

#[async_recursion]
async fn list_object_versions_rek(s3: S3Client, list_version_input: ListObjectVersionsRequest, i: u64) -> ListObjectVersionsOutput {
  match s3.list_object_versions(list_version_input.clone()).await {
    Ok(result) => result,
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in list_object_versions: {}", e);
      } else {
        println!("Something went wrong in list_object_versions (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      list_object_versions_rek(s3, list_version_input, i+1).await
    }
  }
}

#[async_recursion]
async fn delete_objects_rek(s3: S3Client, object_delete_input: DeleteObjectsRequest, i: u64) {
  match s3.delete_objects(object_delete_input.clone()).await {
    Ok(_) => {},
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in delete_objects: {}", e);
      } else {
        println!("Something went wrong in delete_objects (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      delete_objects_rek(s3, object_delete_input, i+1).await
    }
  }
}

#[async_recursion]
async fn list_objects_rek(s3: S3Client, list_objects_input: ListObjectsV2Request, i: u64) -> ListObjectsV2Output {
  match s3.list_objects_v2(list_objects_input.clone()).await {
    Ok(result) => result,
    Err(e) => {
      let wait_time = 2000 + 1000 * u64::pow(i, 2) as u64;
      if i > 20 {
        panic!("Retry limit reached in list_objects: {}", e);
      } else {
        println!("Something went wrong in list_objects (retrying in {} ms): {}", wait_time, e);
      }
      sleep(Duration::from_millis(wait_time));
      list_objects_rek(s3, list_objects_input, i+1).await
    }
  }
}

async fn cleanup_resources(stack_name: String, region: Region) {
  let client = CloudFormationClient::new(region.clone());
  //TODO Cleanup ECR
  //     Cleanup manual edited AWS::IAM::Group
  //                           AWS::IAM::Role
  //                           AWS::Route53::HostedZone
  let s3 = S3Client::new(region.clone());
  let resource_input = DescribeStackResourcesInput {
    logical_resource_id: None,
    physical_resource_id: None,
    stack_name: Some(stack_name),
  };
  for resource in describe_stack_resources_rek(client, resource_input, 0).await.iter() {
    match &*resource.resource_type {
      "AWS::S3::Bucket" => {
        let bucket = resource.clone().physical_resource_id.expect("No physical resource id provided");
        println!("Deleting content from bucket {}", bucket.bold());
        let version_input = GetBucketVersioningRequest {
          bucket: bucket.clone()
        };
        if get_bucket_versioning_rek(s3.clone(), version_input, 0).await {
          let mut key_token = None;
          let mut version_id_marker = None;
          loop {
            let list_version_input = ListObjectVersionsRequest {
              bucket: bucket.clone(),
              delimiter: None,
              encoding_type: None,
              key_marker: key_token.clone(),
              max_keys: None,
              prefix: None,
              version_id_marker: version_id_marker.clone()
            };
            let result = list_object_versions_rek(s3.clone(), list_version_input, 0).await;
            if result.versions.is_some() || result.delete_markers.is_some() {
              let mut to_be_deleted: Vec<ObjectIdentifier> = vec![];
              if result.versions.is_some() {
                to_be_deleted.extend(result.versions.expect("No object versions received").iter().map(|version| ObjectIdentifier {
                  key: version.clone().key.expect("No key in version"),
                  version_id: version.clone().version_id
                }));
              }
              if result.delete_markers.is_some() {
                to_be_deleted.extend(result.delete_markers.expect("No delete_markers found").iter().map(|delete_marker| ObjectIdentifier {
                  key: delete_marker.clone().key.expect("No key in version"),
                  version_id: delete_marker.clone().version_id
                }));
              }
              let object_delete_input = DeleteObjectsRequest {
                bucket: bucket.clone(),
                bypass_governance_retention: None,
                delete: Delete {
                  objects: to_be_deleted,
                  quiet: None,
                },
                mfa: None,
                request_payer: None,
              };
              delete_objects_rek(s3.clone(), object_delete_input, 0);
            }
            key_token = result.next_key_marker;
            version_id_marker = result.next_version_id_marker;
            if !result.is_truncated.unwrap_or(false) {
              break;
            }
          }
        } else {
          let mut token = None;
          loop {
            let list_objects_input = ListObjectsV2Request {
              bucket: bucket.clone(),
              continuation_token: token.clone(),
              delimiter: None,
              encoding_type: None,
              fetch_owner: None,
              max_keys: None,
              prefix: None,
              request_payer: None,
              start_after: None,
            };
            let result = list_objects_rek(s3.clone(), list_objects_input, 0).await;
            if result.contents.is_some() {
              let object_delete_input = DeleteObjectsRequest {
                bucket: bucket.clone(),
                bypass_governance_retention: None,
                delete: Delete {
                  objects: result.contents.expect("No objects listed").iter().map(|object| ObjectIdentifier {
                    key: object.clone().key.expect("No object key received"),
                    version_id: None,
                  }).collect(),
                  quiet: None,
                },
                mfa: None,
                request_payer: None,
              };
              delete_objects_rek(s3.clone(), object_delete_input, 0);
            }
            token = result.continuation_token;
            if !result.is_truncated.unwrap_or(false) {
              break;
            }
          }
        }
      }
      _ => {}
    }
  }
}
