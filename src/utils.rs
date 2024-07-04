use rusoto_cloudformation::*;
use colored::*;
use std::time::Duration;
use std::thread::sleep;
use serde_json::{Value};
use chrono::*;
use async_recursion::async_recursion;
use aws_sdk_cloudformation::Client;

pub fn pretty_panic(message: String) {
    println!("{}", message.red().bold());
    ::std::process::exit(1);
}

pub fn pretty_print_stack_events(mut events: Vec<StackEvent>, start_time: DateTime<Local>) {
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


pub async fn lookup_stackid_to_name(stack_name: String, client: Client) -> String {
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

pub fn match_change_color(change_type: String, replacement: bool, msg: String) -> ColoredString {
    return match &change_type[..] {
        "Remove" => msg.red(),
        "Modify" => if replacement { msg.black().on_yellow() } else { msg.yellow() },
        "Add" => msg.green(),
        _ => msg.magenta()
    };
}

pub fn match_status_color(status: &str, msg: &str) -> ColoredString {
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
pub fn pretty_print_resource_change(change: Change) {
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

pub fn value_to_string(v: &Value) -> Option<String> {
    let mut val = None;
    match v {
        e @ Value::Number(_) | e @ Value::Bool(_) => val = Some(e.to_string()),
        Value::String(s) => val = Some(s.to_string()),
        _ => {}
    }
    return val;
}