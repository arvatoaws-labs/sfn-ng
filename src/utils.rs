use aws_sdk_cloudformation::Client as CloudFormationClient;
use aws_sdk_cloudformation::types::{Change, StackEvent};
use colored::*;
use std::time::Duration;
use serde_json::{Value};
use chrono::*;
use async_recursion::async_recursion;

pub fn pretty_panic(message: String) {
    println!("{}", message.red().bold());
    ::std::process::exit(1);
}

pub fn pretty_print_stack_events(mut events: Vec<StackEvent>, start_time: DateTime<Local>) {
    events.sort_by(|x, y| x.timestamp().cmp(&y.timestamp()));
    for line in &events {
        let event_time = NaiveDateTime::parse_from_str(&line.timestamp().unwrap().to_string(), "%Y-%m-%dT%H:%M:%S%.3fZ").unwrap().and_utc().with_timezone(&Local);
        if start_time.lt(&event_time) {
            println!("{:25.25} {:70.70} {:50.50} {:}",
                     match_status_color(line.resource_status().unwrap().as_str(), line.timestamp().unwrap().to_string().as_str()),
                     match_status_color(line.resource_status().unwrap().as_str(), line.logical_resource_id().unwrap()),
                     match_status_color(line.resource_status().unwrap().as_str(), line.resource_status().unwrap().as_str()),
                     match_status_color(line.resource_status().unwrap().as_str(), line.resource_status_reason().unwrap_or(""))
            );
        }
    }
}


pub async fn lookup_stackid_to_name(stack_name: String, client: CloudFormationClient) -> String {
    lookup_stackid_to_name_rek(stack_name, client, 0).await
}

#[async_recursion]
async fn lookup_stackid_to_name_rek(stack_name: String, client: CloudFormationClient, i: u64) -> String {
    let describe_input = client.describe_stacks().stack_name(stack_name.clone());
    match describe_input.send().await {
        Ok(result) => {
            result.stacks().iter().max_by_key(|s| s.creation_time()).expect("Max failed in stack describe").stack_id().expect("Something went wrong describing stack").to_string()
        },
        Err(e) => {
            let wait_time = 2000 + 1000 * i * i;
            if i > 20 {
                panic!("Retry limit reached in lookup stackid to name: {}", e);
            } else {
                println!("Something went wrong in lookup stackid to name (retrying in {} ms): {}", wait_time, e);
            }
            tokio::time::sleep(Duration::from_millis(wait_time)).await;
            lookup_stackid_to_name_rek(stack_name, client, i+1).await
        }
    }
}

pub fn match_change_color(change_type: String, replacement: bool, msg: String) -> ColoredString {
    match &change_type[..] {
        "Remove" => msg.red(),
        "Modify" => if replacement { msg.black().on_yellow() } else { msg.yellow() },
        "Add" => msg.green(),
        _ => msg.magenta()
    }
}

pub fn match_status_color(status: &str, msg: &str) -> ColoredString {
    match status {
        "CREATE_IN_PROGRESS" | "DELETE_IN_PROGRESS" | "UPDATE_IN_PROGRESS" | "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS" | "REVIEW_IN_PROGRESS" | "IMPORT_IN_PROGRESS" => msg.bright_white().italic(),
        "CREATE_FAILED" | "ROLLBACK_FAILED" | "DELETE_FAILED" | "UPDATE_ROLLBACK_FAILED" | "IMPORT_ROLLBACK_FAILED" => msg.red(),
        "ROLLBACK_IN_PROGRESS" | "ROLLBACK_COMPLETE" | "UPDATE_ROLLBACK_IN_PROGRESS" | "UPDATE_ROLLBACK_COMPLETE" | "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS" | "IMPORT_ROLLBACK_IN_PROGRESS" | "IMPORT_ROLLBACK_COMPLETE" => msg.yellow(),
        "CREATE_COMPLETE" | "UPDATE_COMPLETE" | "IMPORT_COMPLETE" => msg.green(),
        "DELETE_COMPLETE" => msg.white().dimmed(),
        _ => msg.magenta()
    }
}

// TODO: improve and include more details and scope info
pub fn pretty_print_resource_change(change: &Change) {
    if let Some(resource) = change.resource_change() {
        let action = resource.action().map(|a| a.as_str().to_string()).unwrap_or("-".to_string());
        // let details = resource.details();
        let logical_resource_id = resource.logical_resource_id().unwrap_or("-");
        let physical_resource_id = resource.physical_resource_id().unwrap_or("-");
        let replacement = resource.replacement().map(|r| matches!(r, aws_sdk_cloudformation::types::Replacement::True)).unwrap_or(false);
        let resource_type = resource.resource_type().unwrap_or("-");
        let scope = resource.scope().iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",");

        println!("{}", match_change_color(action.clone(), replacement, format!("{:6.6} {:7.7} {:50.50} {:50.50} {:70.70} {:}", action, replacement, resource_type, logical_resource_id, physical_resource_id, scope)));
    }
}

pub fn value_to_string(v: &Value) -> Option<String> {
    let mut val = None;
    match v {
        e @ Value::Number(_) | e @ Value::Bool(_) => val = Some(e.to_string()),
        Value::String(s) => val = Some(s.to_string()),
        _ => {}
    }
    val
}