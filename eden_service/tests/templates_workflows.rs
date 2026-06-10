#![cfg(external_db)]

mod common;
mod request;
mod util;

#[path = "templates_workflows/functions.rs"]
mod functions;
#[path = "templates_workflows/pipelines.rs"]
mod pipelines;
#[path = "templates_workflows/template_execution.rs"]
mod template_execution;
#[path = "templates_workflows/templates.rs"]
mod templates;
#[path = "templates_workflows/triggers_ws.rs"]
mod triggers_ws;
#[path = "templates_workflows/workflows.rs"]
mod workflows;
