#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use crate::database::json::JsonOps;
use crate::database::schema::Table;
use crate::database::schema::workflow::WorkflowSchema;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use error::{EpError, WorkflowError};
use format::timestamp::DateTimeWrapper;
use format::{TemplateUuid, WorkflowId, WorkflowUuid};
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInput {
    inputs: HashMap<String, Value>,
}

//TODO store workflow in memory
/// Workflows are stored in the database+cache, and have a defined str
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    id: WorkflowId,              // mutable id
    uuid: WorkflowUuid,          // immutable id
    dag: Dag,                    // workflow DAG
    description: Option<String>, // workflow description
    created_at: DateTimeWrapper, // created at
    updated_at: DateTimeWrapper, // updated at
}

impl Workflow {
    pub fn new(id: WorkflowId, dag: Dag, description: Option<String>) -> Self {
        let now = DateTimeWrapper::now();

        Self {
            id,
            uuid: WorkflowUuid::new_uuid(),
            dag,
            description,
            created_at: now.clone(),
            updated_at: now,
        }
    }
    pub fn id(&self) -> WorkflowId {
        self.id.clone()
    }
    pub fn uuid(&self) -> WorkflowUuid {
        self.uuid.clone()
    }
    pub fn dag(&self) -> Dag {
        self.dag.clone()
    }
    pub fn description(&self) -> String {
        self.description.clone().unwrap_or_default()
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at.as_datetime()
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at.as_datetime()
    }
    pub async fn run(&mut self, inputs: WorkflowInput) -> Result<String, EpError> {
        self.execute(
            Arc::new(RwLock::new(WorkflowMemory::new(inputs.inputs))),
            Arc::new(RwLock::new(WorkflowProgress::new(self.dag().nodes))),
        )
        .await
    }
    async fn execute(&mut self, memory: Arc<RwLock<WorkflowMemory>>, progress: Arc<RwLock<WorkflowProgress>>) -> Result<String, EpError> {
        match self.dag.execute(memory.clone(), progress).await {
            Ok(_) => {
                memory.blocking_write().complete();
                serde_json::to_string(&memory.blocking_read().outputs).map_err(EpError::workflow)
            }
            Err(e) => Err(e),
        }
    }
    pub fn from_schema(value: WorkflowSchema) -> Self {
        Self {
            id: value.id(),
            uuid: value.uuid(),
            dag: value.dag(),
            description: value.description().map(|s| s.to_string()),
            created_at: DateTimeWrapper::from(value.created_at()),
            updated_at: DateTimeWrapper::from(value.created_at()),
        }
    }
}

//TODO build a `RunWorkflow` from database
#[derive(Debug, Clone)]
pub struct RunWorkflow {
    workflow: Workflow,                      // workflow data
    memory: Arc<RwLock<WorkflowMemory>>,     // workflow memory
    progress: Arc<RwLock<WorkflowProgress>>, // workflow progress
}

impl RunWorkflow {
    // TODO: remove this allow after todo!() is implemented.
    #[allow(clippy::diverging_sub_expression)]
    pub async fn run(_workflow_id: String, _workflow_input: WorkflowInput, _workflow: Workflow) -> Result<String, EpError> {
        let _workflow = todo!("pull workflow data from cache+database");

        // let mut running_workflow = Self {
        //     workflow,
        //     memory: Arc::new(RwLock::new(WorkflowMemory::new(workflow_input.inputs))),
        //     progress: Arc::new(RwLock::new(WorkflowProgress::new(workflow.dag.nodes))),
        // };

        //TODO execute the workflow
        // running_workflow
        //     .workflow
        //     .execute(running_workflow.memory, running_workflow.progress)
        //     .await
    }
}

/// local counter for DAG progress status
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkflowProgress {
    node_status: HashMap<Uuid, bool>, // node processing status
    progress: f64,                    // completion %
}

impl WorkflowProgress {
    fn new(dag_nodes: HashMap<Uuid, Node>) -> Self {
        Self {
            node_status: dag_nodes.into_keys().map(|k| (k, false)).collect::<HashMap<Uuid, bool>>(),
            progress: 0_f64,
        }
    }

    fn update_status(&mut self, key: &Uuid) {
        if let Some(status) = self.node_status.get_mut(key) {
            *status = true;
            self.calculate_progress();
        }
    }

    fn calculate_progress(&mut self) {
        self.progress = self.node_status.values().filter(|&status| *status).count() as f64 / self.node_status.len() as f64;
    }

    fn get_progress(&self) -> f64 {
        self.progress
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowMemory {
    inputs: HashMap<String, Value>,        // external data inputs
    outputs: HashMap<Uuid, Value>,         // node data outputs
    started_at: DateTimeWrapper,           // time workflow started
    completed_at: Option<DateTimeWrapper>, // time workflow completed
}

impl WorkflowMemory {
    fn new(inputs: HashMap<String, Value>) -> Self {
        Self {
            inputs,
            outputs: HashMap::new(),
            started_at: DateTimeWrapper::now(),
            completed_at: None,
        }
    }

    fn add_inputs(&mut self, inputs: Vec<(String, Value)>) {
        self.inputs.extend(inputs);
    }

    fn add_output(&mut self, output: (Uuid, Value)) {
        self.outputs.insert(output.0, output.1);
    }

    fn get_input(&self, key: &str) -> Option<&Value> {
        self.inputs.get(key)
    }

    fn get_inputs(&self, key: Vec<String>) -> Vec<Option<&Value>> {
        key.iter().map(|k| self.inputs.get(k)).collect::<Vec<Option<&Value>>>()
    }

    fn get_outputs(&self, key: Vec<Uuid>) -> Vec<Option<&Value>> {
        key.iter().map(|k| self.outputs.get(k)).collect::<Vec<Option<&Value>>>()
    }

    fn start(&mut self) {
        self.started_at = DateTimeWrapper::now()
    }

    fn complete(&mut self) {
        self.completed_at = Some(DateTimeWrapper::now());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct Node {
    id: String,
    uuid: Uuid,
    source: HashSet<Uuid>,
    template_uuid: TemplateUuid,
    input_op: Option<JsonOps>,
    output_op: Option<JsonOps>,
    children: HashSet<Uuid>,
    description: String,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl Node {
    fn new(id: String, template_uuid: TemplateUuid, input_op: Option<JsonOps>, output_op: Option<JsonOps>, description: String) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id,
            uuid: Uuid::new_v4(),
            source: HashSet::new(), // Initialize empty source set
            template_uuid,
            input_op,
            output_op,
            children: HashSet::new(),
            description,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }

    fn add_parent(&mut self, parent_uuid: Uuid) {
        self.source.insert(parent_uuid);
        self.update_timestamp();
    }

    fn remove_parent(&mut self, parent_uuid: &Uuid) {
        self.source.remove(parent_uuid);
        self.update_timestamp();
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct Dag {
    nodes: HashMap<Uuid, Node>,
    // data_source: HashMap<Uuid, NodeData>, // Store for node data
}

impl<'a> FromSql<'a> for Dag {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if *ty != Type::JSONB {
            return Err("Expected JSONB type for DAG".into());
        }

        Ok(serde_json::from_slice::<Dag>(raw)?)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }
}

impl ToSql for Dag {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Verify we're writing to a JSONB column
        if *ty != Type::JSONB {
            return Err("Dag can only be serialized to JSONB".into());
        }

        postgres_types::Json(serde_json::to_value(self)?).to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

impl Dag {
    pub fn new() -> Self {
        Dag {
            nodes: HashMap::new(),
            // data_store: HashMap::new(),
        }
    }

    pub fn add_node(
        &mut self,
        id: String,
        template_uuid: TemplateUuid,
        input_op: Option<JsonOps>,
        output_op: Option<JsonOps>,
        description: String,
    ) -> Uuid {
        let node = Node::new(id, template_uuid, input_op, output_op, description);
        let uuid = node.uuid;
        self.nodes.insert(uuid, node);
        uuid
    }

    pub fn update_node_id(&mut self, uuid: &Uuid, new_id: String) -> Result<(), String> {
        if let Some(node) = self.nodes.get_mut(uuid) {
            node.id = new_id;
            node.update_timestamp();
            Ok(())
        } else {
            Err(format!("Node with ID {:?} not found", uuid))
        }
    }

    pub fn add_edge(&mut self, from: &Uuid, to: &Uuid) -> Result<(), String> {
        if self.has_cycle_with_new_edge(from, to) {
            return Err("Adding this edge would create a cycle".to_string());
        }

        // Add child to source node
        if let Some(from_node) = self.nodes.get_mut(from) {
            from_node.children.insert(*to);
            from_node.update_timestamp();
        } else {
            return Err(format!("Source node {:?} not found", from));
        }

        // Add parent to target node
        if let Some(to_node) = self.nodes.get_mut(to) {
            to_node.add_parent(*from);
        } else {
            return Err(format!("Target node {:?} not found", to));
        }

        Ok(())
    }

    pub fn remove_edge(&mut self, from: &Uuid, to: &Uuid) -> Result<(), String> {
        // Remove child from source node
        if let Some(from_node) = self.nodes.get_mut(from) {
            from_node.children.remove(to);
            from_node.update_timestamp();
        } else {
            return Err(format!("Source node {:?} not found", from));
        }

        // Remove parent from target node
        if let Some(to_node) = self.nodes.get_mut(to) {
            to_node.remove_parent(from);
        } else {
            return Err(format!("Target node {:?} not found", to));
        }

        Ok(())
    }

    pub fn get_parents(&self, node_id: &Uuid) -> Option<&HashSet<Uuid>> {
        self.nodes.get(node_id).map(|node| &node.source)
    }

    pub fn get_templates(&self) -> Vec<TemplateUuid> {
        self.nodes.values().map(|node| node.template_uuid.clone()).collect::<Vec<TemplateUuid>>()
    }

    // Rest of the implementation remains the same...
    pub fn has_cycle_with_new_edge(&self, from: &Uuid, to: &Uuid) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![*to];

        while let Some(node_id) = stack.pop() {
            if node_id == *from {
                return true;
            }

            if visited.insert(node_id)
                && let Some(node) = self.nodes.get(&node_id)
            {
                stack.extend(node.children.iter().copied());
            }
        }

        false
    }
    pub fn topological_sort(&self) -> Option<Vec<Uuid>> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut temp_visited = HashSet::new();

        for node_id in self.nodes.keys() {
            if !visited.contains(node_id) && self.dfs_topological_sort(node_id, &mut visited, &mut temp_visited, &mut result).is_err() {
                return None; // Cycle detected
            }
        }

        result.reverse();
        Some(result)
    }

    // TODO: Consider replacing `Result<(), ()>` with a typed error
    #[allow(clippy::result_unit_err)]
    pub fn dfs_topological_sort(
        &self,
        node_id: &Uuid,
        visited: &mut HashSet<Uuid>,
        temp_visited: &mut HashSet<Uuid>,
        result: &mut Vec<Uuid>,
    ) -> Result<(), ()> {
        if temp_visited.contains(node_id) {
            return Err(()); // Cycle detected
        }

        if !visited.contains(node_id) {
            temp_visited.insert(*node_id);

            if let Some(node) = self.nodes.get(node_id) {
                for child_id in &node.children {
                    self.dfs_topological_sort(child_id, visited, temp_visited, result)?;
                }
            }

            temp_visited.remove(node_id);
            visited.insert(*node_id);
            result.push(*node_id);
        }

        Ok(())
    }
}

impl Dag {
    async fn execute(&mut self, memory: Arc<RwLock<WorkflowMemory>>, progress: Arc<RwLock<WorkflowProgress>>) -> Result<(), EpError> {
        // Get nodes in topological order
        let ordered_nodes = self.topological_sort().ok_or(EpError::Workflow(WorkflowError::CycleDetected))?;

        // Create a channel for node completion notifications
        let (tx, mut rx) = tokio::sync::mpsc::channel(ordered_nodes.len());
        let completed_nodes = Arc::new(RwLock::new(HashSet::new()));

        // Create futures for all nodes
        let node_futures: Vec<_> = ordered_nodes
            .iter()
            .filter_map(|node_id| {
                let node = self.nodes.get(node_id)?.clone();
                let memory_clone = Arc::clone(&memory);
                let progress_clone = Arc::clone(&progress);
                let tx = tx.clone();
                let completed_nodes = Arc::clone(&completed_nodes);

                Some(tokio::spawn(async move {
                    // Wait for all parent nodes to complete
                    while !{
                        let completed = completed_nodes.read().await;
                        node.source.iter().all(|parent| completed.contains(parent))
                    } {
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    }

                    // Process node's input data
                    let input_data = if let Some(input_op) = &node.input_op {
                        let mem = memory_clone.read().await;
                        // Get data from parent nodes and filter out None values
                        let parent_outputs: Vec<Value> =
                            mem.get_outputs(node.source.iter().copied().collect()).iter().filter_map(|value| value.cloned()).collect();

                        // Apply input operations
                        input_op.process_many(&parent_outputs)?
                    } else {
                        json!(null)
                    };

                    //TODO Simulate node processing (replace with actual template execution)
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    // Process output data
                    let output_data = if let Some(output_op) = &node.output_op {
                        output_op.process(&input_data)?
                    } else {
                        input_data
                    };

                    // Update memory with node's output
                    {
                        let mut mem = memory_clone.write().await;
                        mem.add_output((node.uuid, output_data));
                    }

                    // Mark node as completed
                    {
                        let mut prog = progress_clone.write().await;
                        prog.update_status(&node.uuid);
                    }

                    // Mark node as completed in shared state
                    {
                        let mut completed = completed_nodes.write().await;
                        completed.insert(node.uuid);
                    }

                    // Notify completion
                    tx.send(node.uuid).await.map_err(|_| EpError::Workflow(WorkflowError::ChannelSendError))?;

                    Ok::<_, EpError>(())
                }))
            })
            .collect();

        // Drop original sender so channel closes when all nodes complete
        drop(tx);

        // Wait for all nodes to complete while handling notifications
        let mut completed_count = 0;
        while (rx.recv().await).is_some() {
            completed_count += 1;
            if completed_count == ordered_nodes.len() {
                break;
            }
        }

        // Check for any errors in the futures
        for result in futures::future::join_all(node_futures).await {
            match result {
                Ok(node_result) => node_result?,
                Err(e) => {
                    let error_msg = format!("Task join error: {}", e);
                    return Err(EpError::workflow(error_msg));
                }
            }
        }

        Ok(())
    }

    /// Helper function to print a single node and its children recursively
    fn print_node(&self, uuid: &Uuid, visited: &mut HashSet<Uuid>, prefix: &str, is_last: bool) {
        if !visited.insert(*uuid) {
            // Node was already visited, indicate circular reference
            if let Some(node) = self.nodes.get(uuid) {
                println!("{}{}└── ↺ {} ({})", prefix, if is_last { "└" } else { "├" }, node.id, uuid);
            }
            return;
        }

        if let Some(node) = self.nodes.get(uuid) {
            // Print current node
            println!("{}{}──  {} ({})", prefix, if is_last { "└" } else { "├" }, node.id, uuid);

            // Prepare the prefix for children
            let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });

            // Get all children
            let children: Vec<Uuid> = node.children.iter().copied().collect();
            let total_children = children.len();

            // Print each child
            for (idx, child_id) in children.iter().enumerate() {
                self.print_node(child_id, visited, &child_prefix, idx == total_children - 1);
            }
        }

        visited.remove(uuid);
    }

    fn print_structure(&self) {
        println!("\nDAG Structure:");
        println!("==============");

        // Find root nodes (nodes with no parents)
        let root_nodes: Vec<Uuid> = self.nodes.iter().filter(|(_, node)| node.source.is_empty()).map(|(uuid, _)| *uuid).collect();

        // Print from each root
        for root in &root_nodes {
            self.print_node(root, &mut HashSet::new(), "", true);
        }

        // Print orphaned nodes (if any)
        let mut all_referenced: HashSet<Uuid> = HashSet::new();

        // Collect all nodes referenced in children and sources
        for node in self.nodes.values() {
            all_referenced.extend(node.children.iter().copied());
            all_referenced.extend(node.source.iter().copied());
        }

        let orphaned: Vec<Uuid> =
            self.nodes.keys().filter(|uuid| !all_referenced.contains(*uuid) && !root_nodes.contains(*uuid)).copied().collect();

        if !orphaned.is_empty() {
            println!("\nOrphaned Nodes:");
            println!("===============");
            for uuid in &orphaned {
                self.print_node(uuid, &mut HashSet::new(), "", true);
            }
        }

        // Print statistics
        println!("\nDAG Statistics:");
        println!("==============");
        println!("Total nodes: {}", self.nodes.len());
        println!("Root nodes: {}", root_nodes.len());
        println!("Orphaned nodes: {}", orphaned.len());
    }

    /// Prints detailed information about a specific node
    fn print_node_details(&self, uuid: &Uuid) -> Result<(), String> {
        let node = self.nodes.get(uuid).ok_or_else(|| format!("Node with ID {:?} not found", uuid))?;

        println!("\nNode Details:");
        println!("=============");
        println!("ID: {}", node.id);
        println!("UUID: {}", uuid);
        println!("Description: {}", node.description);
        println!("Created at: {}", node.created_at);
        println!("Updated at: {}", node.updated_at);

        println!("\nParents ({}):", node.source.len());
        for parent_id in &node.source {
            if let Some(parent) = self.nodes.get(parent_id) {
                println!("  - {} ({})", parent.id, parent_id);
            }
        }

        println!("\nChildren ({}):", node.children.len());
        for child_id in &node.children {
            if let Some(child) = self.nodes.get(child_id) {
                println!("  - {} ({})", child.id, child_id);
            }
        }

        Ok(())
    }
}

impl fmt::Debug for Dag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Dag {{")?;
        for (uuid, node) in &self.nodes {
            writeln!(f, "  Node {} ({}): description: {}, children: {:?}", node.id, uuid, node.description, node.children)?;
        }
        write!(f, "}}")
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a simple template key for testing
    fn create_test_template_uuid() -> TemplateUuid {
        // Note: Actual implementation will depend on TemplateKey structure
        TemplateUuid::new_uuid()
    }

    #[test]
    fn test_node_creation() {
        let node = Node::new("test_node".to_string(), create_test_template_uuid(), None, None, "Test description".to_string());

        assert_eq!(node.id, "test_node");
        assert_eq!(node.description, "Test description");
        assert!(node.children.is_empty());
        assert!(node.input_op.is_none());
        assert!(node.output_op.is_none());
    }

    #[test]
    fn test_dag_node_addition() {
        let mut dag = Dag::new();
        let node_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First node".to_string());

        assert_eq!(dag.nodes.len(), 1);
        assert!(dag.nodes.contains_key(&node_id));

        let node = &dag.nodes[&node_id];
        assert_eq!(node.id, "node1");
        assert_eq!(node.description, "First node");
    }

    #[test]
    fn test_node_source_tracking() {
        let mut dag = Dag::new();

        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Second node".to_string());

        // Add edge and verify source tracking
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());

        // Check that node2's source includes node1
        let node2 = &dag.nodes[&node2_id];
        assert!(node2.source.contains(&node1_id));

        // Check that node1's source is empty (no parents)
        let node1 = &dag.nodes[&node1_id];
        assert!(node1.source.is_empty());
    }

    #[test]
    fn test_source_removal() {
        let mut dag = Dag::new();

        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Second node".to_string());

        // Add and then remove an edge
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());
        assert!(dag.remove_edge(&node1_id, &node2_id).is_ok());

        // Verify the source is removed
        let node2 = &dag.nodes[&node2_id];
        assert!(!node2.source.contains(&node1_id));
        assert!(node2.source.is_empty());
    }

    #[test]
    fn test_multiple_sources() {
        let mut dag = Dag::new();

        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First source".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Second source".to_string());

        let node3_id = dag.add_node("node3".to_string(), create_test_template_uuid(), None, None, "Target node".to_string());

        // Add multiple sources to node3
        assert!(dag.add_edge(&node1_id, &node3_id).is_ok());
        assert!(dag.add_edge(&node2_id, &node3_id).is_ok());

        // Verify both sources are tracked
        let node3 = &dag.nodes[&node3_id];
        assert!(node3.source.contains(&node1_id));
        assert!(node3.source.contains(&node2_id));
        assert_eq!(node3.source.len(), 2);
    }

    #[test]
    fn test_dag_edge_addition() {
        let mut dag = Dag::new();

        // Create two nodes
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Second node".to_string());

        // Add edge between nodes
        let result = dag.add_edge(&node1_id, &node2_id);
        assert!(result.is_ok());

        // Verify edge exists
        let node1 = &dag.nodes[&node1_id];
        assert!(node1.children.contains(&node2_id));
    }

    #[test]
    fn test_cycle_detection() {
        let mut dag = Dag::new();

        // Create three nodes
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Second node".to_string());

        let node3_id = dag.add_node("node3".to_string(), create_test_template_uuid(), None, None, "Third node".to_string());

        // Add edges to form a potential cycle
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());
        assert!(dag.add_edge(&node2_id, &node3_id).is_ok());

        // Attempting to create a cycle should fail
        let result = dag.add_edge(&node3_id, &node1_id);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Adding this edge would create a cycle".to_string());
    }

    #[test]
    fn test_topological_sort() {
        let mut dag = Dag::new();

        // Create nodes
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Second node".to_string());

        let node3_id = dag.add_node("node3".to_string(), create_test_template_uuid(), None, None, "Third node".to_string());

        // Add edges to create a specific order
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());
        assert!(dag.add_edge(&node2_id, &node3_id).is_ok());

        // Verify topological sort
        let sorted = dag.topological_sort();
        assert!(sorted.is_some());

        let sorted = sorted.unwrap_or_default();
        assert_eq!(sorted.len(), 3);

        // Check order
        let pos1 = sorted.iter().position(|&id| id == node1_id).unwrap_or_default();
        let pos2 = sorted.iter().position(|&id| id == node2_id).unwrap_or_default();
        let pos3 = sorted.iter().position(|&id| id == node3_id).unwrap_or_default();

        assert!(pos1 < pos2);
        assert!(pos2 < pos3);
    }

    #[test]
    fn test_node_update() {
        let mut dag = Dag::new();

        // Create a node
        let node_id = dag.add_node("old_name".to_string(), create_test_template_uuid(), None, None, "Test node".to_string());

        // Update node id
        let result = dag.update_node_id(&node_id, "new_name".to_string());
        assert!(result.is_ok());

        // Verify update
        let node = &dag.nodes[&node_id];
        assert_eq!(node.id, "new_name");
    }

    #[test]
    fn test_invalid_operations() {
        let mut dag = Dag::new();

        // Test updating non-existent node
        let invalid_uuid = Uuid::new_v4();
        let result = dag.update_node_id(&invalid_uuid, "new_name".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), format!("Node with ID {:?} not found", invalid_uuid));

        // Test adding edge with non-existent nodes
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "First node".to_string());

        let invalid_uuid = Uuid::new_v4();

        // Test invalid source node
        let result = dag.add_edge(&invalid_uuid, &node1_id);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), format!("Source node {:?} not found", invalid_uuid));

        // Test invalid target node
        let result = dag.add_edge(&node1_id, &invalid_uuid);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), format!("Target node {:?} not found", invalid_uuid));
    }

    #[test]
    fn test_complex_graph() {
        let mut dag = Dag::new();

        // Create a more complex graph structure
        //     1
        //    / \
        //   2   3
        //    \ /
        //     4
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "Root node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Left child".to_string());

        let node3_id = dag.add_node("node3".to_string(), create_test_template_uuid(), None, None, "Right child".to_string());

        let node4_id = dag.add_node("node4".to_string(), create_test_template_uuid(), None, None, "Leaf node".to_string());

        // Add edges
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());
        assert!(dag.add_edge(&node1_id, &node3_id).is_ok());
        assert!(dag.add_edge(&node2_id, &node4_id).is_ok());
        assert!(dag.add_edge(&node3_id, &node4_id).is_ok());

        // Verify topological sort
        let sorted = dag.topological_sort().unwrap_or_default();

        // Check that node1 comes before nodes 2 and 3
        let pos1 = sorted.iter().position(|&id| id == node1_id).unwrap_or_default();
        let pos2 = sorted.iter().position(|&id| id == node2_id).unwrap_or_default();
        let pos3 = sorted.iter().position(|&id| id == node3_id).unwrap_or_default();
        let pos4 = sorted.iter().position(|&id| id == node4_id).unwrap_or_default();

        assert!(pos1 < pos2);
        assert!(pos1 < pos3);
        assert!(pos2 < pos4);
        assert!(pos3 < pos4);
    }

    #[test]
    fn test_parent_tracking() {
        let mut dag = Dag::new();

        // Create nodes
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "Parent node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Child node".to_string());

        // Add edge
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());

        // Verify parent tracking
        let child_node = &dag.nodes[&node2_id];
        assert!(child_node.source.contains(&node1_id));
        assert_eq!(child_node.source.len(), 1);

        // Test multiple parents
        let node3_id = dag.add_node("node3".to_string(), create_test_template_uuid(), None, None, "Another parent".to_string());

        assert!(dag.add_edge(&node3_id, &node2_id).is_ok());

        let child_node = &dag.nodes[&node2_id];
        assert!(child_node.source.contains(&node1_id));
        assert!(child_node.source.contains(&node3_id));
        assert_eq!(child_node.source.len(), 2);
    }

    #[test]
    fn test_parent_removal() {
        let mut dag = Dag::new();

        // Create nodes
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "Parent node".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Child node".to_string());

        // Add and remove edge
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());
        assert!(dag.remove_edge(&node1_id, &node2_id).is_ok());

        // Verify parent was removed
        let child_node = &dag.nodes[&node2_id];
        assert!(!child_node.source.contains(&node1_id));
        assert!(child_node.source.is_empty());
    }

    #[test]
    fn test_get_parents() {
        let mut dag = Dag::new();

        // Create a diamond-shaped graph
        let node1_id = dag.add_node("node1".to_string(), create_test_template_uuid(), None, None, "Root".to_string());

        let node2_id = dag.add_node("node2".to_string(), create_test_template_uuid(), None, None, "Left".to_string());

        let node3_id = dag.add_node("node3".to_string(), create_test_template_uuid(), None, None, "Right".to_string());

        let node4_id = dag.add_node("node4".to_string(), create_test_template_uuid(), None, None, "Bottom".to_string());

        // Create diamond shape
        assert!(dag.add_edge(&node1_id, &node2_id).is_ok());
        assert!(dag.add_edge(&node1_id, &node3_id).is_ok());
        assert!(dag.add_edge(&node2_id, &node4_id).is_ok());
        assert!(dag.add_edge(&node3_id, &node4_id).is_ok());

        // Verify parents
        let bottom_parents = dag.get_parents(&node4_id).unwrap();
        assert!(bottom_parents.contains(&node2_id));
        assert!(bottom_parents.contains(&node3_id));
        assert_eq!(bottom_parents.len(), 2);

        let middle_parents = dag.get_parents(&node2_id).unwrap();
        assert!(middle_parents.contains(&node1_id));
        assert_eq!(middle_parents.len(), 1);

        let root_parents = dag.get_parents(&node1_id).unwrap();
        assert!(root_parents.is_empty());
    }

    #[test]
    fn test_print_structure() {
        let mut dag = Dag::new();

        // Create a complex graph structure for testing visualization
        //     A
        //    / \
        //   B   C
        //    \ / \
        //     D   E
        //      \ /
        //       F
        let node_a = dag.add_node("A".to_string(), create_test_template_uuid(), None, None, "Root node".to_string());

        let node_b = dag.add_node("B".to_string(), create_test_template_uuid(), None, None, "Level 1 left".to_string());

        let node_c = dag.add_node("C".to_string(), create_test_template_uuid(), None, None, "Level 1 right".to_string());

        let node_d = dag.add_node("D".to_string(), create_test_template_uuid(), None, None, "Level 2 left".to_string());

        let node_e = dag.add_node("E".to_string(), create_test_template_uuid(), None, None, "Level 2 right".to_string());

        let node_f = dag.add_node("F".to_string(), create_test_template_uuid(), None, None, "Level 3".to_string());

        // Create edges
        assert!(dag.add_edge(&node_a, &node_b).is_ok());
        assert!(dag.add_edge(&node_a, &node_c).is_ok());
        assert!(dag.add_edge(&node_b, &node_d).is_ok());
        assert!(dag.add_edge(&node_c, &node_d).is_ok());
        assert!(dag.add_edge(&node_c, &node_e).is_ok());
        assert!(dag.add_edge(&node_d, &node_f).is_ok());
        assert!(dag.add_edge(&node_e, &node_f).is_ok());

        // Create an orphaned node
        dag.add_node("Orphan".to_string(), create_test_template_uuid(), None, None, "Orphaned node".to_string());

        // Print the structure
        println!("\nTest Dag Structure:");
        dag.print_structure();

        // Print details of a specific node
        println!("\nNode D Details:");
        assert!(dag.print_node_details(&node_d).is_ok());
    }
}
