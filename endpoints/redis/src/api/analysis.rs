//! TODO: This module contains a planned feature for Redis command pattern analysis
//! and function discovery. It is not yet integrated into the main codebase.
//! See: FunctionDiscoveryEngine, RuntimePatternMatcher, CommandSequence analysis.
#![allow(dead_code)]

use crate::api::{RedisApi, RedisJsonValue};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct CommandEvent {
    pub client_addr: SocketAddr,
    pub command: RedisApi,
    pub args: Vec<RedisJsonValue>,
    pub timestamp: Instant,
    pub response_time: Option<Duration>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternConfig {
    pub entity_patterns: HashMap<String, EntityTypeConfig>,
    pub framework_patterns: HashMap<String, FrameworkConfig>,
    pub function_signatures: Vec<FunctionSignatureConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityTypeConfig {
    pub name: String,
    pub patterns: Vec<String>, // Regex patterns like "user:\\d+:.*"
    pub aliases: Vec<String>,  // Alternative names
    pub priority: u8,          // Higher priority wins conflicts
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameworkConfig {
    pub name: String,
    pub key_patterns: Vec<String>,
    pub command_patterns: Vec<String>,
    pub confidence_boost: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSignatureConfig {
    pub name: String,
    pub command_sequence: Vec<String>,
    pub entity_flow: Vec<String>, // e.g., ["User:read", "Order:write", "Inventory:write"]
    pub timing_constraints: TimingConstraints,
    pub confidence_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingConstraints {
    pub max_sequence_duration: Option<Duration>,
    pub max_gap_between_commands: Option<Duration>,
    pub requires_transaction: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntityType {
    Known(String),   // Runtime configured entities
    Unknown(String), // Discovered entities
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperationType {
    Read,
    Write,
    ReadWrite,
    Delete,
    Increment,
    ListOp,
    SetOp,
    Transaction,
}

#[derive(Debug, Clone)]
pub struct DiscoveredFunction {
    pub id: String,
    pub name: Option<String>,
    pub command_pattern: Vec<RedisApi>,
    pub entity_access_pattern: Vec<(EntityType, OperationType)>,
    pub key_templates: Vec<String>,
    pub timing_characteristics: TimingProfile,
    pub confidence_score: f64,
    pub occurrence_count: u32,
    pub clients_seen: HashSet<SocketAddr>,
    pub sample_sequences: Vec<CommandSequence>,
}

#[derive(Debug, Clone)]
pub struct CommandSequence {
    pub commands: Vec<RedisApi>,
    pub key_patterns: Vec<String>,
    pub entity_types: Vec<EntityType>,
    pub duration: Duration,
    pub client_addr: SocketAddr,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub struct TimingProfile {
    pub average_duration: Duration,
    pub command_intervals: Vec<Duration>,
    pub is_transactional: bool,
    pub parallelism_detected: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimePatternMatcher {
    config: Arc<RwLock<PatternConfig>>,
    entity_cache: Arc<RwLock<HashMap<String, EntityType>>>,
    similarity_threshold: f64,
}

impl RuntimePatternMatcher {
    pub fn new(config: PatternConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            entity_cache: Arc::new(RwLock::new(HashMap::new())),
            similarity_threshold: 0.8,
        }
    }

    pub async fn update_config(&self, new_config: PatternConfig) {
        let mut config = self.config.write().await;
        *config = new_config;

        // Clear cache to force re-evaluation
        self.entity_cache.write().await.clear();
    }

    pub async fn extract_entity_type(&self, key: &str) -> EntityType {
        // Check cache first
        {
            let cache = self.entity_cache.read().await;
            if let Some(entity_type) = cache.get(key) {
                return entity_type.clone();
            }
        }

        let config = self.config.read().await;
        let mut best_match = None;
        let mut highest_priority = 0;

        // Try configured patterns
        for (entity_name, entity_config) in &config.entity_patterns {
            for pattern in &entity_config.patterns {
                if self.matches_pattern(key, pattern) && entity_config.priority > highest_priority {
                    highest_priority = entity_config.priority;
                    best_match = Some(EntityType::Known(entity_name.clone()));
                }
            }
        }

        let entity_type = best_match.unwrap_or_else(|| {
            // Try to infer from key structure
            self.infer_entity_from_structure(key)
        });

        // Cache the result
        {
            let mut cache = self.entity_cache.write().await;
            cache.insert(key.to_string(), entity_type.clone());
        }

        entity_type
    }

    fn matches_pattern(&self, key: &str, pattern: &str) -> bool {
        // Simple pattern matching - could be enhanced with regex
        if pattern.contains("*") {
            let pattern_parts: Vec<&str> = pattern.split('*').collect();
            if pattern_parts.len() == 2 {
                let prefix = pattern_parts[0];
                let suffix = pattern_parts[1];
                return key.starts_with(prefix) && key.ends_with(suffix);
            }
        }

        key.contains(pattern)
    }

    fn infer_entity_from_structure(&self, key: &str) -> EntityType {
        // Extract potential entity name from key structure
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() >= 2 {
            let potential_entity = parts[0].to_lowercase();
            EntityType::Unknown(potential_entity)
        } else {
            EntityType::Unknown("generic".to_string())
        }
    }

    pub async fn detect_framework(&self, sequences: &[CommandSequence]) -> Option<String> {
        let config = self.config.read().await;

        for (framework_name, framework_config) in &config.framework_patterns {
            let mut confidence = 0.0;

            // Check key patterns
            for sequence in sequences {
                for key_pattern in &sequence.key_patterns {
                    for pattern in &framework_config.key_patterns {
                        if self.matches_pattern(key_pattern, pattern) {
                            confidence += framework_config.confidence_boost;
                        }
                    }
                }
            }

            if confidence > 1.0 {
                return Some(framework_name.clone());
            }
        }

        None
    }

    pub fn extract_key_template(&self, key: &str) -> String {
        let mut template = key.to_string();

        // Replace common ID patterns
        if let Ok(re) = regex::Regex::new(r"\b\d+\b") {
            template = re.replace_all(&template, "{id}").to_string();
        }

        if let Ok(re) = regex::Regex::new(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") {
            template = re.replace_all(&template, "{uuid}").to_string();
        }

        if let Ok(re) = regex::Regex::new(r"\b[0-9a-f]{24}\b") {
            template = re.replace_all(&template, "{objectid}").to_string();
        }

        template
    }
}

pub struct FunctionDiscoveryEngine {
    sequences: Arc<RwLock<HashMap<SocketAddr, VecDeque<CommandEvent>>>>,
    discovered_functions: Arc<RwLock<HashMap<String, DiscoveredFunction>>>,
    pattern_matcher: RuntimePatternMatcher,
    analysis_sender: Sender<Vec<CommandEvent>>,
    sequence_window_size: usize,
}

impl FunctionDiscoveryEngine {
    pub fn new(config: PatternConfig) -> (Self, FunctionAnalysisService) {
        let (sender, receiver) = mpsc::channel();
        let sequences = Arc::new(RwLock::new(HashMap::new()));
        let discovered_functions = Arc::new(RwLock::new(HashMap::new()));
        let pattern_matcher = RuntimePatternMatcher::new(config);

        let engine = FunctionDiscoveryEngine {
            sequences: sequences.clone(),
            discovered_functions: discovered_functions.clone(),
            pattern_matcher: pattern_matcher.clone(),
            analysis_sender: sender,
            sequence_window_size: 10,
        };

        let service = FunctionAnalysisService::new(receiver, sequences, discovered_functions, pattern_matcher);

        (engine, service)
    }

    pub async fn record_command(&self, event: CommandEvent) {
        let mut sequences = self.sequences.write().await;
        let client_sequence = sequences.entry(event.client_addr).or_insert_with(|| VecDeque::with_capacity(50));

        client_sequence.push_back(event.clone());

        if client_sequence.len() > 50 {
            client_sequence.pop_front();
        }

        // Analyze recent sequences
        if client_sequence.len() >= 3 {
            let recent_events: Vec<CommandEvent> = client_sequence.iter().rev().take(self.sequence_window_size).cloned().collect();

            if self.analysis_sender.send(recent_events).is_err() {
                eprintln!("Failed to send sequence for analysis");
            }
        }
    }

    pub async fn update_patterns(&self, config: PatternConfig) {
        self.pattern_matcher.update_config(config).await;
    }

    pub async fn get_discovered_functions(&self) -> Vec<DiscoveredFunction> {
        let functions = self.discovered_functions.read().await;
        functions.values().cloned().collect()
    }

    pub async fn get_function_insights(&self) -> FunctionInsights {
        let functions = self.discovered_functions.read().await;
        let total_functions = functions.len();

        let high_confidence: Vec<_> = functions.values().filter(|f| f.confidence_score > 0.7).cloned().collect();

        let mut entity_interactions: HashMap<EntityType, Vec<EntityType>> = HashMap::new();
        for func in functions.values() {
            let entities: Vec<_> = func.entity_access_pattern.iter().map(|(e, _)| e).collect();
            for window in entities.windows(2) {
                entity_interactions.entry(window[0].clone()).or_default().push(window[1].clone());
            }
        }

        let frequency_ranking: Vec<_> =
            functions.values().map(|f| (f.name.clone().unwrap_or_else(|| f.id.clone()), f.occurrence_count)).collect();

        FunctionInsights {
            total_functions_discovered: total_functions,
            high_confidence_functions: high_confidence,
            entity_interactions,
            function_frequency_ranking: frequency_ranking,
            pattern_variations: self.get_pattern_variations().await,
        }
    }

    async fn get_pattern_variations(&self) -> HashMap<String, Vec<String>> {
        let functions = self.discovered_functions.read().await;
        let mut variations: HashMap<String, Vec<String>> = HashMap::new();

        for func in functions.values() {
            for template in &func.key_templates {
                let base_pattern = self.normalize_pattern(template);
                variations.entry(base_pattern).or_default().push(template.clone());
            }
        }

        variations
    }

    fn normalize_pattern(&self, template: &str) -> String {
        // Group similar patterns together
        template.replace("{id}", "{X}").replace("{uuid}", "{X}").replace("{objectid}", "{X}")
    }
}

pub struct FunctionAnalysisService {
    receiver: Receiver<Vec<CommandEvent>>,
    sequences: Arc<RwLock<HashMap<SocketAddr, VecDeque<CommandEvent>>>>,
    discovered_functions: Arc<RwLock<HashMap<String, DiscoveredFunction>>>,
    pattern_matcher: RuntimePatternMatcher,
    sequence_clusterer: SequenceClusterer,
}

impl FunctionAnalysisService {
    fn new(
        receiver: Receiver<Vec<CommandEvent>>,
        sequences: Arc<RwLock<HashMap<SocketAddr, VecDeque<CommandEvent>>>>,
        discovered_functions: Arc<RwLock<HashMap<String, DiscoveredFunction>>>,
        pattern_matcher: RuntimePatternMatcher,
    ) -> Self {
        Self {
            receiver,
            sequences,
            discovered_functions,
            pattern_matcher,
            sequence_clusterer: SequenceClusterer::new(),
        }
    }

    pub fn start(self) {
        thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(_) => return,
            };
            while let Ok(events) = self.receiver.recv() {
                rt.block_on(async {
                    self.analyze_sequence(events).await;
                });
            }
        });
    }

    async fn analyze_sequence(&self, events: Vec<CommandEvent>) {
        if events.is_empty() {
            return;
        }

        // Convert events to command sequence
        let sequence = self.create_command_sequence(events).await;

        // Try to match with existing functions or create new one
        let function_match = self.sequence_clusterer.find_similar_function(&sequence, &self.discovered_functions).await;

        match function_match {
            FunctionMatch::ExistingFunction(func_id) => {
                self.update_existing_function(func_id, sequence).await;
            }
            FunctionMatch::NewFunction(new_function) => {
                let mut functions = self.discovered_functions.write().await;
                functions.insert(new_function.id.clone(), new_function);
            }
        }
    }

    async fn create_command_sequence(&self, events: Vec<CommandEvent>) -> CommandSequence {
        let commands: Vec<RedisApi> = events.iter().map(|e| e.command.clone()).collect();

        let mut key_patterns = Vec::new();
        let mut entity_types = Vec::new();

        for event in &events {
            for arg in &event.args {
                if let RedisJsonValue::String(key) = arg {
                    let template = self.pattern_matcher.extract_key_template(key);
                    let entity_type = self.pattern_matcher.extract_entity_type(key).await;

                    key_patterns.push(template);
                    entity_types.push(entity_type);
                }
            }
        }

        let duration = if events.len() > 1 {
            if let (Some(last), Some(first)) = (events.last(), events.first()) {
                last.timestamp.duration_since(first.timestamp)
            } else {
                Duration::from_millis(0)
            }
        } else {
            Duration::from_millis(0)
        };

        CommandSequence {
            commands,
            key_patterns,
            entity_types,
            duration,
            client_addr: events[0].client_addr,
            timestamp: events[0].timestamp,
        }
    }

    async fn update_existing_function(&self, func_id: String, sequence: CommandSequence) {
        let mut functions = self.discovered_functions.write().await;
        if let Some(function) = functions.get_mut(&func_id) {
            function.occurrence_count += 1;
            function.clients_seen.insert(sequence.client_addr);

            // Keep sample sequences (limit to prevent memory growth)
            if function.sample_sequences.len() < 5 {
                function.sample_sequences.push(sequence);
            }

            // Update confidence based on consistency
            function.confidence_score = self.calculate_confidence_score(function);
        }
    }

    fn calculate_confidence_score(&self, function: &DiscoveredFunction) -> f64 {
        let base_score = (function.occurrence_count as f64).min(10.0) / 10.0;
        let uniqueness_bonus = if function.clients_seen.len() > 1 { 0.2 } else { 0.0 };
        let consistency_bonus = if function.sample_sequences.len() >= 3 { 0.1 } else { 0.0 };

        (base_score + uniqueness_bonus + consistency_bonus).min(1.0)
    }
}

pub struct SequenceClusterer {
    similarity_threshold: f64,
}

impl SequenceClusterer {
    pub fn new() -> Self {
        Self { similarity_threshold: 0.8 }
    }

    pub async fn find_similar_function(
        &self,
        sequence: &CommandSequence,
        functions: &Arc<RwLock<HashMap<String, DiscoveredFunction>>>,
    ) -> FunctionMatch {
        let functions_map = functions.read().await;

        for (func_id, function) in functions_map.iter() {
            if self.sequences_are_similar(sequence, &function.command_pattern, &function.key_templates) {
                return FunctionMatch::ExistingFunction(func_id.clone());
            }
        }

        // Create new function
        let new_function = self.create_new_function(sequence);
        FunctionMatch::NewFunction(new_function)
    }

    fn sequences_are_similar(&self, sequence: &CommandSequence, existing_commands: &[RedisApi], existing_key_templates: &[String]) -> bool {
        // Check command similarity
        let command_similarity = self.calculate_command_similarity(&sequence.commands, existing_commands);

        // Check key pattern similarity
        let key_similarity = self.calculate_key_similarity(&sequence.key_patterns, existing_key_templates);

        let overall_similarity = (command_similarity + key_similarity) / 2.0;
        overall_similarity >= self.similarity_threshold
    }

    fn calculate_command_similarity(&self, seq1: &[RedisApi], seq2: &[RedisApi]) -> f64 {
        if seq1.is_empty() || seq2.is_empty() {
            return 0.0;
        }

        let matches = seq1.iter().zip(seq2.iter()).filter(|(a, b)| a == b).count();
        matches as f64 / seq1.len().max(seq2.len()) as f64
    }

    fn calculate_key_similarity(&self, patterns1: &[String], patterns2: &[String]) -> f64 {
        if patterns1.is_empty() || patterns2.is_empty() {
            return 0.0;
        }

        let set1: HashSet<_> = patterns1.iter().collect();
        let set2: HashSet<_> = patterns2.iter().collect();

        let intersection = set1.intersection(&set2).count();
        let union = set1.union(&set2).count();

        if union == 0 { 0.0 } else { intersection as f64 / union as f64 }
    }

    fn create_new_function(&self, sequence: &CommandSequence) -> DiscoveredFunction {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        sequence.commands.hash(&mut hasher);
        sequence.key_patterns.hash(&mut hasher);
        let func_id = format!("func_{:x}", hasher.finish());

        let entity_access_pattern = sequence
            .entity_types
            .iter()
            .map(|e| (e.clone(), OperationType::ReadWrite)) // Simplified
            .collect();

        DiscoveredFunction {
            id: func_id,
            name: self.infer_function_name(sequence),
            command_pattern: sequence.commands.clone(),
            entity_access_pattern,
            key_templates: sequence.key_patterns.clone(),
            timing_characteristics: TimingProfile {
                average_duration: sequence.duration,
                command_intervals: Vec::new(),
                is_transactional: sequence.commands.contains(&RedisApi::Multi),
                parallelism_detected: false,
            },
            confidence_score: 0.5,
            occurrence_count: 1,
            clients_seen: [sequence.client_addr].into_iter().collect(),
            sample_sequences: vec![sequence.clone()],
        }
    }

    fn infer_function_name(&self, sequence: &CommandSequence) -> Option<String> {
        // Try to infer function name from patterns
        let entities: HashSet<_> = sequence.entity_types.iter().collect();
        let has_reads = sequence.commands.iter().any(|c| matches!(c, RedisApi::Get | RedisApi::Hget | RedisApi::Mget));
        let has_writes = sequence.commands.iter().any(|c| matches!(c, RedisApi::Set | RedisApi::Hset | RedisApi::Mset));

        if entities.len() == 1 {
            if let Some(entity) = entities.iter().next() {
                match (has_reads, has_writes) {
                    (true, false) => Some(format!("get{:?}", entity)),
                    (false, true) => Some(format!("create{:?}", entity)),
                    (true, true) => Some(format!("update{:?}", entity)),
                    _ => None,
                }
            } else {
                None
            }
        } else if entities.len() > 1 {
            Some("multiEntityOperation".to_string())
        } else {
            None
        }
    }
}

// TODO: Consider boxing to reduce size differences between variants.
#[allow(clippy::large_enum_variant)]
pub enum FunctionMatch {
    ExistingFunction(String),
    NewFunction(DiscoveredFunction),
}

#[derive(Debug)]
pub struct FunctionInsights {
    pub total_functions_discovered: usize,
    pub high_confidence_functions: Vec<DiscoveredFunction>,
    pub entity_interactions: HashMap<EntityType, Vec<EntityType>>,
    pub function_frequency_ranking: Vec<(String, u32)>,
    pub pattern_variations: HashMap<String, Vec<String>>,
}

impl Default for PatternConfig {
    fn default() -> Self {
        let mut entity_patterns = HashMap::new();

        entity_patterns.insert(
            "user".to_string(),
            EntityTypeConfig {
                name: "User".to_string(),
                patterns: vec!["user:*".to_string(), "profile:*".to_string(), "account:*".to_string()],
                aliases: vec!["customer".to_string(), "member".to_string()],
                priority: 10,
            },
        );

        entity_patterns.insert(
            "order".to_string(),
            EntityTypeConfig {
                name: "Order".to_string(),
                patterns: vec!["order:*".to_string(), "purchase:*".to_string()],
                aliases: vec!["transaction".to_string()],
                priority: 9,
            },
        );

        Self {
            entity_patterns,
            framework_patterns: HashMap::new(),
            function_signatures: Vec::new(),
        }
    }
}
