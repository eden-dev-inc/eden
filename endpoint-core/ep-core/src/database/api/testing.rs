// Suppress async_fn_in_trait warning because we don't need to specify auto trait bounds for these traits.
#![allow(async_fn_in_trait)]

use crate::database::template::TemplateFields;
use format::TemplateId;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Hash, PartialEq, Eq)]
pub struct TestId(String);

impl TestId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Main structure for managing test validation lifecycle and results
/// Coordinates test execution, tracks results, and determines overall status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TestingValidation {
    /// Collection of all test cases to be executed
    test_suite: Vec<TestCase>,
    /// Results indexed by test ID for quick lookup
    test_results: HashMap<TestId, TestResult>,
    /// Current state of the entire test suite
    overall_status: TestStatus,
    /// Configuration parameters for test execution
    test_configuration: TestConfiguration,
    /// Timestamp when testing began
    started_at: Option<DateTimeWrapper>,
    /// Timestamp when testing completed
    completed_at: Option<DateTimeWrapper>,
}

impl TestingValidation {
    pub fn new(test_configuration: TestConfiguration) -> Self {
        Self {
            test_suite: Vec::new(),
            test_results: HashMap::new(),
            overall_status: TestStatus::NotStarted,
            test_configuration,
            started_at: None,
            completed_at: None,
        }
    }

    pub fn add_test(&mut self, test_case: TestCase) {
        self.test_suite.push(test_case);
    }

    /// Begin test execution - can only be called from NotStarted status
    pub fn start_testing(&mut self) -> Result<(), String> {
        match self.overall_status {
            TestStatus::NotStarted => {
                self.overall_status = TestStatus::Running;
                self.started_at = Some(DateTimeWrapper::now());
                Ok(())
            }
            _ => Err(format!("Cannot start testing in status: {:?}", self.overall_status)),
        }
    }

    pub fn add_test_result(&mut self, test_id: TestId, result: TestResult) {
        self.test_results.insert(test_id, result);
        self.update_overall_status();
    }

    pub fn complete_testing(&mut self) {
        self.completed_at = Some(DateTimeWrapper::now());
        self.update_overall_status();
    }

    fn update_overall_status(&mut self) {
        if self.test_results.len() < self.test_suite.len() {
            return;
        }

        let total_tests = self.test_results.len() as f64;
        let passed_tests = self.test_results.values().filter(|r| matches!(r.status, TestResultStatus::Passed)).count() as f64;

        let pass_percentage = passed_tests / total_tests;

        if pass_percentage >= self.test_configuration.minimum_pass_percentage {
            self.overall_status = TestStatus::Passed;
        } else if passed_tests > 0.0 {
            self.overall_status = TestStatus::PartiallyPassed;
        } else {
            self.overall_status = TestStatus::Failed;
        }
    }

    pub fn is_ready_for_migration(&self) -> bool {
        matches!(self.overall_status, TestStatus::Passed)
    }

    pub fn get_failed_tests(&self) -> Vec<&TestResult> {
        self.test_results.values().filter(|r| matches!(r.status, TestResultStatus::Failed)).collect()
    }

    // Getters
    pub fn test_suite(&self) -> &Vec<TestCase> {
        &self.test_suite
    }

    pub fn test_results(&self) -> &HashMap<TestId, TestResult> {
        &self.test_results
    }

    pub fn overall_status(&self) -> &TestStatus {
        &self.overall_status
    }

    pub fn test_configuration(&self) -> &TestConfiguration {
        &self.test_configuration
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TestStatus {
    NotStarted,
    Running,
    Passed,
    Failed,
    PartiallyPassed, // Some tests failed but within tolerance
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TestConfiguration {
    parallel_execution: bool,
    fail_fast: bool, // Stop on first failure
    test_timeout_seconds: u32,
    retry_failed_tests: u8,       // Number of retries for failed tests
    minimum_pass_percentage: f64, // e.g., 95% of tests must pass
}

impl Default for TestConfiguration {
    fn default() -> Self {
        Self {
            parallel_execution: true,
            fail_fast: false,
            test_timeout_seconds: 30,
            retry_failed_tests: 2,
            minimum_pass_percentage: 0.95,
        }
    }
}

/// Different categories of tests for comprehensive validation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TestCase {
    /// Validates code syntax and configuration without execution
    SyntaxValidation(SyntaxTest),
    /// Ensures data remains consistent between old and new implementations
    DataConsistency(ConsistencyTest),
    /// Measures system performance under various load conditions
    Performance(PerformanceTest),
    /// Tests interactions between different system components
    Integration(IntegrationTest),
    /// Validates core business rules and workflows
    BusinessLogic(BusinessLogicTest),
    /// Quick tests to verify basic system functionality is working
    SmokeTest(SmokeTest),
}

impl TestCase {
    pub fn get_test_id(&self) -> &TestId {
        match self {
            TestCase::SyntaxValidation(test) => &test.test_id,
            TestCase::DataConsistency(test) => &test.test_id,
            TestCase::Performance(test) => &test.test_id,
            TestCase::Integration(test) => &test.test_id,
            TestCase::BusinessLogic(test) => &test.test_id,
            TestCase::SmokeTest(test) => &test.test_id,
        }
    }

    pub fn get_description(&self) -> &str {
        match self {
            TestCase::SyntaxValidation(test) => &test.description,
            TestCase::DataConsistency(test) => &test.description,
            TestCase::Performance(test) => &test.description,
            TestCase::Integration(test) => &test.description,
            TestCase::BusinessLogic(test) => &test.description,
            TestCase::SmokeTest(test) => &test.description,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SyntaxTest {
    test_id: TestId,
    description: String,
    validate_bindings: bool,
    validate_response_logic: bool,
    validate_routing_config: bool,
}

impl SyntaxTest {
    pub fn new(test_id: TestId, description: String) -> Self {
        Self {
            test_id,
            description,
            validate_bindings: true,
            validate_response_logic: true,
            validate_routing_config: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ConsistencyTest {
    test_id: TestId,
    description: String,
    sample_requests: Vec<TestRequest>,
    compare_old_new_responses: bool,
    tolerance_threshold: f64, // Acceptable difference percentage
    field_comparisons: Vec<FieldComparison>,
}

impl ConsistencyTest {
    pub fn new(test_id: TestId, description: String, sample_requests: Vec<TestRequest>) -> Self {
        Self {
            test_id,
            description,
            sample_requests,
            compare_old_new_responses: true,
            tolerance_threshold: 0.05, // 5% tolerance by default
            field_comparisons: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PerformanceTest {
    test_id: TestId,
    description: String,
    load_pattern: LoadPattern,
    acceptable_latency_ms: u32,
    acceptable_throughput: u32,
    duration_seconds: u32,
    baseline_comparison: bool,
}

impl PerformanceTest {
    pub fn new(test_id: TestId, description: String, load_pattern: LoadPattern) -> Self {
        Self {
            test_id,
            description,
            load_pattern,
            acceptable_latency_ms: 1000, // 1 second default
            acceptable_throughput: 100,  // 100 RPS default
            duration_seconds: 60,        // 1 minute default
            baseline_comparison: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IntegrationTest {
    test_id: TestId,
    description: String,
    order: TestOrder,
    logic: Vec<IntegrationTestLogic>,
}

impl IntegrationTest {
    pub fn new(test_id: TestId, description: String, order: TestOrder, logic: Vec<IntegrationTestLogic>) -> Self {
        Self { test_id, description, order, logic }
    }
}

/// For integration tests where we write or read data, we may first need to instantiate some data.
/// We will then need to validate the test result as `OK` or `ERR`, then validate the data within
/// the response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IntegrationTestLogic {
    preparation_templates: Vec<(TemplateId, TemplateFields)>,
    test_template: TemplateId,
    expected_result: TestValidation,
    rollback_template: Option<TemplateId>,
}

/// Determines the ordering of test requests. If testing sequentially, we wait for each test to
/// finish before moving to the next. Otherwise, we run all requests in parallel
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TestOrder {
    Sequential,
    Parallel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TestValidation {
    Ok(Option<TemplateFields>),
    Err(Option<TemplateFields>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BusinessLogicTest {
    test_id: TestId,
    description: String,
    business_rules: Vec<BusinessRule>,
    expected_outcomes: Vec<ExpectedOutcome>,
    critical_paths: Vec<String>, // Business critical user flows
}

impl BusinessLogicTest {
    pub fn new(test_id: TestId, description: String) -> Self {
        Self {
            test_id,
            description,
            business_rules: Vec::new(),
            expected_outcomes: Vec::new(),
            critical_paths: Vec::new(),
        }
    }
}

/// Smoke tests are lightweight, fast tests that verify basic system functionality
/// Usually run first to catch obvious failures before running more expensive test suites
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SmokeTest {
    test_id: TestId,
    description: String,
    health_checks: Vec<HealthCheck>,
    critical_endpoints: Vec<String>,
    max_execution_time_seconds: u32,
}

impl SmokeTest {
    pub fn new(test_id: TestId, description: String) -> Self {
        Self {
            test_id,
            description,
            health_checks: Vec::new(),
            critical_endpoints: Vec::new(),
            max_execution_time_seconds: 30,
        }
    }
}

// Supporting structures
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TestRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Option<serde_json::Value>,
    expected_status: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FieldComparison {
    field_path: String, // JSONPath to the field
    comparison_type: ComparisonType,
    tolerance: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum ComparisonType {
    Exact,       // Values must be identical
    Numeric,     // Numeric values within tolerance
    Approximate, // String similarity within tolerance
    Ignore,      // Skip comparison for this field
}

/// Different load patterns for performance testing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum LoadPattern {
    Constant {
        rps: u32,
    }, // Steady requests per second
    Ramp {
        start_rps: u32,
        end_rps: u32,
    }, // Gradually increase load
    Spike {
        base_rps: u32,
        spike_rps: u32,
        spike_duration_seconds: u32,
    }, // Traffic spikes
    Burst {
        rps: u32,
        burst_interval_seconds: u32,
    }, // Periodic bursts
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TestScenario {
    name: String,
    steps: Vec<TestStep>,
    setup_required: bool,
    cleanup_required: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TestStep {
    description: String,
    action: TestAction,
    expected_result: ExpectedResult,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TestAction {
    HttpRequest(TestRequest),
    DatabaseQuery { query: String, parameters: Vec<serde_json::Value> },
    ExternalCall { service: String, endpoint: String },
    Wait { duration_ms: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExpectedResult {
    status_code: Option<u16>,
    response_contains: Option<String>,
    response_time_ms: Option<u32>,
    custom_validation: Option<String>, // Custom validation logic
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BusinessRule {
    name: String,
    condition: String, // Business rule condition
    expected_behavior: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExpectedOutcome {
    scenario: String,
    expected_result: serde_json::Value,
    tolerance: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HealthCheck {
    name: String,
    endpoint: String,
    expected_status: u16,
    timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TestResult {
    test_id: TestId,
    status: TestResultStatus,
    execution_time_ms: u64,
    error_message: Option<String>,
    metrics: Option<TestMetrics>,
    retry_count: u8,
    started_at: DateTimeWrapper,
    completed_at: Option<DateTimeWrapper>,
}

impl TestResult {
    pub fn new(test_id: TestId) -> Self {
        Self {
            test_id,
            status: TestResultStatus::Passed, // Will be updated
            execution_time_ms: 0,
            error_message: None,
            metrics: None,
            retry_count: 0,
            started_at: DateTimeWrapper::now(),
            completed_at: None,
        }
    }

    pub fn status(&self) -> &TestResultStatus {
        &self.status
    }

    pub fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }

    pub fn mark_failed(&mut self, error: String) {
        self.status = TestResultStatus::Failed;
        self.error_message = Some(error);
        self.completed_at = Some(DateTimeWrapper::now());
    }

    pub fn mark_passed(&mut self) {
        self.status = TestResultStatus::Passed;
        self.completed_at = Some(DateTimeWrapper::now());
    }

    pub fn mark_timeout(&mut self) {
        self.status = TestResultStatus::TimedOut;
        self.error_message = Some("Test execution timed out".to_string());
        self.completed_at = Some(DateTimeWrapper::now());
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TestResultStatus {
    Passed,
    Failed,
    Skipped,
    TimedOut,
}

/// Performance metrics collected during test execution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TestMetrics {
    requests_sent: u64,
    responses_received: u64,
    average_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
    error_rate: f64,
    throughput_rps: f64,
    memory_usage_mb: Option<f64>,
    cpu_usage_percent: Option<f64>,
}

impl Default for TestMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TestMetrics {
    pub fn new() -> Self {
        Self {
            requests_sent: 0,
            responses_received: 0,
            average_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            error_rate: 0.0,
            throughput_rps: 0.0,
            memory_usage_mb: None,
            cpu_usage_percent: None,
        }
    }
}

/// Trait for implementing test runners for different test types
pub trait TestExecutor {
    async fn execute_syntax_test(&self, test: &SyntaxTest) -> TestResult;
    async fn execute_consistency_test(&self, test: &ConsistencyTest) -> TestResult;
    async fn execute_performance_test(&self, test: &PerformanceTest) -> TestResult;
    async fn execute_integration_test(&self, test: &IntegrationTest) -> TestResult;
    async fn execute_business_logic_test(&self, test: &BusinessLogicTest) -> TestResult;
    async fn execute_smoke_test(&self, test: &SmokeTest) -> TestResult;
}

/// Helper trait for test validation
pub trait TestValidator {
    fn validate_test_configuration(&self, config: &TestConfiguration) -> Result<(), String>;
    fn validate_test_case(&self, test_case: &TestCase) -> Result<(), String>;
    fn can_run_in_parallel(&self, test_case: &TestCase) -> bool;
}
