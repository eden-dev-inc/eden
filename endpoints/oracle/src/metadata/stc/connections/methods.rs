use super::*;
impl OracleConnectionInfo {
    /// Gets the total number of user sessions (all statuses)
    pub fn total_user_sessions(&self) -> u64 {
        self.current_user_sessions
    }

    /// Gets the total number of all sessions (user + background + recursive)
    pub fn total_all_sessions(&self) -> u64 {
        self.current_user_sessions + self.current_background_sessions + self.current_recursive_sessions
    }

    /// Calculates the percentage of shared pool that is free
    pub fn shared_pool_free_percentage(&self) -> f64 {
        ratio_percentage(self.shared_pool_free, self.shared_pool_size)
    }

    /// Calculates the PGA utilization percentage
    pub fn pga_utilization_percentage(&self) -> f64 {
        ratio_percentage(self.total_pga_allocated, self.pga_aggregate_limit)
    }

    /// Checks if session limit is being approached
    pub fn is_approaching_session_limit(&self, threshold_percentage: f64) -> bool {
        self.session_utilization_pct > threshold_percentage
    }

    /// Checks if process limit is being approached
    pub fn is_approaching_process_limit(&self, threshold_percentage: f64) -> bool {
        self.process_utilization_pct > threshold_percentage
    }

    /// Checks if PGA memory usage is high
    pub fn is_pga_usage_high(&self, threshold_percentage: f64) -> bool {
        self.pga_utilization_percentage() > threshold_percentage
    }

    /// Checks if shared pool free memory is low
    pub fn is_shared_pool_low(&self, threshold_percentage: f64) -> bool {
        self.shared_pool_free_percentage() < threshold_percentage
    }

    /// Checks if there are sessions experiencing resource contention
    pub fn has_resource_contention(&self) -> bool {
        self.sessions_waiting > 0 || self.sessions_blocking > 0 || self.pga_over_allocation_count > 0
    }

    /// Gets connection pool efficiency if pool statistics are available
    pub fn connection_pool_efficiency(&self) -> Option<f64> {
        self.connection_pool_stats.as_ref().map(|stats| stats.hit_ratio)
    }

    /// Returns the service with the most connections
    pub fn busiest_service(&self) -> Option<&OracleConnectionsByService> {
        self.connections_by_service.iter().max_by_key(|service| service.total_connections)
    }

    /// Returns the machine with the most connections
    pub fn busiest_machine(&self) -> Option<&OracleConnectionsByMachine> {
        self.connections_by_machine.iter().max_by_key(|machine| machine.total_connections)
    }
}
#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_oracle_connection_info_calculations() {
        let mut connection_info = OracleConnectionInfo {
            current_user_sessions: 80,
            current_background_sessions: 15,
            current_recursive_sessions: 5,
            max_sessions: 200,
            current_processes: 90,
            max_processes: 150,
            shared_pool_size: 1_000_000_000,
            shared_pool_free: 200_000_000,
            total_pga_allocated: 800_000_000,
            pga_aggregate_limit: 2_000_000_000,
            ..OracleConnectionInfo::default()
        };

        assert_eq!(connection_info.total_user_sessions(), 80);
        assert_eq!(connection_info.total_all_sessions(), 100);
        assert_eq!(connection_info.shared_pool_free_percentage(), 20.0);
        assert_eq!(connection_info.pga_utilization_percentage(), 40.0);

        connection_info.session_utilization_pct = 50.0;
        connection_info.process_utilization_pct = 60.0;
        assert!(connection_info.is_approaching_session_limit(40.0));
        assert!(!connection_info.is_approaching_session_limit(60.0));
        assert!(!connection_info.is_approaching_process_limit(90.0));

        connection_info.total_active_sessions = 100;
        assert!(!connection_info.has_resource_contention());
    }

    #[test]
    fn test_oracle_connection_info_methods() {
        let mut connection_info = OracleConnectionInfo::default();

        let pool_stats = OracleConnectionPoolStats {
            pool_name: "test_pool".to_string(),
            active_connections: 10,
            idle_connections: 5,
            busy_connections: 8,
            max_connections: 20,
            min_connections: 2,
            initial_connections: 5,
            increment_connections: 2,
            decrement_connections: 1,
            total_requests: 1000,
            cache_hits: 850,
            cache_misses: 150,
            hit_ratio: 85.0,
        };
        connection_info.connection_pool_stats = Some(pool_stats);

        assert_eq!(connection_info.connection_pool_efficiency(), Some(85.0));

        connection_info.connections_by_service = vec![
            OracleConnectionsByService {
                service_name: "PROD_SERVICE".to_string(),
                total_connections: 100,
                active_connections: 80,
                inactive_connections: 15,
                killed_connections: 5,
                avg_pga_per_connection: 20_000_000,
                longest_idle_time: 3600,
            },
            OracleConnectionsByService {
                service_name: "TEST_SERVICE".to_string(),
                total_connections: 25,
                active_connections: 20,
                inactive_connections: 5,
                killed_connections: 0,
                avg_pga_per_connection: 15_000_000,
                longest_idle_time: 1800,
            },
        ];
        assert_eq!(connection_info.busiest_service().unwrap().service_name, "PROD_SERVICE");

        connection_info.connections_by_machine = vec![
            OracleConnectionsByMachine {
                machine_name: "app-server-01".to_string(),
                total_connections: 50,
                active_connections: 40,
                inactive_connections: 10,
                unique_users: 5,
                avg_pga_per_connection: 25_000_000,
                earliest_logon: DateTimeWrapper::from(Utc::now() - chrono::Duration::hours(8)),
                latest_logon: DateTimeWrapper::from(Utc::now() - chrono::Duration::minutes(5)),
            },
            OracleConnectionsByMachine {
                machine_name: "app-server-02".to_string(),
                total_connections: 35,
                active_connections: 30,
                inactive_connections: 5,
                unique_users: 3,
                avg_pga_per_connection: 22_000_000,
                earliest_logon: DateTimeWrapper::from(Utc::now() - chrono::Duration::hours(6)),
                latest_logon: DateTimeWrapper::from(Utc::now() - chrono::Duration::minutes(2)),
            },
        ];
        assert_eq!(connection_info.busiest_machine().unwrap().machine_name, "app-server-01");

        connection_info.sessions_waiting = 5;
        connection_info.sessions_blocking = 2;
        connection_info.pga_over_allocation_count = 1;
        assert!(connection_info.has_resource_contention());
    }
}
