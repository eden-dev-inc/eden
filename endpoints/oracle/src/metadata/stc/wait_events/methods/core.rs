use super::*;
use function_name::named;

impl OracleWaitEventInfo {
    pub(crate) const HIGH_WAIT_TIME_THRESHOLD: f64 = 60.0;
    pub(crate) const HIGH_SESSION_WAIT_THRESHOLD: u64 = 10;
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    pub(crate) const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut wait_info = OracleWaitEventInfo::default();
        let requests = self.request();

        if let Some(row) = run_single_row(&requests, "wait_event_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            wait_info.total_wait_events = row.get_u64("total_wait_events")?;
            wait_info.total_time_waited_us = row.get_u64("total_time_waited_us")?;
            wait_info.total_waits = row.get_u64("total_waits")?;
            wait_info.avg_wait_time_us = row.get_f64("avg_wait_time_us")?;
            wait_info.max_wait_time_us = row.get_u64("max_wait_time_us")?;
        }

        let wait_class_rows = run_named_query(&requests, "wait_class_summary", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::process_wait_class_data(&mut wait_info, wait_class_rows)?;

        if let Some(row) = run_single_row(&requests, "session_waits", context.clone(), Self::QUERY_TIMEOUT).await? {
            wait_info.sessions_waiting = row.get_u64("sessions_waiting")?;
        }

        if let Some(row) = run_single_row(&requests, "db_time_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            wait_info.db_time_us = row.get_u64("db_time_us")?;
            let cpu_time_us = row.get_u64("cpu_time_us")?;
            if wait_info.db_time_us > 0 {
                wait_info.cpu_time_percent = ratio_percentage(cpu_time_us, wait_info.db_time_us);
                wait_info.wait_time_percent = ratio_percentage(wait_info.total_time_waited_us, wait_info.db_time_us);
            }
        }

        if let Some(row) = run_single_row(&requests, "background_waits", context.clone(), Self::QUERY_TIMEOUT).await? {
            wait_info.background_wait_events = row.get_u64("background_wait_events")?;
        }

        wait_info.wait_health_score = Self::calculate_health_score(&wait_info);
        wait_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&wait_info, context).await?;

        Ok(wait_info)
    }

    pub(crate) fn process_wait_class_data(wait_info: &mut OracleWaitEventInfo, rows: Vec<Row>) -> ResultEP<()> {
        let mut top_wait_time = 0u64;

        for row in rows {
            let wait_class = row.get_string("wait_class")?;
            let total_waits = row.get_u64("total_waits")?;
            let time_waited_us = row.get_u64("time_waited_us")?;

            if time_waited_us > top_wait_time {
                top_wait_time = time_waited_us;
                wait_info.top_wait_class = wait_class.clone();
                if wait_info.total_time_waited_us > 0 {
                    wait_info.top_wait_class_percent = ratio_percentage(time_waited_us, wait_info.total_time_waited_us);
                }
            }

            match wait_class.as_str() {
                "User I/O" | "System I/O" => {
                    wait_info.io_waits += total_waits;
                    wait_info.io_wait_time_us += time_waited_us;
                }
                "Concurrency" => {
                    wait_info.concurrency_waits += total_waits;
                    wait_info.concurrency_wait_time_us += time_waited_us;
                }
                "Application" => {
                    wait_info.application_waits += total_waits;
                    wait_info.application_wait_time_us += time_waited_us;
                }
                "Configuration" => {
                    wait_info.configuration_waits += total_waits;
                    wait_info.configuration_wait_time_us += time_waited_us;
                }
                "Administrative" => {
                    wait_info.administrative_waits += total_waits;
                    wait_info.administrative_wait_time_us += time_waited_us;
                }
                "Network" => {
                    wait_info.network_waits += total_waits;
                    wait_info.network_wait_time_us += time_waited_us;
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub(crate) fn calculate_health_score(wait_info: &OracleWaitEventInfo) -> f64 {
        let mut score: f64 = 100.0;

        if wait_info.wait_time_percent > 80.0 {
            score -= 40.0;
        } else if wait_info.wait_time_percent > 60.0 {
            score -= 30.0;
        } else if wait_info.wait_time_percent > 40.0 {
            score -= 20.0;
        }

        if wait_info.sessions_waiting > 50 {
            score -= 25.0;
        } else if wait_info.sessions_waiting > 20 {
            score -= 15.0;
        } else if wait_info.sessions_waiting > 10 {
            score -= 10.0;
        }

        if wait_info.io_wait_percentage() > 60.0 {
            score -= 20.0;
        } else if wait_info.io_wait_percentage() > 40.0 {
            score -= 15.0;
        } else if wait_info.io_wait_percentage() > 25.0 {
            score -= 10.0;
        }

        if wait_info.concurrency_wait_percentage() > 30.0 {
            score -= 15.0;
        } else if wait_info.concurrency_wait_percentage() > 15.0 {
            score -= 10.0;
        } else if wait_info.concurrency_wait_percentage() > 10.0 {
            score -= 5.0;
        }

        score.clamp(0.0_f64, 100.0_f64)
    }
}
