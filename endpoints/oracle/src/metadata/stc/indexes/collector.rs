use super::*;
use function_name::named;
impl OracleIndexInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(20);

    async fn run_optional_rows(requests: &HashMap<String, QueryInput>, name: &str, context: OracleAsync) -> ResultEP<Option<Vec<Row>>> {
        let Some(query) = requests.get(name) else {
            return Ok(None);
        };

        run_optional_query(query, context, Self::QUERY_TIMEOUT, name, Ok).await
    }

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        _context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut index_info = self.clone();
        index_info.collection_timestamp = DateTimeWrapper::from(Utc::now());
        Ok(index_info)
    }

    #[named]
    pub async fn collect_all_indexes(context: OracleAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<OracleIndexInfo>> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let template = OracleIndexInfo::default();
        let requests = template.request();
        let mut indexes = Vec::new();

        let basic_info_rows = run_named_query(&requests, "index_basic_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        let mut index_map: HashMap<(String, String), OracleIndexInfo> = HashMap::new();

        for row in basic_info_rows {
            let owner = row.get_string("owner")?;
            let index_name = row.get_string("index_name")?;
            let key = (owner.clone(), index_name.clone());

            let index_info = OracleIndexInfo {
                owner,
                index_name,
                table_name: row.get_string("table_name")?,
                tablespace_name: row.get_string("tablespace_name")?,
                index_type: row.get_string("index_type")?,
                uniqueness: row.get_string("uniqueness")?,
                status: row.get_string("status")?,
                visibility: row.get_string("visibility")?,
                created: row.get_datetime("created")?,
                last_analyzed: row.get_opt_datetime("last_analyzed")?,
                compression: row.get_string("compression")?,
                prefix_length: row.get_u32("prefix_length")?,
                column_count: row.get_u32("column_count")?,
                column_names: row.get_string("column_names")?,
                collection_timestamp: DateTimeWrapper::from(Utc::now()),
                ..Default::default()
            };

            index_map.insert(key, index_info);
        }

        // Merge statistics data
        let statistics_rows = run_named_query(&requests, "index_statistics", context.clone(), Self::QUERY_TIMEOUT).await?;

        for row in statistics_rows {
            let owner = row.get_string("owner")?;
            let index_name = row.get_string("index_name")?;
            let key = (owner, index_name);

            if let Some(index_info) = index_map.get_mut(&key) {
                index_info.leaf_blocks = row.get_u64("leaf_blocks")?;
                index_info.distinct_keys = row.get_u64("distinct_keys")?;
                index_info.avg_leaf_blocks_per_key = row.get_f64("avg_leaf_blocks_per_key")?;
                index_info.avg_data_blocks_per_key = row.get_f64("avg_data_blocks_per_key")?;
                index_info.clustering_factor = row.get_u64("clustering_factor")?;
                index_info.num_rows = row.get_u64("num_rows")?;
                index_info.sample_size = row.get_u64("sample_size")?;
                index_info.blevel = row.get_u32("blevel")?;
                index_info.selectivity = row.get_f64("selectivity")?;
            }
        }

        // Merge usage data
        if let Some(usage_rows) = Self::run_optional_rows(&requests, "index_usage", context.clone()).await? {
            for row in usage_rows {
                let owner = row.get_string("owner")?;
                let index_name = row.get_string("index_name")?;
                let key = (owner, index_name);

                if let Some(index_info) = index_map.get_mut(&key) {
                    index_info.total_access_count = row.get_u64("total_access_count")?;
                    index_info.last_used = row.get_opt_datetime("last_used")?;
                    index_info.usage_score = row.get_f64("usage_score")?;
                }
            }
        }

        // Merge storage data
        let storage_rows = run_named_query(&requests, "index_storage", context.clone(), Self::QUERY_TIMEOUT).await?;

        for row in storage_rows {
            let owner = row.get_string("owner")?;
            let index_name = row.get_string("index_name")?;
            let key = (owner, index_name);

            if let Some(index_info) = index_map.get_mut(&key) {
                index_info.index_size_bytes = row.get_u64("index_size_bytes")?;
                index_info.extents = row.get_u32("extents")?;
                index_info.initial_extent = row.get_u64("initial_extent")?;
                index_info.next_extent = row.get_u64("next_extent")?;
                index_info.max_extents = row.get_u32("max_extents")?;
                index_info.pct_increase = row.get_u32("pct_increase")?;
                index_info.pct_free = row.get_u32("pct_free")?;
            }
        }

        // Merge health data
        let health_rows = run_named_query(&requests, "index_health", context.clone(), Self::QUERY_TIMEOUT).await?;

        for row in health_rows {
            let owner = row.get_string("owner")?;
            let index_name = row.get_string("index_name")?;
            let key = (owner, index_name);

            if let Some(index_info) = index_map.get_mut(&key) {
                index_info.fragmentation_level = row.get_f64("fragmentation_level")?;
                index_info.needs_rebuild = row.get_u32("needs_rebuild")? == 1;
                index_info.rebuild_reason = row.get_opt_string("rebuild_reason")?;
                index_info.stale_statistics = row.get_u32("stale_statistics")? == 1;

                // Determine if index is a drop candidate (unused for extended period)
                index_info.drop_candidate =
                    index_info.total_access_count == 0 && index_info.last_used.is_none() && index_info.uniqueness != "UNIQUE"; // Don't suggest dropping unique indexes
            }
        }

        // Merge partition data
        if let Some(partition_rows) = Self::run_optional_rows(&requests, "index_partitions", context.clone()).await? {
            for row in partition_rows {
                let owner = row.get_string("owner")?;
                let index_name = row.get_string("index_name")?;
                let key = (owner, index_name);

                if let Some(index_info) = index_map.get_mut(&key) {
                    index_info.is_partitioned = true;
                    index_info.partition_count = row.get_u32("partition_count")?;
                    index_info.partitioning_type = row.get_opt_string("partitioning_type")?;
                }
            }
        }

        for (_, index_info) in index_map.iter_mut() {
            index_info.calculate_derived_metrics();
        }

        for (_, index_info) in index_map {
            indexes.push(index_info);
        }

        indexes.sort_by(|a, b| a.owner.cmp(&b.owner).then_with(|| a.index_name.cmp(&b.index_name)));

        Ok(indexes)
    }
    fn calculate_derived_metrics(&mut self) {
        if self.num_rows > 0 && self.leaf_blocks > 0 {
            self.efficiency_ratio = (self.num_rows as f64 / self.leaf_blocks as f64) * 100.0;
        }

        // Estimate I/O cost based on b-tree level and clustering factor
        self.avg_io_cost = self.blevel as f64
            + if self.num_rows > 0 && self.clustering_factor > 0 {
                self.clustering_factor as f64 / self.num_rows as f64
            } else {
                1.0
            };

        // Estimate space savings from rebuild using fragmentation ratio
        if self.needs_rebuild && self.index_size_bytes > 0 {
            self.rebuild_space_savings = (self.index_size_bytes as f64 * (self.fragmentation_level / 100.0)) as u64;
        }

        if self.total_access_count == 0 && self.last_used.is_none() {
            self.drop_candidate = true;
        }
    }
}
