#[cfg(feature = "elasticache-aws-integration")]
mod integration {
    use ep_elasticache::api::control_plane::{DescribeCacheClustersInput, ElasticacheControlPlaneClient};

    // To run:
    // cargo test -p ep-elasticache --features elasticache-aws-integration --test control_plane_integration -- --ignored
    // Requires AWS credentials + region in the environment (or an AWS profile configured).
    #[tokio::test]
    #[ignore = "requires AWS credentials and ElastiCache control-plane permissions"]
    async fn describe_cache_clusters_smoke() {
        let client = ElasticacheControlPlaneClient::from_env().await.expect("load AWS SDK config from environment");

        let input = DescribeCacheClustersInput {
            cache_cluster_id: None,
            marker: None,
            max_records: Some(20),
            show_cache_node_info: Some(false),
            show_cache_clusters_not_in_replication_groups: None,
        };

        let _ = client.describe_cache_clusters(input).await.expect("describe cache clusters");
    }
}
