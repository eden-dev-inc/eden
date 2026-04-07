//! Async task functions for Eden API operations.
//!
//! Each function is spawned via tokio and communicates back to the TUI through
//! an mpsc channel of ApiEvent messages.

use std::time::Duration;
use tokio::sync::mpsc;

use crate::eden_api::{EdenApiClient, eden_admin_password, eden_admin_user};
use crate::events::ApiEvent;
use crate::migration::{
    ApiCallStatus, CanaryState, MigrationMode, MigrationStatus, SetupStep, parse_migration_status,
};

pub async fn run_migration_setup(
    tx: mpsc::Sender<ApiEvent>,
    source_url: String,
    dest_url: String,
    source_port: String,
    dest_port: String,
    org_id: String,
    api_base: String,
    mode: MigrationMode,
    canary_state: CanaryState,
) {
    let client = EdenApiClient::new(org_id, api_base);
    let admin_user = eden_admin_user();
    let admin_password = eden_admin_password();

    // API call indices match the order in MigrationState::new()
    const CREATE_ORG: usize = 0;
    const LOGIN: usize = 1;
    const CREATE_SOURCE_EP: usize = 2;
    const CREATE_DEST_EP: usize = 3;
    const CREATE_INTERLAY: usize = 4;
    const CREATE_MIGRATION: usize = 5;
    const ADD_INTERLAY: usize = 6;

    // Step 1: Create organization
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingOrganization))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_ORG,
            status: ApiCallStatus::InProgress,
        })
        .await;

    match client
        .create_organization(&admin_user, &admin_password)
        .await
    {
        Ok(_) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_ORG,
                    status: ApiCallStatus::Success,
                })
                .await;
        }
        Err(e) => {
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_ORG,
                        status: ApiCallStatus::Skipped,
                    })
                    .await;
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_ORG,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    }

    // Step 2: Login
    let _ = tx.send(ApiEvent::SetupProgress(SetupStep::LoggingIn)).await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: LOGIN,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let token = match client.login(&admin_user, &admin_password).await {
        Ok(t) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: LOGIN,
                    status: ApiCallStatus::Success,
                })
                .await;
            t
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: LOGIN,
                    status: ApiCallStatus::Failed(e.clone()),
                })
                .await;
            let _ = tx.send(ApiEvent::SetupFailed(e)).await;
            return;
        }
    };

    let client = client.with_auth(token.clone());

    // Step 3: Create source endpoint
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingSourceEndpoint))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_SOURCE_EP,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let source_ep_id = format!("pg_source_{}", source_port);
    let source_ep = match client.create_endpoint(&source_ep_id, &source_url).await {
        Ok(ep) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_SOURCE_EP,
                    status: ApiCallStatus::Success,
                })
                .await;
            ep
        }
        Err(e) => {
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
                match client.get_endpoint(&source_ep_id).await {
                    Ok(ep) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_SOURCE_EP,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        ep
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_SOURCE_EP,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_SOURCE_EP,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };
    if let Err(e) = client
        .grant_endpoint_data_access(&source_ep_id, &admin_user, "rwx")
        .await
    {
        let _ = tx
            .send(ApiEvent::ApiCallUpdate {
                index: CREATE_SOURCE_EP,
                status: ApiCallStatus::Failed(e.clone()),
            })
            .await;
        let _ = tx.send(ApiEvent::SetupFailed(e)).await;
        return;
    }

    // Step 4: Create destination endpoint
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingDestEndpoint))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_DEST_EP,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let dest_ep_id = format!("pg_dest_{}", dest_port);
    let dest_ep = match client.create_endpoint(&dest_ep_id, &dest_url).await {
        Ok(ep) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_DEST_EP,
                    status: ApiCallStatus::Success,
                })
                .await;
            ep
        }
        Err(e) => {
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
                match client.get_endpoint(&dest_ep_id).await {
                    Ok(ep) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_DEST_EP,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        ep
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_DEST_EP,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_DEST_EP,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };
    if let Err(e) = client
        .grant_endpoint_data_access(&dest_ep_id, &admin_user, "rwx")
        .await
    {
        let _ = tx
            .send(ApiEvent::ApiCallUpdate {
                index: CREATE_DEST_EP,
                status: ApiCallStatus::Failed(e.clone()),
            })
            .await;
        let _ = tx.send(ApiEvent::SetupFailed(e)).await;
        return;
    }

    // Step 5: Create interlay
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingInterlay))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_INTERLAY,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let interlay_id = format!("pg_interlay_{}_{}", source_port, dest_port);
    let interlay = match client
        .create_interlay(&interlay_id, &source_ep.uuid, 5435)
        .await
    {
        Ok(il) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_INTERLAY,
                    status: ApiCallStatus::Success,
                })
                .await;
            il
        }
        Err(e) => {
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
                match client.get_interlay(&interlay_id).await {
                    Ok(il) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_INTERLAY,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        il
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_INTERLAY,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_INTERLAY,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };

    // Step 6: Create migration
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::CreatingMigration))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: CREATE_MIGRATION,
            status: ApiCallStatus::InProgress,
        })
        .await;

    let mode_suffix = match mode {
        MigrationMode::BigBang => "bb",
        MigrationMode::Canary => "canary",
        MigrationMode::BlueGreen => "bg",
    };
    let migration_id = format!("pg_migration_{}_{}_{}", source_port, dest_port, mode_suffix);
    let migration = match client
        .create_migration(&migration_id, mode, &canary_state)
        .await
    {
        Ok(m) => {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: CREATE_MIGRATION,
                    status: ApiCallStatus::Success,
                })
                .await;
            m
        }
        Err(e) => {
            if e.contains("409") || e.contains("already exists") || e.contains("Conflict") {
                let _ = tx
                    .send(ApiEvent::DebugLog(
                        "Migration exists, fetching current state...".to_string(),
                    ))
                    .await;
                match client.get_migration(&migration_id).await {
                    Ok(m) => {
                        let _ = tx
                            .send(ApiEvent::DebugLog(format!(
                                "Existing migration: id={}, status={:?}",
                                m.id, m.status
                            )))
                            .await;
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_MIGRATION,
                                status: ApiCallStatus::Skipped,
                            })
                            .await;
                        m
                    }
                    Err(get_err) => {
                        let _ = tx
                            .send(ApiEvent::ApiCallUpdate {
                                index: CREATE_MIGRATION,
                                status: ApiCallStatus::Failed(get_err.clone()),
                            })
                            .await;
                        let _ = tx.send(ApiEvent::SetupFailed(get_err)).await;
                        return;
                    }
                }
            } else {
                let _ = tx
                    .send(ApiEvent::ApiCallUpdate {
                        index: CREATE_MIGRATION,
                        status: ApiCallStatus::Failed(e.clone()),
                    })
                    .await;
                let _ = tx.send(ApiEvent::SetupFailed(e)).await;
                return;
            }
        }
    };

    // Step 7: Add interlay to migration
    let _ = tx
        .send(ApiEvent::SetupProgress(SetupStep::AddingInterlay))
        .await;
    let _ = tx
        .send(ApiEvent::ApiCallUpdate {
            index: ADD_INTERLAY,
            status: ApiCallStatus::InProgress,
        })
        .await;

    if let Err(e) = client
        .add_interlay_to_migration(
            &migration.id,
            &interlay.id,
            &dest_ep.uuid,
            mode,
            &canary_state,
        )
        .await
    {
        if e.contains("409")
            || e.contains("already exists")
            || e.contains("Conflict")
            || e.contains("already has an active migration")
        {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: ADD_INTERLAY,
                    status: ApiCallStatus::Skipped,
                })
                .await;
        } else {
            let _ = tx
                .send(ApiEvent::ApiCallUpdate {
                    index: ADD_INTERLAY,
                    status: ApiCallStatus::Failed(e.clone()),
                })
                .await;
            let _ = tx.send(ApiEvent::SetupFailed(e)).await;
            return;
        }
    } else {
        let _ = tx
            .send(ApiEvent::ApiCallUpdate {
                index: ADD_INTERLAY,
                status: ApiCallStatus::Success,
            })
            .await;
    }

    // Setup complete
    let _ = tx
        .send(ApiEvent::SetupComplete {
            auth_token: token.clone(),
            source_endpoint_id: source_ep.id,
            dest_endpoint_id: dest_ep.id,
            interlay_id: interlay.id,
            migration_id: migration.id.clone(),
        })
        .await;
    let _ = tx.send(ApiEvent::SetupProgress(SetupStep::Ready)).await;

    // Fetch current migration status
    let _ = tx
        .send(ApiEvent::DebugLog(
            "Fetching current migration status...".to_string(),
        ))
        .await;
    match client.get_migration(&migration.id).await {
        Ok(data) => {
            let status = parse_migration_status(data.status.as_deref());
            let _ = tx
                .send(ApiEvent::DebugLog(format!(
                    "Current migration status: {:?} (from API: {:?})",
                    status, data.status
                )))
                .await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Failed to fetch status: {}", e)))
                .await;
            let status = parse_migration_status(migration.status.as_deref());
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
    }
}

pub async fn trigger_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    match client.trigger_migration(&migration_id).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationTriggered).await;

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                match client.get_migration(&migration_id).await {
                    Ok(data) => {
                        let status = parse_migration_status(data.status.as_deref());
                        let _ = tx
                            .send(ApiEvent::MigrationStatusUpdate {
                                status: status.clone(),
                                force: false,
                            })
                            .await;

                        match status {
                            MigrationStatus::Completed
                            | MigrationStatus::Failed
                            | MigrationStatus::RolledBack => break,
                            _ => {}
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(ApiEvent::MigrationError(e)).await;
                        break;
                    }
                }
            }
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::MigrationError(e)).await;
        }
    }
}

pub async fn refresh_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/refresh",
            migration_id
        )))
        .await;
    if let Err(e) = client.refresh_migration(&migration_id).await {
        let _ = tx
            .send(ApiEvent::DebugLog(format!("Refresh failed: {}", e)))
            .await;
        let _ = tx.send(ApiEvent::MigrationError(e)).await;
        return;
    }

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "GET /migrations/{}",
            migration_id
        )))
        .await;
    match client.get_migration(&migration_id).await {
        Ok(data) => {
            let status = parse_migration_status(data.status.as_deref());
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Status: {:?}", status)))
                .await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Get failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationError(e)).await;
        }
    }
}

pub async fn update_traffic_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
    new_percentage: f64,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let reason = format!("Adjusting canary traffic to {:.0}%", new_percentage * 100.0);
    match client
        .update_traffic_split(&migration_id, new_percentage, &reason)
        .await
    {
        Ok(response) => {
            let _ = tx
                .send(ApiEvent::TrafficUpdated {
                    old_percentage: response.old_percentage,
                    new_percentage: response.new_percentage,
                })
                .await;
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::TrafficUpdateFailed(e)).await;
        }
    }
}

pub async fn complete_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    match client.complete_migration(&migration_id, None).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationCompleted).await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status: MigrationStatus::Completed,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::MigrationCompleteFailed(e)).await;
        }
    }
}

pub async fn toggle_environment_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
    current_active_is_new: bool,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let activate_new = !current_active_is_new;
    match client
        .toggle_environment(&migration_id, activate_new, None)
        .await
    {
        Ok(response) => {
            let _ = tx
                .send(ApiEvent::EnvironmentToggled {
                    previous_active: response.previous_active,
                    new_active: response.new_active,
                })
                .await;
        }
        Err(e) => {
            let _ = tx.send(ApiEvent::EnvironmentToggleFailed(e)).await;
        }
    }
}

pub async fn rollback_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    interlay_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/interlay/{}/rollback",
            migration_id, interlay_id
        )))
        .await;

    match client
        .rollback_interlay(&migration_id, &interlay_id, None)
        .await
    {
        Ok(response) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!(
                    "Rollback response: status={}, interlay={}",
                    response.status, response.interlay_id
                )))
                .await;
            let _ = tx.send(ApiEvent::MigrationRolledBack).await;
            let status = parse_migration_status(Some(&response.status));
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Rollback failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationRollbackFailed(e)).await;
        }
    }
}

pub async fn pause_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/pause",
            migration_id
        )))
        .await;

    match client.pause_migration(&migration_id, None).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationPaused).await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status: MigrationStatus::Paused,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Pause failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationPauseFailed(e)).await;
        }
    }
}

pub async fn resume_migration_task(
    tx: mpsc::Sender<ApiEvent>,
    auth_token: String,
    org_id: String,
    migration_id: String,
    api_base: String,
) {
    let client = EdenApiClient::new(org_id, api_base).with_auth(auth_token);

    let _ = tx
        .send(ApiEvent::DebugLog(format!(
            "POST /migrations/{}/resume",
            migration_id
        )))
        .await;

    match client.resume_migration(&migration_id, None).await {
        Ok(_) => {
            let _ = tx.send(ApiEvent::MigrationResumed).await;
            let _ = tx
                .send(ApiEvent::MigrationStatusUpdate {
                    status: MigrationStatus::Running,
                    force: true,
                })
                .await;
        }
        Err(e) => {
            let _ = tx
                .send(ApiEvent::DebugLog(format!("Resume failed: {}", e)))
                .await;
            let _ = tx.send(ApiEvent::MigrationResumeFailed(e)).await;
        }
    }
}
