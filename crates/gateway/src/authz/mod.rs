//! Gateway trusted-proxy authorization.
//!
//! This module holds the `krabka_authz::Authorizer` and an `ArcSwap`'d
//! `AclCache`. A background task refreshes that cache by polling the broker's
//! `DescribeAcls`.

pub mod auth_layer;
use std::sync::Arc;

use arc_swap::ArcSwap;
pub use auth_layer::{BearerValidator, anonymous, resolve_principal};
use krabka_authz::{AclCache, Authorizer};
use krabka_client_admin::AdminClient;
use krabka_units::prelude::*;
use tokio_util::sync::CancellationToken;

pub struct GatewayAuthz {
    authorizer: Arc<dyn Authorizer>,
    cache: ArcSwap<AclCache>,
}

impl GatewayAuthz {
    #[must_use]
    pub fn new(authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            authorizer,
            cache: ArcSwap::from_pointee(AclCache::default()),
        }
    }

    #[must_use]
    pub fn authorizer(&self) -> &Arc<dyn Authorizer> {
        &self.authorizer
    }

    #[must_use]
    pub fn cache(&self) -> arc_swap::Guard<Arc<AclCache>> {
        self.cache.load()
    }

    /// Poll `DescribeAcls` into the cache until `shutdown`. On an error, log it
    /// and keep the previous snapshot.
    pub async fn run_acl_refresh(
        self: Arc<Self>,
        bootstrap: String,
        refresh: Time,
        shutdown: CancellationToken,
        security: Option<krabka_client_core::security::ClientSecurity>,
    ) {
        self.run_acl_refresh_with_policy(
            bootstrap,
            refresh,
            shutdown,
            security,
            crate::config::GatewayRuntimeConfig::default(),
        )
        .await;
    }

    /// Refresh ACLs with the deployment's client resource policy.
    pub async fn run_acl_refresh_with_policy(
        self: Arc<Self>,
        bootstrap: String,
        refresh: Time,
        shutdown: CancellationToken,
        security: Option<krabka_client_core::security::ClientSecurity>,
        policy: crate::config::GatewayRuntimeConfig,
    ) {
        let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
        loop {
            match Self::fetch(&addrs, security.clone(), &policy).await {
                Ok(entries) => self.cache.store(Arc::new(AclCache::new(entries))),
                Err(e) => {
                    tracing::warn!(error = %e, "ACL refresh failed; keeping prior snapshot");
                }
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(refresh.to_std()) => {}
            }
        }
    }

    async fn fetch(
        addrs: &[String],
        security: Option<krabka_client_core::security::ClientSecurity>,
        policy: &crate::config::GatewayRuntimeConfig,
    ) -> Result<Vec<krabka_metadata::AclEntry>, crate::error::GatewayError> {
        let mut admin = AdminClient::connect_with_options(
            addrs,
            krabka_client_core::ConnectionOptions {
                dns_timeout: krabka_client_core::ClientDnsTimeout::default(),
                connect_timeout: krabka_units::secs(5),
                request_timeout: krabka_units::secs(30),
                client_id: "krabka-operator".to_owned(),
                dispatch_queue_capacity: policy.client_dispatch_queue_capacity,
                frame_max: policy.client_frame_max,
                security: security.map(Box::new),
            },
        )
        .await
        .map_err(|e| crate::error::GatewayError::Other(format!("acl admin connect: {e}")))?;
        let entries = admin
            .describe_acls(&krabka_client_admin::AclEntryFilter::default())
            .await
            .map_err(|e| crate::error::GatewayError::Other(format!("describe_acls: {e}")))?;
        Ok(entries.into_iter().map(acl_entry_from_admin).collect())
    }
}

/// Convert a `krabka_client_admin::AclEntry` into a `krabka_metadata::AclEntry`.
///
/// The two types have the same field names and the same enum variant sets. The
/// admin crate keeps local copies to avoid a broker dependency.
fn acl_entry_from_admin(e: krabka_client_admin::AclEntry) -> krabka_metadata::AclEntry {
    use krabka_client_admin::{
        AclOperation as AO, PatternType as PT, PermissionType as Perm, ResourceType as RT,
    };
    use krabka_metadata::{
        AclEntry as ME, AclOperation as MAO, PatternType as MPT, PermissionType as MPerm,
        ResourceType as MRT,
    };

    let resource_type = match e.resource_type {
        RT::Topic => MRT::Topic,
        RT::Group => MRT::Group,
        RT::Cluster => MRT::Cluster,
        RT::TransactionalId => MRT::TransactionalId,
    };
    let pattern_type = match e.pattern_type {
        PT::Literal => MPT::Literal,
        PT::Prefixed => MPT::Prefixed,
    };
    let operation = match e.operation {
        AO::All => MAO::All,
        AO::Read => MAO::Read,
        AO::Write => MAO::Write,
        AO::Create => MAO::Create,
        AO::Delete => MAO::Delete,
        AO::Alter => MAO::Alter,
        AO::Describe => MAO::Describe,
        AO::ClusterAction => MAO::ClusterAction,
        AO::DescribeConfigs => MAO::DescribeConfigs,
        AO::AlterConfigs => MAO::AlterConfigs,
        AO::IdempotentWrite => MAO::IdempotentWrite,
        AO::TwoPhaseCommit => MAO::TwoPhaseCommit,
    };
    let permission_type = match e.permission_type {
        Perm::Allow => MPerm::Allow,
        Perm::Deny => MPerm::Deny,
    };

    ME {
        resource_type,
        resource_name: e.resource_name,
        pattern_type,
        principal: e.principal,
        host: e.host,
        operation,
        permission_type,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_entry_from_admin_maps_two_phase_commit() {
        // KIP-939: the admin→metadata ACL conversion must carry TwoPhaseCommit
        // through (the operation a 2PC grant on a TransactionalId uses).
        let admin = krabka_client_admin::AclEntry {
            resource_type: krabka_client_admin::ResourceType::TransactionalId,
            resource_name: "my-txn".into(),
            pattern_type: krabka_client_admin::PatternType::Literal,
            principal: "User:flink".into(),
            host: "*".into(),
            operation: krabka_client_admin::AclOperation::TwoPhaseCommit,
            permission_type: krabka_client_admin::PermissionType::Allow,
        };
        let meta = acl_entry_from_admin(admin);
        assert2::assert!(
            meta == krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::TransactionalId,
                resource_name: "my-txn".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:flink".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::TwoPhaseCommit,
                permission_type: krabka_metadata::PermissionType::Allow,
            }
        );
    }
}
