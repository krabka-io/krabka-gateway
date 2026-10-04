//! GitHub webhook firehose: gateway ingress → Streams dispatch → JSON topic.
//!
//! See `demo/github-firehose/README.md` for the signed ingress configuration.
#![recursion_limit = "512"]

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use clap::Parser;
use krabka_client_admin::{AdminClient, CreateTopicSpec, TopicMutationOptions};
use krabka_client_streams::{BuiltTopology, KafkaStreams, StreamsBuilder};
use krabka_units::prelude::*;
use octocrab::models::webhook_events::{WebhookEvent, WebhookEventPayload, WebhookEventType};
use serde_json::{Value, json};

const INPUT_TOPIC: &str = "github-webhooks";
const OUTPUT_TOPIC: &str = "github-firehose";
const APPLICATION_ID: &str = "github-firehose-example";

#[derive(Parser)]
struct Args {
    #[arg(
        long,
        env = "KRABKA_BOOTSTRAP_SERVERS",
        default_value = "127.0.0.1:9092"
    )]
    bootstrap_servers: String,
    /// Create the two demo topics and exit. Run once before starting the demo.
    #[arg(long)]
    create_topics: bool,
}

// Dispatch Octocrab's enum after parsing GitHub's event header. The body is
// decoded into WebhookEvent and its per-event payload models in dispatch().
fn event_kind(event: &WebhookEventType) -> Option<&'static str> {
    match event {
        WebhookEventType::BranchProtectionRule => Some("Branch protection rule"),
        WebhookEventType::CheckRun => Some("Check run"),
        WebhookEventType::CheckSuite => Some("Check suite"),
        WebhookEventType::CodeScanningAlert => Some("Code scanning alert"),
        WebhookEventType::CommitComment => Some("Commit comment"),
        WebhookEventType::Create => Some("Branch or tag created"),
        WebhookEventType::Delete => Some("Branch or tag deleted"),
        WebhookEventType::DependabotAlert => Some("Dependabot alert"),
        WebhookEventType::DeployKey => Some("Deploy key"),
        WebhookEventType::Deployment => Some("Deployment"),
        WebhookEventType::DeploymentProtectionRule => Some("Deployment protection rule"),
        WebhookEventType::DeploymentStatus => Some("Deployment status"),
        WebhookEventType::Discussion => Some("Discussion"),
        WebhookEventType::DiscussionComment => Some("Discussion comment"),
        WebhookEventType::Fork => Some("Fork"),
        WebhookEventType::GithubAppAuthorization => Some("GitHub App authorization"),
        WebhookEventType::Gollum => Some("Wiki pages"),
        WebhookEventType::Installation => Some("App installation"),
        WebhookEventType::InstallationRepositories => Some("App installation repositories"),
        WebhookEventType::InstallationTarget => Some("App installation target"),
        WebhookEventType::IssueComment => Some("Issue comment"),
        WebhookEventType::Issues => Some("Issue"),
        WebhookEventType::Label => Some("Label"),
        WebhookEventType::MarketplacePurchase => Some("Marketplace purchase"),
        WebhookEventType::Member => Some("Repository collaborator"),
        WebhookEventType::Membership => Some("Team membership"),
        WebhookEventType::MergeGroup => Some("Merge group"),
        WebhookEventType::Meta => Some("Webhook metadata"),
        WebhookEventType::Milestone => Some("Milestone"),
        WebhookEventType::OrgBlock => Some("Organization block"),
        WebhookEventType::Organization => Some("Organization"),
        WebhookEventType::Package => Some("Package"),
        WebhookEventType::PageBuild => Some("Pages build"),
        WebhookEventType::PersonalAccessTokenRequest => Some("Personal access token request"),
        WebhookEventType::Ping => Some("Webhook ping"),
        WebhookEventType::Project => Some("Classic project"),
        WebhookEventType::ProjectCard => Some("Classic project card"),
        WebhookEventType::ProjectColumn => Some("Classic project column"),
        WebhookEventType::ProjectsV2 => Some("Project"),
        WebhookEventType::ProjectsV2Item => Some("Project item"),
        WebhookEventType::Public => Some("Repository made public"),
        WebhookEventType::PullRequest => Some("Pull request"),
        WebhookEventType::PullRequestReview => Some("Pull request review"),
        WebhookEventType::PullRequestReviewComment => Some("Pull request review comment"),
        WebhookEventType::PullRequestReviewThread => Some("Pull request review thread"),
        WebhookEventType::Push => Some("Push"),
        WebhookEventType::RegistryPackage => Some("Registry package"),
        WebhookEventType::Release => Some("Release"),
        WebhookEventType::Repository => Some("Repository"),
        WebhookEventType::RepositoryAdvisory => Some("Repository advisory"),
        WebhookEventType::RepositoryDispatch => Some("Repository dispatch"),
        WebhookEventType::RepositoryImport => Some("Repository import"),
        WebhookEventType::RepositoryVulnerabilityAlert => Some("Repository vulnerability alert"),
        WebhookEventType::SecretScanningAlert => Some("Secret scanning alert"),
        WebhookEventType::SecretScanningAlertLocation => Some("Secret scanning alert location"),
        WebhookEventType::SecurityAdvisory => Some("Security advisory"),
        WebhookEventType::SecurityAndAnalysis => Some("Security and analysis settings"),
        WebhookEventType::Sponsorship => Some("Sponsorship"),
        WebhookEventType::Star => Some("Star"),
        WebhookEventType::Status => Some("Commit status"),
        WebhookEventType::Team => Some("Team"),
        WebhookEventType::TeamAdd => Some("Repository added to team"),
        WebhookEventType::Watch => Some("Watch"),
        WebhookEventType::WorkflowDispatch => Some("Workflow dispatch"),
        WebhookEventType::WorkflowJob => Some("Workflow job"),
        WebhookEventType::WorkflowRun => Some("Workflow run"),
        WebhookEventType::Schedule => Some("Scheduled workflow"),
        // These catalog entries are not modeled by Octocrab 0.54.2 yet.
        WebhookEventType::Unknown(name) => match name.as_str() {
            "branch_protection_configuration" => Some("Branch protection configuration"),
            "custom_property" => Some("Custom property"),
            "custom_property_values" => Some("Custom property values"),
            "deployment_review" => Some("Deployment review"),
            "issue_dependencies" => Some("Issue dependencies"),
            "issue_relates_to" => Some("Related issues"),
            "projects_v2_status_update" => Some("Project status update"),
            "repository_ruleset" => Some("Repository ruleset"),
            "secret_scanning_scan" => Some("Secret scanning scan"),
            "sub_issues" => Some("Sub-issues"),
            _ => None,
        },
        // Octocrab marks the enum non-exhaustive.
        _ => None,
    }
}

fn dispatch(event: &str, body: &str) -> String {
    let event_type = serde_json::from_value::<WebhookEventType>(json!(event)).ok();
    let kind = event_type.as_ref().and_then(event_kind);
    let envelope = match serde_json::from_str::<Value>(body) {
        Ok(payload) => {
            let (typed_payload, parse_error) =
                match WebhookEvent::try_from_header_and_body(event, body) {
                    Ok(parsed) => {
                        // Unknown is Octocrab's raw JSON fallback, not a typed model.
                        let typed = if matches!(parsed.specific, WebhookEventPayload::Unknown(_)) {
                            Value::Null
                        } else {
                            json!(parsed)
                        };
                        (typed, None)
                    }
                    Err(error) => (Value::Null, Some(error.to_string())),
                };
            json!({
                "event": event,
                "kind": kind,
                // Keep original fields/actions even when the model lags GitHub.
                "action": payload.get("action"),
                "payload": payload,
                "typed_payload": typed_payload,
                "parse_error": parse_error,
            })
        }
        // Retain malformed input for inspection rather than panicking or losing
        // the rest of a batch. The original topic also retains the raw bytes.
        Err(error) => json!({
            "event": event,
            "kind": kind,
            "error": error.to_string(),
            "raw_payload": body,
        }),
    };
    envelope.to_string()
}

fn topology() -> Result<BuiltTopology> {
    let builder = StreamsBuilder::new();
    builder
        // Raw JSON uses StringSerde, without Confluent framing or a registry.
        .stream::<String, String>([INPUT_TOPIC])
        .map(|event, body| (event.clone(), dispatch(event, body)))
        .peek(|_event, envelope| println!("{envelope}"))
        .to(OUTPUT_TOPIC);
    Ok(builder.build(APPLICATION_ID)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.create_topics {
        let bootstrap = args
            .bootstrap_servers
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let mut admin = AdminClient::connect(&bootstrap).await?;
        let specs = [INPUT_TOPIC, OUTPUT_TOPIC].map(|name| CreateTopicSpec {
            name: name.to_owned(),
            partitions: 1,
            replicas: 1,
            replica_assignments: BTreeMap::new(),
            configs: BTreeMap::new(),
        });
        for outcome in admin
            .create_topics(&specs, TopicMutationOptions::with_timeout(secs(10)))
            .await?
        {
            if let Some(error) = outcome.error {
                bail!("creating {}: {error:?}", outcome.name);
            }
        }
        return Ok(());
    }

    let streams = KafkaStreams::builder()
        .bootstrap(args.bootstrap_servers)
        .application_id(APPLICATION_ID)
        .topology(topology()?)
        .build()
        .await?;
    tokio::signal::ctrl_c().await?;
    streams.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use krabka_client_streams::{StringSerde, TopologyTestDriver};
    use krabka_gateway::webhook_config::{Source, WebhooksFile};

    use super::*;

    // Independent catalog snapshot from GitHub Docs, including App-only,
    // organization-only and classic project events, not just repository hooks.
    const EVENTS: &str = "branch_protection_configuration branch_protection_rule
        check_run check_suite code_scanning_alert commit_comment create
        custom_property custom_property_values delete dependabot_alert deploy_key
        deployment deployment_protection_rule deployment_review deployment_status
        discussion discussion_comment fork github_app_authorization gollum
        installation installation_repositories installation_target issue_comment
        issue_dependencies issue_relates_to issues label marketplace_purchase
        member membership merge_group meta milestone org_block organization package
        page_build personal_access_token_request ping project project_card
        project_column projects_v2 projects_v2_item projects_v2_status_update public
        pull_request pull_request_review pull_request_review_comment
        pull_request_review_thread push registry_package release repository
        repository_advisory repository_dispatch repository_import repository_ruleset
        repository_vulnerability_alert secret_scanning_alert
        secret_scanning_alert_location secret_scanning_scan security_advisory
        security_and_analysis sponsorship star status sub_issues team team_add watch
        workflow_dispatch workflow_job workflow_run";

    #[test]
    fn ingress_config_compiles_and_preserves_the_event_header() {
        let file: WebhooksFile =
            toml::from_str(include_str!("../../../demo/github-firehose/webhooks.toml")).unwrap();
        let compiled = file.compile().unwrap();
        let endpoint = &compiled["github"];
        assert2::assert!(endpoint.target_topic == INPUT_TOPIC);
        assert2::assert!(matches!(&endpoint.key_source,
            Some(Source::Header(name)) if name == "X-GitHub-Event"));
        assert2::assert!(matches!(&endpoint.idempotency_source,
            Some(Source::Header(name)) if name == "X-GitHub-Delivery"));
        assert2::assert!(
            endpoint.secret.as_deref() == Some(b"github-firehose-demo-secret".as_slice())
        );
        assert2::assert!(endpoint.signature_header.as_deref() == Some("X-Hub-Signature-256"));
        assert2::assert!(endpoint.signature_prefix.as_deref() == Some("sha256="));
        assert2::assert!(endpoint.max_body == mebibytes(25));
    }

    #[test]
    fn every_event_and_action_survives_the_topology() {
        let built = topology().unwrap();
        let mut driver = TopologyTestDriver::new(&built).unwrap();
        for event in EVENTS.split_whitespace() {
            for action in [None, Some("opened"), Some("future_action")] {
                // No shared repository or sender: those are not universal.
                let mut payload = json!({"nested": {"future_field": [1, null, "hello"]}});
                if let Some(action) = action {
                    payload["action"] = json!(action);
                }
                driver.pipe_input(
                    INPUT_TOPIC,
                    (StringSerde, StringSerde),
                    Some(event.to_owned()),
                    payload.to_string(),
                    0,
                );
                let (key, body) = driver
                    .read_output(OUTPUT_TOPIC, (StringSerde, StringSerde))
                    .expect("every event is forwarded");
                let envelope: Value = serde_json::from_str(&body).unwrap();
                assert2::assert!(envelope["kind"].is_string());
                assert2::assert!(key == Some(event.to_owned()));
                let mut unlabelled = envelope;
                let fields = unlabelled.as_object_mut().unwrap();
                fields.remove("kind");
                fields.remove("typed_payload");
                fields.remove("parse_error");
                assert2::assert!(
                    unlabelled
                        == json!({
                            "event": event, "action": action, "payload": payload,
                        })
                );
                assert2::assert!(
                    driver
                        .read_output(OUTPUT_TOPIC, (StringSerde, StringSerde))
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn event_bodies_are_decoded_into_octocrab_models() {
        let built = topology().unwrap();
        let mut driver = TopologyTestDriver::new(&built).unwrap();
        for (event, variant, kind, specific) in [
            (
                "ping",
                "Ping",
                "Webhook ping",
                json!({
                    "zen": "Design for failure.", "hook_id": 42, "hook": null,
                }),
            ),
            (
                "create",
                "Create",
                "Branch or tag created",
                json!({
                    "ref": "feature", "ref_type": "branch", "master_branch": "main",
                    "pusher_type": "user", "description": null, "enterprise": null,
                }),
            ),
            (
                "push",
                "Push",
                "Push",
                json!({
                    "ref": "refs/heads/main", "before": "abc", "after": "def",
                    "commits": [], "compare": "https://github.com/octocat/demo/compare/abc...def",
                    "created": false, "deleted": false, "forced": false,
                    "pusher": {"name": "octocat", "email": "octocat@example.com"},
                    "base_ref": null, "head_commit": null, "enterprise": null,
                }),
            ),
            (
                "workflow_dispatch",
                "WorkflowDispatch",
                "Workflow dispatch",
                json!({
                    "ref": "refs/heads/main", "workflow": "CI", "inputs": {"count": 2},
                    "enterprise": null,
                }),
            ),
            (
                "installation",
                "Installation",
                "App installation",
                json!({
                    "action": "created", "repositories": [], "requester": null,
                    "enterprise": null,
                }),
            ),
        ] {
            let mut payload = specific.clone();
            payload["future_field"] = json!({"enabled": true});
            driver.pipe_input(
                INPUT_TOPIC,
                (StringSerde, StringSerde),
                Some(event.to_owned()),
                payload.to_string(),
                0,
            );
            let (key, body) = driver
                .read_output(OUTPUT_TOPIC, (StringSerde, StringSerde))
                .unwrap();
            let envelope: Value = serde_json::from_str(&body).unwrap();
            let mut typed = json!({
                "sender": null, "repository": null, "organization": null, "installation": null,
            });
            typed[variant] = specific;
            assert2::assert!(key == Some(event.to_owned()));
            assert2::assert!(
                envelope
                    == json!({
                        "event": event, "kind": kind, "action": payload.get("action"),
                        "payload": payload, "typed_payload": typed, "parse_error": null,
                    })
            );
        }
    }

    #[test]
    fn invalid_typed_fields_and_new_actions_keep_the_original_body() {
        let built = topology().unwrap();
        let mut driver = TopologyTestDriver::new(&built).unwrap();
        for (event, kind, payload) in [
            ("push", "Push", json!({"ref": 42})),
            (
                "create",
                "Branch or tag created",
                json!({
                    "ref": 42, "ref_type": "branch", "master_branch": "main", "pusher_type": "user",
                }),
            ),
            (
                "installation",
                "App installation",
                json!({"action": "future_action"}),
            ),
        ] {
            driver.pipe_input(
                INPUT_TOPIC,
                (StringSerde, StringSerde),
                Some(event.to_owned()),
                payload.to_string(),
                0,
            );
            let (_key, body) = driver
                .read_output(OUTPUT_TOPIC, (StringSerde, StringSerde))
                .unwrap();
            let mut envelope: Value = serde_json::from_str(&body).unwrap();
            assert2::assert!(envelope["parse_error"].is_string());
            envelope.as_object_mut().unwrap().remove("parse_error");
            assert2::assert!(
                envelope
                    == json!({
                        "event": event, "kind": kind, "action": payload.get("action"),
                        "payload": payload, "typed_payload": null,
                    })
            );
        }
    }

    #[test]
    fn unknown_events_missing_keys_and_malformed_json_are_retained() {
        let built = topology().unwrap();
        let mut driver = TopologyTestDriver::new(&built).unwrap();
        for (event, body) in [
            (
                Some("future_event"),
                r#"{"action":"future_action","new":true}"#,
            ),
            (None, r#"{"zen":"Keep it logically awesome."}"#),
            (Some("push"), "not JSON"),
        ] {
            driver.pipe_input(
                INPUT_TOPIC,
                (StringSerde, StringSerde),
                event.map(str::to_owned),
                body.to_owned(),
                0,
            );
            let (_key, output) = driver
                .read_output(OUTPUT_TOPIC, (StringSerde, StringSerde))
                .unwrap();
            let envelope: Value = serde_json::from_str(&output).unwrap();
            let expected = match serde_json::from_str::<Value>(body) {
                Ok(payload) => json!({
                    "event": event.unwrap_or_default(), "kind": null,
                    "action": payload.get("action"), "payload": payload,
                    "typed_payload": null, "parse_error": null,
                }),
                Err(error) => json!({
                    "event": "push", "kind": "Push",
                    "error": error.to_string(), "raw_payload": body,
                }),
            };
            assert2::assert!(envelope == expected);
        }
    }
}
