// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use pretty_assertions::assert_eq;
use rama::{Context, Layer as _, Service as _};
use serde::Deserialize;
use tansu_broker::{
    Error,
    coordinator::group::administrator::Controller,
    service::{auth, coordinator, storage},
};
use tansu_sans_io::{
    AlterConfigsRequest, ApiKey as _, ApiVersionsRequest, ApiVersionsResponse,
    CreatePartitionsRequest, DeleteAclsRequest, DescribeLogDirsRequest, ElectLeadersRequest,
    EndTxnRequest, ErrorCode, MetadataRequest, OffsetDeleteRequest, PushTelemetryRequest,
    RootMessageMeta,
    alter_configs_request::AlterConfigsResource,
    create_partitions_request::CreatePartitionsTopic,
    describe_log_dirs_request::DescribableLogDirTopic,
    elect_leaders_request::TopicPartitions,
    offset_delete_request::{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic},
};
use tansu_service::{
    BytesFrameLayer, BytesLayer, FrameBytesLayer, FrameRouteService, RequestFrameLayer,
};
use tansu_storage::{StorageCertification, StorageContainer};
use url::Url;

const LEDGER_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/compatibility/kafka-4.2-ledger.json"
);
const PHASE_LOG_MANIFEST_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../phase-logs/index.json");

const PHASE_LOG_REQUIRED_HEADINGS: [&str; 9] = [
    "Date",
    "Phase File",
    "Intent",
    "Files Touched",
    "Ledger Rows Touched",
    "Tests Added",
    "Verification Commands",
    "Outcomes",
    "Residual Risks",
];

const PHASE_LOG_ALLOWED_STATUSES: [&str; 3] = ["legacy-unlogged", "legacy-complete", "in-progress"];

const PHASE_07_MANIFEST_PROOF: &str =
    "tansu-broker/tests/compatibility_contract.rs::phase_log_manifest_contract_covers_phase_docs";

const ALLOWED_PROFILES: [&str; 6] = [
    "tansu-native",
    "kafka-parity-postgres",
    "kafka-parity-sqlite",
    "kafka-parity-s3-dynostore",
    "kafka-parity-slatedb",
    "compat-preview",
];

const ALLOWED_STORAGE_ENGINES: [&str; 6] = [
    "postgres",
    "sqlite",
    "s3-dynostore",
    "slatedb",
    "turso",
    "memory",
];

const ALLOWED_PROFILE_STATUSES: [&str; 4] = ["green", "yellow", "red", "gray"];
const ALLOWED_STORAGE_STATUSES: [&str; 6] = [
    "production-parity",
    "limited-parity",
    "development-test-only",
    "unsupported",
    "uncertified",
    "not-applicable",
];
const ALLOWED_ROUTE_STATUSES: [&str; 4] = [
    "routed",
    "not-routed",
    "safe-error-routed",
    "not-client-facing",
];
const ALLOWED_SEMANTIC_STATUSES: [&str; 6] = [
    "uncertified",
    "partial",
    "compatible",
    "safe-error",
    "unsupported",
    "not-targeted",
];

fn storage_certification_label(certification: StorageCertification) -> &'static str {
    match certification {
        StorageCertification::ProductionParity => "production-parity",
        StorageCertification::LimitedParity => "limited-parity",
        StorageCertification::DevelopmentTestOnly => "development-test-only",
        StorageCertification::Unsupported => "unsupported",
        StorageCertification::Uncertified => "uncertified",
        StorageCertification::NotApplicable => "not-applicable",
    }
}

fn phase06_storage_matrix() -> BTreeMap<&'static str, StorageCertification> {
    BTreeMap::from([
        ("postgres", StorageCertification::ProductionParity),
        ("sqlite", StorageCertification::LimitedParity),
        ("s3-dynostore", StorageCertification::LimitedParity),
        ("slatedb", StorageCertification::LimitedParity),
        ("turso", StorageCertification::Uncertified),
        ("memory", StorageCertification::DevelopmentTestOnly),
    ])
}

const PHASE_06_STORAGE_PROOFS: [&str; 5] = [
    "tansu-storage/tests/log_contract.rs::empty_partition_offsets_are_zero",
    "tansu-storage/tests/log_contract.rs::produce_assigns_contiguous_offsets",
    "tansu-storage/tests/log_contract.rs::fetch_reads_written_records",
    "tansu-storage/tests/log_contract.rs::timestamp_lookup_before_first_record_returns_first_offset",
    "tansu-storage/tests/log_contract.rs::null_storage_reports_log_features_unsupported",
];

#[derive(Debug, Deserialize)]
struct Ledger {
    schema_version: u32,
    baseline: serde_json::Value,
    profiles: Vec<String>,
    storage_engines: Vec<String>,
    apis: Vec<ApiRow>,
}

#[derive(Debug, Deserialize)]
struct ApiRow {
    api_key: i16,
    name: String,
    descriptor_versions: VersionRange,
    codec_status: String,
    route_status: String,
    current_advertised_versions: Option<VersionRange>,
    approved_advertised_versions: Option<VersionRange>,
    semantic_status: String,
    phase_owner: String,
    profiles: BTreeMap<String, String>,
    storage: BTreeMap<String, String>,
    proof: Proof,
    notes: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct VersionRange {
    min: i16,
    max: i16,
}

#[derive(Debug, Deserialize)]
struct Proof {
    unit_tests: Vec<String>,
    differential_tests: Vec<String>,
    client_tests: serde_json::Value,
    failure_modes: String,
}

#[derive(Debug, Deserialize)]
struct PhaseLogManifest {
    schema_version: u32,
    phases: Vec<PhaseLogEntry>,
}

#[derive(Debug, Deserialize)]
struct PhaseLogEntry {
    phase: String,
    phase_file: String,
    status: String,
    log_file: Option<String>,
    ledger_api_keys: Vec<i16>,
    primary_paths: Vec<String>,
    verification_commands: Vec<String>,
    residual_risks: Vec<String>,
}

fn load_ledger() -> Ledger {
    let path = Path::new(LEDGER_PATH);
    let json = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!("failed to read ledger at {}: {err}", path.display());
    });

    serde_json::from_str(&json).unwrap_or_else(|err| {
        panic!("failed to parse ledger at {}: {err}", path.display());
    })
}

fn load_phase_log_manifest() -> PhaseLogManifest {
    let path = Path::new(PHASE_LOG_MANIFEST_PATH);
    let json = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!(
            "failed to read phase log manifest at {}: {err}",
            path.display()
        );
    });

    serde_json::from_str(&json).unwrap_or_else(|err| {
        panic!(
            "failed to parse phase log manifest at {}: {err}",
            path.display()
        );
    })
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn phase_docs() -> Vec<String> {
    let mut docs = fs::read_dir(repo_root().join("tips/phases"))
        .unwrap_or_else(|err| panic!("failed to read tips/phases: {err}"))
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("md"))
        .map(|path| {
            format!(
                "tips/phases/{}",
                path.file_name().unwrap().to_string_lossy()
            )
        })
        .collect::<Vec<_>>();

    docs.sort();
    docs
}

fn section_bullets(contents: &str, heading: &str) -> Vec<String> {
    let target = format!("## {heading}");
    let mut bullets = Vec::new();
    let mut in_section = false;

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed == target {
            in_section = true;
            continue;
        }
        if in_section && trimmed.starts_with("## ") {
            break;
        }
        if in_section {
            if let Some(item) = trimmed.strip_prefix("- ") {
                bullets.push(item.to_string());
            }
        }
    }

    bullets
}

fn assert_structured_phase_log(path: &Path) {
    let contents = fs::read_to_string(path).unwrap_or_else(|err| {
        panic!(
            "failed to read structured phase log at {}: {err}",
            path.display()
        );
    });

    for heading in PHASE_LOG_REQUIRED_HEADINGS {
        assert!(
            contents
                .lines()
                .any(|line| line.trim() == format!("## {heading}")),
            "structured phase log {} is missing heading ## {heading}",
            path.display()
        );
    }

    let files_touched = section_bullets(&contents, "Files Touched");
    assert!(
        !files_touched.is_empty(),
        "structured phase log {} must list touched files",
        path.display()
    );

    let root = repo_root();
    for file in files_touched {
        let touched = root.join(&file);
        assert!(
            touched.exists(),
            "structured phase log {} references missing file {}",
            path.display(),
            touched.display()
        );
    }

    assert!(
        !section_bullets(&contents, "Ledger Rows Touched").is_empty(),
        "structured phase log {} must list touched ledger rows",
        path.display()
    );
    assert!(
        !section_bullets(&contents, "Tests Added").is_empty(),
        "structured phase log {} must list added tests",
        path.display()
    );
    assert!(
        !section_bullets(&contents, "Verification Commands").is_empty(),
        "structured phase log {} must list verification commands",
        path.display()
    );
    assert!(
        !section_bullets(&contents, "Outcomes").is_empty(),
        "structured phase log {} must list outcomes",
        path.display()
    );
    assert!(
        !section_bullets(&contents, "Residual Risks").is_empty(),
        "structured phase log {} must list residual risks",
        path.display()
    );
}

fn ledger_by_key<'a>(ledger: &'a Ledger) -> BTreeMap<i16, &'a ApiRow> {
    let mut rows = BTreeMap::new();

    for row in &ledger.apis {
        assert!(
            rows.insert(row.api_key, row).is_none(),
            "duplicate ledger row for api_key {}",
            row.api_key
        );
    }

    rows
}

fn assert_allowed_value(value: &str, allowed: &[&str]) {
    assert!(
        allowed.contains(&value),
        "unexpected value {value:?}, expected one of {allowed:?}"
    );
}

fn approved_advertised_versions(ledger: &Ledger) -> BTreeMap<i16, VersionRange> {
    ledger
        .apis
        .iter()
        .filter_map(|row| {
            row.approved_advertised_versions
                .clone()
                .map(|version| (row.api_key, version))
        })
        .collect()
}

#[test]
fn ledger_covers_every_protocol_request() {
    let ledger = load_ledger();
    let rows = ledger_by_key(&ledger);
    let requests = RootMessageMeta::messages().requests();

    assert_eq!(1, ledger.schema_version);
    assert_eq!(
        Some("4.2.0"),
        ledger
            .baseline
            .get("kafka_version")
            .and_then(|value| value.as_str())
    );
    assert_eq!(
        Some("4.3.0"),
        ledger
            .baseline
            .get("future_tracked")
            .and_then(|value| value.as_str())
    );
    assert_eq!(requests.len(), rows.len());

    for (api_key, meta) in requests {
        let row = rows.get(api_key).unwrap_or_else(|| {
            panic!("missing ledger row for api_key {api_key}");
        });

        assert_eq!(meta.name, row.name);
        assert_eq!(meta.version.valid.start, row.descriptor_versions.min);
        assert_eq!(meta.version.valid.end, row.descriptor_versions.max);
    }

    for api_key in rows.keys() {
        assert!(
            requests.contains_key(api_key),
            "ledger row references api_key {api_key} that is not present in protocol metadata"
        );
    }
}

#[tokio::test]
async fn ledger_covers_every_current_route() -> Result<(), Error> {
    let ledger = load_ledger();
    let rows = ledger_by_key(&ledger);
    let routes = current_route_keys().await?;
    let route_keys = routes.into_iter().collect::<BTreeSet<_>>();
    let request_keys = RootMessageMeta::messages()
        .requests()
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();

    assert!(route_keys.is_subset(&request_keys));

    for api_key in &route_keys {
        let row = rows.get(api_key).unwrap_or_else(|| {
            panic!("missing ledger row for routed api_key {api_key}");
        });

        assert_ne!(
            "not-routed", row.route_status,
            "routed api_key {api_key} must not be marked not-routed"
        );
        assert_eq!(
            row.approved_advertised_versions.clone(),
            row.current_advertised_versions.clone(),
            "routed api_key {api_key} must only advertise approved versions"
        );
    }

    let routed_rows = rows
        .values()
        .filter(|row| row.route_status != "not-routed")
        .map(|row| row.api_key)
        .collect::<BTreeSet<_>>();

    assert_eq!(route_keys, routed_rows);

    Ok(())
}

#[test]
fn no_compatible_status_without_proof() {
    let ledger = load_ledger();

    for row in &ledger.apis {
        if row.semantic_status == "compatible" {
            assert!(
                !row.proof.unit_tests.is_empty()
                    || !row.proof.differential_tests.is_empty()
                    || row
                        .proof
                        .client_tests
                        .as_object()
                        .is_some_and(|object| !object.is_empty()),
                "compatible api_key {} must carry proof",
                row.api_key
            );
            assert!(
                row.profiles.values().any(|status| status == "green"),
                "compatible api_key {} must be green in at least one profile",
                row.api_key
            );
        }
    }
}

#[test]
fn ledger_has_required_profiles_and_storage_tiers() {
    let ledger = load_ledger();

    let profiles = ledger
        .profiles
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage_engines = ledger
        .storage_engines
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    assert_eq!(ALLOWED_PROFILES.as_slice(), profiles.as_slice());
    assert_eq!(
        ALLOWED_STORAGE_ENGINES.as_slice(),
        storage_engines.as_slice()
    );

    for row in &ledger.apis {
        assert_eq!(ALLOWED_PROFILES.len(), row.profiles.len());
        assert_eq!(ALLOWED_STORAGE_ENGINES.len(), row.storage.len());

        for profile in ALLOWED_PROFILES {
            let status = row
                .profiles
                .get(profile)
                .unwrap_or_else(|| panic!("missing profile {profile} for api_key {}", row.api_key));
            assert_allowed_value(status.as_str(), &ALLOWED_PROFILE_STATUSES);
        }

        for storage in ALLOWED_STORAGE_ENGINES {
            let status = row.storage.get(storage).unwrap_or_else(|| {
                panic!(
                    "missing storage engine {storage} for api_key {}",
                    row.api_key
                )
            });
            assert_allowed_value(status.as_str(), &ALLOWED_STORAGE_STATUSES);
        }

        assert_allowed_value(row.route_status.as_str(), &ALLOWED_ROUTE_STATUSES);
        assert_allowed_value(row.semantic_status.as_str(), &ALLOWED_SEMANTIC_STATUSES);
        assert_allowed_value(row.codec_status.as_str(), &["present", "missing"]);
        assert_eq!("untested", row.proof.failure_modes);
        match row.api_key {
            0 => {
                assert_eq!("07", row.phase_owner, "api_key {}", row.api_key);
                assert!(
                    row.proof
                        .differential_tests
                        .iter()
                        .any(|test| test == PHASE_07_MANIFEST_PROOF),
                    "api_key {} must include phase 07 manifest proof",
                    row.api_key
                );
                for (engine, certification) in phase06_storage_matrix() {
                    let expected = storage_certification_label(certification);
                    let actual = row
                        .storage
                        .get(engine)
                        .map(String::as_str)
                        .unwrap_or_else(|| {
                            panic!(
                                "missing phase 06 storage tier for {engine} on api_key {}",
                                row.api_key
                            )
                        });
                    assert_eq!(
                        expected, actual,
                        "api_key {} storage tier mismatch for {engine}",
                        row.api_key
                    );
                }

                for proof in PHASE_06_STORAGE_PROOFS {
                    assert!(
                        row.proof.unit_tests.iter().any(|test| test == proof),
                        "api_key {} must include proof entry {proof}",
                        row.api_key
                    );
                }
            }
            1 | 2 => {
                assert_eq!("06", row.phase_owner, "api_key {}", row.api_key);

                for (engine, certification) in phase06_storage_matrix() {
                    let expected = storage_certification_label(certification);
                    let actual = row
                        .storage
                        .get(engine)
                        .map(String::as_str)
                        .unwrap_or_else(|| {
                            panic!(
                                "missing phase 06 storage tier for {engine} on api_key {}",
                                row.api_key
                            )
                        });
                    assert_eq!(
                        expected, actual,
                        "api_key {} storage tier mismatch for {engine}",
                        row.api_key
                    );
                }

                for proof in PHASE_06_STORAGE_PROOFS {
                    assert!(
                        row.proof.unit_tests.iter().any(|test| test == proof),
                        "api_key {} must include proof entry {proof}",
                        row.api_key
                    );
                }
            }
            3 | 18 => {
                assert_eq!("02", row.phase_owner, "api_key {}", row.api_key);
                assert!(
                    row.approved_advertised_versions.is_some(),
                    "api_key {} must carry approved advertised versions",
                    row.api_key
                );
            }
            23 => {
                assert_eq!("08", row.phase_owner, "api_key {}", row.api_key);
                assert!(row.approved_advertised_versions.is_none());
            }
            _ if row.route_status == "safe-error-routed" => {
                assert_eq!("05", row.phase_owner, "api_key {}", row.api_key)
            }
            _ => assert_eq!("01", row.phase_owner, "api_key {}", row.api_key),
        }
        if let Some(approved) = &row.approved_advertised_versions {
            assert_eq!(
                Some(approved.clone()),
                row.current_advertised_versions.clone()
            );
        } else {
            assert!(
                row.current_advertised_versions.is_none(),
                "api_key {} must not advertise unapproved versions",
                row.api_key
            );
        }
        assert!(
            !row.notes.is_empty(),
            "api_key {} must carry notes",
            row.api_key
        );
        assert!(
            row.proof.client_tests.is_object(),
            "api_key {} must store client_tests as an object",
            row.api_key
        );
    }
}

#[test]
fn phase_log_manifest_contract_covers_phase_docs() {
    let ledger = load_ledger();
    let manifest = load_phase_log_manifest();
    let docs = phase_docs();
    let doc_paths = docs.iter().cloned().collect::<BTreeSet<_>>();
    let entries = manifest
        .phases
        .iter()
        .map(|entry| (entry.phase.clone(), entry))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(1, manifest.schema_version);
    assert_eq!(docs.len(), manifest.phases.len());

    for doc in &docs {
        let phase_file = doc.as_str();
        let phase = doc
            .strip_prefix("tips/phases/")
            .and_then(|path| path.strip_suffix(".md"))
            .and_then(|path| path.split_once('-'))
            .map(|(phase, _)| phase)
            .unwrap_or_else(|| panic!("unexpected phase doc name {doc}"));
        let entry = entries
            .get(phase)
            .unwrap_or_else(|| panic!("missing manifest entry for phase {phase}"));

        assert_eq!(phase, entry.phase, "phase mismatch for {doc}");
        assert_eq!(
            phase_file, entry.phase_file,
            "phase file mismatch for phase {phase}"
        );
        let expected_keys = ledger
            .apis
            .iter()
            .filter(|row| row.phase_owner == entry.phase)
            .map(|row| row.api_key)
            .collect::<BTreeSet<_>>();
        let actual_keys = entry
            .ledger_api_keys
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            expected_keys, actual_keys,
            "ledger api keys mismatch for phase {phase}"
        );
        assert!(
            doc_paths.contains(entry.phase_file.as_str()),
            "manifest references unknown phase doc {}",
            entry.phase_file
        );
        assert_allowed_value(entry.status.as_str(), &PHASE_LOG_ALLOWED_STATUSES);
        assert!(
            !entry.primary_paths.is_empty(),
            "manifest entry for phase {phase} must list primary paths"
        );
        assert!(
            !entry.verification_commands.is_empty(),
            "manifest entry for phase {phase} must list verification commands"
        );
        assert!(
            !entry.residual_risks.is_empty(),
            "manifest entry for phase {phase} must list residual risks"
        );

        for path in &entry.primary_paths {
            assert!(
                repo_root().join(path).exists(),
                "manifest entry for phase {phase} references missing primary path {path}"
            );
        }

        match entry.status.as_str() {
            "legacy-unlogged" => {
                assert!(
                    entry.log_file.is_none(),
                    "legacy-unlogged phase {phase} must not carry a log file"
                );
            }
            "legacy-complete" => {
                let log_file = entry.log_file.as_ref().unwrap_or_else(|| {
                    panic!("legacy-complete phase {phase} must carry a log file")
                });
                assert!(
                    repo_root().join(log_file).exists(),
                    "legacy-complete phase {phase} references missing log file {log_file}"
                );
            }
            "in-progress" => {
                let log_file = entry
                    .log_file
                    .as_ref()
                    .unwrap_or_else(|| panic!("in-progress phase {phase} must carry a log file"));
                let log_path = repo_root().join(log_file);
                assert!(
                    log_path.exists(),
                    "in-progress phase {phase} references missing log file {log_file}"
                );
                assert_structured_phase_log(&log_path);
            }
            other => panic!("unexpected manifest status {other} for phase {phase}"),
        }
    }

    let manifest_phases = manifest
        .phases
        .iter()
        .map(|entry| entry.phase.as_str())
        .collect::<BTreeSet<_>>();
    let ledger_phases = ledger
        .apis
        .iter()
        .map(|row| row.phase_owner.as_str())
        .collect::<BTreeSet<_>>();

    assert!(
        ledger_phases.is_subset(&manifest_phases),
        "manifest is missing one or more ledger phase owners"
    );
}

#[test]
fn broker_registry_matches_ledger_approved_versions() {
    let ledger = load_ledger();
    let expected = approved_advertised_versions(&ledger);

    let actual = tansu_broker::service::advertised_versions()
        .into_iter()
        .map(|(api_key, range)| {
            (
                api_key,
                VersionRange {
                    min: range.min_version,
                    max: range.max_version,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(expected, actual);
}

#[tokio::test]
async fn broker_route_stack_advertises_only_approved_versions() -> Result<(), Error> {
    let storage = StorageContainer::builder()
        .cluster_id("compatibility-contract")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("null://")?)
        .build()
        .await?;

    let coordinator = Controller::with_storage(storage.clone())?;
    let builder = storage::services(
        FrameRouteService::<(), Error>::builder()
            .with_advertised_versions(tansu_broker::service::advertised_versions()),
        storage,
    )?;
    let builder = coordinator::services(builder, coordinator)?;
    let builder = auth::services(builder)?;
    let route = builder.build()?;

    let service = (
        RequestFrameLayer,
        FrameBytesLayer,
        BytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    let response = service
        .serve(
            Context::default(),
            ApiVersionsRequest::default()
                .client_software_name(Some("tansu".into()))
                .client_software_version(Some("broker".into())),
        )
        .await
        .expect("broker route stack ApiVersions request should succeed");

    let api_versions: ApiVersionsResponse = response;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(api_versions.error_code)
            .expect("broker route stack error code should decode"),
    );

    let api_versions = api_versions
        .api_keys
        .unwrap_or_default()
        .into_iter()
        .map(|api_version| {
            (
                api_version.api_key,
                api_version.min_version,
                api_version.max_version,
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        vec![
            (MetadataRequest::KEY, 12, 12),
            (ApiVersionsRequest::KEY, 0, 4)
        ],
        api_versions
    );

    Ok(())
}

#[tokio::test]
async fn broker_safe_error_routes_return_structured_errors() -> Result<(), Error> {
    let storage = StorageContainer::builder()
        .cluster_id("compatibility-contract")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("null://")?)
        .build()
        .await?;

    let coordinator = Controller::with_storage(storage.clone())?;
    let builder = storage::services(
        FrameRouteService::<(), Error>::builder()
            .with_advertised_versions(tansu_broker::service::advertised_versions()),
        storage,
    )?;
    let builder = coordinator::services(builder, coordinator)?;
    let builder = auth::services(builder)?;
    let builder = tansu_broker::service::safe_errors::services(builder)?;
    let route = builder.build()?;

    let service = (
        RequestFrameLayer,
        FrameBytesLayer,
        BytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    let ctx = Context::default();

    let alter_configs = service
        .serve(
            ctx.clone(),
            AlterConfigsRequest::default().resources(Some(vec![
                AlterConfigsResource::default()
                    .resource_type(2)
                    .resource_name("test".into())
                    .configs(Some(Vec::new())),
            ])),
        )
        .await?;
    let alter_configs_responses = alter_configs.responses.unwrap_or_default();
    assert_eq!(1, alter_configs_responses.len());
    assert_eq!(
        i16::from(ErrorCode::UnknownServerError),
        alter_configs_responses[0].error_code
    );

    let create_partitions = service
        .serve(
            ctx.clone(),
            CreatePartitionsRequest::default().topics(Some(
                [CreatePartitionsTopic::default()
                    .name("test".into())
                    .count(2)]
                .into(),
            )),
        )
        .await?;
    let create_partitions_results = create_partitions.results.unwrap_or_default();
    assert_eq!(1, create_partitions_results.len());
    assert_eq!(
        i16::from(ErrorCode::UnknownServerError),
        create_partitions_results[0].error_code
    );

    let delete_acls = service
        .serve(
            ctx.clone(),
            DeleteAclsRequest::default().filters(Some(Vec::new())),
        )
        .await?;
    let delete_acls_filter_results = delete_acls.filter_results.unwrap_or_default();
    assert!(delete_acls_filter_results.is_empty());

    let describe_log_dirs = service
        .serve(
            ctx.clone(),
            DescribeLogDirsRequest::default().topics(Some(
                [DescribableLogDirTopic::default()
                    .topic("test".into())
                    .partitions(Some([0].into()))]
                .into(),
            )),
        )
        .await?;
    assert_eq!(
        Some(i16::from(ErrorCode::UnknownServerError)),
        describe_log_dirs.error_code
    );

    let elect_leaders = service
        .serve(
            ctx.clone(),
            ElectLeadersRequest::default()
                .election_type(Some(0))
                .topic_partitions(Some(
                    [TopicPartitions::default()
                        .topic("test".into())
                        .partitions(Some([0].into()))]
                    .into(),
                ))
                .timeout_ms(1),
        )
        .await?;
    assert_eq!(
        Some(i16::from(ErrorCode::UnknownServerError)),
        elect_leaders.error_code
    );

    let end_txn = service
        .serve(
            ctx.clone(),
            EndTxnRequest::default()
                .transactional_id("txn-1".into())
                .producer_id(123)
                .producer_epoch(1)
                .committed(true),
        )
        .await?;
    assert_eq!(i16::from(ErrorCode::UnknownServerError), end_txn.error_code);

    let offset_delete = service
        .serve(
            ctx.clone(),
            OffsetDeleteRequest::default()
                .group_id("group-1".into())
                .topics(Some(
                    [OffsetDeleteRequestTopic::default()
                        .name("test".into())
                        .partitions(Some(
                            [OffsetDeleteRequestPartition::default().partition_index(0)].into(),
                        ))]
                    .into(),
                )),
        )
        .await?;
    assert_eq!(
        i16::from(ErrorCode::UnknownServerError),
        offset_delete.error_code
    );
    let offset_delete_topics = offset_delete.topics.unwrap_or_default();
    assert_eq!(1, offset_delete_topics.len());
    let offset_delete_partitions = offset_delete_topics[0]
        .partitions
        .clone()
        .unwrap_or_default();
    assert_eq!(
        i16::from(ErrorCode::UnknownServerError),
        offset_delete_partitions[0].error_code
    );

    let push_telemetry = service
        .serve(ctx.clone(), PushTelemetryRequest::default())
        .await?;
    assert_eq!(
        i16::from(ErrorCode::UnknownServerError),
        push_telemetry.error_code
    );

    Ok(())
}

#[tokio::test]
async fn route_coverage_report_matches_ledger() -> Result<(), Error> {
    let ledger = load_ledger();
    let rows = ledger_by_key(&ledger);
    let route_keys = current_route_keys()
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let request_keys = RootMessageMeta::messages()
        .requests()
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    let safe_error_routed = rows
        .values()
        .filter(|row| row.route_status == "safe-error-routed")
        .map(|row| row.api_key)
        .collect::<BTreeSet<_>>();

    assert!(route_keys.is_subset(&request_keys));
    assert_eq!(
        vec![26, 31, 33, 35, 37, 43, 47, 72]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        safe_error_routed
    );

    let report = serde_json::json!({
        "protocol_request_keys": request_keys,
        "broker_route_keys": route_keys,
        "safe_error_routed_keys": safe_error_routed,
        "not_routed_keys": rows
            .values()
            .filter(|row| row.route_status == "not-routed")
            .map(|row| row.api_key)
            .collect::<BTreeSet<_>>(),
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize route coverage report")
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn dump_compatibility_ledger_seed() -> Result<(), Error> {
    let routes = current_route_keys().await?;
    let mut requests = RootMessageMeta::messages()
        .requests()
        .iter()
        .map(|(api_key, meta)| {
            serde_json::json!({
                "api_key": api_key,
                "name": meta.name,
                "descriptor_versions": {
                    "min": meta.version.valid.start,
                    "max": meta.version.valid.end,
                },
            })
        })
        .collect::<Vec<_>>();
    requests.sort_by_key(|value| value["api_key"].as_i64().unwrap());

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 1,
            "requests": requests,
            "route_keys": routes,
        }))
        .expect("serialize compatibility seed")
    );

    Ok(())
}

async fn current_route_keys() -> Result<Vec<i16>, Error> {
    let storage = StorageContainer::builder()
        .cluster_id("compatibility-contract")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("null://")?)
        .build()
        .await?;

    let coordinator = Controller::with_storage(storage.clone())?;
    let builder = storage::services(FrameRouteService::<(), Error>::builder(), storage)?;
    let builder = coordinator::services(builder, coordinator)?;
    let builder = auth::services(builder)?;
    let builder = tansu_broker::service::safe_errors::services(builder)?;
    let route = builder.build()?;

    Ok(route.route_keys())
}

#[tokio::test]
async fn broker_rejects_unsupported_request_versions() -> Result<(), Error> {
    use tansu_sans_io::{Body, Frame, Header};

    let storage = StorageContainer::builder()
        .cluster_id("compatibility-contract")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("null://")?)
        .build()
        .await?;

    let coordinator = Controller::with_storage(storage.clone())?;
    let builder = storage::services(
        FrameRouteService::<(), Error>::builder().with_advertised_versions({
            let mut v = tansu_broker::service::advertised_versions();
            _ = v.insert(
                tansu_sans_io::FetchRequest::KEY,
                tansu_service::ApiVersionRange {
                    min_version: 0,
                    max_version: 11,
                },
            );
            v
        }),
        storage,
    )?;
    let builder = coordinator::services(builder, coordinator)?;
    let builder = auth::services(builder)?;
    let builder = tansu_broker::service::safe_errors::services(builder)?;
    let route = builder.build()?;

    let fetch_frame = Frame {
        size: 0,
        header: Header::Request {
            api_key: tansu_sans_io::FetchRequest::KEY,
            api_version: 99,
            correlation_id: 5678,
            client_id: Some("test-client".into()),
        },
        body: Body::FetchRequest(tansu_sans_io::FetchRequest::default()),
    };

    let fetch_response_frame = route.serve(Context::default(), fetch_frame).await?;
    assert_eq!(fetch_response_frame.correlation_id()?, 5678);

    match fetch_response_frame.body {
        Body::FetchResponse(resp) => {
            assert_eq!(
                resp.error_code,
                Some(i16::from(ErrorCode::UnsupportedVersion))
            );
        }
        _ => panic!("Expected FetchResponse"),
    }

    Ok(())
}
