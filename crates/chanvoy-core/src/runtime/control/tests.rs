use super::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::PathBuf};

fn vendor() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../schemas/runtime/control/v1")
}
fn fixture(root: ControlRoot) -> Vec<u8> {
    std::fs::read(vendor().join(format!(
        "fixtures/chanvoy-runtime-control-v1/{}-positive.json",
        root.name()
    )))
    .unwrap()
}
fn padded(root: ControlRoot) -> Vec<u8> {
    let value: Value = serde_json::from_slice(&fixture(root)).unwrap();
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.resize(root.max_bytes(), b' ');
    bytes
}

#[test]
fn public_pin_matches_all_vendored_bytes_and_embedded_roots() {
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(vendor().join("manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["source_repository"],
        "https://github.com/lanytehq/crucible"
    );
    assert_eq!(
        manifest["source_commit"],
        "71f77894b7ac5d6065f20ecbbbaaa936409128f6"
    );
    assert_eq!(manifest["source_tag"], "v0.2.0");
    for (file, pin) in manifest["files"].as_object().unwrap() {
        assert_eq!(pin["source_path"], format!("schemas/ipc/local/{file}"));
        let bytes = std::fs::read(vendor().join(file)).unwrap();
        assert_eq!(
            pin["sha256"],
            format!("{:x}", Sha256::digest(&bytes)),
            "{file}"
        );
    }
    assert_eq!(
        manifest["files"]["chanvoy-runtime-control-v1.schema.json"]["sha256"],
        "020258a9cb7d4a7cb6263bae91add7d8457f83adeaa9ca35476259923ade7784"
    );
    let roots: Value = serde_json::from_slice(
        &std::fs::read(vendor().join("chanvoy-runtime-control-v1.roots.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        roots["roots"],
        json!(ControlRoot::ALL.map(ControlRoot::name))
    );
    assert_eq!(
        roots["budgets"]["rpc_frame_utf8_bytes_excluding_newline"],
        RPC_MAX_BYTES
    );
    assert_eq!(
        roots["budgets"]["bootstrap_utf8_bytes"],
        BOOTSTRAP_MAX_BYTES
    );
    assert_eq!(
        roots["budgets"]["readiness_utf8_bytes"],
        READINESS_MAX_BYTES
    );
    let family: Value = serde_json::from_slice(
        &std::fs::read(vendor().join("chanvoy-runtime-control-v1.schema.json")).unwrap(),
    )
    .unwrap();
    for root in ControlRoot::ALL {
        let text =
            std::fs::read_to_string(vendor().join(format!("{}.selected.schema.json", root.name())))
                .unwrap();
        assert_eq!(root.schema(), text);
        let selected: Value = serde_json::from_str(root.schema()).unwrap();
        assert_eq!(selected["$defs"], family["$defs"]);
        assert_eq!(selected["$ref"], format!("#/$defs/{}", root.name()));
        root.validator().unwrap();
    }
}

#[tokio::test]
async fn all_upstream_cases_use_actual_strict_parser_validator_and_reader() {
    let manifest: Vec<Value> = serde_json::from_slice(
        &std::fs::read(vendor().join("fixtures/chanvoy-runtime-control-v1/manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest.len(), 100);
    let mut coverage = BTreeSet::new();
    for case in manifest {
        let root = ControlRoot::ALL
            .into_iter()
            .find(|r| case["root"] == r.name())
            .unwrap();
        let path = case["data"]
            .as_str()
            .unwrap()
            .strip_prefix("schemas/ipc/local/")
            .unwrap();
        let mut bytes = std::fs::read(vendor().join(path)).unwrap();
        let expected = case["expected_valid"].as_bool().unwrap();
        coverage.insert((root.name(), expected));
        let decoded = ControlDocument::decode(root, &bytes);
        assert_eq!(decoded.is_ok(), expected, "{case}: {decoded:?}");
        if let Err(error) = &decoded {
            assert!(
                matches!(error, ControlError::InvalidContract),
                "{case}: {error}"
            );
        }
        if let Ok(doc) = decoded {
            assert_eq!(doc.root(), root);
            assert_eq!(
                ControlDocument::decode(root, &doc.encode().unwrap())
                    .unwrap()
                    .value(),
                doc.value()
            );
        }
        let streamed = if root.is_rpc() {
            // Fixture pretty printing is not RPC framing; serialize its exact
            // value into the protocol's single-line encoding for the reader.
            bytes = serde_json::to_vec(&serde_json::from_slice::<Value>(&bytes).unwrap()).unwrap();
            bytes.push(b'\n');
            read_rpc(&mut bytes.as_slice(), root).await
        } else {
            read_document(&mut bytes.as_slice(), root).await
        };
        assert_eq!(streamed.is_ok(), expected, "stream {case}: {streamed:?}");
        if let Err(error) = streamed {
            assert!(
                matches!(error, ControlError::InvalidContract),
                "{case}: {error}"
            );
        }
    }
    assert_eq!(coverage.len(), 18);
}

#[test]
fn strict_json_refuses_duplicates_at_every_depth_and_non_json_input() {
    for text in [
        r#"{"id":1,"id":2}"#,
        r#"{"a":{"b":1,"b":2}}"#,
        r#"{"a":[{"b":1,"\u0062":2}]}"#,
        r#"{"x":NaN}"#,
        r#"{"x":Infinity}"#,
        r#"{"x":1e999}"#,
        r#"{"x":01}"#,
        r#"{"x":1,}"#,
        "{}{}",
        "[]",
        "null",
        "true",
        "1",
        "\u{feff}{}",
        "{} trailing",
    ] {
        assert!(
            matches!(
                ControlDocument::decode(ControlRoot::RetireRequest, text.as_bytes()),
                Err(ControlError::InvalidJson)
            ),
            "{text}"
        );
    }
    let nested = format!("{{\"a\":{}0{}}}", "[".repeat(130), "]".repeat(130));
    assert!(matches!(
        ControlDocument::decode(ControlRoot::RetireRequest, nested.as_bytes()),
        Err(ControlError::InvalidJson)
    ));
    assert!(matches!(
        ControlDocument::decode(ControlRoot::RetireRequest, &[b'{', 0xff, b'}']),
        Err(ControlError::InvalidUtf8)
    ));
    // A permissive Value parser would silently retain the last duplicate id.
    let doc: Value = serde_json::from_slice(&fixture(ControlRoot::RetireRequest)).unwrap();
    let mut bytes = serde_json::to_vec(&doc).unwrap();
    bytes.pop();
    bytes.extend_from_slice(format!(",\"id\":{}}}", doc["id"]).as_bytes());
    assert!(ControlRoot::RetireRequest
        .validator()
        .unwrap()
        .is_valid(&serde_json::from_slice::<Value>(&bytes).unwrap()));
    assert!(matches!(
        ControlDocument::decode(ControlRoot::RetireRequest, &bytes),
        Err(ControlError::InvalidJson)
    ));
}

#[test]
fn wrong_root_cannot_promote_daemon_claims_to_native_or_no_ack_reports() {
    let bytes = fixture(ControlRoot::ObservationResponse);
    assert!(ControlDocument::decode(ControlRoot::ObservationResponse, &bytes).is_ok());
    for root in [
        ControlRoot::CliLifecycleReport,
        ControlRoot::RetirementNoAckReport,
        ControlRoot::Readiness,
    ] {
        assert!(ControlDocument::decode(root, &bytes).is_err());
    }
    let no_ack = fixture(ControlRoot::RetirementNoAckReport);
    assert!(ControlDocument::decode(ControlRoot::RetireResponse, &no_ack).is_err());
    let native = fixture(ControlRoot::CliLifecycleReport);
    assert!(ControlDocument::decode(ControlRoot::ObservationResponse, &native).is_err());
}

#[tokio::test]
async fn byte_caps_apply_before_parse_and_streams_consume_at_most_cap_plus_one() {
    for root in ControlRoot::ALL {
        let mut bytes = padded(root);
        assert!(
            ControlDocument::decode(root, &bytes).is_ok(),
            "{} at cap",
            root.name()
        );
        bytes.push(b' ');
        assert!(matches!(
            ControlDocument::decode(root, &bytes),
            Err(ControlError::TooLarge)
        ));
        // Invalid UTF8 above cap must report size first, before string parsing.
        bytes[0] = 0xff;
        assert!(matches!(
            ControlDocument::decode(root, &bytes),
            Err(ControlError::TooLarge)
        ));
        bytes.extend_from_slice(b"tail must remain unread");
        let mut source = bytes.as_slice();
        let result = if root.is_rpc() {
            read_rpc(&mut source, root).await
        } else {
            read_document(&mut source, root).await
        };
        assert!(matches!(result, Err(ControlError::TooLarge)));
        assert_eq!(source, b"tail must remain unread");
    }
}

#[tokio::test]
async fn rpc_delimiter_is_outside_budget_and_next_frame_is_preserved() {
    let root = ControlRoot::RetireRequest;
    let mut bytes =
        serde_json::to_vec(&serde_json::from_slice::<Value>(&fixture(root)).unwrap()).unwrap();
    bytes.resize(root.max_bytes(), b' ');
    bytes.extend_from_slice(b"\nnext-frame\n");
    let mut input = bytes.as_slice();
    read_rpc(&mut input, root).await.unwrap();
    assert_eq!(input, b"next-frame\n");
    assert!(matches!(
        read_rpc(&mut b"{}".as_slice(), root).await,
        Err(ControlError::TruncatedFrame)
    ));
    assert!(matches!(
        read_rpc(&mut b"\n".as_slice(), root).await,
        Err(ControlError::InvalidJson)
    ));
    assert!(matches!(
        read_rpc(&mut b"{}\n".as_slice(), ControlRoot::Bootstrap).await,
        Err(ControlError::WrongTransport)
    ));
    assert!(matches!(
        read_document(&mut b"{}".as_slice(), root).await,
        Err(ControlError::WrongTransport)
    ));
}

#[tokio::test]
async fn fragmented_reader_preserves_frames_and_rejects_strict_json_before_schema() {
    let root = ControlRoot::RetireRequest;
    let doc: Value = serde_json::from_slice(&fixture(root)).unwrap();
    let mut line = serde_json::to_vec(&doc).unwrap();
    line.push(b'\n');
    let bytes = [line.clone(), line].concat();
    let mut reader = tokio::io::BufReader::with_capacity(1, bytes.as_slice());
    for _ in 0..2 {
        assert_eq!(read_rpc(&mut reader, root).await.unwrap().value(), &doc);
    }
    for bytes in [
        b"{\"x\":{\"a\":1,\"a\":2}}\n".as_slice(),
        b"{}{}\n",
        b"{\"a\":NaN}\n",
    ] {
        let mut reader = tokio::io::BufReader::with_capacity(1, bytes);
        assert!(matches!(
            read_rpc(&mut reader, root).await,
            Err(ControlError::InvalidJson)
        ));
    }
    let mut reader = tokio::io::BufReader::with_capacity(1, b"{\xff}\n".as_slice());
    assert!(matches!(
        read_rpc(&mut reader, root).await,
        Err(ControlError::InvalidUtf8)
    ));
}

#[test]
fn meta_validation_and_external_reference_controls_fail_closed() {
    assert!(jsonschema::draft202012::options()
        .with_retriever(NoRetrieval)
        .build(&json!({"allOf": []}))
        .is_err());
    for reference in [
        "https://example.invalid/schema.json",
        "file:///never-read-runtime-schema.json",
    ] {
        assert!(jsonschema::draft202012::options()
            .with_retriever(NoRetrieval)
            .build(&json!({"$ref": reference}))
            .is_err());
    }
}

#[test]
fn correlation_checks_ids_and_method_pair_without_claiming_native_binding() {
    let request = ControlDocument::decode(
        ControlRoot::RetireRequest,
        &fixture(ControlRoot::RetireRequest),
    )
    .unwrap();
    let mut response: Value =
        serde_json::from_slice(&fixture(ControlRoot::RetireResponse)).unwrap();
    response["id"] = request.value()["id"].clone();
    let encode = |value: &Value| {
        ControlDocument::decode(
            ControlRoot::RetireResponse,
            &serde_json::to_vec(value).unwrap(),
        )
        .unwrap()
    };
    encode(&response).correlate(&request).unwrap();
    response["id"] = json!("99999999-9999-4999-8999-999999999999");
    assert!(matches!(
        encode(&response).correlate(&request),
        Err(ControlError::Correlation)
    ));
    let mut error: Value = serde_json::from_slice(&fixture(ControlRoot::RpcErrorResponse)).unwrap();
    error["id"] = Value::Null;
    let error = ControlDocument::decode(
        ControlRoot::RpcErrorResponse,
        &serde_json::to_vec(&error).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        error.correlate(&request),
        Err(ControlError::Correlation)
    ));
    assert!(!format!("{request:?}").contains(request.value()["id"].as_str().unwrap()));
}
