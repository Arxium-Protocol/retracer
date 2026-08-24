//! Tier A of the address-extraction design (Retracer_AddressExtraction_Plan.md
//! §2/§4): a declarative `kind_schema.toml` describing which JSON paths in
//! an action's `payload` are addresses, and what role they play. Resolved
//! generically over `(kind: &str, payload: &serde_json::Value)`, so this
//! module never needs to know a chain's actual payload type.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Fixed role vocabulary (plan §4) — free-text roles from a third-party
/// config would be unmanageable, so anything outside this set must go
/// through the explicit `other:<label>` escape valve rather than being
/// silently accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    From,
    To,
    ValidatorSubject,
    Delegator,
    Other(String),
}

impl Role {
    pub fn as_str(&self) -> &str {
        match self {
            Role::From => "from",
            Role::To => "to",
            Role::ValidatorSubject => "validator_subject",
            Role::Delegator => "delegator",
            Role::Other(label) => label,
        }
    }

    fn parse(raw: &str) -> Result<Role> {
        Ok(match raw {
            "from" => Role::From,
            "to" => Role::To,
            "validator_subject" => Role::ValidatorSubject,
            "delegator" => Role::Delegator,
            other => match other.strip_prefix("other:") {
                Some(label) if !label.is_empty() => Role::Other(label.to_string()),
                _ => bail!(
                    "unknown role {other:?} — must be one of from/to/validator_subject/delegator, \
                     or other:<label> for a novel role"
                ),
            },
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default)]
    kind: Vec<RawKind>,
}

#[derive(Debug, Deserialize)]
struct RawKind {
    name: String,
    #[serde(default)]
    roles: Vec<RawRole>,
    /// Declarative projections — payload paths to make queryable. See
    /// [`Projection`].
    #[serde(default)]
    index: Vec<RawIndex>,
}

#[derive(Debug, Deserialize)]
struct RawIndex {
    path: String,
    #[serde(default = "default_projection_type")]
    r#type: String,
}

fn default_projection_type() -> String {
    "text".to_string()
}

#[derive(Debug, Deserialize)]
struct RawRole {
    path: String,
    role: String,
}

/// SQL type a projected payload path is indexed as.
///
/// A closed set, not a free-text SQL type, for the same reason [`Role`] is a
/// closed set — except here it also matters for safety: this string is
/// interpolated into DDL, and accepting arbitrary text from a config file
/// would make `kind_schema.toml` an injection vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionType {
    Text,
    Numeric,
    BigInt,
}

impl ProjectionType {
    /// The cast used in the index expression. Only ever one of these three
    /// literals — never config-supplied text.
    pub fn sql_cast(&self) -> &'static str {
        match self {
            ProjectionType::Text => "TEXT",
            ProjectionType::Numeric => "NUMERIC",
            ProjectionType::BigInt => "BIGINT",
        }
    }

    fn parse(raw: &str) -> Result<ProjectionType> {
        Ok(match raw {
            "text" => ProjectionType::Text,
            "numeric" => ProjectionType::Numeric,
            "bigint" => ProjectionType::BigInt,
            other => bail!("unknown projection type {other:?} — must be text, numeric, or bigint"),
        })
    }
}

/// A payload path a builder has declared queryable.
///
/// This is the cheap alternative to the mapping runtimes every other indexer
/// ships: rather than executing builder-supplied code to derive entities, we
/// let a builder name a JSON path and hand the work to a Postgres expression
/// index. It covers the part of "custom queryable fields" that Spoke Chains
/// actually need, with no generated schema and no user code in-process.
#[derive(Debug, Clone)]
pub struct Projection {
    pub kind: String,
    /// Path segments, already validated as safe identifiers.
    pub segments: Vec<String>,
    pub ty: ProjectionType,
}

impl Projection {
    /// `payload->'a'->'b'->>'c'` for path `a.b.c` — the JSON accessor this
    /// projection indexes. Safe to interpolate because every segment was
    /// checked against [`is_safe_segment`] at parse time.
    pub fn json_accessor(&self) -> String {
        let (last, parents) = self.segments.split_last().expect("validated non-empty");
        let mut expr = "payload".to_string();
        for segment in parents {
            expr.push_str(&format!("->'{segment}'"));
        }
        expr.push_str(&format!("->>'{last}'"));
        expr
    }

    /// Deterministic, always-valid index name. Postgres caps identifiers at 63
    /// bytes and a kind or path can be longer than that, so a readable prefix
    /// is combined with a hash of the full definition: the prefix keeps
    /// `\di` output legible, the hash keeps two projections that truncate to
    /// the same prefix from colliding.
    pub fn index_name(&self) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.kind.hash(&mut hasher);
        self.segments.hash(&mut hasher);
        self.ty.sql_cast().hash(&mut hasher);
        let digest = hasher.finish();

        let sanitize = |s: &str| -> String {
            s.chars()
                .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
                .take(18)
                .collect()
        };
        format!(
            "actions_proj_{}_{}_{:08x}",
            sanitize(&self.kind),
            sanitize(&self.segments.join("_")),
            digest as u32
        )
    }
}

/// Path segments become part of DDL, so they are restricted to plain
/// identifiers. Anything with a quote, backslash, or whitespace is rejected at
/// startup rather than escaped — a payload field named `a'; DROP TABLE` is not
/// a real field, and refusing is a better answer than quoting it correctly.
fn is_safe_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment.len() <= 63
        && segment.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// One resolved `(json path, role)` rule for a given action `kind`.
#[derive(Debug)]
struct FieldRole {
    path: String,
    role: Role,
}

/// Parsed, validated `kind_schema.toml` — maps action `kind` to the
/// address-bearing fields in its payload. Kinds with no entry simply aren't
/// represented in `action_addresses` beyond `from_address` (plan §2).
#[derive(Debug)]
pub struct KindSchema {
    kinds: HashMap<String, Vec<FieldRole>>,
    projections: Vec<Projection>,
}

impl KindSchema {
    pub fn empty() -> KindSchema {
        KindSchema { kinds: HashMap::new(), projections: Vec::new() }
    }

    /// Parses and validates `path` (TOML). Fails loud — an unknown role
    /// string or malformed file is a startup error, not a silent gap.
    pub fn load(path: &Path) -> Result<KindSchema> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading kind schema config {}", path.display()))?;
        Self::parse(&raw).with_context(|| format!("parsing kind schema config {}", path.display()))
    }

    fn parse(raw: &str) -> Result<KindSchema> {
        let config: RawConfig = toml::from_str(raw)?;
        let mut kinds = HashMap::new();
        let mut projections = Vec::new();
        for kind in config.kind {
            let roles = kind
                .roles
                .into_iter()
                .map(|r| Ok(FieldRole { path: r.path, role: Role::parse(&r.role)? }))
                .collect::<Result<Vec<_>>>()
                .with_context(|| format!("kind {:?}", kind.name))?;

            for raw_index in kind.index {
                let ty = ProjectionType::parse(&raw_index.r#type)
                    .with_context(|| format!("kind {:?}", kind.name))?;
                let segments: Vec<String> = raw_index
                    .path
                    .trim_start_matches("$.")
                    .split('.')
                    .map(str::to_string)
                    .collect();
                if segments.is_empty() || !segments.iter().all(|s| is_safe_segment(s)) {
                    bail!(
                        "kind {:?}: index path {:?} is not a plain dotted field path — \
                         segments must be non-empty and contain only letters, digits, \
                         and underscores",
                        kind.name,
                        raw_index.path
                    );
                }
                projections.push(Projection { kind: kind.name.clone(), segments, ty });
            }

            kinds.insert(kind.name, roles);
        }
        Ok(KindSchema { kinds, projections })
    }

    /// Payload paths this config declares queryable, in file order.
    pub fn projections(&self) -> &[Projection] {
        &self.projections
    }

    /// Every `(address, role)` pair this `kind`'s payload implies, per the
    /// loaded config. Paths that don't resolve to a JSON string (missing
    /// field, wrong shape) are skipped rather than treated as an error —
    /// payloads are still stored in full regardless (plan §2).
    pub fn resolve<'a>(&'a self, kind: &str, payload: &serde_json::Value) -> Vec<(String, &'a Role)> {
        let Some(roles) = self.kinds.get(kind) else {
            return Vec::new();
        };
        roles
            .iter()
            .filter_map(|field| resolve_path(payload, &field.path).and_then(|v| v.as_str()).map(|addr| (addr.to_string(), &field.role)))
            .collect()
    }
}

/// Tier B (Retracer_AddressExtraction_Plan.md §6/§8): a dotted-path
/// config (Tier A, `KindSchema`) can't express a conditional or computed
/// address role. An `ActionIndexable` impl claims a `kind` outright and
/// computes its own `(address, role)` pairs for it, in Rust rather than
/// TOML. No impls ship with Arxium's own kinds today — this is an escape
/// hatch for a second builder's payload shape, registered through
/// `retracer-core::run`, which a config-only Tier A can't reach.
pub trait ActionIndexable: Send + Sync {
    /// The action `kind` this impl resolves. Only one impl may claim a given
    /// kind — `AddressExtractor::new` panics on a duplicate, since a silent
    /// "last one wins" would make Tier B strictly worse than the config file
    /// it's replacing for that kind.
    fn kind(&self) -> &str;
    fn resolve(&self, payload: &serde_json::Value) -> Vec<(String, Role)>;
}

/// Tier A (`KindSchema`) plus any registered Tier B `ActionIndexable` impls,
/// behind the single `resolve` call site every consumer (`insert_block`,
/// `SubscribeAccountActions`) already uses. Precedence is per-kind, not
/// merged: a kind claimed by a Tier B impl is resolved by that impl only —
/// Tier A's entry for the same kind, if any, is never consulted — so a given
/// kind has exactly one place defining its roles.
pub struct AddressExtractor {
    tier_a: KindSchema,
    tier_b: HashMap<String, Box<dyn ActionIndexable>>,
}

impl AddressExtractor {
    /// Panics if two Tier B impls claim the same `kind()` — a config error
    /// on the embedder's part, not a runtime condition to degrade under.
    pub fn new(tier_a: KindSchema, tier_b: Vec<Box<dyn ActionIndexable>>) -> AddressExtractor {
        let mut by_kind = HashMap::with_capacity(tier_b.len());
        for extractor in tier_b {
            let kind = extractor.kind().to_string();
            if by_kind.insert(kind.clone(), extractor).is_some() {
                panic!("two Tier B ActionIndexable impls both claim kind {kind:?}");
            }
        }
        AddressExtractor { tier_a, tier_b: by_kind }
    }

    pub fn tier_a_only(tier_a: KindSchema) -> AddressExtractor {
        AddressExtractor { tier_a, tier_b: HashMap::new() }
    }

    pub fn resolve(&self, kind: &str, payload: &serde_json::Value) -> Vec<(String, Role)> {
        if let Some(extractor) = self.tier_b.get(kind) {
            return extractor.resolve(payload);
        }
        self.tier_a.resolve(kind, payload).into_iter().map(|(addr, role)| (addr, role.clone())).collect()
    }
}

/// Minimal dotted-path resolver over `serde_json::Value` — `$.field` or
/// `$.nested.field`. Not a full JSONPath engine (plan §2 deliberately skips
/// that); no array-index support since no known payload needs it (plan §8).
fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let rest = path.strip_prefix("$.")?;
    rest.split('.').try_fold(value, |v, segment| v.as_object()?.get(segment))
}

#[cfg(test)]
mod tests {
    use super::{KindSchema, Projection, ProjectionType};

    fn schema(toml: &str) -> KindSchema {
        KindSchema::parse(toml).expect("valid schema")
    }

    #[test]
    fn projection_paths_become_json_accessors() {
        let p = Projection {
            kind: "Transfer".into(),
            segments: vec!["amount".into()],
            ty: ProjectionType::Numeric,
        };
        assert_eq!(p.json_accessor(), "payload->>'amount'");

        let nested = Projection {
            kind: "Transfer".into(),
            segments: vec!["meta".into(), "memo".into(), "tag".into()],
            ty: ProjectionType::Text,
        };
        // Intermediate hops use `->` (JSON), only the last uses `->>` (text),
        // otherwise the cast has nothing to work on.
        assert_eq!(nested.json_accessor(), "payload->'meta'->'memo'->>'tag'");
    }

    #[test]
    fn projection_parses_dollar_prefixed_paths_like_roles_do() {
        let s = schema(
            r#"
            [[kind]]
            name = "Transfer"
            index = [{ path = "$.amount", type = "numeric" }]
            "#,
        );
        assert_eq!(s.projections().len(), 1);
        assert_eq!(s.projections()[0].segments, vec!["amount".to_string()]);
        assert_eq!(s.projections()[0].ty, ProjectionType::Numeric);
    }

    #[test]
    fn projection_type_defaults_to_text() {
        let s = schema(
            r#"
            [[kind]]
            name = "Transfer"
            index = [{ path = "memo" }]
            "#,
        );
        assert_eq!(s.projections()[0].ty, ProjectionType::Text);
    }

    #[test]
    fn unknown_projection_type_is_a_startup_error() {
        let err = KindSchema::parse(
            r#"
            [[kind]]
            name = "Transfer"
            index = [{ path = "amount", type = "jsonb; DROP TABLE actions" }]
            "#,
        )
        .expect_err("must reject");
        assert!(format!("{err:#}").contains("unknown projection type"));
    }

    /// The projection path is interpolated into DDL, so a path that isn't a
    /// plain identifier has to be refused at startup rather than escaped —
    /// this is the injection boundary for `kind_schema.toml`.
    #[test]
    fn unsafe_projection_paths_are_refused() {
        for path in ["a'); DROP TABLE actions; --", "amount)::text, (1", "with space", "", "a.'b"] {
            let toml = format!(
                r#"
                [[kind]]
                name = "Transfer"
                index = [{{ path = "{path}" }}]
                "#
            );
            assert!(
                KindSchema::parse(&toml).is_err(),
                "path {path:?} should have been refused"
            );
        }
    }

    #[test]
    fn index_names_are_valid_deterministic_and_distinct() {
        let a = Projection {
            kind: "Transfer".into(),
            segments: vec!["amount".into()],
            ty: ProjectionType::Numeric,
        };
        // Same definition, same name — startup must be idempotent, since the
        // DDL relies on CREATE INDEX IF NOT EXISTS matching an existing name.
        assert_eq!(a.index_name(), a.clone().index_name());

        // Differing only by cast still has to produce a distinct index.
        let b = Projection { ty: ProjectionType::Text, ..a.clone() };
        assert_ne!(a.index_name(), b.index_name());

        let long = Projection {
            kind: "A".repeat(90),
            segments: vec!["b".repeat(90)],
            ty: ProjectionType::Text,
        };
        let name = long.index_name();
        assert!(name.len() <= 63, "Postgres truncates identifiers past 63 bytes: {name}");
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "index name must be a bare identifier: {name}"
        );
    }

    use super::*;

    #[test]
    fn resolves_configured_roles_and_skips_unconfigured_kinds() {
        let schema = KindSchema::parse(
            r#"
            [[kind]]
            name = "Transfer"

              [[kind.roles]]
              path = "$.to"
              role = "to"

            [[kind]]
            name = "Stake"

              [[kind.roles]]
              path = "$.validator"
              role = "validator_subject"
            "#,
        )
        .unwrap();

        let transfer_payload = serde_json::json!({"to": "addr1", "amount": 5});
        assert_eq!(schema.resolve("Transfer", &transfer_payload), vec![("addr1".to_string(), &Role::To)]);

        let leave_payload = serde_json::Value::Null;
        assert!(schema.resolve("LeaveValidator", &leave_payload).is_empty());
    }

    #[test]
    fn rejects_unknown_role_strings() {
        let err = KindSchema::parse(
            r#"
            [[kind]]
            name = "Transfer"

              [[kind.roles]]
              path = "$.to"
              role = "recipient"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown role") || format!("{err:#}").contains("unknown role"));
    }

    #[test]
    fn accepts_explicit_other_role() {
        let schema = KindSchema::parse(
            r#"
            [[kind]]
            name = "Custom"

              [[kind.roles]]
              path = "$.counterparty"
              role = "other:counterparty"
            "#,
        )
        .unwrap();
        let payload = serde_json::json!({"counterparty": "addrX"});
        let resolved = schema.resolve("Custom", &payload);
        assert_eq!(resolved, vec![("addrX".to_string(), &Role::Other("counterparty".to_string()))]);
    }

    struct FixedExtractor;
    impl ActionIndexable for FixedExtractor {
        fn kind(&self) -> &str {
            "Transfer"
        }
        fn resolve(&self, _payload: &serde_json::Value) -> Vec<(String, Role)> {
            vec![("computed-addr".to_string(), Role::Other("computed".to_string()))]
        }
    }

    #[test]
    fn tier_b_wins_over_tier_a_for_the_same_kind() {
        let tier_a = KindSchema::parse(
            r#"
            [[kind]]
            name = "Transfer"

              [[kind.roles]]
              path = "$.to"
              role = "to"
            "#,
        )
        .unwrap();
        let extractor = AddressExtractor::new(tier_a, vec![Box::new(FixedExtractor)]);

        let payload = serde_json::json!({"to": "addr1"});
        assert_eq!(
            extractor.resolve("Transfer", &payload),
            vec![("computed-addr".to_string(), Role::Other("computed".to_string()))]
        );
    }

    #[test]
    fn tier_a_still_resolves_kinds_no_tier_b_impl_claims() {
        let tier_a = KindSchema::parse(
            r#"
            [[kind]]
            name = "Transfer"

              [[kind.roles]]
              path = "$.to"
              role = "to"
            "#,
        )
        .unwrap();
        let extractor = AddressExtractor::new(tier_a, vec![Box::new(FixedExtractor)]);

        let payload = serde_json::json!({"validator": "addr2"});
        assert!(extractor.resolve("Stake", &payload).is_empty());
    }

    #[test]
    #[should_panic(expected = "both claim kind")]
    fn duplicate_tier_b_kind_claim_panics() {
        AddressExtractor::new(KindSchema::empty(), vec![Box::new(FixedExtractor), Box::new(FixedExtractor)]);
    }
}
