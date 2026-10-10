//! The generator. Hostile data on demand, reproducible from `--seed`.
//!
//! What it deliberately produces, because these are the cases hand-written fixtures
//! do not surface:
//!
//! * nested objects, at several depths
//! * optional fields present on some lines of a template and absent on others
//! * ten-digit integers that ARE Unix timestamps (`ts_unix`) and ten-digit integers
//!   that are NOT (`order_id`), drawn across the whole ten-digit range - the ones naming
//!   a moment inside the epoch window are indistinguishable to rule 2, which is the
//!   point, and the ones outside it make `order_id` a column of two types, which no
//!   hand-written fixture produces either
//! * unicode string values, including CJK, emoji, RTL and combining accents
//! * empty strings
//! * explicit JSON nulls, which are a different thing from an absent key
//! * arrays, which flattening refuses to descend into
//! * field order shuffled per line, so the fingerprint has to earn its stability

use std::io::Write;

use serde_json::{Map, Value};

use crate::error::GenError;
use kibbr_core::rng::Rng;

const LEVELS: &[&str] = &["debug", "info", "warn", "error", "fatal"];
const EVENTS: &[&str] = &[
    "login_failed",
    "login_ok",
    "cache_miss",
    "payment_declined",
    "index_flush",
    "rate_limited",
];
const SERVICES: &[&str] = &["auth", "gateway", "billing", "search", "ingest"];
const REASONS: &[&str] = &[
    "invalid_password",
    "expired_token",
    "unknown_user",
    "mfa_required",
    "upstream_timeout",
];
const WORDS: &[&str] = &["auth", "login", "retry", "cold", "shard", "eu-west-1", "v2"];
const REGIONS: &[&str] = &["us-east-1", "eu-west-1", "ap-southeast-2"];
const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE"];

/// Unicode that is not decoration: CJK, emoji including a ZWJ sequence, RTL, a
/// combining accent, and a string whose char count and byte length disagree.
const UNICODE: &[&str] = &[
    "ログインに失敗しました",
    "用户未找到",
    "échec d'authentification",
    "🔥 rate limit hit 🚨",
    "👩‍💻 operator override",
    "فشل تسجيل الدخول",
    "e\u{301}chec",
    "ᏣᎳᎩ",
];

/// The generators a field can draw from.
#[derive(Clone, Copy)]
enum Gen {
    /// RFC 3339 - rule 1.
    Rfc3339,
    /// Ten digits, and genuinely a timestamp - rule 2, correctly.
    EpochSecs,
    /// Thirteen digits, milliseconds - rule 2, correctly.
    EpochMillis,
    /// Ten digits, and NOT a timestamp. Rule 2 will call it one anyway.
    OrderId10,
    /// Eleven or twelve digits - falls past rule 2 to rule 6.
    BigInt,
    SmallInt,
    Float,
    Bool,
    Uuid,
    Ipv4,
    Ipv6,
    Email,
    Level,
    Event,
    Service,
    Reason,
    Region,
    Method,
    Unicode,
    /// A present key whose value is the empty string.
    Empty,
    /// A present key whose value is null - not the same as an absent key.
    Null,
    /// A JSON array, which flattening keeps as a raw string.
    Array,
    /// A ten-digit numeric *string* rather than a JSON number: same ladder verdict,
    /// reached from the other side.
    EpochSecsAsString,
}

#[derive(Clone, Copy)]
struct Field {
    /// Dotted path; every segment but the last becomes a nested object.
    path: &'static str,
    gen: Gen,
    /// Percent of lines of this template on which the key is present at all.
    presence: u64,
}

const fn always(path: &'static str, gen: Gen) -> Field {
    Field {
        path,
        gen,
        presence: 100,
    }
}

const fn maybe(path: &'static str, gen: Gen, presence: u64) -> Field {
    Field {
        path,
        gen,
        presence,
    }
}

struct Template {
    fields: Vec<Field>,
}

/// Template 0 is fixed and carries every hostile case, so that even
/// `--templates 1` exercises them.
fn kitchen_sink() -> Template {
    Template {
        fields: vec![
            always("ts", Gen::Rfc3339),
            always("event", Gen::Event),
            always("level", Gen::Level),
            always("ts_unix", Gen::EpochSecs),
            always("order_id", Gen::OrderId10),
            always("request_id", Gen::Uuid),
            always("user.id", Gen::SmallInt),
            always("user.email", Gen::Email),
            always("user.profile.locale.tag", Gen::Unicode),
            always("client.ip", Gen::Ipv4),
            always("tags", Gen::Array),
            always("note", Gen::Empty),
            always("deleted_at", Gen::Null),
            maybe("retry_after", Gen::SmallInt, 45),
            maybe("upstream.latency_ms", Gen::Float, 60),
            maybe("upstream.trace", Gen::EpochSecsAsString, 30),
            maybe("client.ipv6", Gen::Ipv6, 25),
            maybe("reason", Gen::Reason, 70),
        ],
    }
}

fn archetypes() -> Vec<Template> {
    vec![
        kitchen_sink(),
        Template {
            fields: vec![
                always("ts", Gen::Rfc3339),
                always("event", Gen::Event),
                always("level", Gen::Level),
                always("service", Gen::Service),
                always("user_id", Gen::SmallInt),
                maybe("reason", Gen::Reason, 80),
                maybe("attempt", Gen::SmallInt, 50),
            ],
        },
        Template {
            fields: vec![
                always("ts_ms", Gen::EpochMillis),
                always("event", Gen::Event),
                always("http.method", Gen::Method),
                always("http.status", Gen::SmallInt),
                always("http.path", Gen::Unicode),
                always("client.ip", Gen::Ipv4),
                maybe("http.duration_ms", Gen::Float, 90),
                maybe("http.referrer", Gen::Empty, 35),
            ],
        },
        Template {
            fields: vec![
                always("ts_unix", Gen::EpochSecs),
                always("event", Gen::Event),
                always("order_id", Gen::OrderId10),
                always("amount", Gen::Float),
                always("currency", Gen::Unicode),
                always("customer.id", Gen::Uuid),
                always("customer.email", Gen::Email),
                maybe("customer.vat_id", Gen::BigInt, 40),
                maybe("declined_reason", Gen::Null, 55),
            ],
        },
        Template {
            fields: vec![
                always("ts", Gen::Rfc3339),
                always("level", Gen::Level),
                always("service", Gen::Service),
                always("region", Gen::Region),
                always("shard.id", Gen::SmallInt),
                always("shard.leader", Gen::Bool),
                always("shard.rows", Gen::BigInt),
                maybe("shard.checkpoint", Gen::EpochMillis, 65),
            ],
        },
        Template {
            fields: vec![
                always("ts", Gen::Rfc3339),
                always("event", Gen::Event),
                always("cache.key", Gen::Unicode),
                always("cache.hit", Gen::Bool),
                always("cache.ttl", Gen::SmallInt),
                maybe("cache.origin.host", Gen::Ipv6, 50),
                maybe("cache.origin.port", Gen::SmallInt, 50),
                maybe("tags", Gen::Array, 40),
            ],
        },
        Template {
            fields: vec![
                always("ts_unix", Gen::EpochSecs),
                always("level", Gen::Level),
                always("service", Gen::Service),
                always("msg", Gen::Empty),
                always("err.code", Gen::SmallInt),
                always("err.trace_id", Gen::Uuid),
                maybe("err.cause.code", Gen::SmallInt, 60),
                maybe("err.cause.detail", Gen::Unicode, 60),
            ],
        },
        Template {
            fields: vec![
                always("ts", Gen::Rfc3339),
                always("event", Gen::Event),
                always("device.serial", Gen::OrderId10),
                always("device.fw", Gen::Float),
                always("device.online", Gen::Bool),
                always("device.owner.email", Gen::Email),
                maybe("device.owner.nickname", Gen::Unicode, 70),
                maybe("device.last_seen", Gen::EpochSecsAsString, 50),
            ],
        },
    ]
}

/// `--templates T` distinct fingerprint families. Beyond the archetype count,
/// archetypes are reused with an extra nested group appended, so the count is exact.
fn build_templates(count: usize) -> Result<Vec<Template>, GenError> {
    if count == 0 {
        return Err(GenError::NoTemplates);
    }
    let base = archetypes();
    let n = base.len();
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let mut fields = base[i % n].fields.clone();
        if i >= n {
            // Reuse a shape, but give it a distinct key set so it is a distinct
            // template rather than a duplicate fingerprint.
            let group = i / n;
            fields.push(always(extension_path(group), Gen::Unicode));
            fields.push(maybe(extension_epoch_path(group), Gen::OrderId10, 60));
        }
        out.push(Template { fields });
    }
    Ok(out)
}

/// Leaked on purpose: a handful of `'static` key names for the synthesized
/// templates, sized by `--templates` and living as long as the process.
fn extension_path(n: usize) -> &'static str {
    Box::leak(format!("ext{n}.label").into_boxed_str())
}

fn extension_epoch_path(n: usize) -> &'static str {
    Box::leak(format!("ext{n}.ref_id").into_boxed_str())
}

/// A fixed base instant so output depends on the seed and nothing else - never on
/// the wall clock.
const BASE_EPOCH_SECS: i64 = 1_705_314_600; // 2024-01-15T10:30:00Z

fn value_for(gen: Gen, rng: &mut Rng) -> Value {
    match gen {
        Gen::Rfc3339 => {
            let secs = BASE_EPOCH_SECS + rng.between(0, 60 * 60 * 24 * 30);
            Value::String(rfc3339(secs))
        }
        Gen::EpochSecs => Value::from(BASE_EPOCH_SECS + rng.between(0, 60 * 60 * 24 * 30)),
        Gen::EpochSecsAsString => {
            Value::String((BASE_EPOCH_SECS + rng.between(0, 60 * 60 * 24 * 30)).to_string())
        }
        Gen::EpochMillis => Value::from(
            (BASE_EPOCH_SECS + rng.between(0, 60 * 60 * 24 * 30)) * 1000 + rng.between(0, 999),
        ),
        // Ten digits, so rule 2 claims the ones naming a moment inside the epoch window -
        // about a third of this range - as timestamps. All of them are order IDs.
        Gen::OrderId10 => Value::from(rng.between(1_000_000_000, 9_999_999_999)),
        Gen::BigInt => Value::from(rng.between(10_000_000_000, 999_999_999_999)),
        Gen::SmallInt => Value::from(rng.between(0, 9999)),
        Gen::Float => {
            let cents = rng.between(1, 500_000);
            Value::from(cents as f64 / 100.0)
        }
        Gen::Bool => Value::Bool(rng.chance(50)),
        Gen::Uuid => Value::String(uuid(rng)),
        Gen::Ipv4 => Value::String(format!(
            "{}.{}.{}.{}",
            rng.between(1, 223),
            rng.between(0, 255),
            rng.between(0, 255),
            rng.between(1, 254)
        )),
        Gen::Ipv6 => Value::String(format!(
            "2001:db8:{:x}:{:x}::{:x}",
            rng.below(0xffff),
            rng.below(0xffff),
            rng.below(0xffff)
        )),
        Gen::Email => Value::String(format!(
            "u{}@{}.example.com",
            rng.between(1, 99999),
            rng.pick(SERVICES)
        )),
        Gen::Level => Value::String((*rng.pick(LEVELS)).to_string()),
        Gen::Event => Value::String((*rng.pick(EVENTS)).to_string()),
        Gen::Service => Value::String((*rng.pick(SERVICES)).to_string()),
        Gen::Reason => Value::String((*rng.pick(REASONS)).to_string()),
        Gen::Region => Value::String((*rng.pick(REGIONS)).to_string()),
        Gen::Method => Value::String((*rng.pick(METHODS)).to_string()),
        Gen::Unicode => Value::String((*rng.pick(UNICODE)).to_string()),
        // Empty most of the time, so the column is not uniformly empty.
        Gen::Empty => {
            if rng.chance(70) {
                Value::String(String::new())
            } else {
                Value::String((*rng.pick(WORDS)).to_string())
            }
        }
        Gen::Null => Value::Null,
        Gen::Array => {
            let len = rng.below(4) as usize;
            let items: Vec<Value> = (0..len)
                .map(|_| Value::String((*rng.pick(WORDS)).to_string()))
                .collect();
            Value::Array(items)
        }
    }
}

/// Hand-rolled so the output is fixed by the seed alone.
fn rfc3339(secs: i64) -> String {
    // Days since the Unix epoch, then a civil date from the days count.
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's civil-from-days.
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn uuid(rng: &mut Rng) -> String {
    let a = rng.next_u64();
    let b = rng.next_u64();
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        a >> 32,
        (a >> 16) & 0xffff,
        a & 0xfff,
        0x8000 | (b >> 48) & 0x3fff,
        b & 0xffff_ffff_ffff
    )
}

/// Insert a dotted path into a nested object, creating intermediate objects.
/// A non-object already sitting at an intermediate segment is left alone and the
/// insertion is dropped; the generator never produces that collision.
fn insert_path(root: &mut Map<String, Value>, path: &str, value: Value) {
    let mut segments = path.split('.').peekable();
    let mut cur = root;
    while let Some(segment) = segments.next() {
        if segments.peek().is_none() {
            cur.insert(segment.to_string(), value);
            return;
        }
        let entry = cur
            .entry(segment.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        match entry.as_object_mut() {
            Some(next) => cur = next,
            None => return,
        }
    }
}

/// Write `lines` NDJSON records. Byte-identical for a given `(lines, templates, seed)`.
pub fn generate<W: Write>(
    out: &mut W,
    lines: u64,
    templates: usize,
    seed: u64,
) -> Result<(), GenError> {
    let templates = build_templates(templates)?;
    let mut rng = Rng::new(seed);

    for _ in 0..lines {
        let template = &templates[rng.below(templates.len() as u64) as usize];

        // Decide presence first, then shuffle, so the serialized field order varies
        // independently of which fields are present.
        let mut present: Vec<(&str, Gen)> = template
            .fields
            .iter()
            .filter(|f| f.presence >= 100 || rng.chance(f.presence))
            .map(|f| (f.path, f.gen))
            .collect();
        rng.shuffle(&mut present);

        let mut obj = Map::new();
        for (path, gen) in present {
            let value = value_for(gen, &mut rng);
            insert_path(&mut obj, path, value);
        }

        let line = serde_json::to_string(&Value::Object(obj))?;
        out.write_all(line.as_bytes()).map_err(GenError::Write)?;
        out.write_all(b"\n").map_err(GenError::Write)?;
    }
    Ok(())
}
