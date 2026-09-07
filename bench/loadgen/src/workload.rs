use std::sync::Arc;

use bytes::BytesMut;
use serde::Deserialize;
use thiserror::Error;

const MAX_KEY_BYTES: usize = 250;
const MAX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    #[serde(default = "default_seed")]
    pub seed: u64,
    pub keyspace: u64,
    #[serde(default)]
    pub prewarm_fraction: f64,
    pub ops: OpMix,
    pub keys: KeySpec,
    #[serde(default)]
    pub values: ValueSpec,
    #[serde(default)]
    pub prefixes: Vec<WeightedPrefix>,
}

impl WorkloadSpec {
    pub fn parse(input: &str) -> Result<Self, WorkloadError> {
        Ok(toml::from_str(input)?)
    }

    pub fn validate(self) -> Result<Workload, WorkloadError> {
        Workload::try_from(self)
    }
}

fn default_seed() -> u64 {
    42
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpMix {
    pub mg: Option<f64>,
    pub ms: Option<f64>,
    pub md: Option<f64>,
    pub ma: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    #[serde(default)]
    pub distribution: KeyDistribution,
    #[serde(default = "default_theta")]
    pub theta: f64,
    pub length: Range<usize>,
}

fn default_theta() -> f64 {
    0.99
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum KeyDistribution {
    #[default]
    Uniform,
    Zipf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueSpec {
    #[serde(default = "default_buckets")]
    pub buckets: Vec<ValueBucket>,
    #[serde(default = "default_ttl")]
    pub ttl: Range<u32>,
}

impl Default for ValueSpec {
    fn default() -> Self {
        Self {
            buckets: default_buckets(),
            ttl: default_ttl(),
        }
    }
}

fn default_buckets() -> Vec<ValueBucket> {
    vec![ValueBucket {
        weight: 1.0,
        min: 32,
        max: 256,
    }]
}

fn default_ttl() -> Range<u32> {
    Range {
        min: 3600,
        max: 3600,
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueBucket {
    pub weight: f64,
    pub min: usize,
    pub max: usize,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Range<T> {
    pub min: T,
    pub max: T,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeightedPrefix {
    pub value: String,
    pub weight: f64,
}

#[derive(Debug, Error)]
pub enum WorkloadError {
    #[error("parse workload TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid workload: {0}")]
    Invalid(String),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    Mg = 0,
    Ms = 1,
    Md = 2,
    Ma = 3,
}

impl OpKind {
    pub const ALL: [Self; 4] = [Self::Mg, Self::Ms, Self::Md, Self::Ma];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Mg => "mg",
            Self::Ms => "ms",
            Self::Md => "md",
            Self::Ma => "ma",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Workload {
    pub seed: u64,
    keyspace: u64,
    prewarm_fraction: f64,
    key_min: usize,
    key_max: usize,
    ttl: Range<u32>,
    prefixes: Vec<WeightedPrefix>,
    prefix_weights: Option<Choice>,
    key_sampler: KeySampler,
    op_weights: Choice,
    value_weights: Choice,
    buckets: Vec<ValueBucket>,
    max_value: usize,
}

impl TryFrom<WorkloadSpec> for Workload {
    type Error = WorkloadError;

    fn try_from(spec: WorkloadSpec) -> Result<Self, Self::Error> {
        if spec.keyspace == 0 {
            return invalid("keyspace must be greater than zero");
        }
        if !spec.prewarm_fraction.is_finite() || !(0.0..=1.0).contains(&spec.prewarm_fraction) {
            return invalid("prewarm_fraction must be finite and in 0..=1");
        }
        if spec.keys.length.min == 0 || spec.keys.length.min > spec.keys.length.max {
            return invalid("keys.length must satisfy 1 <= min <= max");
        }
        let identity_len = 1 + decimal_len(spec.keyspace);
        if spec.keys.length.max < identity_len {
            return invalid(format!(
                "keys.length.max must be at least {identity_len} for this keyspace"
            ));
        }
        if spec.keys.length.max > MAX_KEY_BYTES {
            return invalid(format!("keys.length.max must be at most {MAX_KEY_BYTES}"));
        }
        if spec.values.ttl.min > spec.values.ttl.max || spec.values.ttl.max > i32::MAX as u32 {
            return invalid(format!(
                "values.ttl must satisfy min <= max <= {}",
                i32::MAX
            ));
        }
        if spec.values.buckets.is_empty() {
            return invalid("values.buckets must not be empty");
        }

        let op_values = [spec.ops.mg, spec.ops.ms, spec.ops.md, spec.ops.ma];
        let mut op_weights = Vec::with_capacity(op_values.len());
        for (name, weight) in ["mg", "ms", "md", "ma"].into_iter().zip(op_values) {
            op_weights.push(optional_weight(&format!("ops.{name}"), weight)?);
        }
        if op_weights.iter().all(|weight| *weight == 0.0) {
            return invalid("at least one operation weight must be configured");
        }

        let mut bucket_weights = Vec::with_capacity(spec.values.buckets.len());
        let mut max_value = 0;
        for (index, bucket) in spec.values.buckets.iter().enumerate() {
            bucket_weights.push(weight(
                &format!("values.buckets[{index}].weight"),
                bucket.weight,
            )?);
            if bucket.min > bucket.max || bucket.max > MAX_VALUE_BYTES {
                return invalid(format!(
                    "values.buckets[{index}] must satisfy min <= max <= {MAX_VALUE_BYTES}"
                ));
            }
            max_value = max_value.max(bucket.max);
        }

        let mut prefix_weights = Vec::with_capacity(spec.prefixes.len());
        for (index, prefix) in spec.prefixes.iter().enumerate() {
            prefix_weights.push(weight(&format!("prefixes[{index}].weight"), prefix.weight)?);
            if prefix
                .value
                .as_bytes()
                .iter()
                .any(|byte| *byte <= b' ' || *byte == 0x7f)
            {
                return invalid(format!(
                    "prefixes[{index}].value is not a valid text key prefix"
                ));
            }
            if prefix.value.len() + spec.keys.length.max > MAX_KEY_BYTES {
                return invalid(format!(
                    "prefixes[{index}].value plus keys.length.max exceeds {MAX_KEY_BYTES} bytes"
                ));
            }
        }

        let key_sampler = match spec.keys.distribution {
            KeyDistribution::Uniform => KeySampler::Uniform,
            KeyDistribution::Zipf => {
                if !spec.keys.theta.is_finite() || spec.keys.theta <= 0.0 || spec.keys.theta >= 1.0
                {
                    return invalid("keys.theta must be finite and in 0..1 for Zipf keys");
                }
                KeySampler::Zipf(Zipf::new(spec.keyspace, spec.keys.theta))
            }
        };

        let prefix_weights = if prefix_weights.is_empty() {
            None
        } else {
            Some(Choice::new(&prefix_weights, "prefix weights")?)
        };

        Ok(Self {
            seed: spec.seed,
            keyspace: spec.keyspace,
            prewarm_fraction: spec.prewarm_fraction,
            key_min: spec.keys.length.min,
            key_max: spec.keys.length.max,
            ttl: spec.values.ttl,
            prefixes: spec.prefixes,
            prefix_weights,
            key_sampler,
            op_weights: Choice::new(&op_weights, "operation weights")?,
            value_weights: Choice::new(&bucket_weights, "value bucket weights")?,
            buckets: spec.values.buckets,
            max_value,
        })
    }
}

impl Workload {
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn generator(self: &Arc<Self>, stream: u64) -> Generator {
        Generator::new(self.clone(), stream)
    }

    pub fn prewarm_keys_per_prefix(&self) -> u64 {
        (self.keyspace as f64 * self.prewarm_fraction).floor() as u64
    }

    pub fn prewarm_prefixes(&self) -> usize {
        self.prefixes.len().max(1)
    }

    pub fn prewarm_total(&self) -> Result<u64, WorkloadError> {
        self.prewarm_keys_per_prefix()
            .checked_mul(self.prewarm_prefixes() as u64)
            .ok_or_else(|| WorkloadError::Invalid("prewarm key count overflows u64".to_string()))
    }
}

fn optional_weight(name: &str, value: Option<f64>) -> Result<f64, WorkloadError> {
    match value {
        Some(value) => weight(name, value),
        None => Ok(0.0),
    }
}

fn weight(name: &str, value: f64) -> Result<f64, WorkloadError> {
    if !value.is_finite() || value <= 0.0 {
        return invalid(format!("{name} must be finite and greater than zero"));
    }
    Ok(value)
}

fn invalid<T>(message: impl Into<String>) -> Result<T, WorkloadError> {
    Err(WorkloadError::Invalid(message.into()))
}

fn decimal_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 10 {
        value /= 10;
        len += 1;
    }
    len
}

#[derive(Clone, Debug)]
struct Choice {
    cdf: Vec<f64>,
}

impl Choice {
    fn new(weights: &[f64], name: &str) -> Result<Self, WorkloadError> {
        let total: f64 = weights.iter().sum();
        if !total.is_finite() || total <= 0.0 {
            return invalid(format!("{name} must have a finite positive sum"));
        }
        let mut sum = 0.0;
        let mut cdf = Vec::with_capacity(weights.len());
        for weight in weights {
            sum += weight / total;
            cdf.push(sum);
        }
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }
        Ok(Self { cdf })
    }

    fn sample(&self, rng: &mut Rng) -> usize {
        let value = rng.next_f64();
        self.cdf
            .iter()
            .position(|limit| value < *limit)
            .unwrap_or(self.cdf.len() - 1)
    }
}

#[derive(Clone, Debug)]
enum KeySampler {
    Uniform,
    Zipf(Zipf),
}

impl KeySampler {
    fn sample(&self, rng: &mut Rng, keyspace: u64) -> u64 {
        match self {
            Self::Uniform => rng.below(keyspace) + 1,
            Self::Zipf(zipf) => zipf.sample(rng),
        }
    }
}

#[derive(Clone, Debug)]
struct Zipf {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipf {
    fn new(n: u64, theta: f64) -> Self {
        if n == 1 {
            return Self {
                n,
                theta,
                alpha: 1.0,
                zetan: 1.0,
                eta: 0.0,
            };
        }
        let exact = n.min(2_000_000);
        let mut zetan = (1..=exact)
            .map(|rank| 1.0 / (rank as f64).powf(theta))
            .sum::<f64>();
        if n > exact {
            zetan +=
                ((n as f64).powf(1.0 - theta) - (exact as f64).powf(1.0 - theta)) / (1.0 - theta);
        }
        let zeta2 = 1.0 + 0.5f64.powf(theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self {
            n,
            theta,
            alpha,
            zetan,
            eta,
        }
    }

    fn sample(&self, rng: &mut Rng) -> u64 {
        if self.n == 1 {
            return 1;
        }
        let uniform = rng.next_f64();
        let scaled = uniform * self.zetan;
        if scaled < 1.0 {
            return 1;
        }
        if scaled < 1.0 + 0.5f64.powf(self.theta) {
            return 2;
        }
        let rank =
            1 + (self.n as f64 * (self.eta * uniform - self.eta + 1.0).powf(self.alpha)) as u64;
        rank.min(self.n)
    }
}

#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    fn below(&mut self, upper: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(upper)) >> 64) as u64
    }

    fn range(&mut self, range: Range<u64>) -> u64 {
        if range.min == range.max {
            range.min
        } else {
            range.min + self.below(range.max - range.min + 1)
        }
    }
}

pub struct Generator {
    workload: Arc<Workload>,
    rng: Rng,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Generator {
    fn new(workload: Arc<Workload>, stream: u64) -> Self {
        let value = (0..workload.max_value)
            .map(|index| b'a' + (index % 26) as u8)
            .collect();
        let seed = workload.seed.wrapping_mul(0x1234_5678_9abc_def1) ^ stream;
        Self {
            workload,
            rng: Rng::new(seed),
            key: Vec::with_capacity(MAX_KEY_BYTES),
            value,
        }
    }

    pub fn next_request(&mut self, out: &mut BytesMut) -> OpKind {
        let kind = OpKind::ALL[self.workload.op_weights.sample(&mut self.rng)];
        let index = self
            .workload
            .key_sampler
            .sample(&mut self.rng, self.workload.keyspace);
        let prefix = self
            .workload
            .prefix_weights
            .as_ref()
            .map(|weights| weights.sample(&mut self.rng));
        self.write_key(index, kind == OpKind::Ma, prefix);
        match kind {
            OpKind::Mg => encode_key_only(out, b"mg ", &self.key, b" v\r\n"),
            OpKind::Ms => {
                let bucket = self.workload.value_weights.sample(&mut self.rng);
                let bucket = self.workload.buckets[bucket];
                let length = self.rng.range(Range {
                    min: bucket.min as u64,
                    max: bucket.max as u64,
                }) as usize;
                let ttl = self.ttl();
                encode_store(out, &self.key, &self.value[..length], ttl);
            }
            OpKind::Md => encode_key_only(out, b"md ", &self.key, b"\r\n"),
            OpKind::Ma => {
                let ttl = self.ttl();
                out.extend_from_slice(b"ma ");
                out.extend_from_slice(&self.key);
                out.extend_from_slice(b" N");
                write_u64(out, u64::from(ttl));
                out.extend_from_slice(b" J0\r\n");
            }
        }
        kind
    }

    pub fn prewarm_request(&mut self, prefix: usize, index: u64, out: &mut BytesMut) {
        self.write_key(
            index,
            false,
            (!self.workload.prefixes.is_empty()).then_some(prefix),
        );
        let bucket = self.workload.value_weights.sample(&mut self.rng);
        let bucket = self.workload.buckets[bucket];
        let length = self.rng.range(Range {
            min: bucket.min as u64,
            max: bucket.max as u64,
        }) as usize;
        let ttl = self.ttl();
        encode_store(out, &self.key, &self.value[..length], ttl);
    }

    fn ttl(&mut self) -> u32 {
        self.rng.range(Range {
            min: u64::from(self.workload.ttl.min),
            max: u64::from(self.workload.ttl.max),
        }) as u32
    }

    fn write_key(&mut self, index: u64, counter: bool, prefix: Option<usize>) {
        self.key.clear();
        if let Some(prefix) = prefix {
            self.key
                .extend_from_slice(self.workload.prefixes[prefix].value.as_bytes());
        }
        let body_start = self.key.len();
        self.key.push(if counter { b'c' } else { b'k' });
        write_u64_vec(&mut self.key, index);
        let width = self.workload.key_max - self.workload.key_min + 1;
        let target = self.workload.key_min + (index.wrapping_mul(0x9e37_79b9) as usize % width);
        while self.key.len() - body_start < target {
            self.key.push(b'_');
        }
    }
}

fn encode_key_only(out: &mut BytesMut, command: &[u8], key: &[u8], suffix: &[u8]) {
    out.extend_from_slice(command);
    out.extend_from_slice(key);
    out.extend_from_slice(suffix);
}

fn encode_store(out: &mut BytesMut, key: &[u8], value: &[u8], ttl: u32) {
    out.extend_from_slice(b"ms ");
    out.extend_from_slice(key);
    out.extend_from_slice(b" ");
    write_u64(out, value.len() as u64);
    out.extend_from_slice(b" T");
    write_u64(out, u64::from(ttl));
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

fn write_u64(out: &mut BytesMut, value: u64) {
    let mut digits = [0; 20];
    out.extend_from_slice(decimal(value, &mut digits));
}

fn write_u64_vec(out: &mut Vec<u8>, value: u64) {
    let mut digits = [0; 20];
    out.extend_from_slice(decimal(value, &mut digits));
}

fn decimal(mut value: u64, digits: &mut [u8; 20]) -> &[u8] {
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            return &digits[start..];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
keyspace = 100
prewarm_fraction = 0.5

[ops]
mg = 4
ms = 2
md = 1
ma = 1

[keys]
distribution = "zipf"
theta = 0.9
length = { min = 8, max = 12 }

[values]
ttl = { min = 10, max = 20 }
buckets = [{ weight = 1, min = 3, max = 3 }]
"#;

    fn valid() -> Workload {
        WorkloadSpec::parse(VALID).unwrap().validate().unwrap()
    }

    #[test]
    fn generation_is_deterministic_and_directly_encoded() {
        let workload = Arc::new(valid());
        let mut first = workload.generator(7);
        let mut second = workload.generator(7);
        for _ in 0..100 {
            let mut left = BytesMut::new();
            let mut right = BytesMut::new();
            assert_eq!(
                first.next_request(&mut left),
                second.next_request(&mut right)
            );
            assert_eq!(left, right);
            assert!(left.ends_with(b"\r\n"));
        }
    }

    #[test]
    fn prewarm_includes_every_prefix_namespace() {
        let input = format!(
            "{VALID}\n[[prefixes]]\nvalue = \"/a/b/\"\nweight = 1\n\n[[prefixes]]\nvalue = \"\"\nweight = 1\n"
        );
        let workload = Arc::new(WorkloadSpec::parse(&input).unwrap().validate().unwrap());
        assert_eq!(workload.prewarm_total().unwrap(), 100);
        let mut generator = workload.generator(1);
        let mut first = BytesMut::new();
        let mut second = BytesMut::new();
        generator.prewarm_request(0, 1, &mut first);
        generator.prewarm_request(1, 1, &mut second);
        assert!(first.starts_with(b"ms /a/b/k1"));
        assert!(second.starts_with(b"ms k1"));
    }

    #[test]
    fn rejects_invalid_weights_and_ranges() {
        for replacement in ["mg = 0", "mg = -1", "mg = nan", "mg = inf"] {
            let input = VALID.replacen("mg = 4", replacement, 1);
            let error = WorkloadSpec::parse(&input)
                .unwrap()
                .validate()
                .unwrap_err()
                .to_string();
            assert!(error.contains("ops.mg"), "{error}");
        }

        let bad_range = VALID.replacen("min = 8, max = 12", "min = 12, max = 8", 1);
        assert!(WorkloadSpec::parse(&bad_range).unwrap().validate().is_err());
        let bad_fraction = VALID.replacen("0.5", "nan", 1);
        assert!(WorkloadSpec::parse(&bad_fraction)
            .unwrap()
            .validate()
            .is_err());
    }

    #[test]
    fn permits_omitted_operation_weights() {
        let input = VALID
            .replace("ms = 2\n", "")
            .replace("md = 1\n", "")
            .replace("ma = 1\n", "");
        assert!(WorkloadSpec::parse(&input).unwrap().validate().is_ok());
    }
}
