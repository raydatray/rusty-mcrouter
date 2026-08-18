// the facts -> metrics boundary: sources read leaf-crate counters at
// scrape time and encode bare prometheus text (no HELP/TYPE - both are
// advisory; the upstream cross-reference lives in the catalog doc).
// the scrape path may allocate; the write path never does.

use std::fmt::Write;

pub trait MetricsSource: Send + Sync {
    fn encode(&self, out: &mut MetricsText);
}

/// metric names and label keys are trusted constants; label values are
/// escaped (pool names come from config, addrs from the wire).
pub struct MetricsText {
    buf: String,
}

impl MetricsText {
    fn new() -> Self {
        Self { buf: String::new() }
    }

    pub fn counter(&mut self, name: &str, labels: &[(&str, &str)], value: u64) {
        self.sample(name, labels, &value.to_string());
    }

    /// i64: a bugged gauge renders as -3, not a u64 wrap
    pub fn gauge(&mut self, name: &str, labels: &[(&str, &str)], value: i64) {
        self.sample(name, labels, &value.to_string());
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: &str) {
        self.buf.push_str(name);
        if !labels.is_empty() {
            self.buf.push('{');
            for (i, (key, val)) in labels.iter().enumerate() {
                if i > 0 {
                    self.buf.push(',');
                }
                self.buf.push_str(key);
                self.buf.push_str("=\"");
                escape_label_value(&mut self.buf, val);
                self.buf.push('"');
            }
            self.buf.push('}');
        }
        let _ = writeln!(self.buf, " {value}");
    }
}

fn escape_label_value(out: &mut String, val: &str) {
    for c in val.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

#[derive(Default)]
pub struct MetricsRegistry {
    sources: Vec<Box<dyn MetricsSource>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, source: Box<dyn MetricsSource>) {
        self.sources.push(source);
    }

    pub fn render(&self) -> String {
        let mut out = MetricsText::new();
        for source in &self.sources {
            source.encode(&mut out);
        }
        out.buf
    }
}

/// declares a MetricsSource over a Vec of counter shards: each field is
/// summed across shards and emitted as one bare sample. matrices and
/// labeled walks don't fit - write those encode() impls by hand.
#[macro_export]
macro_rules! shard_source {
    (
        $(#[$meta:meta])*
        pub struct $source:ident($shard:ty) wrapped {
            $( $kind:ident $field:ident => $name:literal; )*
        }
    ) => {
        $(#[$meta])*
        pub struct $source {
            pub shards: Vec<std::sync::Arc<$shard>>,
        }

        impl $crate::metrics::MetricsSource for $source {
            fn encode(&self, out: &mut $crate::metrics::MetricsText) {
                $( $crate::shard_source!(@emit_wrapped $kind, self, out, $field, $name); )*
            }
        }
    };

    (
        $(#[$meta:meta])*
        pub struct $source:ident($shard:ty) {
            $( $kind:ident $field:ident => $name:literal; )*
        }
    ) => {
        $(#[$meta])*
        pub struct $source {
            pub shards: Vec<std::sync::Arc<$shard>>,
        }

        impl $crate::metrics::MetricsSource for $source {
            fn encode(&self, out: &mut $crate::metrics::MetricsText) {
                use std::sync::atomic::Ordering;
                $( $crate::shard_source!(@emit $kind, self, out, $field, $name); )*
            }
        }
    };

    (@emit counter, $self:ident, $out:ident, $field:ident, $name:literal) => {
        $out.counter(
            $name,
            &[],
            $self.shards.iter().map(|s| s.$field.load(Ordering::Relaxed)).sum::<u64>(),
        );
    };

    (@emit gauge, $self:ident, $out:ident, $field:ident, $name:literal) => {
        $out.gauge(
            $name,
            &[],
            $self.shards.iter().map(|s| s.$field.load(Ordering::Relaxed)).sum::<i64>(),
        );
    };

    (@emit_wrapped counter, $self:ident, $out:ident, $field:ident, $name:literal) => {
        $out.counter(
            $name,
            &[],
            $self.shards.iter().map(|s| s.$field.load()).sum::<u64>(),
        );
    };

    (@emit_wrapped gauge, $self:ident, $out:ident, $field:ident, $name:literal) => {
        $out.gauge(
            $name,
            &[],
            $self.shards.iter().map(|s| s.$field.load()).sum::<i64>(),
        );
    };
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicU64};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn renders_bare_samples() {
        struct Static;
        impl MetricsSource for Static {
            fn encode(&self, out: &mut MetricsText) {
                out.counter("test_requests_total", &[], 42);
                out.counter("test_cmds_total", &[("command", "mg")], 1);
                out.counter("test_cmds_total", &[("command", "ms")], 2);
                out.gauge("test_inflight", &[("proxy", "0")], -3);
            }
        }
        let mut registry = MetricsRegistry::new();
        registry.register(Box::new(Static));
        assert_eq!(
            registry.render(),
            "test_requests_total 42\n\
             test_cmds_total{command=\"mg\"} 1\n\
             test_cmds_total{command=\"ms\"} 2\n\
             test_inflight{proxy=\"0\"} -3\n"
        );
    }

    #[test]
    fn label_values_are_escaped() {
        let mut out = MetricsText::new();
        out.sample("m", &[("k", "a\"b\\c\nd")], "1");
        assert_eq!(out.buf, "m{k=\"a\\\"b\\\\c\\nd\"} 1\n");
    }

    #[test]
    fn multiple_labels_join_with_commas() {
        let mut out = MetricsText::new();
        out.sample("m", &[("a", "1"), ("b", "2")], "9");
        assert_eq!(out.buf, "m{a=\"1\",b=\"2\"} 9\n");
    }

    #[test]
    fn empty_registry_renders_empty() {
        assert_eq!(MetricsRegistry::new().render(), "");
    }

    #[test]
    fn sources_render_in_registration_order() {
        struct A;
        struct B;
        impl MetricsSource for A {
            fn encode(&self, out: &mut MetricsText) {
                out.counter("a", &[], 1);
            }
        }
        impl MetricsSource for B {
            fn encode(&self, out: &mut MetricsText) {
                out.counter("b", &[], 2);
            }
        }
        let mut registry = MetricsRegistry::new();
        registry.register(Box::new(A));
        registry.register(Box::new(B));
        assert_eq!(registry.render(), "a 1\nb 2\n");
    }

    #[test]
    fn shard_source_macro_sums_across_shards() {
        #[derive(Default)]
        struct FakeShard {
            hits: AtomicU64,
            depth: AtomicI64,
        }

        shard_source! {
            pub struct FakeSource(FakeShard) {
                counter hits  => "fake_hits_total";
                gauge   depth => "fake_depth";
            }
        }

        let s1 = Arc::new(FakeShard::default());
        let s2 = Arc::new(FakeShard::default());
        s1.hits.store(3, std::sync::atomic::Ordering::Relaxed);
        s2.hits.store(4, std::sync::atomic::Ordering::Relaxed);
        s1.depth.store(5, std::sync::atomic::Ordering::Relaxed);
        s2.depth.store(-2, std::sync::atomic::Ordering::Relaxed);

        let mut registry = MetricsRegistry::new();
        registry.register(Box::new(FakeSource {
            shards: vec![s1, s2],
        }));
        assert_eq!(registry.render(), "fake_hits_total 7\nfake_depth 3\n");
    }
}
