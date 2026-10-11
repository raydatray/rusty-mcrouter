use std::fmt::Write;

pub trait MetricsSource: Send + Sync {
    fn encode(&self, out: &mut MetricsText);

    fn render(&self) -> String {
        let mut out = MetricsText::new();
        self.encode(&mut out);
        out.buf
    }
}

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

#[cfg(test)]
mod tests {
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
        assert_eq!(
            Static.render(),
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
    fn empty_source_renders_empty() {
        struct Empty;
        impl MetricsSource for Empty {
            fn encode(&self, _out: &mut MetricsText) {}
        }
        assert_eq!(Empty.render(), "");
    }
}
