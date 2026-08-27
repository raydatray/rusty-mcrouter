use std::{
    fmt::{self, Display},
    str::FromStr,
};

use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RoutingPrefix {
    value: String,
    separator: usize,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("routing prefix `{0}` must have form /region/cluster")]
pub struct RoutingPrefixError(String);

impl FromStr for RoutingPrefix {
    type Err = RoutingPrefixError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut value = s.to_owned();

        if !value.starts_with('/') {
            value.insert(0, '/');
        }

        if !value.ends_with('/') {
            value.push('/');
        }

        let body = value
            .strip_prefix('/')
            .and_then(|body| body.strip_suffix('/'))
            .ok_or_else(|| RoutingPrefixError(s.to_owned()))?;

        let (region, cluster) = body
            .split_once('/')
            .ok_or_else(|| RoutingPrefixError(s.to_owned()))?;

        if region.is_empty() || cluster.is_empty() || cluster.contains('/') {
            return Err(RoutingPrefixError(s.to_owned()));
        }

        let separator = region.len() + 1;

        Ok(Self { value, separator })
    }
}

impl RoutingPrefix {
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.value.as_bytes()
    }

    pub fn region(&self) -> &[u8] {
        &self.value.as_bytes()[1..self.separator]
    }

    pub fn cluster(&self) -> &[u8] {
        &self.value.as_bytes()[self.separator + 1..self.value.len() - 1]
    }
}

impl Display for RoutingPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_missing_slashes() {
        for input in ["us/cache", "/us/cache", "us/cache/"] {
            let prefix: RoutingPrefix = input.parse().unwrap();
            assert_eq!(prefix.as_str(), "/us/cache/");
            assert_eq!(prefix.region(), b"us");
            assert_eq!(prefix.cluster(), b"cache");
        }
    }

    #[test]
    fn accepts_wildcard_components() {
        assert!("/*/*/".parse::<RoutingPrefix>().is_ok());
        assert!("/us/*/".parse::<RoutingPrefix>().is_ok());
    }

    #[test]
    fn rejects_malformed_prefixes() {
        for input in ["", "/", "us", "/us/", "//cache/", "/us//", "/a/b/c/"] {
            assert!(input.parse::<RoutingPrefix>().is_err(), "{input}");
        }
    }
}
