use std::path::Path;

use anyhow::Context;
use rusty_mcrouter_config::ConfigDocument;

pub(crate) fn load(path: &Path) -> anyhow::Result<(Vec<u8>, ConfigDocument)> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read config `{}`", path.display()))?;
    let document = parse(&bytes)?;
    Ok((bytes, document))
}

pub(crate) fn parse(bytes: &[u8]) -> anyhow::Result<ConfigDocument> {
    let text = std::str::from_utf8(bytes).context("config is not valid UTF-8")?;
    Ok(rusty_mcrouter_config::parse(text)?)
}
