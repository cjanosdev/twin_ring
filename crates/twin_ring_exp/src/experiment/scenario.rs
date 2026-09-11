//! The disruption to apply to one independently run cache strategy.
use anyhow::{Result, bail};
use clap::ValueEnum;
use serde::Serialize;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Scenario {
    Overload,
    NodeOutage,
}

impl Scenario {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Overload => "overload",
            Self::NodeOutage => "node-outage",
        }
    }
    pub fn protocol(self) -> &'static str {
        match self {
            Self::Overload => "overload_only_v1",
            Self::NodeOutage => "node_outage_v1",
        }
    }
}

impl FromStr for Scenario {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "overload" => Ok(Self::Overload),
            "node-outage" => Ok(Self::NodeOutage),
            _ => bail!("TR_SCENARIO must be overload or node-outage"),
        }
    }
}
