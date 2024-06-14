use anyhow::{bail, Result};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::PathBuf,
};

use ccsds::Apid;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SatSpec {
    pub id: String,
    pub short_name: String,
    pub base_time: u64,
    pub mission: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApidSpec {
    pub num: Apid,
    pub name: String,
    pub max_expected: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProductSpec {
    pub product_id: String,
    #[serde(default)]
    pub sensor: String,
    pub short_name: String,
    pub type_id: String,
    pub gran_len: u64,
    pub apids: Vec<ApidSpec>,
}

impl ProductSpec {
    #[must_use]
    pub fn get_apid(&self, apid: Apid) -> Option<ApidSpec> {
        // FIXME: make this more efficient
        for spec in &self.apids {
            if spec.num == apid {
                return Some(spec.clone());
            }
        }
        None
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RdrSpec {
    pub product: String,
    #[serde(default)]
    pub packed_with: Vec<String>,
}

// Per-satellite RDR configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub origin: String,
    pub mode: String,
    pub distributor: String,
    pub satellite: SatSpec,
    pub products: Vec<ProductSpec>,
    pub rdrs: Vec<RdrSpec>,
}

impl Config {
    fn validate(self) -> Result<Self> {
        // Make sure products only specify valid packed products
        let mut product_ids: HashSet<String> = HashSet::default();
        for product in &self.products {
            product_ids.insert(product.product_id.clone());
        }
        for rdr in &self.rdrs {
            for packed_id in &rdr.packed_with {
                if !product_ids.contains(packed_id) {
                    bail!(
                        "product {} has invalid packed product {}",
                        rdr.product,
                        packed_id
                    );
                }
            }
        }

        Ok(self)
    }

    pub fn with_path(fpath: &PathBuf) -> Result<Config> {
        let fin = File::open(fpath)?;
        let config: Config = serde_yaml::from_reader(fin)?;

        config.validate()
    }

    fn with_data(dat: &str) -> Result<Config> {
        let config: Config = serde_yaml::from_str(dat)?;
        config.validate()
    }
}

use lazy_static::lazy_static;
lazy_static! {
    static ref DEFAULT_CONFIG: HashMap<&'static str, &'static str> = HashMap::from([
        (
            "npp",
            include_str!(concat!(env!("OUT_DIR"), "/npp.config.yaml")),
        ),
        (
            "j01",
            include_str!(concat!(env!("OUT_DIR"), "/j01.config.yaml")),
        ),
        (
            "j02",
            include_str!(concat!(env!("OUT_DIR"), "/j02.config.yaml")),
        ),
        (
            "j03",
            include_str!(concat!(env!("OUT_DIR"), "/j03.config.yaml")),
        ),
    ]);
}

pub fn get_default_content(satid: &str) -> Option<String> {
    DEFAULT_CONFIG.get(satid).map(|s| (*s).to_string())
}

pub fn get_default(satid: &str) -> Result<Config> {
    match DEFAULT_CONFIG.get(satid) {
        Some(dat) => Config::with_data(dat),
        None => bail!("No default config for satellite id {satid}"),
    }
}
