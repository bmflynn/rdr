use std::{collections::HashSet, fs::File, path::PathBuf};

use ccsds::spacepacket::Apid;
use serde::Deserialize;

use crate::error::{Error, Result};

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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
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
                    return Err(Error::ConfigInvalid(format!(
                        "product {} has invalid packed product {}",
                        rdr.product, packed_id
                    )));
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

static NPP_CONFIG: &str = include_str!(concat!(env!("OUT_DIR"), "/npp.config.yaml"));
static J01_CONFIG: &str = include_str!(concat!(env!("OUT_DIR"), "/j01.config.yaml"));
static J02_CONFIG: &str = include_str!(concat!(env!("OUT_DIR"), "/j02.config.yaml"));
static J03_CONFIG: &str = include_str!(concat!(env!("OUT_DIR"), "/j03.config.yaml"));

pub fn get_default_content(satid: &str) -> Option<&'static str> {
    match satid {
        "npp" => Some(NPP_CONFIG),
        "j01" => Some(J01_CONFIG),
        "j02" => Some(J02_CONFIG),
        "j03" => Some(J03_CONFIG),
        _ => None,
    }
}

pub fn get_default(satid: &str) -> Result<Option<Config>> {
    match get_default_content(satid) {
        Some(cfg) => Ok(Some(Config::with_data(cfg)?)),
        None => Ok(None),
    }
}
