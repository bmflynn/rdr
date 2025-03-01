use std::{collections::HashSet, fs::File, path::PathBuf};

use ccsds::spacepacket::Apid;
use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Deserialize)]
pub struct SatSpec {
    /// Satellite id, e.g., npp, j01, etc ...
    pub id: String,
    /// Collection short name, e.g., VIIRS-SCIENCE-RDR. Sometimes referred to as just collection.
    ///
    /// See CDFCB-X, Appendix A
    pub short_name: String,
    /// Mission base time as IET microseconds.
    ///
    /// This is described in the CDFCB as "Time of first ascending node after launch", however, it
    /// has the same value for for all JPSS spacecraft.
    ///
    /// From CDFCB-X, Table 3.5.12.-1
    /// |Spacecraft|Basetime        |
    /// |----------|----------------|
    /// |SNPP      |1698019234000000|
    /// |JPSS-1    |1698019234000000|
    /// |JPSS-2    |1698019234000000|
    /// |JPSS-3    |1698019234000000|
    /// |JPSS-4    |1698019234000000|
    /// |GCOM-W1   |1715904034000000|
    /// |GOSAT-GW  |1767225635000000|
    pub base_time: u64,
    /// Mission, e.g., S-NPP/JPSS
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
    /// The product identifier, e.g., RVIRS, RNSCA, etc...
    ///
    /// See CDFCB-X, Appendix A.
    pub product_id: String,
    #[serde(default)]
    pub sensor: String,
    /// See [SatSpec::short_name]
    pub short_name: String,
    /// Data type, e.g., SCIENCE, DIARY, etc ...
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
    /// Data product id.
    ///
    /// See CDFCB-X Vol 1, Appendix A.
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
static J04_CONFIG: &str = include_str!(concat!(env!("OUT_DIR"), "/j04.config.yaml"));

/// Get default YAML configuration content for `satid`.
pub fn get_default_content(satid: &str) -> Option<&'static str> {
    match satid {
        "npp" => Some(NPP_CONFIG),
        "j01" => Some(J01_CONFIG),
        "j02" => Some(J02_CONFIG),
        "j03" => Some(J03_CONFIG),
        "j04" => Some(J04_CONFIG),
        _ => None,
    }
}

/// Get default [Config] for `satid`, or `None` if `satid` is unknown.
///
/// # Panics
/// If the build-in RDR configuration is not valid.
pub fn get_default(satid: &str) -> Option<Config> {
    Some(Config::with_data(get_default_content(satid)?).expect("invalid built-in RDR config"))
}
