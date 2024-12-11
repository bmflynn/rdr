use ccsds::spacepacket::{Apid, Packet};
use hdf5::{types::FixedAscii, Dataset, Group};
use hifitime::Epoch;
use serde::Serialize;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    path::Path,
};

use crate::error::*;

macro_rules! from_bytes4 {
    ($type:ty, $dat:ident, $start:expr) => {
        <$type>::from_be_bytes([
            $dat[$start],
            $dat[$start + 1],
            $dat[$start + 2],
            $dat[$start + 3],
        ])
    };
}

macro_rules! from_bytes8 {
    ($type:ty, $dat:ident, $start:expr) => {
        <$type>::from_be_bytes([
            $dat[$start],
            $dat[$start + 1],
            $dat[$start + 2],
            $dat[$start + 3],
            $dat[$start + 4],
            $dat[$start + 5],
            $dat[$start + 6],
            $dat[$start + 7],
        ])
    };
}

macro_rules! to_str {
    ($data:expr) => {
        std::str::from_utf8($data)?.trim_matches('\0').to_owned()
    };
}

use crate::config::{Config, ProductSpec, SatSpec};

/// Common RDR data structures and metadata.
#[derive(Clone)]
pub struct Rdr {
    /// The short product id for this rdr, e.g., RVIRS
    pub product_id: String,
    /// Any other products that are packed with this rdr, .e.g., RNSCA
    pub packed_with: Vec<String>,
    /// Granule time in IET microseconds
    pub begin_time_iet: u64,
    /// Granule time in IET microseconds
    pub end_time_iet: u64,
    /// Granule time in IET microseconds
    pub begin_time_utc: u64,
    /// Granule time in IET microseconds
    pub end_time_utc: u64,
    /// Common RDR static header
    pub header: StaticHeader,
    /// Common RDR ``ApidLists`` for each apid
    pub apids: HashMap<Apid, ApidInfo>,
    /// Common RDR ``PacketTrackers`` for each apid
    pub trackers: HashMap<Apid, Vec<PacketTracker>>,
    /// Common RDR packet storage area
    pub storage: VecDeque<Packet>,
    /// Time this RDR was created
    pub created: Epoch,
}

impl Rdr {
    pub fn new(product: &ProductSpec, sat: &SatSpec, gran_iet: u64, gran_utc: u64) -> Self {
        let mut rdr = Rdr {
            product_id: product.product_id.to_string(),
            packed_with: vec!["RNSCA".to_string()],
            begin_time_iet: gran_iet,
            end_time_iet: gran_iet + product.gran_len,
            begin_time_utc: gran_utc,
            end_time_utc: gran_utc + product.gran_len,
            header: StaticHeader::new(gran_iet, sat, product),
            apids: HashMap::default(),
            trackers: HashMap::default(),
            storage: VecDeque::default(),
            created: Epoch::now().expect("failed to get system time"),
        };

        for apid in &product.apids {
            rdr.apids.insert(
                apid.num,
                ApidInfo {
                    name: apid.name.clone(),
                    value: u32::from(apid.num),
                    pkt_tracker_start_idx: 0,
                    pkts_reserved: 0,
                    pkts_received: 0,
                },
            );
            rdr.trackers.insert(apid.num, Vec::default());
        }

        rdr
    }

    fn add_tracker(&mut self, gran_iet: u64, pkt: &Packet) {
        let trackers = self.trackers.entry(pkt.header.apid).or_default();
        let offset = match trackers.last() {
            Some(t) => t.offset + t.size,
            None => 0,
        };
        trackers.push(PacketTracker {
            obs_time: i64::try_from(gran_iet).expect("granule time to fit in i64"),
            sequence_number: i32::from(pkt.header.sequence_id),
            size: i32::try_from(pkt.data.len()).expect("pkt len to fit in i32"),
            offset,
            fill_percent: 0,
        });
    }

    fn update_apid_list(&mut self, pkt: &Packet) {
        let apid_list = self.apids.get_mut(&pkt.header.apid).unwrap_or_else(|| {
            panic!(
                "apid {} to be present in product {} because we already checked for it",
                pkt.header.apid, self.product_id
            )
        });
        apid_list.pkts_reserved += 1;
        apid_list.pkts_received += 1;
    }

    /// Add a packet and update the Common RDR structures and offsets.
    ///
    /// # Panics
    /// If the packet traker offset overflows
    pub fn add_packet(&mut self, gran_iet: u64, pkt: Packet) {
        self.add_tracker(gran_iet, &pkt);
        self.update_apid_list(&pkt);
        self.storage.push_back(pkt);

        // Update static header dynamic offsets
        self.header.pkt_tracker_offset =
            u32::try_from(StaticHeader::LEN + ApidInfo::LEN * self.apids.len()).unwrap();
        let num_trackers: usize = self.trackers.values().map(Vec::len).sum();
        self.header.ap_storage_offset = u32::try_from(
            self.header.pkt_tracker_offset as usize + PacketTracker::LEN * num_trackers,
        )
        .unwrap();
        let num_packet_bytes: usize = self.storage.iter().map(|p| p.data.len()).sum();
        self.header.next_pkt_position = u32::try_from(num_packet_bytes).unwrap();
    }

    /// Compile this RDR into its byte representation.
    ///
    /// # Panics
    /// If structure counts overflow rdr structure types
    #[must_use]
    pub fn compile(&self) -> Vec<u8> {
        let mut dat = Vec::new();
        // Static header should be good-to-go because it's updated on every call to add_packet
        dat.extend_from_slice(&self.header.as_bytes());

        let apids: Vec<u16> = self.apids.keys().copied().collect();
        // let mut apids: Vec<u16> = self.apids.keys().copied().collect();
        // apids.sort_unstable();

        // Write APID lists in numerical order
        let mut tracker_start_idx: u32 = 0;
        for apid in &apids {
            // update the list with current information regarding packet tracker config
            let mut list = self.apids.get(apid).unwrap().clone();
            list.pkt_tracker_start_idx = tracker_start_idx;
            dat.extend_from_slice(&list.as_bytes());
            // Assume tracker and lists have the same apids, and since we're handing apids in
            // order can can just use the num trackers for this apid
            tracker_start_idx += u32::try_from(self.trackers[apid].len()).unwrap();
        }

        for apid in apids {
            for tracker in &self.trackers[&apid] {
                dat.extend_from_slice(&tracker.as_bytes());
            }
        }

        for pkt in &self.storage {
            dat.extend_from_slice(&pkt.data);
        }

        dat
    }
    #[must_use]
    pub fn granule_dt(&self) -> Epoch {
        Epoch::from_unix_milliseconds(self.begin_time_utc as f64)
    }
}

const MAX_STR_LEN: usize = 1024;

impl Display for Rdr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rdr{{product={} granule=({}, {})}}",
            self.product_id,
            self.granule_dt(),
            self.begin_time_iet
        )
    }
}

macro_rules! attr_string {
    ($obj:expr, $name:expr) => {
        $obj.attr($name)?
            .read_2d::<FixedAscii<MAX_STR_LEN>>()?
            .to_string()
    };
}

macro_rules! attr_u64 {
    ($obj:expr, $name:expr) => {
        $obj.attr($name)?.read_2d::<u64>()?[[0, 0]]
    };
}

/// Metadata associated with a particular granule dataset from RDR path
/// /Data_Products/<collection>/<dataset>_Gran_<x>.
#[derive(Debug, Clone, Serialize)]
pub struct GranuleMeta {
    pub instrument: String,
    pub collection: String,
    pub begin_date: String,
    pub begin_time: String,
    pub begin_time_iet: u64,
    pub end_date: String,
    pub end_time: String,
    pub end_time_iet: u64,
    pub creation_date: String,
    pub creation_time: String,
    pub orbit_number: u64,
    pub id: String,
    pub status: String,
    pub version: String,
    pub idps_mode: String,
    pub jpss_doc: String,
    pub leoa_flag: String,
    pub packet_type: Vec<String>,
    pub packet_type_count: Vec<u32>,
    pub percent_missing: f32,
    pub reference_id: String,
    pub software_version: String,
}

impl GranuleMeta {
    fn from_dataset(instrument: &str, collection: &str, ds: &Dataset) -> Result<Self> {
        let packet_type: Vec<String> = ds
            .attr("N_Packet_Type")?
            .read_2d::<FixedAscii<MAX_STR_LEN>>()?
            .as_slice()
            .iter()
            .map(|fa| fa[0].to_string())
            .collect();
        let packet_type_count: Vec<u32> = ds
            .attr("N_Packet_Type_Count")?
            .read_2d::<u64>()?
            .as_slice()
            .iter()
            .map(|v| u32::try_from(v[0]).unwrap())
            .collect();
        Ok(Self {
            instrument: instrument.to_string(),
            collection: collection.to_string(),
            begin_date: attr_string!(&ds, "Beginning_Date"),
            begin_time: attr_string!(&ds, "Beginning_Time"),
            begin_time_iet: attr_u64!(&ds, "N_Beginning_Time_IET"),
            end_date: attr_string!(&ds, "Ending_Date"),
            end_time: attr_string!(&ds, "Ending_Time"),
            end_time_iet: attr_u64!(&ds, "N_Ending_Time_IET"),
            creation_date: attr_string!(&ds, "N_Creation_Date"),
            creation_time: attr_string!(&ds, "N_Creation_Time"),
            orbit_number: attr_u64!(&ds, "N_Beginning_Orbit_Number"),
            id: attr_string!(&ds, "N_Granule_ID"),
            status: attr_string!(&ds, "N_Granule_Status"),
            version: attr_string!(&ds, "N_Granule_Version"),
            idps_mode: attr_string!(&ds, "N_IDPS_Mode"),
            jpss_doc: attr_string!(&ds, "N_JPSS_Document_Ref"),
            leoa_flag: attr_string!(&ds, "N_LEOA_Flag"),
            packet_type,
            packet_type_count,
            percent_missing: 0.0,
            reference_id: attr_string!(&ds, "N_Reference_ID"),
            software_version: attr_string!(&ds, "N_Software_Version"),
        })
    }
}

/// Metadata associated with a particular product group from RDR path
/// /Data_Products/<collection>
#[derive(Debug, Clone, Serialize)]
pub struct ProductMeta {
    pub instrument: String,
    pub collection: String,
    pub processing_domain: String,
}

impl ProductMeta {
    fn from_group(grp: &Group) -> Result<Self> {
        Ok(Self {
            instrument: attr_string!(&grp, "Instrument_Short_Name"),
            collection: attr_string!(&grp, "N_Collection_Short_Name"),
            processing_domain: attr_string!(&grp, "N_Processing_Domain"),
        })
    }
}

/// RDR metadata generally representing the metadata (attributes) available
/// in a HDF5 file.
#[derive(Debug, Clone)]
pub struct Meta {
    pub distributor: String,
    pub mission: String,
    pub dataset_source: String,
    pub created: Epoch,
    pub platform: String,
    pub products: HashMap<String, ProductMeta>,
    pub granules: HashMap<String, Vec<GranuleMeta>>,
}

impl Meta {
    /// Create from the contents of a hdf5 file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = hdf5::File::open(path)?;
        let mut meta = Meta {
            distributor: attr_string!(&file, "Distributor"),
            mission: attr_string!(&file, "Mission_Name"),
            dataset_source: attr_string!(&file, "N_Dataset_Source"),
            platform: attr_string!(&file, "Platform_Short_Name"),
            created: Epoch::now().expect("failed to get system time"),
            products: HashMap::default(),
            granules: HashMap::default(),
        };

        let data_products = file.group("Data_Products")?;
        for product_group in data_products.groups()? {
            let product_meta = ProductMeta::from_group(&product_group)?;
            let product_name = &product_meta.collection.clone();

            // all datasets in product group, skipping _Aggr b/c we'll create our own aggr
            let gran_datasets = product_group
                .datasets()?
                .into_iter()
                .filter(|d| !d.name().ends_with("_Aggr"));
            for gran_dataset in gran_datasets {
                let gran_meta = GranuleMeta::from_dataset(
                    &product_meta.instrument,
                    &product_meta.collection,
                    &gran_dataset,
                )?;
                meta.granules
                    .entry(product_name.to_string())
                    .or_default()
                    .push(gran_meta);
            }

            meta.products.insert(product_name.clone(), product_meta);
        }

        Ok(meta)
    }

    /// Create a Meta configured for all products in `product_ids`.
    ///
    /// Returns `None` if either product are not found in `config`.
    pub fn from_products(product_ids: &[String], config: &Config) -> Option<Self> {
        let products = config
            .products
            .iter()
            .filter(|p| product_ids.contains(&p.short_name))
            .collect::<Vec<&ProductSpec>>();
        if products.is_empty() {
            return None;
        }
        Some(Meta {
            distributor: config.distributor.clone(),
            mission: config.satellite.mission.clone(),
            dataset_source: config.distributor.clone(),
            created: Epoch::now().expect("failed to get system time"),
            platform: config.satellite.short_name.clone(),
            products: products
                .iter()
                .map(|p| {
                    (
                        p.short_name.clone(),
                        ProductMeta {
                            instrument: p.sensor.clone(),
                            collection: p.short_name.clone(),
                            processing_domain: "ops".to_string(),
                        },
                    )
                })
                .collect(),
            granules: products
                .iter()
                .map(|p| (p.short_name.clone(), Vec::default()))
                .collect(),
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct StaticHeader {
    pub satellite: String, // 4-bytes
    pub sensor: String,    // 16-bytes
    pub type_id: String,   // 16-bytes
    pub num_apids: u32,
    pub apid_list_offset: u32,
    pub pkt_tracker_offset: u32,
    pub ap_storage_offset: u32,
    pub next_pkt_position: u32,
    pub start_boundary: i64,
    pub end_boundary: i64,
}

impl StaticHeader {
    pub const LEN: usize = 72;

    pub fn new(gran_iet: u64, sat: &SatSpec, product: &ProductSpec) -> Self {
        StaticHeader {
            satellite: sat.id.clone(),
            sensor: product.sensor.clone(),
            type_id: product.type_id.clone(),
            num_apids: u32::try_from(product.apids.len()).unwrap(),
            apid_list_offset: 72,
            pkt_tracker_offset: 0,
            ap_storage_offset: 0,
            next_pkt_position: 0,
            start_boundary: i64::try_from(gran_iet).expect("start_boundary time to fit in i64"),
            end_boundary: i64::try_from(gran_iet + product.gran_len)
                .expect("end_boundary time to fit in i64"),
        }
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < StaticHeader::LEN {
            return Err(Error::NotEnoughBytes("StaticHeader"));
        }
        let rdr = Self {
            satellite: to_str!(&data[0..4]),
            sensor: to_str!(&data[4..20]),
            type_id: to_str!(&data[20..36]),
            num_apids: from_bytes4!(u32, data, 36),
            apid_list_offset: from_bytes4!(u32, data, 40),
            pkt_tracker_offset: from_bytes4!(u32, data, 44),
            ap_storage_offset: from_bytes4!(u32, data, 48),
            next_pkt_position: from_bytes4!(u32, data, 52),
            start_boundary: from_bytes8!(i64, data, 56),
            end_boundary: from_bytes8!(i64, data, 64),
        };

        Ok(rdr)
    }

    #[must_use]
    pub fn as_bytes(&self) -> [u8; Self::LEN] {
        let mut buf = [0u8; Self::LEN];
        copy_with_len(&mut buf[..4], self.satellite.as_bytes(), 4);
        copy_with_len(&mut buf[4..20], self.sensor.as_bytes(), 4);
        copy_with_len(&mut buf[20..26], self.type_id.as_bytes(), 4);
        buf[36..40].copy_from_slice(&self.num_apids.to_be_bytes());
        buf[40..44].copy_from_slice(&self.apid_list_offset.to_be_bytes());
        buf[44..48].copy_from_slice(&self.pkt_tracker_offset.to_be_bytes());
        buf[48..52].copy_from_slice(&self.ap_storage_offset.to_be_bytes());
        buf[52..56].copy_from_slice(&self.next_pkt_position.to_be_bytes());
        buf[56..64].copy_from_slice(&self.start_boundary.to_be_bytes());
        buf[64..72].copy_from_slice(&self.end_boundary.to_be_bytes());

        buf
    }
}

/// Entry in the APID List.
#[derive(Debug, Clone)]
pub struct ApidInfo {
    pub name: String,
    pub value: u32,
    pub pkt_tracker_start_idx: u32,
    pub pkts_reserved: u32,
    pub pkts_received: u32,
}

impl ApidInfo {
    pub const LEN: usize = 32;

    #[must_use]
    pub fn as_bytes(&self) -> [u8; Self::LEN] {
        let mut buf = [0u8; Self::LEN];
        copy_with_len(&mut buf[..16], self.name.as_bytes(), 16);
        buf[16..20].copy_from_slice(&self.value.to_be_bytes());
        buf[20..24].copy_from_slice(&self.pkt_tracker_start_idx.to_be_bytes());
        buf[24..28].copy_from_slice(&self.pkts_reserved.to_be_bytes());
        buf[28..32].copy_from_slice(&self.pkts_received.to_be_bytes());

        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < ApidInfo::LEN {
            return Err(Error::NotEnoughBytes("ApidInfo"));
        }
        let info = Self {
            name: to_str!(&data[0..16]),
            value: from_bytes4!(u32, data, 16),
            pkt_tracker_start_idx: from_bytes4!(u32, data, 20),
            pkts_reserved: from_bytes4!(u32, data, 24),
            pkts_received: from_bytes4!(u32, data, 28),
        };

        Ok(info)
    }

    pub fn all_from_bytes(data: &[u8]) -> Result<Vec<Self>> {
        Ok(data
            .chunks(ApidInfo::LEN)
            .filter_map(|chunk| Self::from_bytes(chunk).ok())
            .collect::<Vec<Self>>())
    }
}

#[derive(Debug, Clone)]
pub struct PacketTracker {
    pub obs_time: i64,
    pub sequence_number: i32,
    pub size: i32,
    pub offset: i32,
    pub fill_percent: i32,
}

impl PacketTracker {
    pub const LEN: usize = 24;

    #[must_use]
    pub fn as_bytes(&self) -> [u8; Self::LEN] {
        let mut buf = [0u8; Self::LEN];
        buf[0..8].copy_from_slice(&self.obs_time.to_be_bytes());
        buf[8..12].copy_from_slice(&self.sequence_number.to_be_bytes());
        buf[12..16].copy_from_slice(&self.size.to_be_bytes());
        buf[16..20].copy_from_slice(&self.offset.to_be_bytes());
        buf[20..24].copy_from_slice(&self.offset.to_be_bytes());

        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < PacketTracker::LEN {
            return Err(Error::NotEnoughBytes("PacketTracker"));
        }
        let tracker = Self {
            obs_time: from_bytes8!(i64, data, 0),
            sequence_number: from_bytes4!(i32, data, 8),
            size: from_bytes4!(i32, data, 12),
            offset: from_bytes4!(i32, data, 16),
            fill_percent: from_bytes4!(i32, data, 20),
        };

        Ok(tracker)
    }
}

fn copy_with_len<'a>(dst: &'a mut [u8], src: &'a [u8], len: usize) {
    if src.len() < len {
        dst[..src.len()].copy_from_slice(src);
        for x in dst.iter_mut().skip(src.len()).take(len) {
            *x = 0;
        }
    } else {
        dst[..len].copy_from_slice(&src[..len]);
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn fixture_file(name: &str) -> PathBuf {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name);
        assert!(path.exists(), "fixture path '{path:?}' does not exist");
        path
    }

    mod meta {
        use super::*;

        #[test]
        fn test_meta_from_file() {
            let path = fixture_file("RCRIS-RNSCA_j02_d20240627_t1930197_e1943077_b00001_c20240627194303766000_drlu_ops.h5");

            let meta = Meta::from_file(path).expect("failed creating meta for known good file");

            assert_eq!(
                meta.mission, "S-NPP/JPSS",
                "mission does not match, maybe an issue getting string attributes"
            );
            assert_eq!(
                meta.products.len(),
                2,
                "expected 2 products, got {}",
                meta.products.len()
            );
            assert_eq!(meta.granules["CRIS-SCIENCE-RDR"].len(), 24);
            let gran = &meta.granules["CRIS-SCIENCE-RDR"][0];
            assert_eq!(gran.packet_type.len(), 82);

            dbg!(meta);
        }
    }
}
