use ccsds::spacepacket::{Apid, Packet};
use hdf5::{types::FixedAscii, Dataset, Group};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    path::Path,
};
use tracing::{debug, trace};

use crate::{
    error::{Error, RdrError, Result},
    Time,
};

macro_rules! try_h5 {
    ($obj:expr, $msg:expr) => {
        $obj.map_err(|e| Error::Hdf5Sys(format!("{}: {}", $msg.to_string(), e)))
    };
}

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

/// Compute the RDR granule start time in IET microseconds.
///
/// This is generated the spacecraft mission base time which seems to be based on when
/// SNPP was launched and the same for the currently flying spacecraft.
pub fn get_granule_start(iet: u64, gran_len: u64, base_time: u64) -> u64 {
    let seconds_since_base = iet - base_time;
    // granule number relative to base_time
    let granule_number = seconds_since_base / gran_len;
    // number of micro seconds since base_time
    let ms = granule_number * gran_len;
    // convert back to IET
    ms + base_time
}

/// Compuate the value used for N_Granule_ID
///
/// # Errors
/// If `rdr_iet` is less than the configured satellite base time
pub fn granule_id(sat_short_name: &str, base_time: u64, rdr_iet: u64) -> Result<String> {
    if rdr_iet < base_time {
        return Err(Error::RdrError(RdrError::InvalidGranuleStart(rdr_iet)));
    }
    let t = (rdr_iet - base_time) / 100_000;
    Ok(format!("{}{:012}", sat_short_name.to_uppercase(), t))
}

/// Data structure for collection neccessary metadata and data necessary to write a single
/// [CommonRdr] to an RDR file.
#[derive(Clone, Debug)]
pub struct Rdr {
    /// Standard RDR granule metadata.
    pub meta: GranuleMeta,
    pub product_id: String,
    /// The bytes making up the raw common RDR. See [RdrData].
    pub data: Vec<u8>,
}

impl Rdr {
    /// Create a new empty RDR.
    ///
    /// # Errors
    /// If the time is not valid for the given specs
    pub fn new(sat: &SatSpec, product: &ProductSpec, time: &Time) -> Result<Self> {
        Ok(Self {
            meta: GranuleMeta::new(time.clone(), sat, product)?,
            product_id: product.product_id.to_string(),
            data: vec![],
        })
    }

    pub fn from_data(
        sat: &SatSpec,
        product: &ProductSpec,
        time: &Time,
        data: &RdrData,
    ) -> Result<Self> {
        let mut meta = GranuleMeta::new(time.clone(), sat, product)?;
        let mut names: Vec<String> = Vec::default();
        let mut counts: Vec<u32> = Vec::default();
        for a in data.apid_list.values() {
            names.push(a.name.to_string());
            counts.push(a.pkts_received);
        }
        meta.packet_type_count = counts;
        meta.packet_type = names;
        Ok(Self {
            meta,
            product_id: product.product_id.to_string(),
            data: data.compile()?,
        })
    }
}

/// Collects metadata and packets into a common RDR structures.
#[derive(Debug, Clone)]
pub struct RdrData {
    pub short_name: String,
    pub header: StaticHeader,
    pub apid_list: HashMap<Apid, ApidInfo>,
    pub trackers: HashMap<Apid, Vec<PacketTracker>>,
    pub ap_storage: VecDeque<(u64, Packet)>,
    pub ap_storage_offset: i32,
}

impl RdrData {
    pub fn new(sat: &SatSpec, product: &ProductSpec, time: &Time) -> Self {
        Self {
            short_name: product.short_name.to_string(),
            apid_list: product
                .apids
                .iter()
                .map(|a| (a.num, ApidInfo::new(&a.name, a.num)))
                .collect(),
            header: StaticHeader::new(time, sat.short_name.to_string(), product),
            trackers: HashMap::default(),
            ap_storage: VecDeque::default(),
            ap_storage_offset: 0,
        }
    }

    /// Add a packet.
    ///
    /// # Errors
    /// On packet decode errors, typically, numerical overflow of expected header value types.
    pub fn add_packet(&mut self, pkt_time: &Time, pkt: Packet) -> Result<()> {
        let info = self
            .apid_list
            .get_mut(&pkt.header.apid)
            .ok_or(RdrError::InvalidPacket(pkt.header))?;
        info.pkts_reserved += 1;
        info.pkts_received += 1;

        let pkt_size =
            i32::try_from(pkt.data.len()).map_err(|_| RdrError::InvalidPacket(pkt.header))?;
        let trackers = self.trackers.entry(pkt.header.apid).or_default();
        trackers.push(PacketTracker {
            obs_time: i64::try_from(pkt_time.iet())
                .map_err(|_| RdrError::InvalidTime(pkt_time.iet()))?,
            sequence_number: i32::from(pkt.header.sequence_id),
            size: pkt_size,
            offset: self.ap_storage_offset,
            // FIXME: How to figure out
            fill_percent: 0,
        });

        self.ap_storage.push_back((pkt_time.iet(), pkt));
        self.ap_storage_offset += pkt_size;

        Ok(())
    }

    /// Create an [Rdr] from the current builder state.
    ///
    /// # Panics
    /// If structure counts overflow rdr structure types
    pub fn compile(&self) -> Result<Vec<u8>> {
        let mut apids = self.apid_list.keys().collect::<Vec<_>>();
        apids.sort_unstable();
        let mut apid_list = self.apid_list.clone();

        // Compute and set the packet_tracker_offset based on the APID-first-seen order.
        let mut tracker_offset: u32 = 0;
        for apid in &apids {
            let info = apid_list
                .get_mut(apid)
                .expect("apid_list must be init'd in new");
            info.pkt_tracker_start_idx = tracker_offset;
            tracker_offset += info.pkts_received;
        }

        // Fill out computed header fields
        let mut header = self.header.clone();
        header.pkt_tracker_offset = header.apid_list_offset
            + u32::try_from(apid_list.len() * ApidInfo::LEN).map_err(RdrError::IntError)?;
        let tracker_count: u32 = self
            .trackers
            .values()
            .map(|v| u32::try_from(v.len()).expect("number of trackers does not fit in u32"))
            .sum();
        header.ap_storage_offset =
            header.pkt_tracker_offset + tracker_count * PacketTracker::LEN as u32;
        header.next_pkt_position = self.ap_storage_offset as u32;

        // start by writing static header
        let mut data = Vec::from(header.as_bytes());

        // Write apid list in the order in which apids were first seen.
        for apid in &apids {
            let info = apid_list
                .get(apid)
                .expect("apid_list must be init'd in new");
            data.extend_from_slice(&info.as_bytes());
        }

        // Write trackers. This must be done in apid list order because that's how we set the
        // info.pkt_tracker_start_idx above.
        for apid in &apids {
            if let Some(trackers) = self.trackers.get(apid) {
                for tracker in trackers {
                    data.extend_from_slice(&tracker.as_bytes());
                }
            }
        }

        // Finally, packets get written in the order they were received. The packet trackers have
        // their offset based on writing packets in this order.
        for (_, pkt) in &self.ap_storage {
            data.extend_from_slice(&pkt.data);
        }

        Ok(data)
    }
}

const MAX_STR_LEN: usize = 1024;

impl Display for Rdr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rdr{{product={} time={}T{}}}",
            self.meta.collection, self.meta.begin_date, self.meta.begin_time,
        )
    }
}

macro_rules! attr_string {
    ($obj:expr, $name:expr) => {
        $obj.attr($name)?
            .read_2d::<FixedAscii<MAX_STR_LEN>>()
            .map_err(|e| Error::Hdf5Other(format!("reading string attr {}: {}", $name, e)))?[[0, 0]]
        .to_string()
    };
}

macro_rules! attr_u64 {
    ($obj:expr, $name:expr) => {
        $obj.attr($name)?
            .read_2d::<u64>()
            .map_err(|e| Error::Hdf5Other(format!("reading u64 attr {}: {}", $name, e)))?[[0, 0]]
    };
}

pub fn rdr_filename_meta(rdrs: &[Rdr]) -> (Time, Time, Vec<String>) {
    let mut start = Time::now().iet();
    let mut end = 0;
    let mut product_ids: HashSet<String> = HashSet::default();
    for rdr in rdrs {
        // Only science types determine file time. There should only be one science type but we
        // leave that to the caller and just compute times based on all science types.
        if rdr.meta.collection.contains("SCIENCE") {
            start = std::cmp::min(start, rdr.meta.begin_time_iet);
            end = std::cmp::max(end, rdr.meta.end_time_iet);
        }
        product_ids.insert(rdr.product_id.to_string());
    }
    let mut product_ids = Vec::from_iter(product_ids);
    product_ids.sort();

    (Time::from_iet(start), Time::from_iet(end), product_ids)
}

/// Create an IDPS style RDR filename
pub fn filename(
    satid: &str,
    origin: &str,
    mode: &str,
    created: &Time,
    start: &Time,
    end: &Time,
    product_ids: &[String],
) -> String {
    format!(
        // FIXME: hard-coded orbit number
        "{}_{}_d{}_t{}_e{}_b00000_c{}_{}u_{}.h5",
        product_ids.join("-"),
        satid,
        start.format_utc("%Y%m%d"),
        &start.format_utc("%H%M%S%f")[..7],
        &end.format_utc("%H%M%S%f")[..7],
        &created.format_utc("%Y%m%d%H%M%S%f")[..20],
        &origin[..3],
        mode,
    )
}

pub fn attr_date(dt: &Time) -> String {
    dt.format_utc("%Y%m%d")
}

pub fn attr_time(dt: &Time) -> String {
    // Avoid floating point rouding issues by just rendering micros directly
    format!("{}.{}Z", dt.format_utc("%H%M%S"), dt.iet() % 1_000_000)
}

#[derive(Debug, Clone, Serialize)]
pub struct AggrMeta {
    pub begin_orbit_nubmer: u32,
    pub end_orbit_number: u32,
    pub num_granules: u32,
    pub begin_date: String,
    pub begin_time: String,
    pub begin_granule_id: String,
    pub end_date: String,
    pub end_time: String,
    pub end_granule_id: String,
}

impl AggrMeta {
    /// Create meta from the provided [Rdr]s.
    ///
    /// # Panics
    /// If `rdrs` is empty
    pub fn from_rdrs(rdrs: &Vec<Rdr>) -> Self {
        assert!(!rdrs.is_empty());
        let mut start_rdr: Option<&Rdr> = None;
        let mut end_rdr: Option<&Rdr> = None;
        let mut count: u32 = 0;
        for rdr in rdrs {
            start_rdr = Some(std::cmp::min_by(start_rdr.unwrap_or(rdr), rdr, |a, b| {
                a.meta.begin_time_iet.cmp(&b.meta.begin_time_iet)
            }));
            end_rdr = Some(std::cmp::max_by(end_rdr.unwrap_or(rdr), rdr, |a, b| {
                a.meta.end_time_iet.cmp(&b.meta.end_time_iet)
            }));
            count += 1;
        }

        let start_rdr = start_rdr.unwrap();
        let end_rdr = end_rdr.unwrap();
        Self {
            begin_orbit_nubmer: 1,
            end_orbit_number: 1,
            num_granules: count,
            begin_date: start_rdr.meta.begin_date.clone(),
            begin_time: start_rdr.meta.begin_time.clone(),
            begin_granule_id: start_rdr.meta.id.to_string(),
            end_date: end_rdr.meta.end_date.clone(),
            end_time: end_rdr.meta.end_time.clone(),
            end_granule_id: end_rdr.meta.id.to_string(),
        }
    }
}

/// Metadata associated with a particular granule dataset from RDR path
/// /Data_Products/<collection>/<dataset>_Gran_<x>.
#[derive(Debug, Clone, Serialize)]
pub struct GranuleMeta {
    pub instrument: String,
    pub collection: String,
    #[serde(skip)]
    pub begin: Time,
    pub begin_date: String,
    pub begin_time: String,
    pub begin_time_iet: u64,
    #[serde(skip)]
    pub end: Time,
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
    const DEFAULT_VERSION: &str = "A1";
    const DEFAULT_STATUS: &str = "N/A";
    const DEFAULT_LEOA_FLAG: &str = "Off";
    const DEFAULT_MODE: &str = "dev";

    pub fn new(time: Time, sat: &SatSpec, product: &ProductSpec) -> Result<Self> {
        let created = Time::now();
        let begin = &time;
        let end = &Time::from_iet(begin.iet() + product.gran_len);
        let id = granule_id(&sat.short_name, sat.base_time, begin.iet())?;

        Ok(Self {
            instrument: product.sensor.to_string(),
            collection: product.short_name.to_string(),
            begin: begin.clone(),
            begin_date: attr_date(begin),
            begin_time: attr_time(begin),
            begin_time_iet: begin.iet(),
            end: end.clone(),
            end_date: attr_date(end),
            end_time: attr_time(end),
            end_time_iet: end.iet(),
            creation_date: attr_date(&created),
            creation_time: attr_time(&created),
            orbit_number: 1,
            id: id.to_string(),
            status: Self::DEFAULT_STATUS.to_string(),
            version: Self::DEFAULT_VERSION.to_string(),
            idps_mode: Self::DEFAULT_MODE.to_string(),
            jpss_doc: "".to_string(),
            leoa_flag: Self::DEFAULT_LEOA_FLAG.to_string(),
            packet_type: Vec::default(),
            packet_type_count: Vec::default(),
            percent_missing: 0.0,
            reference_id: format!("{}:{}:{}", product.short_name, id, Self::DEFAULT_VERSION),
            software_version: concat!("rdr", env!("CARGO_PKG_VERSION")).to_string(),
        })
    }

    /// Read RDR grnaule metadata from a [Dataset].
    fn from_dataset(instrument: &str, collection: &str, ds: &Dataset) -> Result<Self> {
        // Read packet type
        let attr = try_h5!(ds.attr("N_Packet_Type"), "accessing N_Packet_Type")?;
        let packet_type: Vec<String> = try_h5!(
            attr.read_2d::<FixedAscii<MAX_STR_LEN>>(),
            "reading N_Packet_Type"
        )?
        .as_slice()
        .ok_or(Error::Hdf5Other(
            "failed to create slice for N_Packet_Type".to_string(),
        ))
        .into_iter()
        .flat_map(|x| x.iter())
        .map(|fa| fa.to_string())
        .collect();

        // Read packet type count
        let packet_type_count: Vec<u32> = ds
            .attr("N_Packet_Type_Count")?
            .read_2d::<u64>()?
            .as_slice()
            .ok_or(Error::Hdf5Other("failed to read dataset".to_string()))?
            .iter()
            .map(|v| u32::try_from(*v).unwrap_or_default())
            .collect();

        let begin = Time::from_iet(attr_u64!(&ds, "N_Beginning_Time_IET"));
        let end = Time::from_iet(attr_u64!(&ds, "N_Ending_Time_IET"));
        Ok(Self {
            instrument: instrument.to_string(),
            collection: collection.to_string(),
            begin,
            begin_date: attr_string!(&ds, "Beginning_Date"),
            begin_time: attr_string!(&ds, "Beginning_Time"),
            begin_time_iet: attr_u64!(&ds, "N_Beginning_Time_IET"),
            end,
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
    pub dataset_type: String,
}

impl ProductMeta {
    const DEFAULT_TYPE_TAG: &str = "RDR";
    const DEFAULT_PROC_DOMAIN: &str = "dev";

    pub fn from_rdr(rdr: &Rdr) -> Self {
        Self {
            instrument: rdr.meta.instrument.to_string(),
            collection: rdr.meta.collection.to_string(),
            processing_domain: Self::DEFAULT_PROC_DOMAIN.to_string(),
            dataset_type: Self::DEFAULT_TYPE_TAG.to_string(),
        }
    }

    fn from_product(product: &ProductSpec) -> Self {
        Self {
            instrument: product.sensor.to_string(),
            collection: product.short_name.to_string(),
            processing_domain: Self::DEFAULT_PROC_DOMAIN.to_string(),
            dataset_type: Self::DEFAULT_TYPE_TAG.to_string(),
        }
    }

    fn from_group(grp: &Group) -> Result<Self> {
        Ok(Self {
            instrument: attr_string!(&grp, "Instrument_Short_Name"),
            collection: attr_string!(&grp, "N_Collection_Short_Name"),
            processing_domain: attr_string!(&grp, "N_Processing_Domain"),
            dataset_type: attr_string!(&grp, "N_Dataset_Type_Tag"),
        })
    }
}

/// RDR metadata generally representing the metadata (attributes) available
/// in a HDF5 file.
#[derive(Debug, Clone, Serialize)]
pub struct Meta {
    pub distributor: String,
    pub mission: String,
    pub dataset_source: String,
    pub created: Time,
    pub platform: String,
    /// Product name to metadata
    pub products: HashMap<String, ProductMeta>,
    /// Product name to the granules for that product
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
            created: Time::now(),
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
            created: Time::now(),
            platform: config.satellite.short_name.clone(),
            products: products
                .iter()
                .map(|p| (p.short_name.clone(), ProductMeta::from_product(p)))
                .collect(),
            granules: products
                .iter()
                .map(|p| (p.short_name.clone(), Vec::default()))
                .collect(),
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, PartialEq)]
pub struct StaticHeader {
    pub satellite: String, // 4-bytes
    pub sensor: String,    // 16-bytes
    pub type_id: String,   // 16-bytes
    pub num_apids: u32,
    pub apid_list_offset: u32,
    pub pkt_tracker_offset: u32,
    pub ap_storage_offset: u32,
    pub next_pkt_position: u32,
    pub start_boundary: u64,
    pub end_boundary: u64,
}

impl StaticHeader {
    pub const LEN: usize = 72;

    pub fn new(time: &Time, sat: String, product: &ProductSpec) -> Self {
        let start_iet = time.iet();
        let end_iet = start_iet + product.gran_len;
        StaticHeader {
            satellite: sat.clone(),
            sensor: product.sensor.clone(),
            type_id: product.type_id.clone(),
            num_apids: u32::try_from(product.apids.len()).unwrap(),
            apid_list_offset: u32::try_from(Self::LEN).unwrap(),
            pkt_tracker_offset: 0,
            ap_storage_offset: 0,
            next_pkt_position: 0,
            start_boundary: start_iet,
            end_boundary: end_iet,
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
            start_boundary: from_bytes8!(u64, data, 56),
            end_boundary: from_bytes8!(u64, data, 64),
        };

        Ok(rdr)
    }

    #[must_use]
    pub fn as_bytes(&self) -> [u8; Self::LEN] {
        let mut buf = [0u8; Self::LEN];
        copy_with_len(&mut buf[..4], self.satellite.as_bytes(), 4);
        copy_with_len(&mut buf[4..20], self.sensor.as_bytes(), 16);
        copy_with_len(&mut buf[20..36], self.type_id.as_bytes(), 16);
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
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ApidInfo {
    pub name: String,
    pub value: u32,
    pub pkt_tracker_start_idx: u32,
    pub pkts_reserved: u32,
    pub pkts_received: u32,
}

impl ApidInfo {
    pub const LEN: usize = 32;

    pub fn new(name: &str, val: u16) -> Self {
        ApidInfo {
            name: name.to_string(),
            value: val as u32,
            pkt_tracker_start_idx: u32::MAX,
            pkts_reserved: 0,
            pkts_received: 0,
        }
    }

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

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PacketTracker {
    /// Observation time as IET microseconds
    pub obs_time: i64,
    /// Sequence number of this trackers packet
    pub sequence_number: i32,
    /// Size in bytes of this tracker packet
    pub size: i32,
    /// Offset to this trackers packet in the AP storage
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
        buf[20..24].copy_from_slice(&self.fill_percent.to_be_bytes());

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

/// The JPSS Common RDR data structures.
///
/// See: JPSS CDFCB Vol II - RDR Formats. Unfortunatley, this document is not generally available
/// for download.
#[derive(Debug, Clone, Serialize)]
pub struct CommonRdr {
    pub static_header: StaticHeader,
    pub apid_list: Vec<ApidInfo>,
    pub packet_trackers: Vec<PacketTracker>,
}

impl CommonRdr {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let static_header = StaticHeader::from_bytes(&data[..StaticHeader::LEN])?;
        let mut apid_list: Vec<ApidInfo> = Vec::default();
        let start = static_header.apid_list_offset as usize;
        assert_eq!(start, StaticHeader::LEN);
        let end = static_header.pkt_tracker_offset as usize;
        for buf in data[start..end].chunks(ApidInfo::LEN) {
            if buf.len() < ApidInfo::LEN {
                debug!("ApidInfo data < {}; bailing!", ApidInfo::LEN);
                break;
            }
            apid_list.push(ApidInfo::from_bytes(buf)?);
        }

        let mut packet_trackers: Vec<PacketTracker> = Vec::default();
        let start = static_header.pkt_tracker_offset as usize;
        let end = static_header.ap_storage_offset as usize;
        for buf in data[start..end].chunks(PacketTracker::LEN) {
            if buf.len() < PacketTracker::LEN {
                debug!("packet tracker data < {}; bailing!", PacketTracker::LEN);
                break;
            }
            let tracker = PacketTracker::from_bytes(buf)?;
            trace!("{tracker:?}");
            packet_trackers.push(tracker);
        }

        Ok(CommonRdr {
            static_header,
            apid_list,
            packet_trackers,
        })
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

    const BASE_TIME: u64 = 1698019234000000;

    fn fixture_file(name: &str) -> PathBuf {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name);
        assert!(path.exists(), "fixture path '{path:?}' does not exist");
        path
    }

    #[test]
    fn test_get_granule_start() {
        // test data from an ERB rdr with expected value produced by edosl0util.rdrgen.get_granule_start
        let pkt_time_iet: u64 = 2112504636060127;
        let gran_len: u64 = 85350000;
        let expected: u64 = 2112504609700000;
        let zult = get_granule_start(pkt_time_iet, gran_len, BASE_TIME);
        assert_eq!(
            expected,
            zult,
            "expected {}, got {}; expected-zult={}",
            expected,
            zult,
            expected - zult,
        );
    }

    #[test]
    fn test_granule_id() {
        let rdr_iet = 2112504394000000;
        let zult = granule_id("NPP", BASE_TIME, rdr_iet).unwrap();
        assert_eq!(zult, "NPP004144851600");
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

    #[test]
    fn test_staticheader() {
        let hdr = StaticHeader {
            satellite: "NPP".to_string(),
            sensor: "VIIRS".to_string(),
            type_id: "SCIENCE".to_string(),
            num_apids: 10,
            apid_list_offset: 20,
            pkt_tracker_offset: 30,
            ap_storage_offset: 40,
            next_pkt_position: 50,
            start_boundary: Time::now().iet(),
            end_boundary: Time::now().iet(),
        };

        let dat = hdr.as_bytes();
        let zult = StaticHeader::from_bytes(&dat).expect("from_bytes failed");

        assert_eq!(hdr, zult);
    }

    #[test]
    fn test_apidinfo() {
        let info = ApidInfo {
            name: "BAND".to_string(),
            value: 999,
            pkt_tracker_start_idx: 10,
            pkts_reserved: 20,
            pkts_received: 30,
        };

        let dat = info.as_bytes();
        let zult = ApidInfo::from_bytes(&dat).expect("from_bytes failed");

        assert_eq!(info, zult);
    }

    #[test]
    fn test_packettracker() {
        let tracker = PacketTracker {
            obs_time: Time::now().iet() as i64,
            sequence_number: 10,
            size: 20,
            offset: 30,
            fill_percent: 40,
        };

        let dat = tracker.as_bytes();
        let zult = PacketTracker::from_bytes(&dat).unwrap();
        assert_eq!(tracker, zult);
    }

    mod filename {
        use hifitime::Epoch;
        use std::str::FromStr;

        use super::*;

        #[test]
        fn packed_rdrs() {
            let time = Time::from_epoch(Epoch::from_str("2020-01-01T12:13:14.123456Z").unwrap());
            let fname = filename(
                "npp",
                "origin",
                "ops",
                &Time::now(), // created
                &time,
                &time,
                &["RNSCA".to_string(), "RVIRS".to_string()],
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RNSCA-RVIRS");

            assert!(
                fname.contains("d20200101_t1213141_e"),
                "Filename does not contain date string"
            );
        }

        #[test]
        fn no_packed_rdrs() {
            let time = Time::from_epoch(Epoch::from_str("2020-01-01T12:13:14.123456Z").unwrap());
            let fname = filename(
                "npp",
                "origin",
                "ops",
                &time,
                &time,
                &time,
                &["RVIRS".to_string()],
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RVIRS");
            assert!(
                fname.contains("d20200101_t1213141_e"),
                "Filename does not contain date string"
            );
        }
    }
}
