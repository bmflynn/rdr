use anyhow::Context;
use ccsds::{Apid, Packet};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};

use crate::config::{ProductSpec, SatSpec};

#[derive(Clone)]
pub struct Rdr {
    /// The product for this rdr
    pub product: ProductSpec,
    /// Any other products that are packed with this rdr, .e.g., RNSCA
    pub packed_with: Vec<String>,
    /// Granule time in IET microseconds
    pub granule_time: u64,
    /// Granule time in UTC microseconds
    pub granule_utc: u64,
    /// Common RDR static header
    pub header: StaticHeader,
    /// Common RDR ``ApidLists`` for each apid
    pub apids: HashMap<Apid, ApidList>,
    /// Common RDR ``PacketTrackers`` for each apid
    pub trackers: HashMap<Apid, Vec<PacketTracker>>,
    /// Common RDR packet storage area
    pub storage: VecDeque<Packet>,
    /// Time this RDR was created
    pub created: DateTime<Utc>,
}

impl Rdr {
    #[must_use]
    pub fn new(
        gran_utc: u64,
        gran_iet: u64,
        sat: &SatSpec,
        product: &ProductSpec,
        packed_with: Vec<String>,
    ) -> Self {
        let mut rdr = Self {
            product: product.clone(),
            packed_with,
            granule_time: gran_iet,
            granule_utc: gran_utc,
            header: StaticHeader::new(gran_iet, sat, product),
            apids: HashMap::default(),
            trackers: HashMap::default(),
            storage: VecDeque::default(),
            created: Utc::now(),
        };

        for apid in &product.apids {
            rdr.apids.insert(
                apid.num,
                ApidList {
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

    fn update_apid_list(&mut self, product: &ProductSpec, pkt: &Packet) {
        let apid = product
            .get_apid(pkt.header.apid)
            .with_context(|| {
                format!(
                    "apid {} not in product {}",
                    pkt.header.apid, product.type_id
                )
            })
            .expect("apid to be present because we already checked for it");
        let apid_list = self
            .apids
            .entry(pkt.header.apid)
            .or_insert_with(|| ApidList {
                name: apid.name,
                value: u32::from(apid.num),
                pkt_tracker_start_idx: 0,
                pkts_reserved: 0,
                pkts_received: 0,
            });
        apid_list.pkts_reserved += 1;
        apid_list.pkts_received += 1;
    }

    /// Add a packet and update the Common RDR structures and offsets.
    ///
    /// # Panics
    /// If the packet traker offset overflows
    pub fn add_packet(&mut self, gran_iet: u64, pkt: Packet, product: &ProductSpec) {
        self.add_tracker(gran_iet, &pkt);
        self.update_apid_list(product, &pkt);
        self.storage.push_back(pkt);

        // Update static header dynamic offsets
        self.header.pkt_tracker_offset =
            u32::try_from(StaticHeader::LEN + ApidList::LEN * self.apids.len()).unwrap();
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

        let apids: Vec<u16> = self.product.apids.iter().map(|p| p.num).collect();
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

#[derive(Clone)]
pub struct ApidList {
    pub name: String,
    pub value: u32,
    pub pkt_tracker_start_idx: u32,
    pub pkts_reserved: u32,
    pub pkts_received: u32,
}

impl ApidList {
    pub const LEN: usize = 32;
    pub fn as_bytes(&self) -> [u8; Self::LEN] {
        let mut buf = [0u8; Self::LEN];
        copy_with_len(&mut buf[..16], self.name.as_bytes(), 16);
        buf[16..20].copy_from_slice(&self.value.to_be_bytes());
        buf[20..24].copy_from_slice(&self.pkt_tracker_start_idx.to_be_bytes());
        buf[24..28].copy_from_slice(&self.pkts_reserved.to_be_bytes());
        buf[28..32].copy_from_slice(&self.pkts_received.to_be_bytes());

        buf
    }
}

#[derive(Clone)]
pub struct PacketTracker {
    pub obs_time: i64,
    pub sequence_number: i32,
    pub size: i32,
    pub offset: i32,
    pub fill_percent: i32,
}

impl PacketTracker {
    pub const LEN: usize = 24;

    pub fn as_bytes(&self) -> [u8; Self::LEN] {
        let mut buf = [0u8; Self::LEN];
        buf[0..8].copy_from_slice(&self.obs_time.to_be_bytes());
        buf[8..12].copy_from_slice(&self.sequence_number.to_be_bytes());
        buf[12..16].copy_from_slice(&self.size.to_be_bytes());
        buf[16..20].copy_from_slice(&self.offset.to_be_bytes());
        buf[20..24].copy_from_slice(&self.offset.to_be_bytes());

        buf
    }
}

fn copy_with_len<'a>(dst: &'a mut [u8], src: &'a [u8], len: usize) {
    if src.len() < len {
        dst[..src.len()].copy_from_slice(src);
        for i in src.len()..len {
            dst[i] = 0;
        }
    } else {
        dst[..len].copy_from_slice(&src[..len]);
    }
}
