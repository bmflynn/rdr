use std::collections::{HashMap, HashSet, VecDeque};

use ccsds::spacepacket::{Apid, Packet, PacketGroup, TimecodeDecoder};
use tracing::{trace, warn};

use crate::{
    config::{ProductSpec, RdrSpec, SatSpec},
    error::Result,
    get_granule_start,
    rdr::Rdr,
    Error, RdrData, RdrError, Time,
};

/// Collects individual product Rdr data.
pub struct Collector {
    sat: SatSpec,
    /// Maps the promary RDR products ids to the ids of products they're packed with
    primary_ids: HashMap<String, Vec<String>>,
    /// ids of all packed products we're collecting
    packed_ids: HashSet<String>,
    /// Maps product_id to spec
    products: HashMap<String, ProductSpec>,
    /// Maps apids to product_id. If a packet apid is not in this map it cannot be added
    ids: HashMap<Apid, String>,

    /// Maps product and RDR granule time to an RDR
    primary: HashMap<(String, Time), RdrData>,
    /// Maps packed product and RDR granule time to an RDR
    packed: HashMap<(String, Time), RdrData>,
}

impl Collector {
    #[must_use]
    pub fn new(sat: SatSpec, rdrs: &[RdrSpec], products: &[ProductSpec]) -> Self {
        let mut collector = Collector {
            sat,
            primary_ids: HashMap::default(),
            packed_ids: HashSet::default(),
            products: HashMap::default(),
            ids: HashMap::default(),
            primary: HashMap::default(),
            packed: HashMap::default(),
        };

        for product in products {
            collector
                .products
                .insert(product.product_id.clone(), product.clone());
            for apid in &product.apids {
                collector.ids.insert(apid.num, product.product_id.clone());
            }
        }

        for rdr in rdrs {
            collector
                .primary_ids
                .insert(rdr.product.clone(), rdr.packed_with.clone());
            for prod_id in &rdr.packed_with {
                collector.packed_ids.insert(prod_id.clone());
            }
        }

        collector
    }

    /// Get all overlapping configured packed products.
    ///
    /// This is all granules where the packet granule start is within its granule length of
    /// the start of the primary granule start and less than the primary granule end.
    fn overlapping_packed_rdrs(&self, rdr: &Rdr) -> Result<Vec<Rdr>> {
        let primary_gran_start = rdr.meta.begin_time_iet as i64;
        let primary_gran_end = rdr.meta.end_time_iet as i64;
        let mut packed = Vec::default();

        for packed_id in &self.packed_ids {
            let packed_product = self.products.get(packed_id).expect("spec for existing id");
            let Ok(packed_gran_len) = i64::try_from(packed_product.gran_len) else {
                return Err(Error::ConfigInvalid(
                    "gran_len cannot be convert to i64".to_string(),
                ));
            };

            for ((_, packed_time), data) in &self.packed {
                let packed_gran_start = packed_time.iet() as i64;

                if packed_gran_start > primary_gran_start - packed_gran_len
                    && packed_gran_start < primary_gran_end
                {
                    let rdr = match data.compile() {
                        Ok(r) => r,
                        Err(err) => {
                            warn!("failed to compile rdr data: {err}");
                            continue;
                        }
                    };
                    packed.push(rdr);
                }
            }
        }
        trace!(
            "{} overlapping granules for start={primary_gran_start} end={primary_gran_end}",
            packed.len()
        );
        Ok(packed)
    }

    /// Add the provided packet to this collector returning any primary [Rdr]s that are complete,
    /// along with any overlapping packed products.
    ///
    /// The current primary granule can never be complete because we may not yet have all the
    /// overlapping packed data, so only the second to last granule is checked.
    ///
    /// # Errors
    /// If the RDR granule time computed from the packet time is invalid for the spacecraft
    /// configuration.
    pub fn add(&mut self, pkt_time: &Time, pkt: Packet) -> Result<Option<Vec<Rdr>>> {
        // The the product for this packet's apid
        let Some(prod_id) = self.ids.get(&pkt.header.apid) else {
            return Ok(None);
        };
        let product = self.products.get(prod_id).expect("spec for existing id");

        // The granule time this packet belongs to, i.e., the one it gets added to
        let gran_time = Time::from_iet(get_granule_start(
            pkt_time.iet(),
            product.gran_len,
            self.sat.base_time,
        ));
        if gran_time.iet() < self.sat.base_time {
            return Err(Error::RdrError(RdrError::InvalidGranuleStart(
                gran_time.iet(),
            )));
        }

        // If this packet is for a primary product RDR add it to the primary collection
        let key = (product.product_id.clone(), gran_time.clone());
        if self.primary_ids.contains_key(prod_id) {
            {
                let data = self.primary.entry(key).or_insert_with(|| {
                    trace!(
                        "new primary granule product_id={} granule={:?}",
                        product.product_id,
                        gran_time,
                    );
                    RdrData::new(&self.sat, product, &gran_time)
                });
                data.add_packet(pkt_time, pkt)?;
            }

            // If the second to last primary granule exists we assume it has had a chance to get
            // any overlapping packed products it may need, so we consider it "complete".
            let second_to_last_key = (
                product.product_id.clone(),
                Time::from_iet(gran_time.iet() - product.gran_len * 2),
            );
            if let Some(data) = self.primary.remove(&second_to_last_key) {
                let rdr = match data.compile() {
                    Ok(r) => r,
                    Err(err) => {
                        warn!("failed to compile rdr data: {err}");
                        return Ok(None);
                    }
                };
                let packed = self.overlapping_packed_rdrs(&rdr)?;
                let mut rdrs = vec![rdr];
                rdrs.extend_from_slice(&packed);
                Ok(Some(rdrs))
            } else {
                Ok(None)
            }
        } else {
            assert!(self.packed_ids.contains(&product.product_id));
            // FIXME: Figure out how to clean up packed products
            let data = self.packed.entry(key).or_insert_with(|| {
                trace!(
                    "new packed granule product_id={} time={:?}",
                    product.product_id,
                    gran_time,
                );
                RdrData::new(&self.sat, product, &gran_time)
            });
            data.add_packet(pkt_time, pkt)?;
            Ok(None)
        }
    }

    pub fn finish(mut self) -> Result<Vec<Vec<Rdr>>> {
        let mut keys: Vec<(String, Time)> = self.primary.keys().map(|k| (*k).clone()).collect();
        keys.sort_by(|a, b| a.1.cmp(&b.1));

        let mut finished = Vec::default();
        for (pid, time) in &keys {
            let key = (pid.clone(), time.clone());
            let data = self
                .primary
                .remove(&key)
                .expect("exists because we created keys above");
            let rdr = match data.compile() {
                Ok(r) => r,
                Err(err) => {
                    warn!("failed to compile rdr data: {err}");
                    continue;
                }
            };

            let packed = self.overlapping_packed_rdrs(&rdr)?;
            let mut rdrs = vec![rdr];
            rdrs.extend_from_slice(&packed);
            finished.push(rdrs);
        }

        Ok(finished)
    }
}

/// Iterator that produces tuples of `Packet` and their time.
pub struct PacketTimeIter<P>
where
    P: Iterator<Item = PacketGroup>,
{
    time_decoder: TimecodeDecoder,
    groups: P,
    cache: VecDeque<(Packet, Time)>,
}

impl<P> PacketTimeIter<P>
where
    P: Iterator<Item = PacketGroup>,
{
    pub fn new(groups: P) -> Self {
        PacketTimeIter {
            cache: VecDeque::default(),
            time_decoder: TimecodeDecoder::new(ccsds::timecode::Format::Cds {
                num_day: 2,
                num_submillis: 2,
            }),
            groups,
        }
    }
}

impl<P> Iterator for PacketTimeIter<P>
where
    P: Iterator<Item = PacketGroup>,
{
    type Item = (Packet, Time);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.is_empty() {
            let group = self.groups.next()?;
            assert!(
                !group.packets.is_empty(),
                "should never get empty packet group"
            );
            let first = &group.packets[0];
            let Ok(epoch) = self.time_decoder.decode(first) else {
                warn!("failed to decode time from {:?}", first);
                return None;
            };
            let time = Time::from_epoch(epoch);

            for pkt in group.packets {
                self.cache.push_back((pkt, time.clone()));
            }
        }
        self.cache.pop_front()
    }
}
