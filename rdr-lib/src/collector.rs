use std::collections::{HashMap, HashSet, VecDeque};

use ccsds::spacepacket::{Apid, Packet, PacketGroup, TimecodeDecoder};
use tracing::trace;

use crate::{
    config::{ProductSpec, RdrSpec, SatSpec},
    get_granule_start,
    rdr::Rdr,
    Time,
};

/// Collects individual product Rdr data.
///
/// This does not collect packed products that will be necessary to create the
/// final RDR.
pub struct Collector {
    sat: SatSpec,
    // Maps the promary RDR products ids to the ids of products they're packed with
    primary_ids: HashMap<String, Vec<String>>,
    // ids of all packed products we're collecting
    packed_ids: HashSet<String>,
    // Maps product_id to spec
    products: HashMap<String, ProductSpec>,
    // Maps apids to product_id. If a packet apid is not in this map it cannot be added
    ids: HashMap<Apid, String>,

    // Maps product and RDR granule time to an RDR
    primary: HashMap<(String, Time), Rdr>,
    // Maps packed product and RDR granule time to an RDR
    packed: HashMap<(String, Time), Rdr>,
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
    fn overlapping_packed_granules(&self, product: &ProductSpec, rdr: &Rdr) -> Vec<Rdr> {
        let mut packed = Vec::default();
        for packed_id in &rdr.packed_with {
            let packed_product = self.products.get(packed_id).expect("spec for existing id");
            for ((_, packed_time), packed_rdr) in &self.packed {
                let packed_gran_start = packed_time.iet() as i64;
                let primary_gran_start = rdr.time.iet() as i64;
                let primary_gran_end = rdr.time.iet() as i64 + product.gran_len as i64;
                let packed_gran_len = i64::try_from(packed_product.gran_len).unwrap();

                if packed_gran_start > primary_gran_start - packed_gran_len
                    && packed_gran_start < primary_gran_end
                {
                    packed.push((*packed_rdr).clone());
                }
            }
        }
        packed
    }

    pub fn add(&mut self, pkt_time: &Time, pkt: Packet) -> Option<Vec<Rdr>> {
        // The the product for this packet's apid
        let prod_id = self.ids.get(&pkt.header.apid)?;
        let product = self.products.get(prod_id).expect("spec for existing id");

        // The granule time this packet belongs to, i.e., the one it gets added to
        let gran_time = Time::from_iet(get_granule_start(
            pkt_time.iet(),
            product.gran_len,
            self.sat.base_time,
        ));

        let key = (prod_id.clone(), gran_time.clone());

        // If this product is for a primary product RDR add it to the primary collection
        if self.primary_ids.contains_key(prod_id) {
            {
                let rdr = self.primary.entry(key).or_insert_with(|| {
                    trace!(
                        "new primary granule product_id={} granule={:?}",
                        product.product_id,
                        gran_time,
                    );
                    Rdr::new(product, &self.sat, gran_time.clone())
                });
                rdr.add_packet(pkt_time, pkt);
            }

            // Check to see if the second to last granule is complete. We can't check the
            // last granule because it may not yet have enough data available for the packed
            // products.
            let second_to_last_key = (
                prod_id.clone(),
                Time::from_iet(gran_time.iet() - product.gran_len * 2),
            );
            if self.primary.contains_key(&second_to_last_key) {
                let mut rdrs = vec![self.primary.remove(&second_to_last_key).unwrap()]; // already verified it exists
                rdrs.extend_from_slice(&self.overlapping_packed_granules(product, &rdrs[0]));
                Some(rdrs)
            } else {
                None
            }
        } else {
            // FIXME: Figure out how to clean up packed products
            let rdr = self.packed.entry(key).or_insert_with(|| {
                trace!(
                    "new packed granule product_id={} granule={:?}",
                    product.product_id,
                    gran_time,
                );
                Rdr::new(product, &self.sat, gran_time)
            });
            rdr.add_packet(pkt_time, pkt);
            None
        }
    }

    pub fn finish(mut self) -> Vec<Vec<Rdr>> {
        let mut keys: Vec<(String, Time)> = self.primary.keys().map(|k| (*k).clone()).collect();
        keys.sort_by(|a, b| a.1.cmp(&b.1));

        let mut finished = Vec::default();
        for key in &keys {
            let product = self.products.get(&key.0).unwrap(); // we have a primary rdr, so this product exists
            let mut rdrs = vec![self.primary.remove(key).unwrap()]; // already verified it exists
            rdrs.extend_from_slice(&self.overlapping_packed_granules(product, &rdrs[0]));
            finished.push(rdrs);
        }

        finished
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
            let time = Time::from_epoch(self.time_decoder.decode(first).unwrap());

            for pkt in group.packets {
                self.cache.push_back((pkt, time.clone()));
            }
        }
        self.cache.pop_front()
    }
}
