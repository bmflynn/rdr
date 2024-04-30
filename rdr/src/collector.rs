use std::collections::{HashMap, VecDeque};

use ccsds::{Apid, Packet, PacketGroup};
use tracing::trace;

use crate::{
    config::{ProductSpec, SatSpec},
    rdr::Rdr,
    time::TimeFcn,
};

/// Collects individual product Rdr data.
///
/// This does not collect packed products that will be necessary to create the
/// final RDR.
pub struct Collector {
    sat: SatSpec,
    specs: HashMap<String, ProductSpec>,
    ids: HashMap<Apid, String>,

    primary: HashMap<(String, u64), Rdr>,
    packed: HashMap<(String, u64), Rdr>,
}

impl Collector {
    #[must_use]
    pub fn new(sat: SatSpec, products: &[ProductSpec]) -> Self {
        let mut collector = Collector {
            sat,
            specs: HashMap::default(),
            ids: HashMap::default(),
            primary: HashMap::default(),
            packed: HashMap::default(),
        };

        for product in products {
            collector
                .specs
                .insert(product.product_id.clone(), product.clone());
            for apid in &product.apids {
                collector.ids.insert(apid.num, product.product_id.clone());
            }
        }

        collector
    }

    fn gran_times(&self, utc: u64, iet: u64, spec: &ProductSpec) -> (u64, u64) {
        let gran_len = spec.gran_len;
        let base_time = self.sat.base_time;

        (
            (utc / gran_len) / gran_len * gran_len,
            (iet - base_time) / gran_len * gran_len + base_time,
        )
    }

    /// Get all overlapping configured packed products.
    ///
    /// This is all granules where the packet granule start is within its granule length of
    /// the start of the primary granule start and less than the primary granule end.
    fn overlapping_packed_granules(&self, product: &ProductSpec, rdr: &Rdr) -> Vec<Rdr> {
        let mut packed = Vec::default();
        for packed_id in &product.packed_with {
            let packed_product = self.specs.get(packed_id).expect("spec for existing id");
            for (key, packed_rdr) in &self.packed {
                let packed_gran_start = i64::try_from(key.1).unwrap();
                let primary_gran_start = i64::try_from(rdr.granule_time).unwrap();
                let primary_gran_end = i64::try_from(rdr.granule_time + product.gran_len).unwrap();
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

    pub fn add(&mut self, pkt_utc: u64, pkt_iet: u64, pkt: Packet) -> Option<(Rdr, Vec<Rdr>)> {
        if !self.ids.contains_key(&pkt.header.apid) {
            return None; // apid has no configured product
        }
        let prod_id = &self.ids[&pkt.header.apid];
        let product = self.specs.get(prod_id).expect("spec for existing id");
        let (gran_utc, gran_iet) = self.gran_times(pkt_utc, pkt_iet, product);

        let key = (prod_id.clone(), gran_iet);
        if product.primary {
            {
                let rdr = self.primary.entry(key).or_insert_with(|| {
                    trace!(
                        "new primary granule product_id={} granule={}",
                        product.product_id,
                        gran_iet
                    );
                    Rdr::new(gran_utc, gran_iet, &self.sat, product)
                });
                rdr.add_packet(gran_iet, pkt, product);
            }

            // Check to see if the second to last granule is complete. We can't check the
            // last granule because it may not yet have enough data available for the packed
            // products.
            let second_to_last_key = (prod_id.clone(), gran_iet - product.gran_len * 2);
            if self.primary.contains_key(&second_to_last_key) {
                let primary = self.primary.remove(&second_to_last_key).unwrap(); // already verified it exists
                let packed = self.overlapping_packed_granules(product, &primary);
                Some((primary, packed))
            } else {
                None
            }
        } else {
            // FIXME: Figure out how to clean up packed products
            let rdr = self.packed.entry(key).or_insert_with(|| {
                trace!(
                    "new packed granule product_id={} granule={}",
                    product.product_id,
                    gran_iet
                );
                Rdr::new(gran_utc, gran_iet, &self.sat, product)
            });
            rdr.add_packet(gran_iet, pkt, product);
            None
        }
    }

    pub fn finish(mut self) -> Vec<(Rdr, Vec<Rdr>)> {
        let mut keys: Vec<(String, u64)> = self.primary.keys().map(|k| (*k).clone()).collect();
        keys.sort_by(|a, b| a.1.cmp(&b.1));

        let mut finished = Vec::default();
        for key in &keys {
            let product = self.specs.get(&key.0).unwrap(); // we have a primary rdr, so this product exists
            let primary = self.primary.remove(key).unwrap(); // already checked it exists
            let packed = self.overlapping_packed_granules(product, &primary);
            finished.push((primary, packed));
        }

        finished
    }
}

/// Iterator that produces tuples of `Packet` and their IET time.
pub struct PacketTimeIter<P>
where
    P: Iterator<Item = PacketGroup>,
{
    decode_iet: Box<TimeFcn>,
    groups: P,
    cache: VecDeque<(Packet, u64, u64)>,
}

impl<P> PacketTimeIter<P>
where
    P: Iterator<Item = PacketGroup>,
{
    pub fn new(groups: P, ied_decoder: Box<TimeFcn>) -> Self {
        PacketTimeIter {
            cache: VecDeque::default(),
            decode_iet: ied_decoder,
            groups,
        }
    }
}

impl<P> Iterator for PacketTimeIter<P>
where
    P: Iterator<Item = PacketGroup>,
{
    type Item = (Packet, u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.is_empty() {
            let group = self.groups.next()?;
            assert!(
                !group.packets.is_empty(),
                "should never get empty packet group"
            );
            let first = &group.packets[0];
            let (utc, iet) = (self.decode_iet)(first);

            for pkt in group.packets {
                self.cache.push_back((pkt, utc, iet));
            }
        }
        self.cache.pop_front()
    }
}
