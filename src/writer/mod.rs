mod hdfc;

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use hdf5::{
    types::{FixedAscii, VarLenAscii},
    File,
};
use hifitime::Epoch;
use ndarray::{arr1, arr2, Dim};

use crate::{
    config::{Config, ProductSpec, SatSpec},
    rdr::{ApidInfo, Rdr},
    utils::format_epoch,
};

/// Write a string attr with specific len with shape [1, 1]
macro_rules! wattstr {
    ($obj:expr, $name:expr, $value:expr, $maxlen:expr) => {
        $obj.new_attr_builder()
            .with_data::<'_, _, _, Dim<[usize; 2]>>(&arr2(&[[FixedAscii::<$maxlen>::from_ascii(
                &(($value.clone())[..std::cmp::min($maxlen, $value.len())]),
            )
            .with_context(|| format!("setting str data attr {} with value {}", $name, $value))?]]))
            .create($name)
            .with_context(|| format!("creating str data attr {} with value {}", $name, $value))?;
    };
}

/// Write a u64 attr
macro_rules! wattu64 {
    ($obj:expr, $name:expr, $value:expr) => {
        $obj.new_attr_builder()
            .with_data::<'_, _, u64, Dim<[usize; 2]>>(&arr2(&[[$value]]))
            .create($name)
            .with_context(|| format!("creating str data attr {} with value {}", $name, $value))?;
    };
}

/// The business-end of writing RDRs
///
/// # Errors
/// All errors creating or wri
pub fn write_hdf5(config: &Config, rdrs: &[Rdr], created: Epoch, dest: &Path) -> Result<PathBuf> {
    let fpath = dest.join(filename(
        &config.satellite.id,
        &config.origin,
        &config.mode,
        created,
        rdrs,
    ));

    let products = config
        .products
        .clone()
        .into_iter()
        .map(|p| (p.product_id.clone(), p))
        .collect::<HashMap<String, ProductSpec>>();

    let file = File::create(&fpath).with_context(|| format!("opening {fpath:?}"))?;

    wattstr!(file, "Distributor", config.origin, 4);
    wattstr!(file, "Mission_Name", config.satellite.mission.clone(), 10);
    wattstr!(file, "Platform_Short_Name", config.satellite.id.clone(), 3);
    wattstr!(file, "N_Dataset_Source", config.origin, 4);
    wattstr!(file, "N_HDF_Creation_Date", attr_date(created), 8);
    wattstr!(file, "N_HDF_Creation_Time", attr_time(created), 14);

    file.create_group("All_Data").context("creating All_Data")?;
    file.create_group("Data_Products")
        .context("creating Data_Products")?;

    for (idx, rdr) in rdrs.iter().enumerate() {
        let product = products.get(&rdr.product_id).unwrap();
        let path = write_rdr_to_alldata(&file, idx, rdr, product)?;
        write_rdr_to_dataproducts(&file, config, rdr, product, &path)?;
    }

    for product in products.values() {
        let rdrs = rdrs
            .iter()
            .filter(|&r| r.product_id == product.product_id)
            .cloned()
            .collect::<Vec<Rdr>>();
        write_aggr_group(&file, &config.satellite, &rdrs, product)?;
    }

    Ok(fpath)
}

fn attr_date(dt: Epoch) -> String {
    format_epoch(dt, "%Y%m%d")
}

fn attr_time(dt: Epoch) -> String {
    format_epoch(dt, "%H:%M:%S.%fZ")
}

/// Create an IDPS style RDR filename
fn filename(satid: &str, origin: &str, mode: &str, created: Epoch, rdrs: &[Rdr]) -> String {
    // dedup product ids
    let mut product_ids = rdrs
        .iter()
        .flat_map(|r| std::iter::once(r.product_id.clone()).chain(r.packed_with.clone()))
        .collect::<HashSet<String>>()
        .iter()
        .cloned()
        .collect::<Vec<String>>();
    product_ids.sort();

    let start = granule_dt(rdrs.iter().map(|r| r.begin_time_utc).min().unwrap_or(0));
    let end = granule_dt(rdrs.iter().map(|r| r.end_time_utc).max().unwrap_or(0));

    format!(
        "{}_{}_d{}_t{}_e{}_c{}_{}u_{}.h5",
        product_ids.join("-"),
        satid,
        format_epoch(start, "%Y%m%d"),
        &format_epoch(start, "%H%M%S%f")[..7],
        &format_epoch(end, "%H%M%S%f")[..7],
        &format_epoch(created, "%Y%m%d%H%M%S%f")[..20],
        &origin[..3],
        mode,
    )
}

fn write_rdr_to_alldata(
    file: &File,
    gran_idx: usize,
    rdr: &Rdr,
    product: &ProductSpec,
) -> Result<String> {
    if file.group("All_Data").is_err() {
        file.create_group("All_Data")
            .with_context(|| "h5 group create error name=All_Data".to_string())?;
    }
    let name = format!(
        "/All_Data/{}_All/RawApplicationPackets_{gran_idx}",
        product.short_name
    );
    file.new_dataset_builder()
        .with_data(&arr1(&rdr.compile()[..]))
        .create(name.clone().as_str())
        .with_context(|| format!("h5 dataset create error name={name}"))?;
    Ok(name)
}

// FIXME: This is a big mess
fn set_product_dataset_attrs(
    file: &File,
    config: &Config,
    rdr: &Rdr,
    product: &ProductSpec,
    dataset_path: &str,
) -> Result<()> {
    let (start_dt, end_dt) = granule_dt_range(rdr);
    let ver = "A1";

    let dataset = file
        .dataset(dataset_path)
        .unwrap_or_else(|_| panic!("expected just written dataset {dataset_path} to exist"));

    let gran_id = granule_id(&config.satellite, rdr);
    wattstr!(dataset, "Beginning_Date", attr_date(start_dt), 8);
    wattstr!(dataset, "Beginning_Time", attr_time(start_dt), 14);
    wattstr!(dataset, "Ending_Date", attr_date(end_dt), 8);
    wattstr!(dataset, "Ending_Time", attr_time(end_dt), 14);
    wattstr!(dataset, "N_Creation_Date", attr_date(rdr.created), 8);
    wattstr!(dataset, "N_Creation_Time", attr_time(rdr.created), 14);
    wattstr!(dataset, "N_Granule_Status", "N/A".to_string(), 3);
    wattstr!(dataset, "N_Granule_Version", ver, 2);
    wattstr!(dataset, "N_JPSS_Document_Ref", String::new(), 52);
    wattstr!(dataset, "N_LEOA_Flag", "Off".to_string(), 3);
    wattstr!(
        dataset,
        "N_Reference_ID",
        format!("{}:{}:{}", product.short_name, gran_id, ver),
        35
    );
    wattstr!(dataset, "N_Granule_ID", gran_id, 15);
    wattstr!(dataset, "N_IDPS_Mode", config.mode.clone(), 3);
    wattstr!(dataset, "N_Software_Version", String::new(), 19);

    wattu64!(dataset, "N_Beginning_Orbit_Number", 0);
    wattu64!(dataset, "N_Beginning_Time_IET", rdr.begin_time_iet);
    wattu64!(dataset, "N_Ending_Time_IET", rdr.end_time_iet);

    let apids: HashMap<&str, &ApidInfo> =
        rdr.apids.values().map(|a| (a.name.as_str(), a)).collect();
    let mut names: Vec<&str> = apids.keys().copied().collect();
    names.sort_unstable();
    let name = "N_Packet_Type";
    let mut pkt_type_arr: Vec<VarLenAscii> = Vec::default();
    let mut pkt_type_cnt_arr: Vec<u64> = Vec::default();
    for (i, name) in names.iter().enumerate() {
        let apid = apids
            .get(name)
            .expect("apid must be present because names are created from same map");
        let ascii = VarLenAscii::from_ascii(apid.name.as_bytes())
            .with_context(|| format!("h5 attr ascii error name={name} val[{i}]={apid:?}"))?;
        pkt_type_arr.push(ascii);
        pkt_type_cnt_arr.push(apid.pkts_received.into());
    }
    dataset
        .new_attr::<VarLenAscii>()
        .shape([pkt_type_arr.len(), 1])
        .create(name)
        .with_context(|| format!("h5 attr create error name={name}"))?
        .write_raw(&pkt_type_arr)
        .with_context(|| format!("h5 attr write error name={name}"))?;

    let name = "N_Packet_Type_Count";
    dataset
        .new_attr::<u64>()
        .shape([pkt_type_cnt_arr.len(), 1])
        .create(name)
        .with_context(|| format!("h5 attr create error name={name}"))?
        .write_raw(&pkt_type_cnt_arr)
        .with_context(|| format!("h5 attr write error name={name}"))?;

    // TODO: Compute missing percent.
    // This should be based on received vs. expected packet counts
    let (name, val) = ("N_Percent_Missing_Data", 0.0);
    dataset
        .new_attr::<f32>()
        .shape([1, 1])
        .create(name)
        .with_context(|| format!("h5 attr create error name={name} val={val}"))?
        .write_raw(&[val])
        .with_context(|| format!("h5 attr write error name={name} val={val}"))?;

    Ok(())
}

fn write_rdr_to_dataproducts(
    file: &File,
    config: &Config,
    rdr: &Rdr,
    product: &ProductSpec,
    src_path: &str,
) -> Result<()> {
    let group_name = format!("Data_Products/{}", product.short_name);
    if file.group(&group_name).is_err() {
        file.create_group(&group_name)
            .with_context(|| format!("h5 group create error name={group_name}"))?;
    }
    let mut writer = hdfc::DataProductsRefWriter::default();
    let dataset_path = writer.write_ref(file, product, src_path)?;

    set_product_dataset_attrs(file, config, rdr, product, &dataset_path)?;

    Ok(())
}

fn granule_dt(utc: u64) -> Epoch {
    Epoch::from_utc_seconds(utc as f64 / 1_000_000.0)
}

fn granule_dt_range(rdr: &Rdr) -> (Epoch, Epoch) {
    (granule_dt(rdr.begin_time_utc), granule_dt(rdr.end_time_utc))
}

fn granule_id(sat: &SatSpec, rdr: &Rdr) -> String {
    format!(
        "{}{:012}",
        sat.id.to_uppercase(),
        (rdr.begin_time_iet - sat.base_time) / 100_000
    )
}

fn write_aggr_group(file: &File, sat: &SatSpec, rdrs: &[Rdr], product: &ProductSpec) -> Result<()> {
    if rdrs.is_empty() {
        return Ok(());
    }
    let name = format!("/Data_Products/{0}/{0}_Aggr", product.short_name);
    let group = file
        .create_group(&name)
        .with_context(|| format!("h5 group create error name={name}"))?;

    for (name, val) in [
        ("AggregateBeginningOrbitNumber", 0usize),
        ("AggregateEndingOrbitNumber", 0usize),
        ("AggregateNumberGranules", rdrs.len()),
    ] {
        let attr = group
            .new_attr::<usize>()
            .shape([1, 1])
            .create(name)
            .with_context(|| format!("h5 attr create error name={name} val={val}"))?;

        attr.write_raw(&[val])
            .with_context(|| format!("h5 attr write error name={name} val={val}"))?;
    }

    let mut start_rdr = &rdrs[0];
    let mut end_rdr = &rdrs[rdrs.len() - 1];
    for rdr in rdrs {
        if rdr.begin_time_utc > start_rdr.begin_time_utc {
            start_rdr = rdr;
        }
        if rdr.begin_time_utc < end_rdr.begin_time_utc {
            end_rdr = rdr;
        }
    }
    let start_dt = granule_dt(start_rdr.begin_time_utc);
    let end_dt = granule_dt(end_rdr.end_time_utc);

    for (name, val) in [
        ("AggregateBeginningDate", format_epoch(start_dt, "%Y%m%d")),
        (
            "AggregateBeginningTime",
            format_epoch(start_dt, "%H:%M:%S.%fZ"),
        ),
        ("AggregateBeginningGranuleID", granule_id(sat, start_rdr)),
        ("AggregateEndingDate", format_epoch(end_dt, "%Y%m%d")),
        ("AggregateEndingTime", format_epoch(end_dt, "%H:%M:%S.%fZ")),
        ("AggregateEndingGranuleID", granule_id(sat, end_rdr)),
    ] {
        let attr = group
            .new_attr::<VarLenAscii>()
            .shape([1, 1])
            .create(name)
            .with_context(|| format!("h5 attr create error name={name} val={val}"))?;

        let ascii = VarLenAscii::from_ascii(&val)
            .with_context(|| format!("h5 attr ascii error name={name} val={val}"))?;

        attr.write_raw(&[ascii])
            .with_context(|| format!("h5 attr write error name={name} val={val}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod filename {
        use std::{
            collections::{HashMap, VecDeque},
            str::FromStr,
        };

        use crate::{
            config::get_default,
            rdr::{Rdr, StaticHeader},
            utils::now,
        };

        use super::*;

        fn parse_epoch(fmt: &str) -> Epoch {
            Epoch::from_str(fmt).unwrap()
        }

        #[test]
        fn packed_rdrs() {
            let config = get_default("npp").unwrap();
            let primary = config
                .products
                .iter()
                .filter(|p| p.product_id == "RVIRS")
                .collect::<Vec<_>>()[0];

            let dt = parse_epoch("2000-01-01T12:13:14Z");
            let ts = (dt.to_unix_seconds() * 1_000_000.0) as u64;
            let fname = filename(
                "npp",
                "origin",
                "ops",
                now(),
                &[Rdr {
                    product_id: primary.product_id.clone(),
                    packed_with: vec!["RNSCA".to_string()],
                    begin_time_iet: ts,
                    end_time_iet: ts + primary.gran_len,
                    begin_time_utc: ts,
                    end_time_utc: ts + primary.gran_len,
                    header: StaticHeader::default(),
                    apids: HashMap::default(),
                    trackers: HashMap::default(),
                    storage: VecDeque::default(),
                    created: now(),
                }],
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RNSCA-RVIRS");
        }

        #[test]
        fn no_packed_rdrs() {
            let config = get_default("npp").unwrap();
            let primary = config
                .products
                .iter()
                .filter(|p| p.product_id == "RVIRS")
                .collect::<Vec<_>>()[0];

            let dt = parse_epoch("2000-01-01T12:13:14Z");
            let ts = (dt.to_unix_seconds() * 1_000_000.0) as u64;
            let fname = filename(
                "npp",
                "origin",
                "ops",
                now(),
                &[Rdr {
                    product_id: primary.product_id.clone(),
                    packed_with: Vec::default(),
                    begin_time_iet: ts,
                    end_time_iet: ts + primary.gran_len,
                    begin_time_utc: ts,
                    end_time_utc: ts + primary.gran_len,
                    header: StaticHeader::default(),
                    apids: HashMap::default(),
                    trackers: HashMap::default(),
                    storage: VecDeque::default(),
                    created: now(),
                }],
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RVIRS");
        }
    }
}
