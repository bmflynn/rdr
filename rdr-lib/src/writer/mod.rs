mod hdfc;

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use hdf5::{
    types::{FixedAscii, VarLenAscii},
    File,
};
use hdfc::{create_dataproducts_aggr_dataset, create_dataproducts_gran_dataset};
use ndarray::{arr1, arr2, Dim};

use crate::{
    config::{Config, ProductSpec},
    error::{H5Error, Result},
    rdr::{ApidInfo, Rdr},
    Time,
};

macro_rules! try_h5 {
    ($obj:expr, $msg:expr) => {
        $obj.map_err(|e| H5Error::Other(format!("{}: {}", $msg.to_string(), e)))
    };
}

/// Write a string attr with specific len with shape [1, 1]
macro_rules! wattstr {
    ($obj:expr, $name:expr, $value:expr, $maxlen:expr) => {
        try_h5!(
            $obj.new_attr_builder()
                .with_data::<'_, _, _, Dim<[usize; 2]>>(&arr2(&[[
                    FixedAscii::<$maxlen>::from_ascii(
                        &(($value.clone())[..std::cmp::min($maxlen, $value.len())]),
                    )
                    .map_err(|e| {
                        H5Error::Other(format!(
                            "creating ascii value {} for {}: {e}",
                            $name, $value
                        ))
                    })?
                ]]))
                .create($name),
            format!("writeing str attr {} value {}", $name, $value)
        )?;
    };
}

/// Write a u64 attr
macro_rules! wattu64 {
    ($obj:expr, $name:expr, $value:expr) => {
        try_h5!(
            $obj.new_attr_builder()
                .with_data::<'_, _, u64, Dim<[usize; 2]>>(&arr2(&[[$value]]))
                .create($name),
            format!("creating u64 attr {} value={}", $name, $value)
        )?
    };
}

/// The business-end of writing RDRs
///
/// # Errors
/// All errors creating or wri
pub fn write_hdf5(config: &Config, rdrs: &[Rdr], created: &Time, dest: &Path) -> Result<PathBuf> {
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

    let file = try_h5!(File::create(&fpath), format!("opening {fpath:?}"))?;

    wattstr!(file, "Distributor", config.origin, 4);
    wattstr!(file, "Mission_Name", config.satellite.mission.clone(), 20);
    wattstr!(
        file,
        "Platform_Short_Name",
        config.satellite.short_name.clone(),
        3
    );
    wattstr!(file, "N_Dataset_Source", config.origin, 4);
    wattstr!(file, "N_HDF_Creation_Date", attr_date(created), 8);
    wattstr!(file, "N_HDF_Creation_Time", attr_time(created), 16);

    try_h5!(file.create_group("All_Data"), "creating All_Data")?;
    try_h5!(file.create_group("Data_Products"), "creating Data_Products")?;

    let mut indexes: HashMap<String, usize> = HashMap::default();
    for rdr in rdrs.iter() {
        let idx = indexes.entry(rdr.product_id.clone()).or_default();
        let product = products.get(&rdr.product_id).unwrap();
        let path = try_h5!(
            write_rdr_to_alldata(&file, *idx, rdr, product),
            format!("writing {} rdr {idx} to All_Data", product.short_name)
        )?;
        try_h5!(
            write_rdr_to_dataproducts(&file, config, rdr, product, &path),
            format!("writing {} rdr {idx} to Data_Products", product.short_name)
        )?;
        *idx += 1;
    }

    for product in products.values() {
        let rdrs = rdrs
            .iter()
            .filter(|&r| r.product_id == product.product_id)
            .cloned()
            .collect::<Vec<Rdr>>();
        try_h5!(
            write_aggr_dataset(&file, &rdrs, product),
            format!("writing {} rdr aggr to Data_Products", product.short_name)
        )?;
    }

    Ok(fpath)
}

fn attr_date(dt: &Time) -> String {
    dt.format_utc("%Y%m%d")
}

fn attr_time(dt: &Time) -> String {
    // Avoid floating point rouding issues by just rendering micros directly
    format!("{}.{}Z", dt.format_utc("%H%M%S"), dt.iet() % 1_000_000)
}

/// Create an IDPS style RDR filename
fn filename(satid: &str, origin: &str, mode: &str, created: &Time, rdrs: &[Rdr]) -> String {
    // Gather product ids for the first component, sorted alphanumerically
    let mut product_ids = rdrs
        .iter()
        .flat_map(|r| std::iter::once(r.product_id.clone()).chain(r.packed_with.clone()))
        .collect::<HashSet<String>>()
        .iter()
        .cloned()
        .collect::<Vec<String>>();
    product_ids.sort();

    // FIXME: assuming the first RDR is the primary
    let start = rdrs[0].time.clone();
    let end = Time::from_iet(start.iet() + rdrs[0].gran_len);

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

fn write_rdr_to_alldata(
    file: &File,
    gran_idx: usize,
    rdr: &Rdr,
    product: &ProductSpec,
) -> Result<String> {
    if file.group("All_Data").is_err() {
        try_h5!(file.create_group("All_Data"), "creating All_Data")?;
    }
    let name = format!(
        "/All_Data/{}_All/RawApplicationPackets_{gran_idx}",
        product.short_name
    );
    try_h5!(
        file.new_dataset_builder()
            .with_data(&arr1(&rdr.as_bytes()?[..]))
            .create(name.clone().as_str()),
        format!("creating dataset {name}")
    )?;
    Ok(name)
}

/// Create Data_Products/<shortname> and set attribtes returning group path.
fn write_product_group(file: &File, config: &Config, product: &ProductSpec) -> Result<String> {
    let group_name = format!("Data_Products/{}", product.short_name);
    if file.group(&group_name).is_err() {
        let group = try_h5!(
            file.create_group(&group_name),
            format!("creating group {group_name}")
        )?;

        wattstr!(group, "Instrument_Short_Name", product.sensor, 10);
        wattstr!(group, "N_Collection_Short_Name", product.short_name, 20);
        wattstr!(group, "N_Dataset_Type_Tag", "RDR".to_string(), 3);
        wattstr!(group, "N_Processing_Domain", config.mode, 3);
    }
    Ok(group_name)
}

/// Write required attributes for Data_Products/<shortname>/<shortname>_Gran_<X> dataset.
fn write_product_dataset_attrs(
    file: &File,
    config: &Config,
    rdr: &Rdr,
    product: &ProductSpec,
    dataset_path: &str,
) -> Result<()> {
    let ver = "A1";
    let start = &rdr.time;
    let end = &Time::from_iet(start.iet() + rdr.gran_len);

    let dataset = file
        .dataset(dataset_path)
        .unwrap_or_else(|_| panic!("expected just written dataset {dataset_path} to exist"));

    wattstr!(dataset, "Beginning_Date", attr_date(start), 8);
    wattstr!(dataset, "Beginning_Time", attr_time(start), 16);
    wattstr!(dataset, "Ending_Date", attr_date(end), 8);
    wattstr!(dataset, "Ending_Time", attr_time(end), 16);
    wattstr!(dataset, "N_Creation_Date", attr_date(&rdr.created), 8);
    wattstr!(dataset, "N_Creation_Time", attr_time(&rdr.created), 16);
    wattstr!(dataset, "N_Granule_Status", "N/A".to_string(), 3);
    wattstr!(dataset, "N_Granule_Version", ver, 2);
    wattstr!(dataset, "N_JPSS_Document_Ref", String::new(), 52);
    wattstr!(dataset, "N_LEOA_Flag", "Off".to_string(), 3);
    wattstr!(
        dataset,
        "N_Reference_ID",
        format!("{}:{}:{}", product.short_name, rdr.id, ver),
        39
    );
    wattstr!(dataset, "N_Granule_ID", rdr.id, 15);
    wattstr!(dataset, "N_IDPS_Mode", config.mode.clone(), 3);
    wattstr!(dataset, "N_Software_Version", String::new(), 19);

    wattu64!(dataset, "N_Beginning_Orbit_Number", 1);
    wattu64!(dataset, "N_Beginning_Time_IET", rdr.time.iet());
    wattu64!(dataset, "N_Ending_Time_IET", rdr.time.iet() + rdr.gran_len);

    let apids: HashMap<&str, &ApidInfo> = rdr
        .data
        .apid_list
        .values()
        .map(|a| (a.name.as_str(), a))
        .collect();
    let mut names: Vec<&str> = apids.keys().copied().collect();
    names.sort_unstable();
    let name = "N_Packet_Type";
    let mut pkt_type_arr: Vec<[FixedAscii<17>; 1]> = Vec::default();
    let mut pkt_type_cnt_arr: Vec<u64> = Vec::default();
    for name in names.iter() {
        let apid = apids
            .get(name)
            .expect("apid must be present because names are created from same map");
        if apid.pkts_received == 0 {
            continue;
        }
        let ascii = try_h5!(
            FixedAscii::<17>::from_ascii(apid.name.as_bytes()),
            format!("creating packet type attr ascii for {name}")
        )?;
        pkt_type_arr.push([ascii]);
        pkt_type_cnt_arr.push(apid.pkts_received.into());
    }
    let attr = try_h5!(
        dataset
            .new_attr::<FixedAscii<17>>()
            .shape([pkt_type_arr.len(), 1])
            .create(name),
        format!("creating packet type attr ascii for {name}")
    )?;
    let arr = ndarray::arr2(&pkt_type_arr);
    try_h5!(attr.write(&arr), "writing packet type attr")?;

    let name = "N_Packet_Type_Count";
    let attr = try_h5!(
        dataset
            .new_attr::<u64>()
            .shape([pkt_type_cnt_arr.len(), 1])
            .create(name),
        "creating packet type count attr"
    )?;
    try_h5!(
        attr.write_raw(&pkt_type_cnt_arr),
        "writing packet type count attr"
    )?;

    // TODO: Compute missing percent.
    // This should be based on received vs. expected packet counts
    let (name, val) = ("N_Percent_Missing_Data", 0.0);
    let attr = try_h5!(
        dataset.new_attr::<f32>().shape([1, 1]).create(name),
        format!("creating {name} attr")
    )?;
    try_h5!(attr.write_raw(&[val]), format!("writing {name} attr"))?;

    Ok(())
}

/// Write [Rdr] to Data_Products/<shortname>/<shortname>_Gran_<X>.
///
/// This write a dataset reference type to data that actually resides in All_Data, so that
/// must be created first.
fn write_rdr_to_dataproducts(
    file: &File,
    config: &Config,
    rdr: &Rdr,
    product: &ProductSpec,
    src_path: &str,
) -> Result<()> {
    write_product_group(file, config, product)?;
    let dataset_path = create_dataproducts_gran_dataset(file, product, src_path)?;
    write_product_dataset_attrs(file, config, rdr, product, &dataset_path)?;

    Ok(())
}

/// Write the Data_Products/<shortname>/<shortname_Aggr dataset
fn write_aggr_dataset(file: &File, rdrs: &[Rdr], product: &ProductSpec) -> Result<()> {
    if rdrs.is_empty() {
        return Ok(());
    }
    let group_name = format!("All_Data/{}_All", product.short_name);
    if file.group(&group_name).is_err() {
        try_h5!(
            file.create_group(&group_name),
            format!("creating group {group_name}")
        )?;
    }

    let dataset_path = try_h5!(
        create_dataproducts_aggr_dataset(file, product),
        "writing aggr object ref dataset"
    )?;

    let dataset = try_h5!(
        file.dataset(&dataset_path),
        format!("opening dataset {dataset_path}")
    )?;
    for (name, val) in [
        ("AggregateBeginningOrbitNumber", 0usize),
        ("AggregateEndingOrbitNumber", 0usize),
        ("AggregateNumberGranules", rdrs.len()),
    ] {
        let attr = try_h5!(
            dataset.new_attr::<usize>().shape([1, 1]).create(name),
            format!("creating {dataset_path} attr {name}")
        )?;

        try_h5!(
            attr.write_raw(&[val]),
            format!("writing {dataset_path} attr {name} value={val:?}")
        )?;
    }

    let mut start_rdr = &rdrs[0];
    let mut end_rdr = &rdrs[rdrs.len() - 1];
    for rdr in rdrs {
        if rdr.time > start_rdr.time {
            start_rdr = rdr;
        }
        if rdr.time < end_rdr.time {
            end_rdr = rdr;
        }
    }

    for (name, val) in [
        ("AggregateBeginningDate", attr_date(&start_rdr.time)),
        ("AggregateBeginningTime", attr_time(&start_rdr.time)),
        ("AggregateBeginningGranuleID", start_rdr.id.clone()),
        ("AggregateEndingDate", attr_date(&end_rdr.time)),
        ("AggregateEndingTime", attr_time(&end_rdr.time)),
        ("AggregateEndingGranuleID", end_rdr.id.clone()),
    ] {
        let attr = try_h5!(
            dataset.new_attr::<VarLenAscii>().shape([1, 1]).create(name),
            format!("creating {dataset_path} attr {name}")
        )?;

        let ascii = try_h5!(
            VarLenAscii::from_ascii(&val),
            format!("init {dataset_path} attr {name} ascii value={val:?}")
        )?;

        try_h5!(
            attr.write_raw(&[ascii]),
            format!("writing {dataset_path} attr {name} value={val:?}")
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod filename {
        use hifitime::Epoch;
        use std::str::FromStr;

        use crate::{config::get_default, rdr::Rdr};

        use super::*;

        #[test]
        fn packed_rdrs() {
            let config = get_default("npp").unwrap().unwrap();
            let primary = config
                .products
                .iter()
                .filter(|p| p.product_id == "RVIRS")
                .collect::<Vec<_>>()[0];

            let time = Time::from_epoch(Epoch::from_str("2000-01-01T12:13:14.123456Z").unwrap());
            let fname = filename(
                "npp",
                "origin",
                "ops",
                &Time::now(), // created
                &[Rdr::new(primary, &config.satellite, time.clone())],
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RNSCA-RVIRS");

            assert!(
                fname.contains("d20000101_t1213141_e"),
                "Filename does not contain date string"
            );
        }

        #[test]
        fn no_packed_rdrs() {
            let config = get_default("npp").unwrap().unwrap();
            let primary = config
                .products
                .iter()
                .filter(|p| p.product_id == "RVIRS")
                .collect::<Vec<_>>()[0];

            let time = Time::from_epoch(Epoch::from_str("2000-01-01T12:13:14.123456Z").unwrap());
            let fname = filename(
                "npp",
                "origin",
                "ops",
                &Time::now(), // created
                &[Rdr::new(primary, &config.satellite, time.clone())],
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RVIRS");
            assert!(
                fname.contains("d20000101_t1213141_e"),
                "Filename does not contain date string"
            );
        }
    }
}
