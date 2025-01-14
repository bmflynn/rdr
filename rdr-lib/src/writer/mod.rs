mod hdfc;

use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use hdf5::{types::FixedAscii, File};
use hdfc::{create_dataproducts_aggr_dataset, create_dataproducts_gran_dataset};
use ndarray::{arr1, arr2, Dim};

use crate::{
    attr_date, attr_time,
    error::{Error, Result},
    rdr::Rdr,
    AggrMeta, GranuleMeta, Meta, ProductMeta, Time,
};

/// Write a string attr with specific len with shape [1, 1]
macro_rules! wattstr {
    ($obj:expr, $name:expr, $value:expr, $maxlen:expr) => {
        $obj.new_attr_builder()
            .with_data::<'_, _, _, Dim<[usize; 2]>>(&arr2(&[[FixedAscii::<$maxlen>::from_ascii(
                &(($value.clone())[..std::cmp::min($maxlen, $value.len())]),
            )
            .map_err(|e| {
                Error::Hdf5Other(format!(
                    "creating ascii value {} for {}: {e}",
                    $name, $value
                ))
            })?]]))
            .create($name)
            .map_err(|e| {
                Error::Hdf5Other(format!(
                    "creating ascii value {} for {}: {e}",
                    $name, $value
                ))
            })?
    };
}

/// Write a u64 attr
macro_rules! wattnum {
    ($obj:expr, $ty:ty, $name:expr, $value:expr) => {
        $obj.new_attr_builder()
            .with_data::<'_, _, $ty, Dim<[usize; 2]>>(&arr2(&[[$value]]))
            .create($name)
            .map_err(|e| {
                Error::Hdf5Other(format!(
                    "creating numeric attr {} value={}: {e}",
                    $name, $value
                ))
            })?
    };
}

/// Write a JPSS H5 RDR file from the provided RDR metadata and granule data.
pub fn create_rdr<P: AsRef<Path> + fmt::Debug>(fpath: P, meta: Meta, rdrs: &[Rdr]) -> Result<()> {
    let file = File::create(&fpath)?;

    write_rdr_meta(
        &file,
        &meta.distributor,
        &meta.mission,
        &meta.platform,
        &meta.dataset_source,
        &meta.created,
    )?;

    // Make sure top-level required groups exist
    file.create_group("All_Data")?;
    file.create_group("Data_Products")?;

    // Write RDR granule datasets (All_Data, Data_Products)
    let mut short_names: HashSet<String> = HashSet::default();
    let mut indexes: HashMap<String, usize> = HashMap::default();
    for rdr in rdrs.iter() {
        let gran_idx = indexes.get(&rdr.meta.collection).unwrap_or(&0);
        write_rdr_granule(&file, *gran_idx, rdr)?;
        short_names.insert(rdr.meta.collection.to_string());
        indexes.insert(rdr.meta.collection.to_string(), gran_idx + 1);
    }

    // Write RDR Aggr datasets (Data_Products)
    for short_name in short_names {
        let rdrs = rdrs
            .iter()
            .filter(|&r| r.meta.collection == short_name)
            .cloned()
            .collect::<Vec<Rdr>>();
        let meta = AggrMeta::from_rdrs(&rdrs);
        write_aggr_dataset(&file, &short_name, &meta)?
    }

    Ok(())
}

pub fn write_rdr_meta(
    file: &File,
    dist: &str,
    mission: &str,
    plat: &str,
    source: &str,
    created: &Time,
) -> Result<()> {
    wattstr!(file, "Distributor", dist, 4);
    wattstr!(file, "Mission_Name", mission, 20);
    wattstr!(file, "Platform_Short_Name", plat, 3);
    wattstr!(file, "N_Dataset_Source", source, 4);
    wattstr!(file, "N_HDF_Creation_Date", attr_date(created), 8);
    wattstr!(file, "N_HDF_Creation_Time", attr_time(created), 16);
    Ok(())
}

pub fn write_rdr_granule(file: &File, gran_idx: usize, rdr: &Rdr) -> Result<()> {
    let rawdata_path = write_rdr_to_alldata(file, gran_idx, rdr)?;
    let product_meta = ProductMeta::from_rdr(rdr);
    write_dataproduct_group(file, &product_meta)?;

    let dataset_path = create_dataproducts_gran_dataset(file, &rdr.meta.collection, &rawdata_path)
        .map_err(|e| {
            Error::Hdf5Sys(format!(
                "creating {} rdr {gran_idx} {rawdata_path}: {e}",
                rdr.meta.collection
            ))
        })?;

    write_product_dataset_attrs(file, &rdr.meta, &dataset_path)?;

    Ok(())
}

fn write_rdr_to_alldata(file: &File, gran_idx: usize, rdr: &Rdr) -> Result<String> {
    if file.group("All_Data").is_err() {
        file.create_group("All_Data")?;
    }
    let name = format!(
        "/All_Data/{}_All/RawApplicationPackets_{gran_idx}",
        rdr.meta.collection
    );
    file.new_dataset_builder()
        .with_data(&arr1(&rdr.data))
        .create(name.clone().as_str())?;
    Ok(name)
}

/// Create Data_Products/<shortname> and set attribtes returning group path.
fn write_dataproduct_group(file: &File, meta: &ProductMeta) -> Result<String> {
    if file.group("Data_Products").is_err() {
        file.create_group("Data_Products")?;
    }
    let group_name = format!("Data_Products/{}", meta.collection);
    if file.group(&group_name).is_err() {
        let group = file.create_group(&group_name)?;

        wattstr!(group, "Instrument_Short_Name", meta.instrument, 10);
        wattstr!(group, "N_Collection_Short_Name", meta.collection, 20);
        wattstr!(group, "N_Dataset_Type_Tag", meta.dataset_type, 3);
        wattstr!(group, "N_Processing_Domain", meta.processing_domain, 3);
    }
    Ok(group_name)
}

/// Write attribute data from `meta` to the `Data_Products/<shortname>/<shortname>_Gran_<X>` dataset.
fn write_product_dataset_attrs(file: &File, meta: &GranuleMeta, dataset_path: &str) -> Result<()> {
    let dataset = file
        .dataset(dataset_path)
        .unwrap_or_else(|_| panic!("expected just written dataset {dataset_path} to exist"));

    wattstr!(dataset, "Beginning_Date", meta.begin_date, 8);
    wattstr!(dataset, "Beginning_Time", meta.begin_time, 16);
    wattstr!(dataset, "Ending_Date", meta.end_date, 8);
    wattstr!(dataset, "Ending_Time", meta.end_time, 16);
    wattstr!(dataset, "N_Creation_Date", meta.creation_date, 8);
    wattstr!(dataset, "N_Creation_Time", meta.creation_time, 16);
    wattstr!(dataset, "N_Granule_Status", meta.status, 3);
    wattstr!(dataset, "N_Granule_Version", meta.version, 2);
    wattstr!(dataset, "N_JPSS_Document_Ref", meta.jpss_doc, 52);
    wattstr!(dataset, "N_LEOA_Flag", meta.leoa_flag, 3);
    wattstr!(dataset, "N_Reference_ID", meta.reference_id, 39);
    wattstr!(dataset, "N_Granule_ID", meta.id, 15);
    wattstr!(dataset, "N_IDPS_Mode", meta.idps_mode, 3);
    wattstr!(dataset, "N_Software_Version", meta.software_version, 19);
    wattnum!(dataset, u64, "N_Beginning_Orbit_Number", meta.orbit_number);
    wattnum!(dataset, u64, "N_Beginning_Time_IET", meta.begin_time_iet);
    wattnum!(dataset, u64, "N_Ending_Time_IET", meta.end_time_iet);

    // Compute packet type/count arrays
    let mut pkt_type_arr: Vec<[FixedAscii<17>; 1]> = Vec::default();
    let mut pkt_type_cnt_arr: Vec<u64> = Vec::default();
    for (name, count) in meta.packet_type.iter().zip(&meta.packet_type_count) {
        let ascii = FixedAscii::<17>::from_ascii(name.as_bytes()).map_err(|e| {
            Error::Hdf5Other(format!("creating packet type attr ascii for {name}: {e}"))
        })?;
        pkt_type_arr.push([ascii]);
        pkt_type_cnt_arr.push(u64::from(*count));
    }

    // Write N_Packet_Type
    let name = "N_Packet_Type";
    let attr = dataset
        .new_attr::<FixedAscii<17>>()
        .shape([pkt_type_arr.len(), 1])
        .create(name)
        .map_err(|e| Error::Hdf5Other(format!("creating attr N_Packet_Type for {name}: {e}")))?;
    let arr = ndarray::arr2(&pkt_type_arr);
    attr.write(&arr)
        .map_err(|e| Error::Hdf5Other(format!("writing N_Packet_Type for {name}: {e}")))?;

    let name = "N_Packet_Type_Count";
    let attr = dataset
        .new_attr::<u64>()
        .shape([pkt_type_cnt_arr.len(), 1])
        .create(name)
        .map_err(|e| Error::Hdf5Other(format!("creating attr N_Packet_Count for {name}: {e}")))?;
    attr.write_raw(&pkt_type_cnt_arr)
        .map_err(|e| Error::Hdf5Other(format!("writing N_Packet_Count for {name}: {e}")))?;

    let (name, val) = ("N_Percent_Missing_Data", meta.percent_missing);
    let attr = dataset
        .new_attr::<f32>()
        .shape([1, 1])
        .create(name)
        .map_err(|e| Error::Hdf5Other(format!("creating attr {name}: {e}")))?;
    attr.write_raw(&[val])
        .map_err(|e| Error::Hdf5Other(format!("writing attr {name}: {e}")))?;

    Ok(())
}

/// Write the Data_Products/<shortname>/<shortname_Aggr dataset
fn write_aggr_dataset(file: &File, short_name: &str, meta: &AggrMeta) -> Result<()> {
    let group_name = format!("All_Data/{}_All", short_name);
    if file.group(&group_name).is_err() {
        file.create_group(&group_name)?;
    }

    let dataset_path = create_dataproducts_aggr_dataset(file, short_name)
        .map_err(|e| Error::Hdf5Sys(format!("creating aggr dataset for {short_name}: {e}")))?;
    let dataset = file
        .dataset(&dataset_path)
        .map_err(|e| Error::Hdf5Other(format!("opening dataset {dataset_path}: {e}")))?;

    wattnum!(
        dataset,
        u32,
        "AggregateBeginningOrbitNumber",
        meta.begin_orbit_nubmer
    );
    wattnum!(
        dataset,
        u32,
        "AggregateEndingOrbitNumber",
        meta.end_orbit_number
    );
    wattnum!(dataset, u32, "AggregateNumberGranules", meta.num_granules);

    wattstr!(
        dataset,
        "AggregateBeginningDate",
        meta.begin_date.to_string(),
        20
    );
    wattstr!(
        dataset,
        "AggregateBeginningTime",
        meta.begin_time.to_string(),
        20
    );
    wattstr!(
        dataset,
        "AggregateBeginningGranuleID",
        meta.begin_granule_id.to_string(),
        20
    );
    wattstr!(
        dataset,
        "AggregateEndingDate",
        meta.end_date.to_string(),
        20
    );
    wattstr!(
        dataset,
        "AggregateEndingTime",
        meta.end_time.to_string(),
        20
    );
    wattstr!(
        dataset,
        "AggregateEndingGranuleID",
        meta.end_granule_id.to_string(),
        20
    );
    Ok(())
}
