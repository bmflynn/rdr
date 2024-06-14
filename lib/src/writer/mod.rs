mod hdfc;

use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, Utc};
use hdf5::{types::VarLenAscii, File};
use ndarray::arr1;

use crate::{
    config::{Config, ProductSpec, SatSpec},
    prelude::*,
    rdr::{Rdr, RdrWriter},
};

/// The business-end of writing RDRs
///
/// # Errors
/// All errors creating or wri
pub fn write_hdf5(
    config: &Config,
    rdrs: &[RdrWriter],
    dest: &Path,
) -> crate::error::Result<PathBuf> {
    let primary = &rdrs[0];
    let packed = &rdrs[1..];

    let fpath = dest.join(filename(
        &config.satellite,
        &config.origin,
        &config.mode,
        &primary.inner,
    ));

    let mut file = File::create(&fpath).with_context(|| format!("opening {fpath:?}"))?;

    set_global_attrs(config, &mut file, &primary.inner.created).context("setting global attrs")?;

    // Handle the primary RDR
    let path = write_rdr_to_alldata(&file, 0, primary)?;
    write_rdr_to_dataproducts(&file, config, primary, &path)?;
    write_aggr_group(&file, &config.satellite, rdrs, &primary.inner.product)?;

    // Handle the packed products
    if !packed.is_empty() {
        for (idx, rdr) in packed.iter().enumerate() {
            let path = write_rdr_to_alldata(&file, idx, rdr)?;
            write_rdr_to_dataproducts(&file, config, rdr, &path)?;
        }
        write_aggr_group(&file, &config.satellite, packed, &packed[0].inner.product)?;
    }

    Ok(fpath)
}

fn attr_date(dt: &DateTime<Utc>) -> String {
    dt.format("%Y%m%d").to_string()
}

fn attr_time(dt: &DateTime<Utc>) -> String {
    dt.format("%H:%M:%S.%fZ").to_string()
}

/// Create an IDPS style RDR filename
fn filename(sat: &SatSpec, origin: &str, mode: &str, rdr: &Rdr) -> String {
    let mut product_ids = [
        vec![rdr.product.product_id.clone()],
        rdr.packed_with.clone(),
    ]
    .concat();
    product_ids.sort();

    let (start, end) = granule_dt_range(rdr);

    format!(
        "{}_{}_d{}_t{}_e{}_c{}_{}u_{}.h5",
        product_ids.join("-"),
        sat.id,
        &start.format("%Y%m%d").to_string(),
        &start.format("%H%M%S%f").to_string()[..7],
        &end.format("%H%M%S%f").to_string()[..7],
        &rdr.created.format("%Y%m%d%H%M%S%f").to_string()[..20],
        &origin[..3],
        &mode[..3],
    )
}

fn set_global_attrs(config: &Config, file: &mut File, created: &DateTime<Utc>) -> Result<()> {
    for (name, val) in [
        ("Distributor", config.distributor.clone()),
        ("Mission_Name", config.satellite.mission.clone()),
        ("Platform_Short_Name", config.satellite.short_name.clone()),
        ("N_Dataset_Source", config.origin.clone()),
        ("N_HDF_Creation_Date", attr_date(created)),
        ("N_HDF_Creation_Time", attr_time(created)),
    ] {
        let attr = file
            .new_attr::<VarLenAscii>()
            .shape([1, 1])
            .create(name)
            .map_err(|e| Error::Hdf5 {
                name: format!("name={name} val={val}"),
                msg: e.to_string(),
            })?;

        let ascii = VarLenAscii::from_ascii(&val).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;

        attr.write_raw(&[ascii]).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;
    }

    Ok(())
}

fn write_rdr_to_alldata(file: &File, gran_idx: usize, rdr: &RdrWriter) -> Result<String> {
    if file.group("All_Data").is_err() {
        file.create_group("All_Data").map_err(|e| Error::Hdf5 {
            name: "All_Data".to_string(),
            msg: e.to_string(),
        })?;
    }
    let name = format!(
        "/All_Data/{}_All/RawApplicationPackets_{gran_idx}",
        rdr.inner.product.short_name
    );
    file.new_dataset_builder()
        .with_data(&arr1(&rdr.compile()[..]))
        .create(name.clone().as_str())
        .map_err(|e| Error::Hdf5 {
            name: name.to_string(),
            msg: e.to_string(),
        })?;
    Ok(name)
}

// FIXME: This is a big mess
fn set_product_dataset_attrs(
    file: &File,
    config: &Config,
    rdr: &Rdr,
    dataset_path: &str,
) -> Result<()> {
    let (start_dt, end_dt) = granule_dt_range(rdr);
    let gran_id = granule_id(&config.satellite, rdr);
    let ver = "A1";

    let dataset = file
        .dataset(dataset_path)
        .unwrap_or_else(|_| panic!("expected just written dataset {dataset_path} to exist"));
    for (name, val) in [
        ("Beginning_Date", attr_date(&start_dt)),
        ("Beginning_Time", attr_time(&start_dt)),
        ("Ending_Date", attr_date(&end_dt)),
        ("Ending_Time", attr_time(&end_dt)),
        ("N_Creation_Date", attr_date(&rdr.created)),
        ("N_Creation_Time", attr_time(&rdr.created)),
        ("N_Granule_Status", "N/A".to_string()),
        ("N_Granule_Version", ver.to_string()),
        ("N_JPSS_Document_Ref", String::new()),
        ("N_LEOA_Flag", "Off".to_string()),
        ("N_Primary_Label", "Primary".to_string()),
        (
            "N_Reference_ID",
            format!("{}:{}:{}", rdr.product.short_name, gran_id, ver),
        ),
        ("N_Granule_ID", gran_id),
        ("N_IDPS_Mode", config.mode.clone()),
        ("N_Software_Version", String::new()),
    ] {
        let attr = dataset
            .new_attr::<VarLenAscii>()
            .shape([1, 1])
            .create(name)
            .map_err(|e| Error::Hdf5 {
                name: format!("name={name} val={val}"),
                msg: e.to_string(),
            })?;

        let ascii = VarLenAscii::from_ascii(&val).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;

        attr.write_raw(&[ascii]).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;
    }

    for (name, val) in [
        ("N_Beginning_Orbit_Number", 0),
        ("N_Beginning_Time_IET", rdr.granule_time),
        ("N_Ending_Time_IET", rdr.granule_time + rdr.product.gran_len),
    ] {
        let attr = dataset
            .new_attr::<u64>()
            .shape([1, 1])
            .create(name)
            .map_err(|e| Error::Hdf5 {
                name: format!("name={name} val={val}"),
                msg: e.to_string(),
            })?;

        attr.write_raw(&[val]).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;
    }

    let name = "N_Packet_Type";
    let apid_names: Vec<String> = rdr.product.apids.iter().map(|a| a.name.clone()).collect();
    let mut pkt_type_arr: Vec<VarLenAscii> = Vec::default();
    for (i, x) in apid_names.iter().enumerate() {
        let ascii = VarLenAscii::from_ascii(x.as_bytes()).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val[{i}]={x:?}"),
            msg: e.to_string(),
        })?;
        pkt_type_arr.push(ascii);
    }
    let attr = dataset
        .new_attr::<VarLenAscii>()
        .packed(true)
        .shape([pkt_type_arr.len(), 1])
        .create(name)
        .map_err(|e| Error::Hdf5 {
            name: format!("creating name={name} val={apid_names:?}"),
            msg: e.to_string(),
        })?;
    attr.write_raw(&pkt_type_arr).map_err(|e| Error::Hdf5 {
        name: format!("writing name={name} val={apid_names:?}"),
        msg: e.to_string(),
    })?;

    // TODO: Get/compute N_Packet_Type_Count
    // Packet counts could be tracked as part of Rdr object

    // TODO: Compute missing percent.
    // This should be based on received vs. expected packet counts
    let (name, val) = ("N_Percent_Missing_Data", 0.0);
    let attr = dataset
        .new_attr::<f32>()
        .shape([1, 1])
        .create(name)
        .map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;

    attr.write_raw(&[val]).map_err(|e| Error::Hdf5 {
        name: format!("name={name} val={val}"),
        msg: e.to_string(),
    })?;

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn write_rdr_to_dataproducts(
    file: &File,
    config: &Config,
    rdr: &RdrWriter,
    src_path: &str,
) -> Result<()> {
    let group_name = format!("Data_Products/{}", rdr.inner.product.short_name);
    if file.group(&group_name).is_err() {
        file.create_group(&group_name).map_err(|e| Error::Hdf5 {
            name: group_name,
            msg: e.to_string(),
        })?;
    }
    let mut writer = hdfc::DataProductsRefWriter::default();
    let dataset_path = writer.write_ref(file, &rdr.inner, src_path)?;

    set_product_dataset_attrs(file, config, &rdr.inner, &dataset_path)?;

    Ok(())
}

fn granule_dt(utc: u64) -> DateTime<Utc> {
    let start_ns = i64::try_from(utc * 1000).unwrap_or(0);
    DateTime::from_timestamp_nanos(start_ns)
}

fn granule_dt_range(rdr: &Rdr) -> (DateTime<Utc>, DateTime<Utc>) {
    (
        granule_dt(rdr.granule_utc),
        granule_dt(rdr.granule_utc + rdr.product.gran_len * 100),
    )
}

fn granule_id(sat: &SatSpec, rdr: &Rdr) -> String {
    format!(
        "{}{:012}",
        sat.id.to_uppercase(),
        (rdr.granule_time - sat.base_time) / 100_000
    )
}

fn write_aggr_group(
    file: &File,
    sat: &SatSpec,
    rdrs: &[RdrWriter],
    product: &ProductSpec,
) -> Result<()> {
    if rdrs.is_empty() {
        return Ok(());
    }
    let name = format!("/Data_Products/{0}/{0}_Aggr", product.short_name);
    let group = file.create_group(&name).map_err(|e| Error::Hdf5 {
        name: name.to_string(),
        msg: e.to_string(),
    })?;

    for (name, val) in [
        ("AggregateBeginningOrbitNumber", 0usize),
        ("AggregateEndingOrbitNumber", 0usize),
        ("AggregateNumberGranules", rdrs.len()),
    ] {
        let attr = group
            .new_attr::<usize>()
            .shape([1, 1])
            .create(name)
            .map_err(|e| Error::Hdf5 {
                name: format!("name={name} val={val}"),
                msg: e.to_string(),
            })?;

        attr.write_raw(&[val]).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;
    }

    let mut start_rdr = &rdrs[0];
    let mut end_rdr = &rdrs[rdrs.len() - 1];
    for rdr in rdrs {
        if rdr.inner.granule_utc > start_rdr.inner.granule_utc {
            start_rdr = rdr;
        }
        if rdr.inner.granule_utc < end_rdr.inner.granule_utc {
            end_rdr = rdr;
        }
    }
    let start_dt = granule_dt(start_rdr.inner.granule_utc);
    let end_dt = granule_dt(end_rdr.inner.granule_utc);

    for (name, val) in [
        (
            "AggregateBeginningDate",
            start_dt.format("%Y%m%d").to_string(),
        ),
        (
            "AggregateBeginningTime",
            start_dt.format("%H:%M:%S.%fZ").to_string(),
        ),
        (
            "AggregateBeginningGranuleID",
            granule_id(sat, &start_rdr.inner),
        ),
        ("AggregateEndingDate", end_dt.format("%Y%m%d").to_string()),
        (
            "AggregateEndingTime",
            end_dt.format("%H:%M:%S.%fZ").to_string(),
        ),
        ("AggregateEndingGranuleID", granule_id(sat, &end_rdr.inner)),
    ] {
        let attr = group
            .new_attr::<VarLenAscii>()
            .shape([1, 1])
            .create(name)
            .map_err(|e| Error::Hdf5 {
                name: format!("name={name} val={val}"),
                msg: e.to_string(),
            })?;

        let ascii = VarLenAscii::from_ascii(&val).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;

        attr.write_raw(&[ascii]).map_err(|e| Error::Hdf5 {
            name: format!("name={name} val={val}"),
            msg: e.to_string(),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod filename {
        use std::collections::{HashMap, VecDeque};

        use crate::{config::get_default, rdr::Rdr, rdr::StaticHeader};

        use super::*;

        #[test]
        fn packed_rdrs() {
            let config = get_default("npp").unwrap();
            let primary = config
                .products
                .iter()
                .filter(|p| p.product_id == "RVIRS")
                .collect::<Vec<_>>()[0];

            let dt = "2000-01-01T12:13:14Z".parse::<DateTime<Utc>>().unwrap();
            let fname = filename(
                &SatSpec {
                    id: "npp".to_string(),
                    short_name: "short_name".to_string(),
                    base_time: 0,
                    mission: "mission".to_string(),
                },
                "origin",
                "mode",
                &Rdr {
                    product: primary.clone(),
                    packed_with: vec!["RNSCA".to_string()],
                    granule_time: u64::try_from(dt.timestamp_micros()).unwrap(),
                    granule_utc: u64::try_from(dt.timestamp_micros()).unwrap(),
                    header: StaticHeader::default(),
                    apids: HashMap::default(),
                    trackers: HashMap::default(),
                    storage: VecDeque::default(),
                    created: Utc::now(),
                },
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

            let dt = "2000-01-01T12:13:14Z".parse::<DateTime<Utc>>().unwrap();
            let fname = filename(
                &SatSpec {
                    id: "npp".to_string(),
                    short_name: "short_name".to_string(),
                    base_time: 0,
                    mission: "mission".to_string(),
                },
                "origin",
                "mode",
                &Rdr {
                    product: primary.clone(),
                    packed_with: Vec::default(),
                    granule_time: dt.timestamp_micros() as u64,
                    granule_utc: dt.timestamp_micros() as u64,
                    header: StaticHeader::default(),
                    apids: HashMap::default(),
                    trackers: HashMap::default(),
                    storage: VecDeque::default(),
                    created: Utc::now(),
                },
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RVIRS");
        }
    }
}
