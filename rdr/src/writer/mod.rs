mod hdfc;

use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, Utc};
use hdf5::{types::VarLenAscii, File};
use ndarray::arr1;

use crate::{
    config::{Config, ProductSpec, SatSpec},
    prelude::*,
    rdr::Rdr,
};

/// The business-end of writing RDRs
///
/// # Errors
/// All errors creating or wri
pub fn write_hdf5(
    config: &Config,
    rdr: &Rdr,
    packed: &[Rdr],
    dest: &Path,
) -> crate::error::Result<PathBuf> {
    let created = Utc::now();
    let fpath = dest.join(filename(
        &config.satellite,
        &config.origin,
        &config.mode,
        rdr,
        &created,
    ));

    let mut file = File::create(&fpath).with_context(|| format!("opening {fpath:?}"))?;

    set_global_attrs(config, &mut file, &created).context("setting global attrs")?;

    // Handle the primary RDR
    let path = write_rdr_to_alldata(&file, 0, rdr)?;
    write_rdr_to_dataproducts(&file, rdr, &path)?;
    write_aggr_group(&file, 1, &rdr.product)?;

    // Handle the packed products
    if !packed.is_empty() {
        for (idx, rdr) in packed.iter().enumerate() {
            let path = write_rdr_to_alldata(&file, idx, rdr)?;
            write_rdr_to_dataproducts(&file, rdr, &path)?;
        }
        write_aggr_group(&file, 1, &packed[0].product)?;
    }

    Ok(fpath)
}

/// Create an IDPS style RDR filename
fn filename(sat: &SatSpec, origin: &str, mode: &str, rdr: &Rdr, created: &DateTime<Utc>) -> String {
    let mut product_ids = [
        vec![rdr.product.product_id.clone()],
        rdr.packed_with.clone(),
    ]
    .concat();
    product_ids.sort();

    let start_ns = i64::try_from(rdr.granule_utc * 1000).unwrap_or(0);
    let start_dt: DateTime<Utc> = DateTime::from_timestamp_nanos(start_ns);
    let end_ns = i64::try_from(rdr.granule_utc + rdr.product.gran_len * 1000).unwrap_or(0);
    let end_dt: DateTime<Utc> = DateTime::from_timestamp_nanos(end_ns);

    format!(
        "{}_{}_d{}_t{}_e{}_c{}_{}u_{}.h5",
        product_ids.join("-"),
        sat.id,
        start_dt.format("%Y%m%d"),
        &start_dt.format("%H%M%S%f").to_string()[..7],
        &end_dt.format("%H%M%S%f").to_string()[..7],
        &created.format("%Y%m%d%H%M%S%f").to_string()[..20],
        &origin[..3],
        &mode[..3],
    )
}

fn set_global_attrs(config: &Config, file: &mut File, created: &DateTime<Utc>) -> Result<()> {
    for (name, val) in [
        ("Distributor", &config.distributor),
        ("Mission_Name", &config.satellite.mission),
        ("Platform_Short_Name", &config.satellite.short_name),
        ("N_Dataset_Source", &config.origin),
        ("N_HDF_Creation_Date", &created.format("%Y%m%d").to_string()),
        (
            "N_HDF_Creation_Time",
            &format!(
                "{}.{}Z",
                &created.format("%H%M%S").to_string(),
                &created.format("%f").to_string()[..6]
            ),
        ),
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

fn write_rdr_to_alldata(file: &File, gran_idx: usize, rdr: &Rdr) -> Result<String> {
    if file.group("All_Data").is_err() {
        file.create_group("All_Data").map_err(|e| Error::Hdf5 {
            name: "All_Data".to_string(),
            msg: e.to_string(),
        })?;
    }
    let name = format!(
        "/All_Data/{}_All/RawApplicationPackets_{gran_idx}",
        rdr.product.short_name
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

fn write_rdr_to_dataproducts(file: &File, rdr: &Rdr, src_path: &str) -> Result<()> {
    let group_name = format!("Data_Products/{}", rdr.product.short_name);
    if file.group(&group_name).is_err() {
        file.create_group(&group_name).map_err(|e| Error::Hdf5 {
            name: group_name,
            msg: e.to_string(),
        })?;
    }
    let mut writer = hdfc::DataProductsRefWriter::default();
    writer.write_ref(file, rdr, src_path)?;
    /*
                "Beginning_Date": self._format_date_attr(gran_iet),
                "Beginning_Time": self._format_time_attr(gran_iet),
                "Ending_Date": self._format_date_attr(gran_end_iet),
                "Ending_Time": self._format_time_attr(gran_end_iet),
                "N_Beginning_Orbit_Number": np.uint64(self._orbit_num),
                "N_Beginning_Time_IET": np.uint64(gran_iet),
                "N_Creation_Date": self._format_date_attr(creation_time),
                "N_Creation_Time": self._format_time_attr(creation_time),
                "N_Ending_Time_IET": np.uint64(gran_end_iet),
                "N_Granule_ID": gran_id,
                "N_Granule_Status": "N/A",
                "N_Granule_Version": gran_ver,
                "N_IDPS_Mode": self._domain,
                "N_JPSS_Document_Ref": rdr_type.document,
                "N_LEOA_Flag": "Off",
                "N_Packet_Type": [a.name for a in blob_info.apids],
                "N_Packet_Type_Count": [
                    np.uint64(a.pkts_received) for a in blob_info.apids
                ],
                "N_Percent_Missing_Data": np.float32(
                    self._calc_percent_missing(blob_info)
                ),
                "N_Primary_Label": "Primary",  # TODO: find out what this is
                "N_Reference_ID": ":".join([rdr_type.short_name, gran_id, gran_ver]),
                "N_Software_Version": self._software_ver,
    */
    Ok(())
}

fn write_aggr_group(file: &File, num_rdrs: usize, product: &ProductSpec) -> Result<()> {
    let name = format!("/Data_Products/{0}/{0}_Aggr", product.short_name);
    let group = file.create_group(&name).map_err(|e| Error::Hdf5 {
        name: name.to_string(),
        msg: e.to_string(),
    })?;

    for (name, val) in [
        ("AggregateBeginningOrbitNumber", 0usize),
        ("AggregateEndingOrbitNumber", 0usize),
        ("AggregateNumberGranules", num_rdrs),
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

    for (name, val) in [
        ("AggregateBeginningDate", ""),
        ("AggregateBeginningTime", ""),
        ("AggregateBeginningGranuleID", ""),
        ("AggregateEndingDate", ""),
        ("AggregateEndingTime", ""),
        ("AggregateEndingGranuleID", ""),
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

        use crate::{config::get_default, rdr::StaticHeader};

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
                    granule_time: dt.timestamp_micros() as u64,
                    granule_utc: dt.timestamp_micros() as u64,
                    header: StaticHeader::default(),
                    apids: HashMap::default(),
                    trackers: HashMap::default(),
                    storage: VecDeque::default(),
                },
                &Utc::now(),
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
                },
                &Utc::now(),
            );

            let (prefix, _) = fname.split_once('_').unwrap();
            assert_eq!(prefix, "RVIRS");
        }
    }
}
