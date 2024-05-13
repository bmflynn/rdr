use std::{
    ffi::{c_char, CString},
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use hdf5::{types::VarLenAscii, File};
use hdf5_sys::{
    h5::hsize_t,
    h5d::{H5Dclose, H5Dcreate2, H5Dget_space, H5Dopen2, H5Dwrite},
    h5g::{H5Gclose, H5Gopen},
    h5i::{hid_t, H5I_INVALID_HID},
    h5p::H5P_DEFAULT,
    h5r::{hdset_reg_ref_t, H5R_type_t::H5R_DATASET_REGION, H5Rcreate},
    h5s::{H5Sclose, H5Screate_simple, H5Sselect_all, H5S_ALL},
    h5t::H5T_STD_REF_DSETREG,
};
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

    set_global_attrs(&config, &mut file, &created).context("setting global attrs")?;

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
    let mut writer = DataProductsRefWriter::default();
    writer.write_ref(file, rdr, src_path)?;

    Ok(())
}

macro_rules! cstr {
    ($s:expr) => {
        CString::new($s)
            .with_context(|| format!("creating c_str from {}", $s))?
            .as_ptr()
            .cast::<c_char>()
    };
}

macro_rules! chkid {
    ($id:expr, $name:expr, $msg:expr) => {
        if $id == H5I_INVALID_HID {
            return Err(Error::Hdf5 {
                name: $name,
                msg: $msg,
            });
        }
    };
}

macro_rules! chkerr {
    ($id:expr, $name:expr, $msg:expr) => {
        if $id < 0 {
            return Err(Error::Hdf5 {
                name: $name,
                msg: $msg,
            });
        }
    };
}

/// Helper for writing the Data_Products region reference that cleans up low-level h5
/// resource on drop
#[derive(Default)]
struct DataProductsRefWriter {
    src_group_id: hid_t,
    src_dataset_id: hid_t,
    src_dataspace_id: hid_t,
    dst_group_id: hid_t,
    dst_dataset_id: hid_t,
}

impl DataProductsRefWriter {
    fn write_ref(&mut self, file: &File, rdr: &Rdr, src_path: &str) -> Result<()> {
        let (src_group_path, _) = src_path
            .rsplit_once('/')
            .expect("dataset path to have 3 parts");
        self.src_group_id = unsafe { H5Gopen(file.id(), cstr!(src_group_path), H5P_DEFAULT) };
        chkid!(
            self.src_group_id,
            src_group_path.to_string(),
            format!("opening source group: {src_group_path}")
        );

        self.src_dataset_id =
            unsafe { H5Dopen2(file.id(), cstr!(src_path.to_string()), H5P_DEFAULT) };
        chkid!(
            self.src_dataset_id,
            src_path.to_string(),
            format!("opening source dataset: {src_path}")
        );

        self.src_dataspace_id = unsafe { H5Dget_space(self.src_dataset_id) };
        chkid!(
            self.src_dataspace_id,
            src_path.to_string(),
            "getting source dataspace".to_string()
        );

        let errid = unsafe { H5Sselect_all(self.src_dataspace_id) };
        chkerr!(
            errid,
            src_path.to_string(),
            "selecting dataspace".to_string()
        );
        let (_, src_dataset_name) = src_path
            .rsplit_once('/')
            .expect("dataset path to have 3 parts");

        let mut ref_id: hdset_reg_ref_t = [0; 12];
        let errid = unsafe {
            H5Rcreate(
                ref_id.as_mut_ptr().cast(),
                self.src_group_id,
                cstr!(src_dataset_name),
                H5R_DATASET_REGION,
                self.src_dataspace_id,
            )
        };
        chkerr!(
            errid,
            src_dataset_name.to_string(),
            format!("creating reference to source dataset {src_dataset_name}")
        );

        let dst_group_path = format!("/Data_Products/{0}", rdr.product.short_name,);
        self.dst_group_id =
            unsafe { H5Gopen(file.id(), cstr!(dst_group_path.to_string()), H5P_DEFAULT) };
        chkid!(
            self.dst_group_id,
            dst_group_path.to_string(),
            format!("opening dest group: {dst_group_path}")
        );

        let dim = [1 as hsize_t];
        let maxdim = [1 as hsize_t];
        let space_id = unsafe { H5Screate_simple(1, dim.as_ptr(), maxdim.as_ptr()) };
        chkid!(
            space_id,
            src_dataset_name.to_string(),
            "creating dest dataset dataspace".to_string()
        );

        // Use the index from the RawAP dataset for the product dataset
        let sidx = src_dataset_name
            .rsplit('_')
            .next()
            .expect("dataset name to end with _{idx}");
        let dst_dataset_name = format!("{}_Gran_{sidx}", rdr.product.short_name,);
        self.dst_dataset_id = unsafe {
            H5Dcreate2(
                self.dst_group_id,
                cstr!(dst_dataset_name.clone()),
                *H5T_STD_REF_DSETREG,
                space_id,
                H5P_DEFAULT,
                H5P_DEFAULT,
                H5P_DEFAULT,
            )
        };
        chkid!(
            self.dst_dataset_id,
            dst_dataset_name.to_string(),
            format!("creating dest dataset with reference: {dst_dataset_name}")
        );

        let errid = unsafe {
            H5Dwrite(
                self.dst_dataset_id,
                *H5T_STD_REF_DSETREG,
                H5S_ALL,
                H5S_ALL,
                H5P_DEFAULT,
                ref_id.as_ptr().cast(),
            )
        };
        chkerr!(
            errid,
            dst_dataset_name,
            "writing ref to dest dataset".to_string()
        );

        Ok(())
    }
}

impl Drop for DataProductsRefWriter {
    fn drop(&mut self) {
        unsafe {
            H5Gclose(self.src_group_id);
            H5Sclose(self.src_dataspace_id);
            H5Dclose(self.src_dataset_id);
            H5Gclose(self.dst_group_id);
            H5Dclose(self.dst_dataset_id);
        }
    }
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
