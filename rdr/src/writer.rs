use std::{
    ffi::{c_char, c_ulonglong, c_void, CString},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use hdf5::{types::VarLenAscii, File, Group, Selection};
use hdf5_sys::{
    h5::hsize_t,
    h5d::{H5Dclose, H5Dcreate1, H5Dcreate2, H5Dget_space, H5Dopen, H5Dwrite},
    h5f::{H5Fclose, H5Fopen, H5F_ACC_RDWR},
    h5g::{H5Gclose, H5Gopen},
    h5i::{hid_t, H5I_INVALID_HID},
    h5p::H5P_DEFAULT,
    h5r::{
        H5R_type_t::{self, H5R_DATASET_REGION},
        H5Rcreate,
    },
    h5s::{H5Sclose, H5Screate, H5Screate_simple, H5Sselect_all, H5S_ALL, H5S_UNLIMITED},
    h5t::{H5T_class_t::H5T_REFERENCE, H5T_STD_REF_DSETREG},
};
use ndarray::arr1;
use tracing::trace;

use crate::{config::Config, rdr::Rdr};

/// Create an IDPS style RDR filename
pub fn filename(config: &Config, rdr: &Rdr, created: &DateTime<Utc>) -> String {
    let mut product_ids = [
        vec![rdr.product.product_id.clone()],
        rdr.product.packed_with.clone(),
    ]
    .concat();
    product_ids.sort();

    let start_ns = i64::try_from(rdr.granule_utc * 1000).unwrap();
    let start_dt: DateTime<Utc> = DateTime::from_timestamp_nanos(start_ns);
    let end_ns = i64::try_from(rdr.granule_utc + rdr.product.gran_len * 1000).unwrap();
    let end_dt: DateTime<Utc> = DateTime::from_timestamp_nanos(end_ns);

    format!(
        "{}_{}_d{}_t{}_e{}_c{}_{}u_{}.h5",
        product_ids.join("-"),
        config.satellite.id,
        start_dt.format("%Y%m%d"),
        &start_dt.format("%H%M%S%f").to_string()[..7],
        &end_dt.format("%H%M%S%f").to_string()[..7],
        &created.format("%Y%m%d%H%M%S%f").to_string()[..20],
        &config.origin[..3],
        &config.mode[..3],
    )
}

pub fn write_hdf5(config: &Config, rdr: &Rdr, packed: &[Rdr], dest: &Path) -> Result<PathBuf> {
    if !dest.is_dir() {
        bail!("dest must be a directory");
    }
    let created = Utc::now();
    let fpath = dest.join(filename(config, rdr, &created));

    {
        let mut file = File::create(&fpath)?;

        set_global_attrs(&mut file, config, &created)?;

        let all_data_group = file.create_group("All_Data")?;
        write_alldata_group(&all_data_group, &[rdr.clone()])?;
        write_alldata_group(&all_data_group, packed)?;

        file.create_group(&format!("Data_Products/{}", rdr.product.short_name))?;
        if !packed.is_empty() {
            file.create_group(&format!("Data_Products/{}", packed[0].product.short_name))?;
        }
    }

    write_dataproducts_group(&fpath, &[rdr.clone()])?;
    // write_dataproducts_group(&fpath, packed)?;

    Ok(fpath)
}

fn set_global_attrs(file: &mut File, config: &Config, created: &DateTime<Utc>) -> Result<()> {
    for (name, val) in [
        ("Distributor", "arch"),
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
        file.new_attr::<VarLenAscii>()
            .shape([1, 1])
            .create(name)
            .with_context(|| format!("creating {name} attribute"))?
            .write_raw(&[VarLenAscii::from_ascii(&val)
                .with_context(|| format!("failed to create FixedAscii for {name}"))?])
            .with_context(|| format!("writing {name} attribute"))?;
    }

    Ok(())
}

fn write_alldata_group(group: &Group, rdrs: &[Rdr]) -> Result<()> {
    if rdrs.is_empty() {
        return Ok(());
    }
    let subgroup = group.create_group(&format!("{}_All", rdrs[0].product.short_name))?;
    for (idx, rdr) in rdrs.iter().enumerate() {
        let name = format!("RawApplicationPackets_{idx}");
        subgroup
            .new_dataset_builder()
            .with_data(&arr1(&rdr.compile()[..]))
            .create(name.clone().as_str())
            .with_context(|| format!("creating {name}"))?;
    }

    Ok(())
}

macro_rules! cstr {
    ($s:expr) => {
        CString::new($s)?.as_ptr().cast::<c_char>()
    };
}

#[allow(clippy::too_many_lines)]
fn write_dataproducts_group(fpath: &Path, rdrs: &[Rdr]) -> Result<()> {
    if rdrs.is_empty() {
        return Ok(());
    }

    let file_id = unsafe {
        let id = H5Fopen(
            cstr!(fpath.as_os_str().as_bytes()),
            H5F_ACC_RDWR,
            H5P_DEFAULT,
        );
        if id == H5I_INVALID_HID {
            bail!("failed to open file {fpath:?}: {id}");
        }
        id
    };

    let src_grp_id = unsafe {
        let grp_name = format!("/All_Data/{}_All", rdrs[0].product.short_name);
        let id = H5Gopen(
            file_id,
            CString::new(grp_name.clone())?.as_ptr().cast::<c_char>(),
            H5P_DEFAULT,
        );
        if id == H5I_INVALID_HID {
            bail!("failed to open source group {grp_name}");
        }
        id
    };

    // Assume group was already created
    let dst_grp_id = unsafe {
        let grp_name = format!("/Data_Products/{}", rdrs[0].product.short_name);
        let id = H5Gopen(
            file_id,
            CString::new(grp_name.clone())?.as_ptr().cast::<c_char>(),
            H5P_DEFAULT,
        );
        if id == H5I_INVALID_HID {
            bail!("failed to open dest group {grp_name}");
        }
        id
    };

    for (idx, rdr) in rdrs.iter().enumerate() {
        let src_path = format!(
            "/All_Data/{}_All/RawApplicationPackets_{}",
            rdr.product.short_name, idx
        );
        let dst_path = format!(
            "/Data_Products/{}/{}_Gran_{}",
            rdr.product.short_name, rdr.product.short_name, idx
        );
        let mut failed = false;
        unsafe {
            // 1. Create or open the dataset that contains the region
            let src_dataset_id = H5Dopen(file_id, cstr!(src_path.clone()), H5P_DEFAULT);
            trace!(
                "source dataset invalid: {}",
                src_dataset_id == H5I_INVALID_HID
            );
            failed |= src_dataset_id == H5I_INVALID_HID;

            // 2. Get the dataspace for the dataset
            let src_space_id = H5Dget_space(src_dataset_id);
            trace!(
                "source dataspace invalid: {}",
                src_space_id == H5I_INVALID_HID
            );
            failed |= src_space_id == H5I_INVALID_HID;

            // 3. Define a selection that specifies the region
            let select_err = H5Sselect_all(src_space_id);
            trace!("select source error: {}", select_err < 0);
            failed |= select_err < 0;

            // 4. Create a region reference using the dataset and dataspace with selection
            let src_grp_id = H5Gopen(
                file_id,
                cstr!(format!("All_Data/{}_All", rdr.product.short_name)),
                H5P_DEFAULT,
            );
            trace!("open source group error: {}", src_grp_id == H5I_INVALID_HID);
            failed |= src_grp_id == H5I_INVALID_HID;
            let ref_id: hid_t = 0;
            let ref_err = H5Rcreate(
                ref_id as *mut _,
                src_grp_id,
                cstr!(format!("RawApplicationPackets_{idx}")),
                H5R_DATASET_REGION,
                src_space_id,
            );
            trace!("create reference error: {}", ref_err < 0);
            failed |= ref_err < 0;

            H5Gclose(src_grp_id);
            H5Sclose(src_space_id);
            H5Dclose(src_dataset_id);

            if failed {
                bail!("failed to create source dataset reference");
            }

            //
            //
            // let dims: *const hsize_t = &(1 as hsize_t);
            // let dst_space_id = H5Screate_simple(1, dims, std::ptr::null());
            //
            // let dst_dataset_id = H5Dcreate2(
            //     file_id,
            //     CString::new(dst_path.clone())?.as_ptr().cast::<c_char>(),
            //     *H5T_STD_REF_DSETREG,
            //     dst_space_id as hid_t,
            //     H5P_DEFAULT,
            //     H5P_DEFAULT,
            //     H5P_DEFAULT,
            // );
            //
            //
            //
            // H5Dclose(src_dataset_id);
            // H5Dclose(dst_dataset_id);
            // H5Sclose(dst_space_id);
            // H5Sclose(src_space_id);
            //
            // if dst_space_id == H5I_INVALID_HID {
            //     bail!("failed to create simple dataspace for {dst_path}");
            // }
            // if dst_dataset_id == H5I_INVALID_HID {
            //     bail!("failed to create dataset for {dst_path}");
            // }
            // if src_space_id == H5I_INVALID_HID {
            //     bail!("failed to get id for source dataspace {src_path}");
            // }
            // if src_dataset_id == H5I_INVALID_HID {
            //     bail!("failed to get id for source dataset {src_path}");
            // }
            // if select_err < 0 {
            //     bail!("failed to select dataspace for {src_path}");
            // }
            // if ref_err < 0 {
            //     bail!("failed to create reference to {src_path} for {dst_path}");
            // }
        }
    }

    unsafe {
        if H5Gclose(src_grp_id) < 0 {
            bail!("failed to close src group");
        }
        if H5Gclose(dst_grp_id) < 0 {
            bail!("failed to close dst group");
        }
        if H5Fclose(file_id) < 0 {
            bail!("failed to close h5 file");
        }
    }

    Ok(())
}

/*
 {'AggregateBeginningDate': array([[b'20170927']], dtype='|S8'),
 'AggregateBeginningGranuleID': array([[b'NPP001871926926']], dtype='|S15'),
 'AggregateBeginningOrbitNumber': array([[0]], dtype=uint64),
 'AggregateBeginningTime': array([[b'135809.600000Z']], dtype='|S14'),
 'AggregateEndingDate': array([[b'20170927']], dtype='|S8'),
 'AggregateEndingGranuleID': array([[b'NPP001871926926']], dtype='|S15'),
 'AggregateEndingOrbitNumber': array([[0]], dtype=uint64),
 'AggregateEndingTime': array([[b'135934.950000Z']], dtype='|S14'),
 'AggregateNumberGranules': array([[1]], dtype=uint64)}
*/
fn set_attr_group_attrs() -> Result<()> {
    todo!();
}

#[cfg(test)]
mod tests {
    use super::*;

    mod filename {
        use std::collections::{HashMap, VecDeque};

        use crate::{config::get_default, rdr::StaticHeader};

        use super::*;

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
                &config,
                &Rdr {
                    product: primary.clone(),
                    granule_time: dt.timestamp_micros() as u64,
                    granule_utc: dt.timestamp_micros() as u64,
                    header: StaticHeader::default(),
                    apids: HashMap::default(),
                    trackers: HashMap::default(),
                    storage: VecDeque::default(),
                },
                &Utc::now(),
            );

            assert_eq!(fname, "RNSCA-RVIRS");
        }
    }
}
