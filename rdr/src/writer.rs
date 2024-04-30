use std::{
    ffi::{c_char, CString},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
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

    let mut file = File::create(&fpath)?;

    set_global_attrs(&mut file, config, &created)?;

    file.create_group("All_Data")?;
    file.create_group("Data_Products")?;

    file.create_group(&format!("Data_Products/{}", rdr.product.short_name))?;
    write_aggr_group(&file, &[rdr.clone()])?;
    let path = write_rdr_to_alldata(&file, 0, rdr)?;
    write_rdr_to_dataproducts(&file, rdr, &path)?;

    for (idx, rdr) in packed.iter().enumerate() {
        let group_name = &format!("Data_Products/{}", rdr.product.short_name);
        if file.group(group_name).is_err() {
            file.create_group(group_name)?;
            write_aggr_group(&file, packed)?;
        }
        let path = write_rdr_to_alldata(&file, idx, rdr)?;
        write_rdr_to_dataproducts(&file, rdr, &path)?;
    }

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

fn write_rdr_to_alldata(file: &File, gran_idx: usize, rdr: &Rdr) -> Result<String> {
    let name = format!(
        "/All_Data/{}_All/RawApplicationPackets_{gran_idx}",
        rdr.product.short_name
    );
    file.new_dataset_builder()
        .with_data(&arr1(&rdr.compile()[..]))
        .create(name.clone().as_str())
        .with_context(|| format!("creating {name}"))?;
    Ok(name)
}

fn write_rdr_to_dataproducts(file: &File, rdr: &Rdr, src_path: &str) -> Result<()> {
    let mut writer = DataProductsRefWriter::default();
    writer.write_ref(file, rdr, src_path)?;
    Ok(())
}

macro_rules! cstr {
    ($s:expr) => {
        CString::new($s)?.as_ptr().cast::<c_char>()
    };
}

macro_rules! chkid {
    ($id:expr, $msg:expr) => {
        if $id == H5I_INVALID_HID {
            bail!($msg);
        }
    };
}

macro_rules! chkerr {
    ($id:expr, $msg:expr) => {
        if $id < 0 {
            bail!($msg);
        }
    };
}

/// Helper that cleans up low-level h5 resource on drop
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
            format!("opening source group: {src_group_path}")
        );

        self.src_dataset_id =
            unsafe { H5Dopen2(file.id(), cstr!(src_path.to_string()), H5P_DEFAULT) };
        chkid!(
            self.src_dataset_id,
            format!("opening source dataset: {src_path}")
        );

        self.src_dataspace_id = unsafe { H5Dget_space(self.src_dataset_id) };
        chkid!(self.src_dataspace_id, "getting source dataspace");

        let errid = unsafe { H5Sselect_all(self.src_dataspace_id) };
        chkerr!(errid, "selecting dataspace");
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
            format!("creating reference to source dataset {src_dataset_name}")
        );

        let dst_group_path = format!("/Data_Products/{0}", rdr.product.short_name,);
        self.dst_group_id =
            unsafe { H5Gopen(file.id(), cstr!(dst_group_path.to_string()), H5P_DEFAULT) };
        chkid!(
            self.dst_group_id,
            format!("opening dest group: {dst_group_path}")
        );

        let dim = [1 as hsize_t];
        let maxdim = [1 as hsize_t];
        let space_id = unsafe { H5Screate_simple(1, dim.as_ptr(), maxdim.as_ptr()) };
        chkid!(space_id, "creating dest dataset dataspace");

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
        chkerr!(errid, "writing ref to dest dataset");

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

fn write_aggr_group(file: &File, rdrs: &[Rdr]) -> Result<()> {
    if rdrs.is_empty() {
        return Ok(());
    }
    let group = file.create_group(&format!(
        "/Data_Products/{0}/{0}_Aggr",
        rdrs[0].product.short_name
    ))?;

    for (name, val) in [
        ("AggregateBeginningOrbitNumber", 0usize),
        ("AggregateEndingOrbitNumber", 0usize),
        ("AggregateNumberGranules", rdrs.len()),
    ] {
        group
            .new_attr::<usize>()
            .shape([1, 1])
            .create(name)?
            .write_raw(&[val])?;
    }

    for (name, val) in [
        ("AggregateBeginningDate", ""),
        ("AggregateBeginningTime", ""),
        ("AggregateBeginningGranuleID", ""),
        ("AggregateEndingDate", ""),
        ("AggregateEndingTime", ""),
        ("AggregateEndingGranuleID", ""),
    ] {
        group
            .new_attr::<VarLenAscii>()
            .shape([1, 1])
            .create(name)
            .with_context(|| format!("creating {name} attribute"))?
            .write_raw(&[VarLenAscii::from_ascii(&val)
                .with_context(|| format!("failed to create FixedAscii for {name}"))?])
            .with_context(|| format!("writing {name} attribute"))?;
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
