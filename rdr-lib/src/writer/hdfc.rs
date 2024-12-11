use hdf5::File;
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
use std::ffi::{c_char, CString};

use crate::config::ProductSpec;
use crate::error::{Error, Result};

macro_rules! cstr {
    ($s:expr) => {
        match CString::new($s) {
            Ok(s) => s,
            Err(n) => CString::new($s[..n.nul_position()].to_string())
                .expect("nul byte was removed this should not fail"),
        }
        .as_ptr()
        .cast::<c_char>()
    };
}

macro_rules! chkid {
    ($id:expr, $name:expr, $msg:expr) => {
        if $id == H5I_INVALID_HID {
            return Err(Error::Hdf5C(format!("invalid hdf5 id: {}", $id)));
        }
    };
}

macro_rules! chkerr {
    ($id:expr, $name:expr, $msg:expr) => {
        if $id < 0 {
            return Err(Error::Hdf5C(format!(
                "err={} object={}: {}",
                $id, $name, $msg
            )));
        }
    };
}

/// Helper for writing the Data_Products region reference that cleans up low-level h5
/// resource on drop
#[derive(Default)]
pub(crate) struct DataProductsRefWriter {
    src_group_id: hid_t,
    src_dataset_id: hid_t,
    src_dataspace_id: hid_t,
    dst_group_id: hid_t,
    dst_dataset_id: hid_t,
}

impl DataProductsRefWriter {
    pub fn write_ref(
        &mut self,
        file: &File,
        product: &ProductSpec,
        src_path: &str,
    ) -> Result<String> {
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

        let dst_group_path = format!("/Data_Products/{0}", product.short_name,);
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
        let dst_dataset_name = format!("{}_Gran_{sidx}", product.short_name,);
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

        Ok(format!("{dst_group_path}/{dst_dataset_name}"))
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
