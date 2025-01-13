use hdf5::File;
use hdf5_sys::{
    h5::hsize_t,
    h5d::{H5Dclose, H5Dcreate2, H5Dget_space, H5Dopen2, H5Dwrite},
    h5g::{H5Gclose, H5Gopen},
    h5i::H5I_INVALID_HID,
    h5p::{H5Pcreate, H5Pset_create_intermediate_group, H5P_CLS_LINK_CREATE, H5P_DEFAULT},
    h5r::{
        hdset_reg_ref_t, hobj_ref_t,
        H5R_type_t::{H5R_DATASET_REGION, H5R_OBJECT},
        H5Rcreate,
    },
    h5s::{H5Sclose, H5Screate_simple, H5Sselect_all, H5S_ALL},
    h5t::{H5T_STD_REF_DSETREG, H5T_STD_REF_OBJ},
};
use std::ffi::{c_char, c_void, CString};

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
    ($id:expr, $path:expr, $msg:expr) => {
        if $id == H5I_INVALID_HID {
            return Err(format!("{} path={}", $msg, $path));
        }
    };
}

macro_rules! chkerr {
    ($id:expr, $path:expr, $msg:expr) => {
        if $id < 0 {
            return Err(format!("{} path={}", $msg, $path));
        }
    };
}

/// Create Data_Prodcuts/<shortname>/<shortname>_Gran_<x> dataset that will contain a region
/// reference to the data in All_Data/<shortname>_All/RawApplicationPackets_<x>.
///
/// This only creates the dataset, not any required attributes.
///
/// `src_path` is the H5 path to the source data for the reference in /All_Data
pub(crate) fn create_dataproducts_gran_dataset(
    file: &File,
    short_name: &str,
    src_path: &str,
) -> std::result::Result<String, String> {
    let Some((src_group_path, src_dataset_name)) = src_path.rsplit_once('/') else {
        return Err("invalid source path".to_string());
    };
    let src_group_id = unsafe { H5Gopen(file.id(), cstr!(src_group_path), H5P_DEFAULT) };
    chkid!(
        src_group_id,
        src_group_path.to_string(),
        format!("opening source group: {src_group_path}")
    );

    let src_dataset_id = unsafe { H5Dopen2(file.id(), cstr!(src_path.to_string()), H5P_DEFAULT) };
    chkid!(
        src_dataset_id,
        src_path.to_string(),
        format!("opening source dataset: {src_path}")
    );

    let src_dataspace_id = unsafe { H5Dget_space(src_dataset_id) };
    chkid!(
        src_dataspace_id,
        src_path.to_string(),
        "getting source dataspace".to_string()
    );

    let errid = unsafe { H5Sselect_all(src_dataspace_id) };
    chkerr!(
        errid,
        src_path.to_string(),
        "selecting dataspace".to_string()
    );

    let mut ref_id: hdset_reg_ref_t = [0; 12];
    let errid = unsafe {
        H5Rcreate(
            ref_id.as_mut_ptr().cast(),
            src_group_id,
            cstr!(src_dataset_name),
            H5R_DATASET_REGION,
            src_dataspace_id,
        )
    };
    chkerr!(
        errid,
        src_dataset_name.to_string(),
        format!("creating reference to source dataset {src_dataset_name}")
    );

    let dst_group_path = format!("/Data_Products/{0}", short_name);
    let dst_group_id =
        unsafe { H5Gopen(file.id(), cstr!(dst_group_path.to_string()), H5P_DEFAULT) };
    chkid!(
        dst_group_id,
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
    let dst_dataset_name = format!("{}_Gran_{sidx}", short_name);
    let dst_dataset_id = unsafe {
        H5Dcreate2(
            dst_group_id,
            cstr!(dst_dataset_name.clone()),
            *H5T_STD_REF_DSETREG,
            space_id,
            H5P_DEFAULT,
            H5P_DEFAULT,
            H5P_DEFAULT,
        )
    };
    chkid!(
        dst_dataset_id,
        dst_dataset_name.to_string(),
        "creating dest dataset reference"
    );

    let errid = unsafe {
        H5Dwrite(
            dst_dataset_id,
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

    unsafe {
        H5Gclose(src_group_id);
        H5Sclose(src_dataspace_id);
        H5Dclose(src_dataset_id);
        H5Gclose(dst_group_id);
        H5Dclose(dst_dataset_id);
    }

    Ok(format!("{dst_group_path}/{dst_dataset_name}"))
}

/// Create Data_Prodcuts/<shortname>/<shortname>_Aggr dataset containing an object reference
/// to the group in All_Data/<shortname>_All.
///
/// This only creates the dataset, not any required attributes.
pub(crate) fn create_dataproducts_aggr_dataset(
    file: &File,
    short_name: &str,
) -> std::result::Result<String, String> {
    // Create an object reference to the source group that will be written to aggr dataset
    let src_group_path = format!("/All_Data/{0}_All", short_name);
    let mut ref_id: hobj_ref_t = 0;
    let errid = unsafe {
        H5Rcreate(
            // reference to ref_id to a mutable raw pointer
            &mut ref_id as *mut _ as *mut c_void,
            file.id(),
            cstr!(src_group_path.to_string()),
            H5R_OBJECT,
            -1,
        )
    };
    chkerr!(
        errid,
        src_group_path.to_string(),
        format!("creating ref to group: {src_group_path}")
    );

    // Now, create the dataset in that group
    let dst_dataset_path = format!("/Data_Products/{0}/{0}_Aggr", short_name);
    let dim = [1 as hsize_t];
    let space_id = unsafe { H5Screate_simple(1, dim.as_ptr(), std::ptr::null()) };
    chkid!(space_id, &dst_dataset_path, "creating dataset dataspace");

    // Set properties to automatically create intermediate groups
    let lcpl_id = unsafe { H5Pcreate(*H5P_CLS_LINK_CREATE) };
    chkid!(
        lcpl_id,
        &dst_dataset_path,
        "creating dataset link properites"
    );
    let errid = unsafe { H5Pset_create_intermediate_group(lcpl_id, 1) };
    chkerr!(errid, &dst_dataset_path, "setting dataset link properites");

    // Create the dataset with reference data type
    let dst_dataset_id = unsafe {
        H5Dcreate2(
            file.id(),
            cstr!(dst_dataset_path.clone()),
            *H5T_STD_REF_OBJ,
            space_id,
            lcpl_id,
            H5P_DEFAULT,
            H5P_DEFAULT,
        )
    };
    chkid!(
        dst_dataset_id,
        dst_dataset_path,
        "creating dataset w/reference"
    );

    // Write the ref to our dataset
    let refs: [hobj_ref_t; 1] = [ref_id];
    let errid = unsafe {
        H5Dwrite(
            dst_dataset_id,
            *H5T_STD_REF_OBJ,
            H5S_ALL,
            H5S_ALL,
            H5P_DEFAULT,
            refs.as_ptr().cast(),
        )
    };
    chkerr!(errid, dst_dataset_path, "writing ref to dataset");

    unsafe {
        H5Sclose(space_id);
        H5Dclose(dst_dataset_id);
    }

    Ok(dst_dataset_path)
}
