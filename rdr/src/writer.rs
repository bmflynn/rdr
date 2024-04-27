use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use hdf5::{types::VarLenAscii, File, Group};
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

pub fn write_hdf5(config: &Config, rdr: &Rdr, packed: &[Rdr], dest: &Path) -> Result<()> {
    if !dest.is_dir() {
        bail!("dest must be a directory");
    }
    let created = Utc::now();
    let fpath = dest.join(filename(config, rdr, &created));

    let mut file = File::create(fpath)?;

    set_global_attrs(&mut file, config, &created)?;

    let all_data_group = file.create_group("All_Data")?;

    create_alldata_group(&all_data_group, &[rdr.clone()])?;
    create_alldata_group(&all_data_group, packed)?;

    todo!()
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
            .with_context(|| format!("creating {} attribute", name))?
            .write_raw(&[VarLenAscii::from_ascii(&val)
                .with_context(|| format!("failed to create FixedAscii for {}", name))?])
            .with_context(|| format!("writing {} attribute", name))?;
    }

    Ok(())
}

fn create_alldata_group(group: &Group, rdrs: &[Rdr]) -> Result<()> {
    if rdrs.is_empty() {
        bail!("At least 1 RDR is required");
    }
    let subgroup = group.create_group(&format!("{}_All", rdrs[0].product.short_name))?;
    for (idx, rdr) in rdrs.iter().enumerate() {
        let name = format!("RawApplicationPackets_{idx}");
        subgroup
            .new_dataset_builder()
            .with_data(&arr1(&rdr.compile()[..]))
            .create(name.clone().as_str())
            .with_context(|| format!("creating {}", name))?;
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
