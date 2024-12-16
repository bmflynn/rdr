use anyhow::{Context, Result};
use hdf5::types::FixedAscii;
use rdr::{CommonRdr, StaticHeader};
use std::fs::{write, File};
use std::path::Path;
use tracing::debug;

pub fn extract<P: AsRef<Path>>(
    input: P,
    short_name: Option<String>,
    granule_id: Option<String>,
) -> Result<()> {
    let file = hdf5::File::open(&input)
        .with_context(|| format!("failed to open {:?}", input.as_ref().to_path_buf()))?;

    let all_data = file.group("All_Data").context("failed to open /All_Data")?;
    for group in all_data
        .groups()
        .context("failed to get /All_Data groups")?
    {
        if let Some(short_name) = short_name.as_ref() {
            if !group.name().ends_with(&format!("{short_name}_All")) {
                debug!("skipping group {}", group.name());
                continue;
            }
        }
        for dataset in group
            .datasets()
            .with_context(|| format!("failed to get {} groups", group.name()))?
        {
            let dataset_path = dataset.name();
            let id = get_granule_id(&file, &dataset_path)?;
            let short_name = dataset_path.split("/").nth(2).unwrap().replace("_All", "");

            if let Some(granule_id) = granule_id.as_ref() {
                if id != *granule_id {
                    debug!("skipping granule {short_name} {id}");
                    continue;
                }
            }

            // read entire common rdr data bytes
            let arr = dataset
                .read_1d::<u8>()
                .with_context(|| format!("reading {}", dataset.name()))?;
            let data = arr.as_slice().unwrap();

            let common_rdr = CommonRdr::from_bytes(data)?;
            let fname = format!("{short_name}_{id}.json");
            let file = File::create(&fname).with_context(|| format!("creating {fname}"))?;
            serde_json::to_writer_pretty(&file, &common_rdr)?;

            write(
                format!("{short_name}_{id}.static_header.dat"),
                &data[..StaticHeader::LEN],
            )?;

            let header = common_rdr.static_header;
            write(
                format!("{short_name}_{id}.apid_list.dat"),
                &data[header.apid_list_offset as usize..header.pkt_tracker_offset as usize],
            )?;

            write(
                format!("{short_name}_{id}.packet_trackers.dat"),
                &data[header.pkt_tracker_offset as usize..header.ap_storage_offset as usize],
            )?;

            write(
                format!("{short_name}_{id}.ap_storage.dat"),
                &data[header.ap_storage_offset as usize..header.next_pkt_position as usize],
            )?;
        }
    }

    Ok(())
}

fn get_granule_id(file: &hdf5::File, dataset_path: &str) -> Result<String> {
    let gran_num: u64 = dataset_path.split("_").last().unwrap().parse().unwrap();
    let short_name = dataset_path.split("/").nth(2).unwrap().replace("_All", "");
    let path = format!("Data_Products/{short_name}/{short_name}_Gran_{gran_num}");

    let dataset = file
        .dataset(&path)
        .with_context(|| format!("opening dataset {path}"))?;
    let attr = dataset
        .attr("N_Granule_ID")
        .context("getting attr {path}:N_Granule_ID")?;
    Ok(attr
        .read_2d::<FixedAscii<20>>()
        .context("reading attr {path}:N_Granule_ID")?[[0, 0]]
    .to_string())
}
