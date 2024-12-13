use anyhow::Result;
use std::{collections::HashMap, path::Path};

use rdr::{GranuleMeta, Meta};

pub fn info<P: AsRef<Path>>(
    input: P,
    short_name: Option<String>,
    granule_id: Option<String>,
) -> Result<()> {
    let mut meta = Meta::from_file(input)?;

    if let Some(short_name) = short_name {
        meta.products.retain(|s, _| *s == short_name);
    }

    if let Some(granule_id) = granule_id {
        let mut to_save: HashMap<String, Vec<GranuleMeta>> = HashMap::default();
        for (product_name, granules) in meta.granules.iter() {
            let mut granules_to_save: Vec<GranuleMeta> = Vec::default();
            for g in granules.iter() {
                if g.id == granule_id {
                    granules_to_save.push(g.clone());
                }
            }
            to_save.insert(product_name.to_string(), granules_to_save);
        }
        meta.granules = to_save;
    }

    print!("{}", serde_json::to_string_pretty(&meta)?);

    Ok(())
}
