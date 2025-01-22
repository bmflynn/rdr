mod command_aggr;
mod command_create;
mod command_deaggr;
mod command_dump;
mod command_extract;
mod command_info;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use std::{
    io::{stderr, stdout, Write},
    path::PathBuf,
};
use tempfile::TempDir;
use tracing::info;
use tracing_subscriber::EnvFilter;

use rdr::config::get_default_content;

fn version() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        " (hdf5:",
        env!("H5_VERSION"),
        ")"
    )
}

/// Tool for manipulating JPSS RDR HDF5 files.
///
/// NOTE: VIIRS, CRIS, ATMS SCIENCE and SPACECRAFT data are well supported, however, support for
///     OMPS, CERES, and non-SCIENCE types is a bit more sparse. If you have a need to support one
///     these types you please create a new issue or comment on an existing one at the project URL
///     below.
///
/// Repository: <https://github.com/bmflynn/rdr>
#[derive(Parser)]
#[command(version=version(), about, long_about, disable_help_subcommand = true)]
struct Cli {
    /// Logging level filters, e.g., debug, info, warn, etc ...
    #[arg(short, long, default_value = "info")]
    logging: String,

    #[command(subcommand)]
    commands: Commands,
}

fn parse_valid_satellite(sat: &str) -> Result<String, String> {
    let valid_satellites = ["npp", "j01", "j02", "j03"];
    if valid_satellites.contains(&sat) {
        Ok(String::from(sat))
    } else {
        Err(format! {"expected one of {}", valid_satellites.join(", ")})
    }
}

#[derive(Args)]
#[group(multiple = false, required = true)]
struct Configs {
    /// Use the built-in default configuration for this satellite id; one of npp, j01, j02, or j03.
    #[arg(short, long, value_name = "name", value_parser=parse_valid_satellite)]
    satellite: Option<String>,

    /// YAML decode configuration file to use, rather than a embeded default config. See the
    /// config subcommand to view embeded configuration.
    #[arg(short, long, value_name = "path")]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create an RDR from spacepacket/level-0 data.
    ///
    /// The default configuration should be good for most cases, but if you want to try your
    /// luck at modifying the default configuration or adding support for a new spacecraft you can
    /// start by dumping the provided default configuration using the `config` sub-command and
    /// modify from there.
    Create {
        #[command(flatten)]
        configs: Configs,

        /// Output directory.
        #[arg(short, long, value_name = "path", default_value = "output")]
        output: PathBuf,

        /// One or more packet data file.
        ///
        /// The input will be merged before processing and need not be in any particular order.
        #[arg(value_name = "path")]
        input: Vec<PathBuf>,
    },
    /// Extract raw spacepacket data to Level-0 PDS files.
    ///
    /// Level-0 PDS files will follow the NASA Level-0 naming conventions.
    Dump {
        /// RDR file to dump
        #[arg(value_name = "path")]
        input: PathBuf,
    },
    /// Aggregate multiple RDRs into a single aggregated RDR.
    Aggr {
        /// One or more RDR file to include in the output. At least one RDR is required.
        #[arg(value_name = "paths")]
        inputs: Vec<PathBuf>,
        /// Persistent working directory.
        ///
        /// If not specified a temporary directory is used that will be deleted before exit.
        #[arg(short, long)]
        workdir: Option<PathBuf>,
    },
    /// Deaggregate an aggregated RDR.
    ///
    /// Produces a new single RDR for each contained SCIENCE data product packed with all
    /// overlapping SPACECRAFT data.
    #[command(hide = true)]
    Deagg {
        /// RDR file to deaggregate into native resolution RDRs.
        #[arg(value_name = "path")]
        input: PathBuf,
    },
    /// Output the default configuration.
    Config {
        /// Satellite to show the config for
        #[arg(value_name = "sat", value_parser=parse_valid_satellite)]
        satellite: String,
    },
    /// Generate JSON containing file and dataset attributes and values.
    Info {
        #[arg(value_name = "path")]
        input: PathBuf,
        #[arg(short, long)]
        short_name: Option<String>,
        #[arg(short, long)]
        granule_id: Option<String>,
    },
    /// Extracts Common RDR metadata and data structures.
    ///
    /// This will produce a JSON metadata file of the group and dataset attributes and a raw data
    /// file RDR granule datasets in the file.
    Extract {
        #[arg(value_name = "path")]
        input: PathBuf,
        #[arg(short, long)]
        short_name: Option<String>,
        #[arg(short, long)]
        granule_id: Option<String>,
        /// Directory for extracted artifacts
        #[arg(short, long)]
        outdir: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(stderr)
        .with_ansi(false)
        .without_time()
        .with_env_filter(EnvFilter::new(cli.logging))
        .init();

    info!("hdf5 version={}", env!("H5_VERSION"));

    match cli.commands {
        Commands::Create {
            configs,
            input,
            output,
        } => {
            crate::command_create::create(configs.satellite, configs.config, &input, output)?;
        }
        Commands::Dump { input } => {
            crate::command_dump::dump(&input, true)?;
        }
        Commands::Config { satellite } => {
            let Some(content) = get_default_content(&satellite) else {
                bail!("no config for {satellite}");
            };
            stdout().write_all(content.as_bytes())?;
        }
        Commands::Aggr { inputs, workdir } => {
            if inputs.is_empty() {
                bail!("No inputs specified");
            }

            let mut tmpdir: Option<TempDir> = None;
            let workdir = match &workdir {
                Some(p) => p,
                None => {
                    tmpdir = Some(TempDir::new().context("creating tempdir")?);
                    tmpdir.as_ref().unwrap().path()
                }
            };
            let fpath = crate::command_aggr::aggreggate(&inputs, workdir)?;
            info!("saved {fpath:?}");
            if let Some(tmpdir) = tmpdir {
                tmpdir.close().context("removing tmpdir")?;
            }
        }
        Commands::Deagg { .. } => {
            unimplemented!()
        }
        Commands::Info {
            input,
            short_name,
            granule_id,
        } => {
            crate::command_info::info(input, short_name, granule_id)?;
        }
        Commands::Extract {
            input,
            short_name,
            granule_id,
            outdir,
        } => {
            let outdir = outdir.unwrap_or(std::env::current_dir()?);
            crate::command_extract::extract(input, outdir, short_name, granule_id)?;
        }
    }

    Ok(())
}
