mod command_create;
mod command_info;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use std::{
    io::{stdout, Write},
    path::PathBuf,
};

use rdr::config::get_default_content;

/// Create JPSS RDR HDF5 files from CCSDS spacepacket data files.
///
/// Blah, blah, blah...
#[derive(Parser)]
#[command(version, about, long_about)]
struct Cli {
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
#[group(multiple = false)]
struct Configs {
    /// Use the build-in default configuration for this satellite id; one of npp, j01, j02, or j03.
    #[arg(short, long, value_name = "SAT", value_parser=parse_valid_satellite)]
    satellite: Option<String>,

    /// YAML decode configuration file to use, rather than a provided default. The format
    /// of the configuration
    #[arg(short, long, value_name = "PATH")]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create an RDR from spacepacket/level-0 data.
    Create {
        #[command(flatten)]
        configs: Configs,

        /// leap-seconds.list file to use. A default list file is included at build time,
        /// however, if the included list file becomes out of data this option can be used
        /// to specify one. A list file can be obtained from IERS at
        /// https://hpiers.obspm.fr/iers/bul/bulc/ntp/leap-seconds.list.
        #[arg(short, long)]
        leap_seconds: Option<PathBuf>,

        /// One or more packet data file. The packet data must be sorted in time and sequence id order.
        #[arg(required = true)]
        input: Vec<PathBuf>,
    },
    /// Output the default configuration
    Config {
        /// Satellite to show the config for
        #[arg(value_name = "SAT", value_parser=parse_valid_satellite)]
        satellite: String,
    },
    Info {
        /// Input file
        #[arg()]
        rdr: PathBuf,
    },
}

fn main() -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false).without_time())
        .with(EnvFilter::from_env("RDR_LOG"))
        .init();

    let cli = Cli::parse();

    match cli.commands {
        Commands::Create {
            configs,
            leap_seconds,
            input,
        } => {
            crate::command_create::create(
                configs.satellite,
                configs.config,
                &leap_seconds,
                &input,
            )?;
        }
        Commands::Config { satellite } => {
            stdout().write_all(get_default_content(&satellite).unwrap().as_bytes())?;
        }
        Commands::Info { rdr } => {
            crate::command_info::info(rdr)?;
        }
    }

    Ok(())
}
