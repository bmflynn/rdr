mod collector;
mod command_create;
mod command_dump;
mod config;
mod rdr;
mod time;
mod writer;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use std::{
    io::{stdout, Write},
    path::PathBuf,
};

use crate::command_dump::dump;
use crate::config::get_default_content;

/// Tool for manipulating JPSS RDR HDF5 files.
#[derive(Parser)]
#[command(version, about, long_about, disable_help_subcommand = true)]
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
    /// start by dumping the provided default configuration using the config sub-command and
    /// modify from there.
    Create {
        #[command(flatten)]
        configs: Configs,

        /// leap-seconds.list file to use.
        ///
        /// A default list file is included at build time,
        /// however, if the included list file becomes out of data this option can be used
        /// to specify one. A list file can be obtained from IERS at
        /// <https://hpiers.obspm.fr/iers/bul/bulc/ntp/leap-seconds.list>.
        #[arg(short, long)]
        leap_seconds: Option<PathBuf>,

        /// One or more packet data file. The packet data must be sorted in time and sequence id order.
        #[arg(short, long, value_name = "path")]
        input: Vec<PathBuf>,

        /// Output directory.
        #[arg(short, long, value_name = "path", default_value = "output")]
        output: PathBuf,
    },
    /// Extract the spacepacket data contained in the RDR.
    Dump {
        /// One or more RDR file
        #[arg(short, long, value_name = "path")]
        input: PathBuf,
    },
    /// Output the default configuration.
    Config {
        /// Satellite to show the config for
        #[arg(value_name = "SAT", value_parser=parse_valid_satellite)]
        satellite: String,
    },
}

fn main() -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(false)
                .without_time()
                .with_ansi(false),
        )
        .with(EnvFilter::from_env("RDR_LOG"))
        .init();

    let cli = Cli::parse();

    match cli.commands {
        Commands::Create {
            configs,
            leap_seconds,
            input,
            output,
        } => {
            crate::command_create::create(
                configs.satellite,
                configs.config,
                leap_seconds,
                &input,
                output,
            )?;
        }
        Commands::Dump { input } => {
            dump(&input, true)?;
        }
        Commands::Config { satellite } => {
            stdout().write_all(get_default_content(&satellite).unwrap().as_bytes())?;
        }
    }

    Ok(())
}
