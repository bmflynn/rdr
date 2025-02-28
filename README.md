# JPSS HDF5 Raw-Data-Record (RDR) Utility
Source: https://github.com/bmflynn/rdr

Tool for creating RDRs from spacepacket files as well as dumping the spacepacket 
data contained in an RDR to NASA EOSDIS style PDS files.

```
Tool for manipulating JPSS RDR HDF5 files

Usage: rdr [OPTIONS] <COMMAND>

Commands:
  create   Create an RDR from spacepacket/level-0 data
  dump     Dump raw spacepacket data to Level-0 PDS files
  aggr     Aggregate multiple RDRs into a single aggregated RDR
  config   Output the default configuration
  info     Generate JSON containing file and dataset attributes and values
  extract  Extracts Common RDR metadata and data structures

Options:
  -l, --logging <LOGGING>  Logging level filters, e.g., debug, info, warn, etc ... [default: info]
  -h, --help               Print help (see more with '--help')
  -V, --version            Print version
```

## Installing

Download and extract the binary from the release archive available
on the [releases](https://github.com/bmflynn/rdr/releases) page.

Alternatively, if you have rust and cargo installed you can run the following:
```
cargo install --locked rdr
```
See [installing rust](https://www.rust-lang.org/tools/install)
