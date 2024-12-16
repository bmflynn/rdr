fn h5version() -> String {
    let ver = hdf5::library_version();
    format!("{}.{}.{}", ver.0, ver.1, ver.2)
}

fn main() {
    println!("cargo::rustc-env=H5_VERSION={}", h5version());
}
