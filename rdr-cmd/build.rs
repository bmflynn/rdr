use std::process::Command;

fn h5version() -> String {
    let ver = hdf5::library_version();
    format!("{}.{}.{}", ver.0, ver.1, ver.2)
}

fn git_sha() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    String::from_utf8(output.stdout).unwrap()
}

fn main() {
    println!("cargo:rustc-env=GIT_SHA={}", git_sha());
    println!("cargo::rustc-env=H5_VERSION={}", h5version());
}
