use rdr::config::get_default;

#[test]
fn load_configs() {
    for sat in ["npp", "j01", "j02", "j03", "j04"] {
        assert!(get_default(sat).is_some(), "{sat} config is invalid");
    }
}
