use std::env;
use std::fs;
use std::io::{Cursor, Read};
use std::path::PathBuf;

use zip::ZipArchive;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");

    // Use custom directory, otherwise use default
    let lib_dir = if let Ok(custom_dir) = env::var("CRSQLITE_LIB_DIR") {
        PathBuf::from(custom_dir)
    } else {
        // OUT_DIR format: ./target/<profile>/build/<pkg-name>/out
        let out_dir = env::var("OUT_DIR").unwrap();
        let lib_dir = PathBuf::from(out_dir)
            .ancestors()
            .nth(5) // Go up from OUT_DIR to project root
            .unwrap()
            .join("crsqlite");

        // Create directory if it doesn't exist
        fs::create_dir_all(&lib_dir).unwrap();
        lib_dir
    };

    download_crsqlite(&lib_dir);
}

/// Downloads the cr-sqlite library based on the target OS and architecture.
fn download_crsqlite(lib_dir: &PathBuf) -> PathBuf {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();

    let (lib_name, download_url) = match (target_os.as_str(), target_arch.as_str()) {
        ("linux", "x86_64") => (
            "crsqlite.so",
            "https://github.com/vlcn-io/cr-sqlite/releases/latest/download/crsqlite-linux-x86_64.zip"
        ),
        ("linux", "aarch64") => (
            "crsqlite.so", 
            "https://github.com/vlcn-io/cr-sqlite/releases/latest/download/crsqlite-linux-aarch64.zip"
        ),
        ("macos", "x86_64") => (
            "crsqlite.dylib",
            "https://github.com/vlcn-io/cr-sqlite/releases/latest/download/crsqlite-darwin-x86_64.zip"
        ),
        ("macos", "aarch64") => (
            "crsqlite.dylib",
            "https://github.com/vlcn-io/cr-sqlite/releases/latest/download/crsqlite-darwin-aarch64.zip"
        ),
        _ => panic!("Unsupported platform: {}-{}", target_os, target_arch),
    };

    let lib_path = lib_dir.join(lib_name);

    if lib_path.exists() {
        // println!(
        //     "cargo::warning=cr-sqlite library already exists at {}",
        //     lib_path.display()
        // );
        println!("Hello world");
        return lib_path;
    }

    // Download and extract the library
    let client = ureq::Agent::new_with_defaults();
    let response = client
        .get(download_url)
        .call()
        .unwrap_or_else(|e| panic!("Failed to download cr-sqlite: {}", e));

    let mut archive_data = Vec::new();
    response
        .into_body()
        .into_reader()
        .read_to_end(&mut archive_data)
        .unwrap_or_else(|e| panic!("Failed to read download: {}", e));

    // Extract based on file type
    if download_url.ends_with(".zip") {
        extract_zip(&archive_data, lib_dir, lib_name);
    }

    if !lib_path.exists() {
        panic!(
            "Failed to extract cr-sqlite library to {}",
            lib_path.display()
        );
    }

    lib_path
}

fn extract_zip(data: &[u8], lib_dir: &PathBuf, lib_name: &str) {
    let cursor = Cursor::new(data);
    let mut archive = ZipArchive::new(cursor).unwrap();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();

        if file.name().ends_with(lib_name) {
            let dest_path = lib_dir.join(lib_name);
            let mut dest_file = fs::File::create(&dest_path).unwrap();
            std::io::copy(&mut file, &mut dest_file).unwrap();
            break;
        }
    }
}
