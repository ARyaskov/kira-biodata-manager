use std::fs;
use std::io;
use std::path::Path;

use zip::ZipArchive;

use crate::error::KiraError;

pub fn extract_zip(zip_path: &Path, target_dir: &Path) -> Result<(), KiraError> {
    let file = fs::File::open(zip_path)
        .map_err(|err| KiraError::Filesystem(format!("open zip {}: {err}", zip_path.display())))?;
    let mut archive =
        ZipArchive::new(file).map_err(|err| KiraError::Filesystem(err.to_string()))?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let entry_path = match entry.enclosed_name() {
            Some(path) => target_dir.join(path),
            None => {
                return Err(KiraError::Filesystem(
                    "zip entry path traversal detected".to_string(),
                ));
            }
        };

        if entry.is_dir() {
            fs::create_dir_all(&entry_path)
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
            continue;
        }

        if let Some(parent) = entry_path.parent() {
            fs::create_dir_all(parent).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        let mut outfile =
            fs::File::create(&entry_path).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        io::copy(&mut entry, &mut outfile).map_err(|err| KiraError::Filesystem(err.to_string()))?;
    }
    Ok(())
}

pub fn validate_zip(zip_path: &Path) -> Result<(), KiraError> {
    let file = fs::File::open(zip_path)
        .map_err(|err| KiraError::Filesystem(format!("open zip {}: {err}", zip_path.display())))?;
    let mut archive =
        ZipArchive::new(file).map_err(|err| KiraError::Filesystem(err.to_string()))?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        if entry.is_dir() {
            continue;
        }
        io::copy(&mut entry, &mut io::sink())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
    }
    Ok(())
}
