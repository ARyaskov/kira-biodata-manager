use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use camino::{Utf8Path, Utf8PathBuf};
use directories::BaseDirs;
use serde::{Deserialize, Serialize};
use tempfile::Builder;

use crate::domain::{GenomeAccession, ProteinFormat, ProteinId};
use crate::error::KiraError;

#[derive(Debug, Clone)]
pub struct Store {
    project_root: Utf8PathBuf,
    cache_root: Utf8PathBuf,
}

impl Store {
    pub fn new() -> Result<Self, KiraError> {
        let cwd = std::env::current_dir().map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let project_root = Utf8PathBuf::from_path_buf(cwd.join(".kira-bm"))
            .map_err(|_| KiraError::Filesystem("invalid project path".to_string()))?;

        let cache_root = BaseDirs::new()
            .and_then(|dirs| {
                Utf8PathBuf::from_path_buf(
                    dirs.home_dir().join(".cache").join("kira-biodata-manager"),
                )
                .ok()
            })
            .ok_or_else(|| {
                KiraError::Filesystem("unable to resolve cache directory".to_string())
            })?;

        Ok(Self {
            project_root,
            cache_root,
        })
    }

    pub fn new_with_paths(project_root: Utf8PathBuf, cache_root: Utf8PathBuf) -> Self {
        Self {
            project_root,
            cache_root,
        }
    }

    pub fn project_root(&self) -> &Utf8Path {
        &self.project_root
    }

    pub fn cache_root(&self) -> &Utf8Path {
        &self.cache_root
    }

    pub fn project_protein_dir(&self, id: &ProteinId) -> Utf8PathBuf {
        self.project_root.join("proteins").join(id.as_str())
    }

    pub fn cache_protein_dir(&self, id: &ProteinId) -> Utf8PathBuf {
        self.cache_root.join("proteins").join(id.as_str())
    }

    pub fn project_protein_path(&self, id: &ProteinId, format: ProteinFormat) -> Utf8PathBuf {
        let dir = self.project_protein_dir(id);
        dir.join(format!("{id}.{}", protein_ext(format)))
    }

    pub fn cache_protein_path(&self, id: &ProteinId, format: ProteinFormat) -> Utf8PathBuf {
        let dir = self.cache_protein_dir(id);
        dir.join(format!("{id}.{}", protein_ext(format)))
    }

    pub fn project_genome_dir(&self, acc: &GenomeAccession) -> Utf8PathBuf {
        self.project_root.join("genomes").join(acc.as_str())
    }

    pub fn cache_genome_dir(&self, acc: &GenomeAccession) -> Utf8PathBuf {
        self.cache_root.join("genomes").join(acc.as_str())
    }

    pub fn project_metadata_path(&self, dataset_type: &str, id: &str) -> Utf8PathBuf {
        self.project_root
            .join("metadata")
            .join(dataset_type)
            .join(format!("{id}.json"))
    }

    pub fn cache_metadata_path(&self, dataset_type: &str, id: &str) -> Utf8PathBuf {
        self.cache_root
            .join("metadata")
            .join(dataset_type)
            .join(format!("{id}.json"))
    }

    pub fn ensure_project_root(&self) -> Result<(), KiraError> {
        fs::create_dir_all(self.project_root.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))
    }

    pub fn ensure_cache_root(&self) -> Result<(), KiraError> {
        fs::create_dir_all(self.cache_root.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))
    }

    pub fn project_exists(&self, path: &Utf8Path) -> bool {
        path.as_std_path().exists()
    }

    pub fn cache_exists(&self, path: &Utf8Path) -> bool {
        path.as_std_path().exists()
    }

    pub fn clear_project(&self) -> Result<(), KiraError> {
        if self.project_root.as_std_path().exists() {
            fs::remove_dir_all(self.project_root.as_std_path())
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        Ok(())
    }

    pub fn write_metadata(path: &Utf8Path, metadata: &Metadata) -> Result<(), KiraError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent.as_std_path())
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        let tmp_path = path.with_extension("json.tmp");
        let content = serde_json::to_vec_pretty(metadata)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(tmp_path.as_std_path(), &content)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::rename(tmp_path.as_std_path(), path.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(())
    }

    pub fn write_bytes_atomic(path: &Utf8Path, content: &[u8]) -> Result<(), KiraError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent.as_std_path())
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        let tmp_path = path.with_extension("tmp");
        fs::write(tmp_path.as_std_path(), content)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::rename(tmp_path.as_std_path(), path.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(())
    }

    pub fn copy_dir_recursive(source: &Utf8Path, dest: &Utf8Path) -> Result<(), KiraError> {
        fs::create_dir_all(dest.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        for entry in walk_dir(source.as_std_path())? {
            let relative = entry.strip_prefix(source.as_std_path()).unwrap();
            let target = dest.as_std_path().join(relative);
            if entry.is_dir() {
                fs::create_dir_all(&target)
                    .map_err(|err| KiraError::Filesystem(err.to_string()))?;
            } else {
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|err| KiraError::Filesystem(err.to_string()))?;
                }
                fs::copy(entry, &target).map_err(|err| KiraError::Filesystem(err.to_string()))?;
            }
        }
        Ok(())
    }

    pub fn copy_dir_atomic(source: &Utf8Path, dest: &Utf8Path) -> Result<(), KiraError> {
        let parent = dest
            .parent()
            .ok_or_else(|| KiraError::Filesystem("invalid destination path".to_string()))?;
        fs::create_dir_all(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_dir = Builder::new()
            .prefix("kira-bm-copy")
            .tempdir_in(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
            .map_err(|_| KiraError::Filesystem("invalid temp dir".to_string()))?;
        Self::copy_dir_recursive(source, &temp_path)?;
        atomic_rename_dir(temp_path.as_std_path(), dest.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(())
    }

    pub fn copy_file_atomic(source: &Utf8Path, dest: &Utf8Path) -> Result<(), KiraError> {
        let parent = dest
            .parent()
            .ok_or_else(|| KiraError::Filesystem("invalid destination path".to_string()))?;
        fs::create_dir_all(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp = tempfile::Builder::new()
            .prefix("kira-bm-file")
            .tempfile_in(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::copy(source.as_std_path(), temp.path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        if dest.as_std_path().exists() {
            fs::remove_file(dest.as_std_path())
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        temp.persist(dest.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(())
    }

    pub fn list_metadata(root: &Utf8Path) -> Result<Vec<Metadata>, KiraError> {
        let metadata_root = root.join("metadata");
        if !metadata_root.as_std_path().exists() {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        for path in walk_dir(metadata_root.as_std_path())? {
            if path.is_file() && path.extension().map(|ext| ext == "json").unwrap_or(false) {
                let content = fs::read_to_string(&path)
                    .map_err(|err| KiraError::Filesystem(err.to_string()))?;
                let metadata: Metadata = serde_json::from_str(&content)
                    .map_err(|err| KiraError::Filesystem(err.to_string()))?;
                entries.push(metadata);
            }
        }
        Ok(entries)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub source: String,
    pub dataset_type: String,
    pub id: String,
    pub format: Option<String>,
    pub downloaded_at: String,
    pub tool: String,
    pub resolved_path: String,
}

fn walk_dir(root: &Path) -> Result<Vec<PathBuf>, KiraError> {
    let mut items = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = fs::read_dir(&path).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        for entry in entries {
            let entry = entry.map_err(|err| KiraError::Filesystem(err.to_string()))?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path.clone());
            }
            items.push(path);
        }
    }
    Ok(items)
}

pub fn atomic_rename_dir(from: &Path, to: &Path) -> io::Result<()> {
    if to.exists() {
        fs::remove_dir_all(to)?;
    }
    fs::rename(from, to)
}

fn protein_ext(format: ProteinFormat) -> &'static str {
    match format {
        ProteinFormat::Cif => "cif",
        ProteinFormat::Pdb => "pdb",
        ProteinFormat::Bcif => "bcif",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ProteinFormat;

    #[test]
    fn layout_paths() {
        let store = Store::new().unwrap();
        let id: ProteinId = "1LYZ".parse().unwrap();
        let acc: GenomeAccession = "GCF_000005845.2".parse().unwrap();

        let protein_path = store.project_protein_path(&id, ProteinFormat::Pdb);
        assert!(protein_path.ends_with("proteins/1LYZ/1LYZ.pdb"));

        let genome_path = store.cache_genome_dir(&acc);
        assert!(genome_path.ends_with("genomes/GCF_000005845.2"));
    }
}
