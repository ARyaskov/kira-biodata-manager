use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::domain::SrrId;
use crate::error::KiraError;
use crate::fs_util;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ToolInfo {
    pub datasets: Option<String>,
    pub sra_toolkit: Option<String>,
}

pub trait SrrClient: Send + Sync {
    fn download_fastq(
        &self,
        id: &SrrId,
        paired: bool,
        destination_dir: &Path,
    ) -> Result<Vec<PathBuf>, KiraError>;
    fn tool_info(&self) -> ToolInfo;
}

#[derive(Debug, Clone)]
pub enum SrrToolStatus {
    Ready,
    Missing { message: String },
}

#[derive(Clone)]
pub struct SystemSrrClient {
    datasets: Option<PathBuf>,
    prefetch: Option<PathBuf>,
    fasterq_dump: Option<PathBuf>,
}

impl SystemSrrClient {
    pub fn new() -> Self {
        Self {
            datasets: find_in_path("datasets"),
            prefetch: find_in_path("prefetch"),
            fasterq_dump: find_in_path("fasterq-dump"),
        }
    }

    pub fn tool_status(&self) -> SrrToolStatus {
        if self.fasterq_dump.is_none() {
            return SrrToolStatus::Missing {
                message: "missing fasterq-dump (SRA Toolkit)".to_string(),
            };
        }
        if self.datasets.is_none() && self.prefetch.is_none() {
            return SrrToolStatus::Missing {
                message: "missing prefetch or datasets (SRA download tool)".to_string(),
            };
        }
        SrrToolStatus::Ready
    }

    fn require_fasterq(&self) -> Result<&PathBuf, KiraError> {
        self.fasterq_dump
            .as_ref()
            .ok_or_else(|| KiraError::MissingTool("fasterq-dump".to_string()))
    }

    fn run_cmd(
        &self,
        program: &Path,
        args: &[String],
        cwd: Option<&Path>,
    ) -> Result<(), KiraError> {
        let mut cmd = Command::new(program);
        cmd.args(args);
        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }
        let output = cmd
            .output()
            .map_err(|err| KiraError::SrrConversion(err.to_string()))?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let message = if stderr.is_empty() {
            format!("command failed: {}", program.display())
        } else {
            stderr
        };
        Err(KiraError::SrrConversion(message))
    }

    fn datasets_download(&self, id: &SrrId, zip_path: &Path) -> Result<(), KiraError> {
        let datasets = self
            .datasets
            .as_ref()
            .ok_or_else(|| KiraError::MissingTool("datasets".to_string()))?;
        let args = vec![
            "download".to_string(),
            "sra".to_string(),
            "run".to_string(),
            id.as_str().to_string(),
            "--filename".to_string(),
            zip_path.to_string_lossy().to_string(),
        ];
        self.run_cmd(datasets.as_path(), &args, None)
    }

    fn prefetch_download(&self, id: &SrrId, out_dir: &Path) -> Result<PathBuf, KiraError> {
        let prefetch = self
            .prefetch
            .as_ref()
            .ok_or_else(|| KiraError::MissingTool("prefetch".to_string()))?;
        let args = vec![
            id.as_str().to_string(),
            "--output-directory".to_string(),
            out_dir.to_string_lossy().to_string(),
        ];
        self.run_cmd(prefetch.as_path(), &args, None)?;
        find_first_ext(out_dir, "sra").ok_or_else(|| {
            KiraError::Filesystem("prefetch did not produce an .sra file".to_string())
        })
    }

    fn fasterq_dump(
        &self,
        sra_path: &Path,
        paired: bool,
        out_dir: &Path,
    ) -> Result<Vec<PathBuf>, KiraError> {
        let fasterq = self.require_fasterq()?;
        let mut args = vec![
            sra_path.to_string_lossy().to_string(),
            "--outdir".to_string(),
            out_dir.to_string_lossy().to_string(),
        ];
        if paired {
            args.push("--split-files".to_string());
        }
        self.run_cmd(fasterq.as_path(), &args, None)?;
        Ok(find_exts(out_dir, "fastq"))
    }
}

impl SrrClient for SystemSrrClient {
    fn download_fastq(
        &self,
        id: &SrrId,
        paired: bool,
        destination_dir: &Path,
    ) -> Result<Vec<PathBuf>, KiraError> {
        fs::create_dir_all(destination_dir)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        if let Some(_) = self.datasets {
            let zip_path = destination_dir.join(format!("{}.zip", id.as_str()));
            self.datasets_download(id, &zip_path)?;
            if zip_path.exists() {
                let extract_dir = destination_dir.join("extract");
                fs::create_dir_all(&extract_dir)
                    .map_err(|err| KiraError::Filesystem(err.to_string()))?;
                fs_util::validate_zip(&zip_path)?;
                fs_util::extract_zip(&zip_path, &extract_dir)?;
                let fastq_files = find_exts(&extract_dir, "fastq");
                if !fastq_files.is_empty() {
                    return Ok(fastq_files);
                }
                if let Some(sra_path) = find_first_ext(&extract_dir, "sra") {
                    return self.fasterq_dump(&sra_path, paired, destination_dir);
                }
            }
        }

        let sra_path = self.prefetch_download(id, destination_dir)?;
        self.fasterq_dump(&sra_path, paired, destination_dir)
    }

    fn tool_info(&self) -> ToolInfo {
        ToolInfo {
            datasets: self
                .datasets
                .as_ref()
                .and_then(|path| tool_version(path, &["--version"])),
            sra_toolkit: self
                .fasterq_dump
                .as_ref()
                .and_then(|path| tool_version(path, &["--version"])),
        }
    }
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for path in std::env::split_paths(&path_var) {
        let exe = path.join(format!("{name}.exe"));
        if exe.exists() {
            return Some(exe);
        }
        let plain = path.join(name);
        if plain.exists() {
            return Some(plain);
        }
    }
    None
}

fn tool_version(path: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new(path).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        None
    } else {
        Some(stdout)
    }
}

fn find_first_ext(root: &Path, ext: &str) -> Option<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = fs::read_dir(&path).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| value.eq_ignore_ascii_case(ext))
                .unwrap_or(false)
            {
                return Some(path);
            }
        }
    }
    None
}

fn find_exts(root: &Path, ext: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        if let Ok(entries) = fs::read_dir(&path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(|value| value.eq_ignore_ascii_case(ext))
                    .unwrap_or(false)
                {
                    out.push(path);
                }
            }
        }
    }
    out
}
