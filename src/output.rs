use std::io::{self, Write};

use serde::Serialize;

use crate::app::{ClearResult, FetchResult, InfoResult, ListResult};

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    Interactive,
    NonInteractive,
}

pub struct JsonOutput;

impl JsonOutput {
    pub fn print_list(result: &ListResult) -> io::Result<()> {
        Self::print_json(result)
    }

    pub fn print_info(result: &InfoResult) -> io::Result<()> {
        Self::print_json(result)
    }

    pub fn print_fetch(result: &FetchResult) -> io::Result<()> {
        Self::print_json(result)
    }

    pub fn print_clear(result: &ClearResult) -> io::Result<()> {
        Self::print_json(result)
    }

    fn print_json<T: Serialize>(value: &T) -> io::Result<()> {
        let json = serde_json::to_string_pretty(value)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        let mut stdout = io::stdout();
        stdout.write_all(json.as_bytes())?;
        stdout.write_all(b"\n")?;
        Ok(())
    }
}

impl crate::app::ProgressSink for JsonOutput {
    fn event(&self, _event: crate::app::ProgressEvent) {}
}
