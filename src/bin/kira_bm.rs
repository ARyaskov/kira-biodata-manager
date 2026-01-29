use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use miette::IntoDiagnostic;
use tracing_subscriber::EnvFilter;

use kira_biodata_manager::app::{App, FetchOptions, ProgressSinkKind};
use kira_biodata_manager::config::ConfigLoader;
use kira_biodata_manager::domain::{DatasetSpecifier, ProteinFormat};
use kira_biodata_manager::error::KiraError;
use kira_biodata_manager::ncbi::{NcbiClient, NcbiHttpClient};
use kira_biodata_manager::output::{JsonOutput, OutputMode};
use kira_biodata_manager::rcsb::{RcsbClient, RcsbHttpClient};
use kira_biodata_manager::store::Store;
use kira_biodata_manager::tui::Tui;

#[derive(Parser)]
#[command(name = "kira-bm")]
#[command(about = "Reproducible bioinformatics dataset manager (npm/cargo/pip for bioinformatics)")]
#[command(version, author)]
struct Cli {
    #[arg(long, global = true)]
    non_interactive: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Manage datasets (alias: data fetch)")]
    Data(DataArgs),
}

#[derive(Args)]
struct DataArgs {
    #[command(subcommand)]
    command: Option<DataCommand>,
}

#[derive(Subcommand)]
enum DataCommand {
    #[command(about = "Fetch datasets")]
    Fetch(FetchArgs),
    #[command(about = "Fetch datasets (alias of fetch)")]
    Add(FetchArgs),
    #[command(about = "List locally available datasets")]
    List,
    #[command(about = "Show dataset info")]
    Info(InfoArgs),
    #[command(about = "Clear project-local store")]
    Clear,
}

#[derive(Args, Clone)]
struct FetchArgs {
    specifier: Option<String>,

    #[arg(long)]
    config: Option<String>,

    #[arg(long)]
    format: Option<ProteinFormat>,

    #[arg(long)]
    force: bool,

    #[arg(long)]
    no_cache: bool,

    #[arg(long)]
    dry_run: bool,
}

#[derive(Args)]
struct InfoArgs {
    specifier: String,
}

fn main() -> ExitCode {
    if let Err(report) = run() {
        eprintln!("{report:?}");
        if let Some(kira) = report.downcast_ref::<KiraError>() {
            return ExitCode::from(map_exit_code(kira));
        }
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn map_exit_code(error: &KiraError) -> u8 {
    match error {
        KiraError::DatasetNotFound(_) => 2,
        KiraError::MissingConfig => 2,
        KiraError::NcbiHttp(_)
        | KiraError::NcbiStatus { .. }
        | KiraError::RcsbHttp(_)
        | KiraError::RcsbStatus { .. } => 3,
        _ => 1,
    }
}

fn run() -> miette::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let output_mode = if cli.non_interactive {
        OutputMode::NonInteractive
    } else {
        OutputMode::Interactive
    };

    let store = Store::new().into_diagnostic()?;

    match cli.command {
        Some(Commands::Data(args)) => run_data(args, store, output_mode),
        None => {
            if matches!(output_mode, OutputMode::Interactive) {
                if let Ok(resolved) = ConfigLoader::resolve(None) {
                    let ncbi = NcbiHttpClient::new().into_diagnostic()?;
                    let rcsb = RcsbHttpClient::new().into_diagnostic()?;
                    let app = App::new(store, ncbi, rcsb);
                    let mut tui = Tui::new(ProgressSinkKind::Fetch);
                    let fetch_options = FetchOptions {
                        force: false,
                        no_cache: false,
                        dry_run: false,
                    };
                    let result = tui.run(move |sink| {
                        app.fetch(None, Some(&resolved), None, fetch_options, sink)
                    })?;
                    tui.finish_fetch(&result)?;
                    print_fetch_summary(&result);
                    Ok(())
                } else {
                    let mut tui = Tui::new(ProgressSinkKind::Fetch);
                    loop {
                        let command = tui.idle_command()?;
                        let Some(command) = command else {
                            break Ok(());
                        };
                        let data_command = parse_tui_command(&command)?;
                        let keep_open =
                            matches!(data_command, DataCommand::Fetch(_) | DataCommand::Add(_));
                        run_data_command(data_command, store.clone(), output_mode)?;
                        if !keep_open {
                            break Ok(());
                        }
                    }
                }
            } else {
                Err(miette::Report::msg(
                    "command required (try `kira-bm data --help`)",
                ))
            }
        }
    }
}

fn run_data(args: DataArgs, store: Store, output_mode: OutputMode) -> miette::Result<()> {
    let command = args.command.unwrap_or(DataCommand::Fetch(FetchArgs {
        specifier: None,
        config: None,
        format: None,
        force: false,
        no_cache: false,
        dry_run: false,
    }));

    run_data_command(command, store, output_mode)
}

fn run_data_command(
    command: DataCommand,
    store: Store,
    output_mode: OutputMode,
) -> miette::Result<()> {
    match command {
        DataCommand::Fetch(args) | DataCommand::Add(args) => {
            let ncbi = NcbiHttpClient::new().into_diagnostic()?;
            let rcsb = RcsbHttpClient::new().into_diagnostic()?;
            let app = App::new(store, ncbi, rcsb);
            run_fetch(args, app, output_mode)
        }
        DataCommand::List => {
            let app = App::new(store, NopNcbi, NopRcsb);
            run_list(app, output_mode)
        }
        DataCommand::Info(args) => {
            let app = App::new(store, NopNcbi, NopRcsb);
            run_info(args, app, output_mode)
        }
        DataCommand::Clear => {
            let app = App::new(store, NopNcbi, NopRcsb);
            run_clear(app, output_mode)
        }
    }
}

fn print_fetch_summary(result: &kira_biodata_manager::app::FetchResult) {
    let green = "\x1b[32m";
    let yellow = "\x1b[33m";
    let cyan = "\x1b[36m";
    let red = "\x1b[31m";
    let reset = "\x1b[0m";

    println!("{cyan}📦 KIRA-BM summary{reset}");
    println!(
        "{green}✅ Downloaded datasets: {}{reset}",
        result.items.len()
    );
    println!("{yellow}⚠️ Errors: 0{reset}");

    for item in &result.items {
        let action = item.action.as_str();
        let (icon, color) = if action.contains("cache") {
            ("♻️", green)
        } else if action.contains("download") || action.contains("fetched") {
            ("⬇️", cyan)
        } else {
            ("•", yellow)
        };
        println!(
            "{color}{icon} {} {} ({}){reset}",
            item.dataset_type, item.id, action
        );
        if let Some(path) = &item.project_path {
            println!("{color}   📁 project: {path}{reset}");
        }
        if let Some(path) = &item.cache_path {
            println!("{color}   🗃️  cache: {path}{reset}");
        }
    }

    let _ = red;
}

fn parse_tui_command(input: &str) -> miette::Result<DataCommand> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(miette::Report::msg("empty command"));
    }

    let mut parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.first().map(|v| *v == "kira-bm").unwrap_or(false) {
        parts.remove(0);
    }

    if parts.first().map(|v| *v == "data").unwrap_or(false) {
        parts.remove(0);
    }

    if parts.is_empty() {
        return Ok(DataCommand::Fetch(FetchArgs {
            specifier: None,
            config: None,
            format: None,
            force: false,
            no_cache: false,
            dry_run: false,
        }));
    }

    let command = parts[0];
    let rest = &parts[1..];

    match command {
        "fetch" | "add" => Ok(DataCommand::Fetch(FetchArgs {
            specifier: rest.get(0).map(|s| s.to_string()),
            config: None,
            format: None,
            force: false,
            no_cache: false,
            dry_run: false,
        })),
        "list" => Ok(DataCommand::List),
        "info" => {
            let spec = rest
                .get(0)
                .ok_or_else(|| miette::Report::msg("info requires a specifier"))?;
            Ok(DataCommand::Info(InfoArgs {
                specifier: spec.to_string(),
            }))
        }
        "clear" => Ok(DataCommand::Clear),
        _ => {
            if command.contains(':') {
                Ok(DataCommand::Fetch(FetchArgs {
                    specifier: Some(command.to_string()),
                    config: None,
                    format: None,
                    force: false,
                    no_cache: false,
                    dry_run: false,
                }))
            } else {
                Err(miette::Report::msg("unknown command"))
            }
        }
    }
}

#[derive(Clone, Copy)]
struct NopNcbi;
struct NopRcsb;

impl NcbiClient for NopNcbi {
    fn download_protein(
        &self,
        _id: &kira_biodata_manager::domain::ProteinId,
        _format: kira_biodata_manager::domain::ProteinFormat,
        _destination: &std::path::Path,
    ) -> Result<kira_biodata_manager::ncbi::DownloadInfo, KiraError> {
        Err(KiraError::NcbiHttp(
            "NCBI client not configured".to_string(),
        ))
    }

    fn download_genome(
        &self,
        _accession: &kira_biodata_manager::domain::GenomeAccession,
        _include: &[String],
        _destination: &std::path::Path,
    ) -> Result<kira_biodata_manager::ncbi::DownloadInfo, KiraError> {
        Err(KiraError::NcbiHttp(
            "NCBI client not configured".to_string(),
        ))
    }
}

impl RcsbClient for NopRcsb {
    fn download_structure(
        &self,
        _id: &kira_biodata_manager::domain::ProteinId,
        _format: kira_biodata_manager::domain::ProteinFormat,
        _destination: &std::path::Path,
    ) -> Result<(), KiraError> {
        Err(KiraError::RcsbHttp(
            "RCSB client not configured".to_string(),
        ))
    }

    fn fetch_metadata(
        &self,
        _id: &kira_biodata_manager::domain::ProteinId,
    ) -> Result<kira_biodata_manager::rcsb::RcsbMetadata, KiraError> {
        Err(KiraError::RcsbHttp(
            "RCSB client not configured".to_string(),
        ))
    }
}

fn run_fetch<N: NcbiClient + 'static, R: RcsbClient + 'static>(
    args: FetchArgs,
    app: App<N, R>,
    output_mode: OutputMode,
) -> miette::Result<()> {
    let FetchArgs {
        specifier,
        config,
        format,
        force,
        no_cache,
        dry_run,
    } = args;

    let specifier = specifier
        .map(|value| value.parse::<DatasetSpecifier>())
        .transpose()
        .into_diagnostic()?;

    let resolved_config = if specifier.is_none() {
        ConfigLoader::resolve(config.as_deref())
            .into_diagnostic()
            .map(Some)?
    } else {
        None
    };

    let fetch_options = FetchOptions {
        force,
        no_cache,
        dry_run,
    };

    match output_mode {
        OutputMode::NonInteractive => {
            let result = app
                .fetch(
                    specifier,
                    resolved_config.as_ref(),
                    format,
                    fetch_options,
                    &JsonOutput,
                )
                .into_diagnostic()?;
            JsonOutput::print_fetch(&result).into_diagnostic()?;
            Ok(())
        }
        OutputMode::Interactive => {
            let mut tui = Tui::new(ProgressSinkKind::Fetch);
            let result = tui.run(move |sink| {
                app.fetch(
                    specifier,
                    resolved_config.as_ref(),
                    format,
                    fetch_options,
                    sink,
                )
            })?;
            tui.finish_fetch(&result)?;
            Ok(())
        }
    }
}

fn run_list<N: NcbiClient + 'static, R: RcsbClient + 'static>(
    app: App<N, R>,
    output_mode: OutputMode,
) -> miette::Result<()> {
    match output_mode {
        OutputMode::NonInteractive => {
            let result = app.list(&JsonOutput).into_diagnostic()?;
            JsonOutput::print_list(&result).into_diagnostic()?;
            Ok(())
        }
        OutputMode::Interactive => {
            let mut tui = Tui::new(ProgressSinkKind::List);
            let result = tui.run(move |sink| app.list(sink))?;
            tui.finish_list(&result)?;
            Ok(())
        }
    }
}

fn run_info<N: NcbiClient + 'static, R: RcsbClient + 'static>(
    args: InfoArgs,
    app: App<N, R>,
    output_mode: OutputMode,
) -> miette::Result<()> {
    let specifier = args
        .specifier
        .parse::<DatasetSpecifier>()
        .into_diagnostic()?;

    match output_mode {
        OutputMode::NonInteractive => {
            let result = app.info(specifier, &JsonOutput).into_diagnostic()?;
            JsonOutput::print_info(&result).into_diagnostic()?;
            Ok(())
        }
        OutputMode::Interactive => {
            let mut tui = Tui::new(ProgressSinkKind::Info);
            let result = tui.run(move |sink| app.info(specifier, sink))?;
            tui.finish_info(&result)?;
            Ok(())
        }
    }
}

fn run_clear<N: NcbiClient + 'static, R: RcsbClient + 'static>(
    app: App<N, R>,
    output_mode: OutputMode,
) -> miette::Result<()> {
    match output_mode {
        OutputMode::NonInteractive => {
            let result = app.clear(&JsonOutput).into_diagnostic()?;
            JsonOutput::print_clear(&result).into_diagnostic()?;
            Ok(())
        }
        OutputMode::Interactive => {
            let mut tui = Tui::new(ProgressSinkKind::Clear);
            let confirmed = tui.confirm_clear()?;
            if !confirmed {
                return Ok(());
            }
            let _result = tui.run(move |sink| app.clear(sink))?;
            tui.finish_clear()?;
            Ok(())
        }
    }
}
