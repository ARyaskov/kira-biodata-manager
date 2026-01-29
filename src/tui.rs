use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant, SystemTime};
use std::{
    fmt,
    sync::{Arc, Mutex},
    thread,
};

use crossterm::ExecutableCommand;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use miette::IntoDiagnostic;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::app::{ProgressEvent, ProgressSink, ProgressSinkKind};
use crate::error::KiraError;
use crate::store::Store;

const EVENTS_MAX: usize = 6;
const LOGS_MAX: usize = 200;
const HINTS: &[&str] = &[
    "Tip: use TAB to autocomplete commands and specifiers",
    "Tip: try protein:1LYZ or genome:GCF_000005845.2",
    "Tip: / starts history search, : starts command mode",
    "Tip: F2 local browser, F4 logs, F5 config",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Operational,
    DataFocus,
    Logs,
    Help,
    Browser,
    Config,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Command,
    Search,
    Help,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Resolve,
    Prepare,
    Fetch,
    Verify,
    Store,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Phase::Resolve => "Resolve",
            Phase::Prepare => "Prepare",
            Phase::Fetch => "Fetch",
            Phase::Verify => "Verify",
            Phase::Store => "Store",
        }
    }

    fn index(self) -> usize {
        match self {
            Phase::Resolve => 0,
            Phase::Prepare => 1,
            Phase::Fetch => 2,
            Phase::Verify => 3,
            Phase::Store => 4,
        }
    }
}

#[derive(Debug, Clone)]
struct DatasetInfo {
    dataset_type: String,
    id: String,
    format: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone)]
struct StoreSummary {
    project_count: usize,
    project_bytes: u64,
    cache_count: usize,
    cache_bytes: u64,
    cache_ok: bool,
}

#[derive(Debug)]
struct AppState {
    status: String,
    phase: Phase,
    confidence: &'static str,
    req_rate: f64,
    latency_ms: Option<u128>,
    retries: u32,
    events: VecDeque<String>,
    logs: VecDeque<String>,
    view: View,
    input_mode: InputMode,
    dataset: Option<DatasetInfo>,
    store_summary: StoreSummary,
    started: Instant,
    active: bool,
    finished: bool,
    request_count: u64,
    hint_index: usize,
    last_hint_update: Instant,
}

pub struct Tui {
    kind: ProgressSinkKind,
    state: Arc<Mutex<AppState>>,
    input: String,
    history: Vec<String>,
    history_index: Option<usize>,
    log_scroll: u16,
}

struct TuiProgress {
    state: Arc<Mutex<AppState>>,
}

impl ProgressSink for TuiProgress {
    fn event(&self, event: ProgressEvent) {
        if let Ok(mut state) = self.state.lock() {
            let message = event.message.trim().to_string();
            if let Some((phase, payload)) = parse_phase(&message) {
                state.phase = phase;
                state.status = payload.to_string();
                state.confidence = confidence_for(phase);
            } else if let Some(latency) = parse_latency(&message) {
                state.latency_ms = Some(latency);
            } else if message.contains("retry") {
                state.retries = state.retries.saturating_add(1);
            } else {
                state.status = message.clone();
            }

            if message.contains("ncbi.request") || message.contains("rcsb.request") {
                state.request_count = state.request_count.saturating_add(1);
            }

            push_event(&mut state.events, message.clone());
            push_log(&mut state.logs, format!("[{}] {message}", timestamp()));
        }
    }
}

impl Tui {
    pub fn new(kind: ProgressSinkKind) -> Self {
        let summary = compute_store_summary().unwrap_or_else(|| StoreSummary {
            project_count: 0,
            project_bytes: 0,
            cache_count: 0,
            cache_bytes: 0,
            cache_ok: false,
        });
        Self {
            kind,
            state: Arc::new(Mutex::new(AppState {
                status: "ready".to_string(),
                phase: Phase::Resolve,
                confidence: "Low",
                req_rate: 0.0,
                latency_ms: None,
                retries: 0,
                events: VecDeque::new(),
                logs: VecDeque::new(),
                view: View::Operational,
                input_mode: InputMode::Command,
                dataset: None,
                store_summary: summary,
                started: Instant::now(),
                active: false,
                finished: false,
                request_count: 0,
                hint_index: 0,
                last_hint_update: Instant::now(),
            })),
            input: String::new(),
            history: Vec::new(),
            history_index: None,
            log_scroll: 0,
        }
    }

    pub fn run<F, R>(&mut self, f: F) -> miette::Result<R>
    where
        F: FnOnce(&dyn ProgressSink) -> Result<R, KiraError> + Send + 'static,
        R: Send + 'static,
    {
        self.set_active(true);

        let mut stdout = io::stdout();
        enable_raw_mode().into_diagnostic()?;
        stdout.execute(EnterAlternateScreen).into_diagnostic()?;

        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).into_diagnostic()?;
        terminal.clear().into_diagnostic()?;

        let (tx, rx) = std::sync::mpsc::channel();
        let state = self.state.clone();
        let sink = TuiProgress {
            state: state.clone(),
        };
        let handle = thread::spawn(move || tx.send(f(&sink)));

        let mut tick = 0usize;
        loop {
            self.refresh_metrics();
            if let Ok(state) = self.state.lock() {
                let elapsed = state.started.elapsed();
                terminal
                    .draw(|frame| draw_ui(frame, self, &state, tick, elapsed))
                    .into_diagnostic()?;
            }

            if let Ok(result) = rx.try_recv() {
                self.set_active(false);
                disable_raw_mode().into_diagnostic()?;
                let mut stdout = io::stdout();
                stdout.execute(LeaveAlternateScreen).into_diagnostic()?;
                handle.join().ok();
                return result.map_err(|err| miette::Report::new(err));
            }

            if event::poll(Duration::from_millis(120)).into_diagnostic()? {
                if let Event::Key(key) = event::read().into_diagnostic()? {
                    if self.handle_key(key) {
                        break;
                    }
                }
            }

            tick = tick.wrapping_add(1);
        }

        self.set_active(false);
        disable_raw_mode().into_diagnostic()?;
        let mut stdout = io::stdout();
        stdout.execute(LeaveAlternateScreen).into_diagnostic()?;
        Err(miette::Report::msg("aborted"))
    }

    pub fn idle_command(&mut self) -> miette::Result<Option<String>> {
        self.set_active(false);
        if let Ok(mut state) = self.state.lock() {
            state.status = "ready".to_string();
            state.view = View::Operational;
            state.input_mode = InputMode::Command;
        }

        let mut stdout = io::stdout();
        enable_raw_mode().into_diagnostic()?;
        stdout.execute(EnterAlternateScreen).into_diagnostic()?;

        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).into_diagnostic()?;
        terminal.clear().into_diagnostic()?;

        let mut tick = 0usize;
        let mut command: Option<String> = None;
        loop {
            self.refresh_metrics();
            if let Ok(state) = self.state.lock() {
                let elapsed = state.started.elapsed();
                terminal
                    .draw(|frame| draw_ui(frame, self, &state, tick, elapsed))
                    .into_diagnostic()?;
            }

            if event::poll(Duration::from_millis(120)).into_diagnostic()? {
                if let Event::Key(key) = event::read().into_diagnostic()? {
                    if matches!(key.code, KeyCode::Enter) {
                        if let Some(cmd) = self.take_command() {
                            command = Some(cmd);
                            break;
                        }
                    }
                    if self.handle_key(key) {
                        break;
                    }
                }
            }

            tick = tick.wrapping_add(1);
        }

        disable_raw_mode().into_diagnostic()?;
        let mut stdout = io::stdout();
        stdout.execute(LeaveAlternateScreen).into_diagnostic()?;
        Ok(command)
    }

    pub fn finish_fetch(&mut self, result: &crate::app::FetchResult) -> miette::Result<()> {
        if let Some(item) = result.items.first() {
            if let Ok(mut state) = self.state.lock() {
                state.dataset = Some(DatasetInfo {
                    dataset_type: item.dataset_type.clone(),
                    id: item.id.clone(),
                    format: item.format.clone(),
                    source: Some(item.source.clone()),
                });
                state.view = View::DataFocus;
                state.input_mode = InputMode::Command;
            }
        }
        Ok(())
    }

    pub fn finish_list(&mut self, _result: &crate::app::ListResult) -> miette::Result<()> {
        if let Ok(mut state) = self.state.lock() {
            state.view = View::Operational;
        }
        Ok(())
    }

    pub fn finish_info(&mut self, result: &crate::app::InfoResult) -> miette::Result<()> {
        if let Ok(mut state) = self.state.lock() {
            state.dataset = Some(DatasetInfo {
                dataset_type: result.dataset_type.clone(),
                id: result.id.clone(),
                format: result.format.clone(),
                source: result.source.clone(),
            });
            state.view = View::DataFocus;
            state.input_mode = InputMode::Command;
        }
        Ok(())
    }

    pub fn finish_clear(&mut self) -> miette::Result<()> {
        if let Ok(mut state) = self.state.lock() {
            state.store_summary = compute_store_summary().unwrap_or(state.store_summary.clone());
            state.view = View::Operational;
        }
        Ok(())
    }

    pub fn confirm_clear(&mut self) -> miette::Result<bool> {
        let mut stdout = io::stdout();
        enable_raw_mode().into_diagnostic()?;
        stdout.execute(EnterAlternateScreen).into_diagnostic()?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).into_diagnostic()?;

        let confirmed = loop {
            terminal
                .draw(|frame| {
                    let block = Block::default().borders(Borders::ALL).title("Confirm");
                    let text = Paragraph::new(vec![
                        Line::from("Clear project store?"),
                        Line::from("Press y to confirm, n to cancel."),
                    ])
                    .alignment(Alignment::Center)
                    .block(block);
                    frame.render_widget(text, frame.area());
                })
                .into_diagnostic()?;

            if event::poll(Duration::from_millis(100)).into_diagnostic()? {
                if let Event::Key(key) = event::read().into_diagnostic()? {
                    match key.code {
                        KeyCode::Char('y') | KeyCode::Char('Y') => break true,
                        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => break false,
                        _ => {}
                    }
                }
            }
        };

        disable_raw_mode().into_diagnostic()?;
        let mut stdout = io::stdout();
        stdout.execute(LeaveAlternateScreen).into_diagnostic()?;
        Ok(confirmed)
    }
    fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.kind != KeyEventKind::Press {
            return false;
        }
        if matches!(key.code, KeyCode::F(1)) {
            self.set_view(View::Help);
            self.set_input_mode(InputMode::Help);
            return false;
        }
        if matches!(key.code, KeyCode::F(2)) {
            self.set_view(View::Browser);
            return false;
        }
        if matches!(key.code, KeyCode::F(3)) {
            self.set_view(View::Operational);
            self.set_input_mode(InputMode::Search);
            return false;
        }
        if matches!(key.code, KeyCode::F(4)) {
            self.set_view(View::Logs);
            return false;
        }
        if matches!(key.code, KeyCode::F(5)) {
            self.set_view(View::Config);
            return false;
        }

        match key.code {
            KeyCode::Char('q') => {
                if !self.input.is_empty() {
                    self.input.push('q');
                    return false;
                }
                if self.is_active() {
                    return false;
                }
                return true;
            }
            KeyCode::Esc => {
                return true;
            }
            KeyCode::Char(':') => {
                if self.input.is_empty() {
                    self.set_input_mode(InputMode::Command);
                    self.input.clear();
                } else {
                    self.input.push(':');
                }
            }
            KeyCode::Char('/') => {
                if self.input.is_empty() {
                    self.set_input_mode(InputMode::Search);
                    self.input.clear();
                } else {
                    self.input.push('/');
                }
            }
            KeyCode::Char('?') => {
                if self.input.is_empty() {
                    self.set_view(View::Help);
                    self.set_input_mode(InputMode::Help);
                    self.input.clear();
                } else {
                    self.input.push('?');
                }
            }
            KeyCode::Tab => {
                let suggestion = self.autocomplete();
                self.set_input_text(&suggestion);
            }
            KeyCode::Up => self.history_up(),
            KeyCode::Down => self.history_down(),
            KeyCode::PageUp => self.scroll_logs(-5),
            KeyCode::PageDown => self.scroll_logs(5),
            KeyCode::Enter => {
                if self.input_mode() == InputMode::Search {
                    if let Some(best) = self.best_history_match() {
                        self.set_input_text(&best);
                        self.set_input_mode(InputMode::Command);
                    }
                }
            }
            KeyCode::Backspace => {
                self.input.pop();
            }
            _ => {
                if let KeyCode::Char(ch) = key.code {
                    self.input.push(ch);
                }
            }
        }
        false
    }

    fn take_command(&mut self) -> Option<String> {
        let current = self.input.trim().to_string();
        if current.is_empty() {
            return None;
        }
        self.history.push(current.clone());
        self.history_index = None;
        self.input.clear();
        Some(current)
    }

    fn input_mode(&self) -> InputMode {
        self.state
            .lock()
            .map(|state| state.input_mode)
            .unwrap_or(InputMode::Command)
    }

    fn set_input_mode(&self, mode: InputMode) {
        if let Ok(mut state) = self.state.lock() {
            state.input_mode = mode;
        }
    }

    fn set_view(&self, view: View) {
        if let Ok(mut state) = self.state.lock() {
            state.view = view;
        }
    }

    fn is_active(&self) -> bool {
        self.state.lock().map(|state| state.active).unwrap_or(false)
    }

    fn set_active(&self, active: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.active = active;
            state.finished = !active;
            state.started = Instant::now();
            state.request_count = 0;
            state.retries = 0;
            state.latency_ms = None;
            state.phase = Phase::Resolve;
            state.confidence = "Low";
        }
    }

    fn refresh_metrics(&self) {
        if let Ok(mut state) = self.state.lock() {
            if state.active {
                let elapsed = state.started.elapsed().as_secs_f64().max(0.1);
                state.req_rate = (state.request_count as f64) / elapsed;
            } else {
                state.req_rate = 0.0;
            }
            if state.last_hint_update.elapsed() >= Duration::from_secs(5) {
                state.hint_index = (state.hint_index + 1) % HINTS.len().max(1);
                state.last_hint_update = Instant::now();
            }
            if let Some(summary) = compute_store_summary() {
                state.store_summary = summary;
            }
        }
    }

    fn history_up(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let next = match self.history_index {
            Some(index) if index > 0 => index - 1,
            Some(_) => 0,
            None => self.history.len().saturating_sub(1),
        };
        self.history_index = Some(next);
        if let Some(value) = self.history.get(next).cloned() {
            self.set_input_text(&value);
        }
    }

    fn history_down(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let next = match self.history_index {
            Some(index) if index + 1 < self.history.len() => index + 1,
            _ => {
                self.history_index = None;
                self.set_input_text("");
                return;
            }
        };
        self.history_index = Some(next);
        if let Some(value) = self.history.get(next).cloned() {
            self.set_input_text(&value);
        }
    }

    fn set_input_text(&mut self, value: &str) {
        self.input = value.to_string();
    }

    fn autocomplete(&mut self) -> String {
        let current = self.input.trim();
        if current.starts_with("pro") && !current.contains(':') {
            return "protein:".to_string();
        }
        if current.starts_with("gen") && !current.contains(':') {
            return "genome:".to_string();
        }
        if current.starts_with("data f") {
            return "data fetch ".to_string();
        }
        if current.starts_with("data i") {
            return "data info ".to_string();
        }
        if current.starts_with("data l") {
            return "data list".to_string();
        }
        if current.starts_with("data c") {
            return "data clear".to_string();
        }
        if current.starts_with("data fetch pro") {
            return "data fetch protein:".to_string();
        }
        if current.starts_with("data fetch gen") {
            return "data fetch genome:".to_string();
        }
        self.best_history_match()
            .unwrap_or_else(|| current.to_string())
    }

    fn best_history_match(&self) -> Option<String> {
        let needle = self.input.trim();
        if needle.is_empty() {
            return None;
        }
        let mut best: Option<(usize, &String)> = None;
        for entry in &self.history {
            if let Some(score) = fuzzy_score(needle, entry) {
                match best {
                    Some((best_score, _)) if score <= best_score => {}
                    _ => best = Some((score, entry)),
                }
            }
        }
        best.map(|(_, entry)| entry.clone())
    }

    fn scroll_logs(&mut self, delta: i16) {
        let max = self.state.lock().map(|state| state.logs.len()).unwrap_or(0);
        let max_scroll = max.saturating_sub(1) as i16;
        let next = (self.log_scroll as i16 + delta).clamp(0, max_scroll);
        self.log_scroll = next as u16;
    }
}

fn draw_ui(
    frame: &mut ratatui::Frame,
    tui: &Tui,
    state: &AppState,
    tick: usize,
    elapsed: Duration,
) {
    match state.view {
        View::Operational | View::Config | View::Browser => {
            draw_operational(frame, tui, state, tick, elapsed)
        }
        View::DataFocus => draw_data_focus(frame, tui, state),
        View::Logs => draw_logs(frame, tui, state),
        View::Help => draw_help(frame),
    }
}

fn draw_operational(
    frame: &mut ratatui::Frame,
    tui: &Tui,
    state: &AppState,
    tick: usize,
    elapsed: Duration,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(4),
        ])
        .split(frame.area());

    let header = draw_header(state, tui.kind, tick);
    frame.render_widget(header, chunks[0]);

    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(chunks[1]);

    let status = draw_status_panel(state, elapsed);
    frame.render_widget(status, main[0]);

    let details = draw_details_panel(state);
    frame.render_widget(details, main[1]);

    draw_command_line(frame, tui, state, tick, chunks[2]);
}

fn draw_data_focus(frame: &mut ratatui::Frame, tui: &Tui, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header = Paragraph::new(Line::from("KIRA-BM :: DATA VIEW"))
        .block(Block::default().borders(Borders::BOTTOM))
        .alignment(Alignment::Left)
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(header, chunks[0]);

    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(4)])
        .split(chunks[1]);

    let dataset = state.dataset.clone();
    let info_lines = if let Some(info) = dataset {
        vec![
            Line::from(vec![
                Span::styled("Dataset: ", Style::default().fg(Color::Gray)),
                Span::styled(
                    format!("{} {}", info.dataset_type.to_uppercase(), info.id),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
            Line::from(vec![
                Span::styled("Format: ", Style::default().fg(Color::Gray)),
                Span::raw(info.format.clone().unwrap_or_else(|| "n/a".to_string())),
                Span::styled("   Source: ", Style::default().fg(Color::Gray)),
                Span::raw(info.source.clone().unwrap_or_else(|| "n/a".to_string())),
            ]),
            Line::from(vec![
                Span::styled("Integrity: ", Style::default().fg(Color::Gray)),
                Span::styled("pending", Style::default().fg(Color::Yellow)),
            ]),
            Line::from(vec![
                Span::styled("Cache impact: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
                Span::styled("   Time saved: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
            ]),
        ]
    } else {
        vec![
            Line::from(vec![
                Span::styled("Dataset: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
            ]),
            Line::from(vec![
                Span::styled("Format: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
                Span::styled("   Source: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
            ]),
            Line::from(vec![
                Span::styled("Integrity: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
            ]),
            Line::from(vec![
                Span::styled("Cache impact: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
                Span::styled("   Time saved: ", Style::default().fg(Color::Gray)),
                Span::raw("n/a"),
            ]),
        ]
    };
    let info_block = Paragraph::new(info_lines).wrap(Wrap { trim: true });
    frame.render_widget(info_block, body[0]);

    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(body[1]);

    let ops = Paragraph::new(vec![
        Line::from(Span::styled(
            "OPERATIONS",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from("[v] fetched"),
        Line::from("[ ] verified"),
        Line::from("[ ] indexed"),
        Line::from(""),
        Line::from(vec![
            Span::styled("Cache impact: ", Style::default().fg(Color::Gray)),
            Span::raw("n/a"),
        ]),
        Line::from(vec![
            Span::styled("Time saved: ", Style::default().fg(Color::Gray)),
            Span::raw("n/a"),
        ]),
    ])
    .block(Block::default().borders(Borders::RIGHT));
    frame.render_widget(ops, body_chunks[0]);

    let logs = draw_logs_view(state, tui.log_scroll);
    frame.render_widget(logs, body_chunks[1]);

    draw_command_line(frame, tui, state, 0, chunks[2]);
}

fn draw_logs(frame: &mut ratatui::Frame, tui: &Tui, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header = draw_header(state, tui.kind, 0);
    frame.render_widget(header, chunks[0]);

    let logs = draw_logs_view(state, tui.log_scroll);
    frame.render_widget(logs, chunks[1]);

    draw_command_line(frame, tui, state, 0, chunks[2]);
}

fn draw_help(frame: &mut ratatui::Frame) {
    let block = Block::default().borders(Borders::ALL).title("Help");
    let lines = vec![
        Line::from("F1 Help  F2 Browser  F3 Search  F4 Logs  F5 Config"),
        Line::from(": command mode   / search mode   ? help mode"),
        Line::from("Commands: data fetch|list|info|clear"),
        Line::from("Examples: protein:1LYZ  genome:GCF_000005845.2"),
    ];
    let view = Paragraph::new(lines).block(block).wrap(Wrap { trim: true });
    frame.render_widget(view, frame.area());
}

fn draw_header(state: &AppState, kind: ProgressSinkKind, tick: usize) -> Paragraph<'static> {
    let hb = if tick % 2 == 0 { "*" } else { " " };
    let cache_label = if state.store_summary.cache_ok {
        "cache OK"
    } else {
        "cache ?"
    };
    let cache_color = if state.store_summary.cache_ok {
        Color::Green
    } else {
        Color::Yellow
    };
    let op_label = match kind {
        ProgressSinkKind::Fetch => "Fetch",
        ProgressSinkKind::List => "List",
        ProgressSinkKind::Info => "Info",
        ProgressSinkKind::Clear => "Clear",
    };
    let header_line = Line::from(vec![
        Span::styled(
            "KIRA-BM",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(env!("CARGO_PKG_VERSION"), Style::default().fg(Color::Gray)),
        Span::raw("   Registry: Auto   Mode: Interactive   Op: "),
        Span::styled(op_label, Style::default().fg(Color::Cyan)),
        Span::raw("   "),
        Span::styled(hb, Style::default().fg(Color::Green)),
    ]);
    let store_line = Line::from(vec![
        Span::styled(
            format!(
                "Local store: {} datasets · {} · ",
                state.store_summary.project_count,
                bytes_to_human(state.store_summary.project_bytes)
            ),
            Style::default().fg(Color::Gray),
        ),
        Span::styled(cache_label, Style::default().fg(cache_color)),
        Span::styled(
            format!(
                "   Cache: {} datasets · {}",
                state.store_summary.cache_count,
                bytes_to_human(state.store_summary.cache_bytes)
            ),
            Style::default().fg(Color::Gray),
        ),
    ]);
    Paragraph::new(vec![header_line, store_line])
        .alignment(Alignment::Left)
        .block(Block::default().borders(Borders::BOTTOM))
}

fn draw_status_panel(state: &AppState, elapsed: Duration) -> Paragraph<'static> {
    let progress = phase_progress(state.phase, elapsed);
    let bar = progress_bar(progress);
    let phase_color = if state.active {
        Color::Cyan
    } else if state.finished {
        Color::Green
    } else {
        Color::Yellow
    };
    let req_rate = if state.req_rate > 0.0 {
        format!("{:.1}", state.req_rate)
    } else {
        "0.0".to_string()
    };
    let latency = state
        .latency_ms
        .map(|v| format!("{v} ms"))
        .unwrap_or_else(|| "--".to_string());
    let mut lines = vec![
        Line::from(Span::styled(
            "STATUS / PROGRESS",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled("Phase: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:<7} ", state.phase.label()),
                Style::default().fg(phase_color),
            ),
            Span::raw(bar),
            Span::raw(format!(" {:>3}%", progress)),
        ]),
        Line::from(vec![
            Span::styled("Confidence: ", Style::default().fg(Color::Gray)),
            Span::styled(state.confidence, Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::styled("Req/s: ", Style::default().fg(Color::Gray)),
            Span::raw(req_rate),
            Span::styled("   Latency: ", Style::default().fg(Color::Gray)),
            Span::raw(latency),
        ]),
        Line::from(vec![
            Span::styled("Retries: ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}", state.retries)),
        ]),
        Line::from(Span::styled(
            "Recent events:",
            Style::default().fg(Color::Gray),
        )),
    ];

    for event in state.events.iter().rev().take(3) {
        lines.push(Line::from(format!("- {}", event)));
    }
    lines.push(Line::from(vec![
        Span::styled("Hint: ", Style::default().fg(Color::Gray)),
        Span::styled(HINTS[state.hint_index], Style::default().fg(Color::Gray)),
    ]));

    Paragraph::new(lines)
        .block(Block::default().borders(Borders::RIGHT))
        .wrap(Wrap { trim: true })
}

fn draw_details_panel(state: &AppState) -> Paragraph<'static> {
    let dataset = state.dataset.clone();
    let mut lines = vec![Line::from(Span::styled(
        "DETAILS",
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ))];
    if let Some(info) = dataset {
        lines.push(Line::from(vec![
            Span::styled("Source: ", Style::default().fg(Color::Gray)),
            Span::raw(info.source.unwrap_or_else(|| "n/a".to_string())),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Dataset: ", Style::default().fg(Color::Gray)),
            Span::raw(
                info.format
                    .clone()
                    .unwrap_or_else(|| info.dataset_type.to_uppercase()),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Accession: ", Style::default().fg(Color::Gray)),
            Span::raw(info.id),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Integrity: ", Style::default().fg(Color::Gray)),
            Span::styled("pending", Style::default().fg(Color::Yellow)),
        ]));
    } else {
        lines.push(Line::from(vec![
            Span::styled("Source: ", Style::default().fg(Color::Gray)),
            Span::raw("NCBI"),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Dataset: ", Style::default().fg(Color::Gray)),
            Span::raw("n/a"),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Accession: ", Style::default().fg(Color::Gray)),
            Span::raw("n/a"),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Integrity: ", Style::default().fg(Color::Gray)),
            Span::raw("n/a"),
        ]));
    }

    Paragraph::new(lines).wrap(Wrap { trim: true })
}

fn draw_command_line(
    frame: &mut ratatui::Frame,
    tui: &Tui,
    state: &AppState,
    tick: usize,
    area: Rect,
) {
    let prefix = match state.input_mode {
        InputMode::Command => ": ",
        InputMode::Search => "/ ",
        InputMode::Help => "? ",
    };
    let preview = command_preview(tui, state);
    let placeholder = if tui.input.is_empty() {
        Span::raw("")
    } else {
        Span::styled(tui.input.clone(), Style::default().fg(Color::White))
    };
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                prefix,
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            placeholder,
        ]),
        Line::from(vec![
            Span::styled("= ", Style::default().fg(Color::DarkGray)),
            Span::styled(preview, Style::default().fg(Color::DarkGray)),
        ]),
    ];
    if state.view == View::Browser {
        lines.push(Line::from("Browser: local datasets"));
    } else if state.view == View::Config {
        lines.push(Line::from("Config: kira-bm.json"));
    } else if state.view == View::Logs {
        lines.push(Line::from("Logs: PgUp/PgDown to scroll"));
    }

    let block = Block::default().borders(Borders::TOP);
    let para = Paragraph::new(lines).block(block);
    frame.render_widget(para, area);

    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_millis(0))
        .as_millis();
    let blink_on = (now_ms / 700) % 2 == 0;
    if blink_on {
        let mut cursor_x = area
            .x
            .saturating_add(frame_cursor_x(prefix, &tui.input, state, tick));
        let cursor_y = area.y.saturating_add(1);
        if cursor_x >= area.x.saturating_add(area.width) {
            cursor_x = area.x.saturating_add(area.width.saturating_sub(1));
        }
        frame.set_cursor_position((cursor_x, cursor_y));
    }
}

fn draw_logs_view(state: &AppState, scroll: u16) -> Paragraph<'static> {
    let total = state.logs.len();
    let visible = 9usize;
    let start = total.saturating_sub(scroll as usize + visible);
    let mut lines = Vec::with_capacity(visible + 1);
    lines.push(Line::from(Span::styled(
        "LOGS (scrollable)",
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
    for line in state.logs.iter().skip(start).take(visible) {
        lines.push(Line::from(line.clone()));
    }
    Paragraph::new(lines)
        .block(Block::default())
        .wrap(Wrap { trim: true })
}

fn command_preview(tui: &Tui, state: &AppState) -> String {
    if state.input_mode == InputMode::Search {
        if let Some(best) = tui.best_history_match() {
            return format!("match: {best}");
        }
        return "search history".to_string();
    }
    let raw = tui.input.trim();
    if raw.is_empty() {
        return "ready".to_string();
    }
    if raw.starts_with("protein:") {
        return format!("fetch {}", raw);
    }
    if raw.starts_with("genome:") {
        return format!("fetch {}", raw);
    }
    if raw.starts_with("data ") || raw.starts_with("fetch") || raw.starts_with("list") {
        return raw.to_string();
    }
    raw.to_string()
}

fn phase_progress(phase: Phase, elapsed: Duration) -> u8 {
    let base = ((phase.index() + 1) as f64 / 5.0) * 100.0;
    let wobble = (elapsed.as_millis() % 500) as f64 / 500.0 * 4.0;
    (base + wobble).min(100.0) as u8
}

fn progress_bar(percent: u8) -> String {
    let total = 10;
    let filled = (percent as usize * total) / 100;
    let mut out = String::from("[");
    for i in 0..total {
        out.push(if i < filled { '#' } else { '.' });
    }
    out.push(']');
    out
}

fn confidence_for(phase: Phase) -> &'static str {
    match phase {
        Phase::Resolve => "Low",
        Phase::Prepare => "Low",
        Phase::Fetch => "Medium",
        Phase::Verify => "High",
        Phase::Store => "High",
    }
}

fn parse_phase(message: &str) -> Option<(Phase, &str)> {
    if let Some(rest) = message.strip_prefix("phase=Resolve;") {
        return Some((Phase::Resolve, rest.trim()));
    }
    if let Some(rest) = message.strip_prefix("phase=Prepare;") {
        return Some((Phase::Prepare, rest.trim()));
    }
    if let Some(rest) = message.strip_prefix("phase=Fetch;") {
        return Some((Phase::Fetch, rest.trim()));
    }
    if let Some(rest) = message.strip_prefix("phase=Verify;") {
        return Some((Phase::Verify, rest.trim()));
    }
    if let Some(rest) = message.strip_prefix("phase=Store;") {
        return Some((Phase::Store, rest.trim()));
    }
    None
}

fn parse_latency(message: &str) -> Option<u128> {
    message
        .split("latency_ms=")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse::<u128>().ok())
}

fn push_event(buffer: &mut VecDeque<String>, item: String) {
    buffer.push_back(item);
    while buffer.len() > EVENTS_MAX {
        buffer.pop_front();
    }
}

fn push_log(buffer: &mut VecDeque<String>, item: String) {
    buffer.push_back(item);
    while buffer.len() > LOGS_MAX {
        buffer.pop_front();
    }
}

fn timestamp() -> String {
    let now = SystemTime::now();
    let secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs();
    let mins = (secs / 60) % 60;
    let hours = (secs / 3600) % 24;
    let seconds = secs % 60;
    format!("{hours:02}:{mins:02}:{seconds:02}")
}

fn fuzzy_score(needle: &str, hay: &str) -> Option<usize> {
    let mut score = 0usize;
    let mut iter = hay.chars();
    for ch in needle.chars() {
        let mut found = false;
        while let Some(h) = iter.next() {
            score += 1;
            if h.eq_ignore_ascii_case(&ch) {
                found = true;
                break;
            }
        }
        if !found {
            return None;
        }
    }
    Some(score)
}

fn compute_store_summary() -> Option<StoreSummary> {
    let store = Store::new().ok()?;
    let project = Store::list_metadata(store.project_root()).ok()?;
    let cache = Store::list_metadata(store.cache_root()).ok()?;
    let project_bytes = dir_size(store.project_root().as_std_path()).unwrap_or(0);
    let cache_bytes = dir_size(store.cache_root().as_std_path()).unwrap_or(0);
    Some(StoreSummary {
        project_count: project.len(),
        project_bytes,
        cache_count: cache.len(),
        cache_bytes,
        cache_ok: store.cache_root().as_std_path().exists(),
    })
}

fn dir_size(path: &std::path::Path) -> Option<u64> {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = std::fs::read_dir(&path).ok()?;
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(meta) = entry.metadata() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    Some(total)
}

fn bytes_to_human(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let value = bytes as f64;
    if value >= GB {
        format!("{:.1} GB", value / GB)
    } else if value >= MB {
        format!("{:.1} MB", value / MB)
    } else if value >= KB {
        format!("{:.1} KB", value / KB)
    } else {
        format!("{bytes} B")
    }
}

fn frame_cursor_x(prefix: &str, input: &str, _state: &AppState, _tick: usize) -> u16 {
    (prefix.len() + input.len()) as u16
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}
