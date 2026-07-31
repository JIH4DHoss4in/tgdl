use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use console::{
    AnsiCodeIterator, Key, Style, Term, measure_text_width, strip_ansi_codes, truncate_str,
};

use crate::util::prompt;

static MENU_SELECTIONS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
const ENTER_APPLICATION_SCREEN: &str = "\x1b[?1049h\x1b[2J\x1b[H";
const CLEAR_APPLICATION_PAGE: &str = "\x1b[2J\x1b[H";
const LEAVE_APPLICATION_SCREEN: &str = "\x1b[?1049l";

pub(crate) struct TerminalSession {
    term: Term,
    alternate_screen: bool,
}

pub(crate) fn enter_terminal_session() -> Result<TerminalSession> {
    let term = Term::stdout();
    let alternate_screen = term.is_term();
    if alternate_screen {
        // On Windows this also enables virtual-terminal processing before
        // sending the alternate-screen control sequence.
        let _ = term.features().colors_supported();
        term.write_str(ENTER_APPLICATION_SCREEN)
            .context("failed to open application screen")?;
        term.flush().context("failed to open application screen")?;
    }
    Ok(TerminalSession {
        term,
        alternate_screen,
    })
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        if self.alternate_screen {
            let _ = self.term.show_cursor();
            let _ = self.term.write_str(LEAVE_APPLICATION_SCREEN);
            let _ = self.term.flush();
        }
    }
}

pub(crate) fn select_menu<T: AsRef<str>>(
    title: &str,
    entries: &[T],
    zero_label: &str,
) -> Result<Option<usize>> {
    select_menu_with_body(title, &[], entries, zero_label)
}

pub(crate) fn select_menu_with_body<T: AsRef<str>>(
    title: &str,
    body: &[String],
    entries: &[T],
    zero_label: &str,
) -> Result<Option<usize>> {
    let term = Term::stdout();
    if term.is_term() {
        return select_attended_menu(&term, title, body, entries, zero_label);
    }

    render_page(title, &[])?;
    let width = terminal_width(&term);
    if !body.is_empty() {
        println!("{}", panel_top(&accent_text("OVERVIEW"), width));
        for line in body {
            for row in panel_rows(line, width) {
                println!("{row}");
            }
        }
        println!("{}", panel_bottom(width));
    }
    println!("{}", panel_top(&accent_text("ACTIONS"), width));
    for (index, entry) in entries.iter().enumerate() {
        for content in menu_row_contents(index, entry.as_ref(), usize::MAX, width - 4) {
            println!("{}", panel_row(&content, width));
        }
    }
    for content in menu_row_contents(entries.len(), zero_label, usize::MAX, width - 4) {
        println!("{}", panel_row(&content, width));
    }
    println!("{}", panel_bottom(width));
    loop {
        let answer = prompt("Select: ")?;
        if let Some(selection) = parse_menu_selection(&answer, entries.len()) {
            return Ok(selection);
        }
        print_wrapped(&warning_text(format!(
            "Enter a number from 0 to {}.",
            entries.len()
        )));
    }
}

fn select_attended_menu<T: AsRef<str>>(
    term: &Term,
    title: &str,
    body: &[String],
    entries: &[T],
    zero_label: &str,
) -> Result<Option<usize>> {
    let _cursor = CursorGuard::new(term.clone())?;
    let choice_count = entries.len() + 1;
    let mut selected = remembered_menu_selection(title, choice_count);
    let mut body_offset = 0;
    let menu_width = terminal_width(term);
    let body = body
        .iter()
        .flat_map(|line| wrap_text_lines(line, menu_width.saturating_sub(4)))
        .collect::<Vec<_>>();
    let action_rows = menu_action_rows(entries, zero_label, menu_width.saturating_sub(4));
    let (visible_body, _, _) = menu_body_window(term, &body, body_offset, action_rows);
    render_menu_page(
        term,
        title,
        &visible_body,
        entries,
        zero_label,
        selected,
        menu_width,
    )?;
    loop {
        let previous = selected;
        match term.read_key().context("failed to read terminal key")? {
            Key::ArrowUp | Key::Char('k') => {
                selected = selected.checked_sub(1).unwrap_or(choice_count - 1);
            }
            Key::ArrowDown | Key::Char('j') | Key::Tab => {
                selected = (selected + 1) % choice_count;
            }
            Key::Home => selected = 0,
            Key::End => selected = choice_count - 1,
            Key::PageUp => {
                let (_, page_size, _) = menu_body_window(term, &body, body_offset, action_rows);
                let next_offset = body_offset.saturating_sub(page_size.max(1));
                if next_offset != body_offset {
                    body_offset = next_offset;
                    let (visible_body, _, _) =
                        menu_body_window(term, &body, body_offset, action_rows);
                    render_menu_page(
                        term,
                        title,
                        &visible_body,
                        entries,
                        zero_label,
                        selected,
                        menu_width,
                    )?;
                }
                continue;
            }
            Key::PageDown => {
                let (_, page_size, max_offset) =
                    menu_body_window(term, &body, body_offset, action_rows);
                let next_offset = (body_offset + page_size.max(1)).min(max_offset);
                if next_offset != body_offset {
                    body_offset = next_offset;
                    let (visible_body, _, _) =
                        menu_body_window(term, &body, body_offset, action_rows);
                    render_menu_page(
                        term,
                        title,
                        &visible_body,
                        entries,
                        zero_label,
                        selected,
                        menu_width,
                    )?;
                }
                continue;
            }
            Key::Enter | Key::Char(' ') => {
                if selected < entries.len() {
                    remember_menu_selection(title, selected);
                    return Ok(Some(selected));
                }
                forget_menu_selection(title);
                return Ok(None);
            }
            Key::Escape | Key::Char('q') | Key::Char('Q') | Key::CtrlC => {
                forget_menu_selection(title);
                return Ok(None);
            }
            Key::Char(character) if character.is_ascii_digit() => {
                let number = character.to_digit(10).unwrap_or_default() as usize;
                if number == 0 {
                    forget_menu_selection(title);
                    return Ok(None);
                }
                if number <= entries.len() {
                    remember_menu_selection(title, number - 1);
                    return Ok(Some(number - 1));
                }
            }
            _ => {}
        }
        if selected != previous {
            redraw_menu_selection(term, entries, zero_label, previous, selected, menu_width)?;
        }
    }
}

fn menu_body_window(
    term: &Term,
    body: &[String],
    offset: usize,
    action_rows: usize,
) -> (Vec<String>, usize, usize) {
    let capacity = usize::from(term.size().0)
        .saturating_sub(action_rows + 13)
        .max(1);
    if body.len() <= capacity {
        return (body.to_vec(), capacity, 0);
    }
    if capacity == 1 {
        return (
            vec![muted_text(format!(
                "{} summary rows · PageUp/PageDown",
                body.len()
            ))],
            1,
            body.len().saturating_sub(1),
        );
    }

    let page_size = capacity - 1;
    let max_offset = body.len().saturating_sub(page_size);
    let offset = offset.min(max_offset);
    let end = (offset + page_size).min(body.len());
    let mut visible = body[offset..end].to_vec();
    visible.push(muted_text(format!(
        "Rows {}–{} of {} · PageUp/PageDown",
        offset + 1,
        end,
        body.len()
    )));
    (visible, page_size, max_offset)
}

fn remembered_menu_selection(title: &str, choice_count: usize) -> usize {
    if choice_count == 0 {
        return 0;
    }
    MENU_SELECTIONS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(title)
        .copied()
        .unwrap_or_default()
        .min(choice_count - 1)
}

fn remember_menu_selection(title: &str, selected: usize) {
    MENU_SELECTIONS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(title.to_owned(), selected);
}

fn forget_menu_selection(title: &str) {
    MENU_SELECTIONS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(title);
}

fn render_menu_page<T: AsRef<str>>(
    term: &Term,
    title: &str,
    body: &[String],
    entries: &[T],
    zero_label: &str,
    selected: usize,
    menu_width: usize,
) -> Result<()> {
    render_page_on(term, title, &[])?;
    if !body.is_empty() {
        println!("{}", panel_top(&accent_text("OVERVIEW"), menu_width));
        for line in body {
            for row in panel_rows(line, menu_width) {
                println!("{row}");
            }
        }
        println!("{}", panel_bottom(menu_width));
    }
    println!("{}", panel_top(&accent_text("ACTIONS"), menu_width));
    for (index, entry) in entries.iter().enumerate() {
        for content in menu_row_contents(index, entry.as_ref(), selected, menu_width - 4) {
            println!("{}", panel_row(&content, menu_width));
        }
    }
    for content in menu_row_contents(entries.len(), zero_label, selected, menu_width - 4) {
        println!("{}", panel_row(&content, menu_width));
    }
    println!("{}", panel_bottom(menu_width));
    println!(
        "\n{}",
        muted_text("↑/↓ navigate   Enter select   Esc/q back   Number shortcut")
    );
    Ok(())
}

fn menu_row_contents(index: usize, label: &str, selected: usize, width: usize) -> Vec<String> {
    let shortcut = if label.eq_ignore_ascii_case("exit")
        || label.eq_ignore_ascii_case("back")
        || label.eq_ignore_ascii_case("cancel")
    {
        "0".to_owned()
    } else {
        (index + 1).to_string()
    };
    let prefix = if index == selected {
        format!("  › {shortcut:>2}  ")
    } else {
        format!("    {shortcut:>2}  ")
    };
    let label_width = width.saturating_sub(measure_text_width(&prefix));
    wrap_text_lines(label, label_width)
        .into_iter()
        .enumerate()
        .map(|(line, label)| {
            let prefix = if line == 0 {
                prefix.clone()
            } else {
                " ".repeat(measure_text_width(&prefix))
            };
            if index == selected {
                format!(
                    "{}{}",
                    accent_text(prefix),
                    Style::new().white().bright().bold().apply_to(label)
                )
            } else {
                format!("{}{}", muted_text(prefix), normal_text(label))
            }
        })
        .collect()
}

fn menu_action_rows<T: AsRef<str>>(entries: &[T], zero_label: &str, width: usize) -> usize {
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| menu_row_contents(index, entry.as_ref(), usize::MAX, width).len())
        .sum::<usize>()
        + menu_row_contents(entries.len(), zero_label, usize::MAX, width).len()
}

fn redraw_menu_selection<T: AsRef<str>>(
    term: &Term,
    entries: &[T],
    zero_label: &str,
    previous: usize,
    selected: usize,
    width: usize,
) -> Result<()> {
    redraw_menu_row(term, entries, zero_label, previous, selected, width)?;
    redraw_menu_row(term, entries, zero_label, selected, selected, width)?;
    term.flush().context("failed to redraw menu selection")
}

fn redraw_menu_row<T: AsRef<str>>(
    term: &Term,
    entries: &[T],
    zero_label: &str,
    row: usize,
    selected: usize,
    width: usize,
) -> Result<()> {
    let label = entries.get(row).map(AsRef::as_ref).unwrap_or(zero_label);
    let inner_width = width.saturating_sub(4);
    let contents = menu_row_contents(row, label, selected, inner_width);
    let before = (0..row)
        .map(|index| {
            let label = entries.get(index).map(AsRef::as_ref).unwrap_or(zero_label);
            menu_row_contents(index, label, selected, inner_width).len()
        })
        .sum::<usize>();
    let total = menu_action_rows(entries, zero_label, inner_width);
    // The cursor rests one line below the help text. Between it and a menu
    // row are the remaining choices, lower border, blank line, and help line.
    let distance = total.saturating_sub(before) + 3;
    term.move_cursor_up(distance)
        .context("failed to move to the changed menu row")?;
    for (index, content) in contents.iter().enumerate() {
        term.clear_line()
            .context("failed to clear the changed menu row")?;
        term.write_str(&panel_row(content, width))
            .context("failed to write the changed menu row")?;
        if index + 1 < contents.len() {
            term.move_cursor_down(1)
                .context("failed to move through a wrapped menu row")?;
            term.write_str("\r")
                .context("failed to align a wrapped menu row")?;
        }
    }
    term.move_cursor_down(distance.saturating_sub(contents.len().saturating_sub(1)))
        .context("failed to restore the menu cursor")?;
    term.write_str("\r")
        .context("failed to restore the menu cursor column")?;
    Ok(())
}

fn parse_menu_selection(value: &str, entry_count: usize) -> Option<Option<usize>> {
    let value = value.trim().parse::<usize>().ok()?;
    if value == 0 {
        Some(None)
    } else if value <= entry_count {
        Some(Some(value - 1))
    } else {
        None
    }
}

pub(crate) fn pause() -> Result<()> {
    let term = Term::stdout();
    if term.is_term() {
        println!("\n{}", muted_text("Press Enter or Esc to return"));
        loop {
            match term.read_key().context("failed to read terminal key")? {
                Key::Enter | Key::Escape | Key::Char('q') | Key::Char('Q') => break,
                _ => {}
            }
        }
    } else {
        let _ = prompt("\nPress Enter to continue...");
    }
    Ok(())
}

pub(crate) fn view_data_page(
    title: &str,
    fixed_lines: &[String],
    data_lines: &[String],
) -> Result<()> {
    let term = Term::stdout();
    if !term.is_term() {
        let mut lines = fixed_lines.to_vec();
        lines.extend_from_slice(data_lines);
        render_plain_page_on(&term, title, &lines)?;
        let _ = prompt("\nPress Enter to continue...");
        return Ok(());
    }

    let _cursor = CursorGuard::new(term.clone())?;
    let content = if data_lines.is_empty() {
        vec![muted_text("No data to display.")]
    } else {
        data_lines.to_vec()
    };
    let mut offset = 0;
    let mut rendered_size = None;
    loop {
        let height = usize::from(term.size().0);
        let width = usize::from(term.size().1).max(1);
        let fixed_rows = fixed_lines
            .iter()
            .map(|line| text_visual_rows(line, width))
            .sum::<usize>();
        // Keep the final terminal row unused. Printing a newline from that
        // row scrolls the alternate screen and removes the top border.
        let capacity = data_page_capacity(height, fixed_rows);
        let total_data_rows = content
            .iter()
            .map(|line| text_visual_rows(line, width))
            .sum::<usize>();
        let scrollable = total_data_rows > capacity;
        if !scrollable {
            offset = 0;
        }
        let (visible, next_offset, used_rows) = text_page_window(&content, offset, capacity, width);
        let shown_end = next_offset.max(offset + 1).min(content.len());
        let footer = if scrollable {
            muted_text(format!(
                "Up/Down  PgUp/PgDn  Home/End  Enter/Esc    {}-{}/{}",
                offset + 1,
                shown_end,
                content.len()
            ))
        } else {
            muted_text("Press Enter or Esc to return")
        };

        if scrollable && rendered_size == Some((height, width)) {
            redraw_data_view(
                &term,
                5 + fixed_rows,
                capacity,
                &visible,
                width,
                height.saturating_sub(2),
                &footer,
                height,
            )?;
        } else {
            let mut displayed_data = visible.clone();
            if scrollable {
                displayed_data.extend(std::iter::repeat_n(
                    String::new(),
                    capacity.saturating_sub(used_rows),
                ));
            }
            let mut lines = fixed_lines.to_vec();
            lines.extend(displayed_data);
            render_plain_page_on(&term, title, &lines)?;
            if scrollable {
                let footer_row = height.saturating_sub(2);
                term.move_cursor_to(0, footer_row)
                    .context("failed to position the page footer")?;
                term.clear_line()
                    .context("failed to clear the page footer")?;
                term.write_str(&footer)
                    .context("failed to draw the page footer")?;
                term.move_cursor_to(0, height.saturating_sub(1))
                    .context("failed to park the page cursor")?;
                term.flush().context("failed to draw the data page")?;
            } else {
                println!("{footer}");
            }
        }
        rendered_size = Some((height, width));

        let key = loop {
            let key = term.read_key().context("failed to read page key")?;
            if scrollable
                || matches!(
                    key,
                    Key::Enter | Key::Escape | Key::Char('q') | Key::Char('Q') | Key::CtrlC
                )
            {
                break key;
            }
        };
        match key {
            Key::ArrowUp | Key::Char('k') if scrollable => {
                offset = offset.saturating_sub(1);
            }
            Key::ArrowDown | Key::Char('j') if scrollable => {
                offset = (offset + 1).min(content.len().saturating_sub(1));
            }
            Key::PageUp if scrollable => {
                offset = previous_text_page_start(&content, offset, capacity, width);
            }
            Key::PageDown if scrollable => {
                if next_offset < content.len() {
                    offset = next_offset;
                }
            }
            Key::Home if scrollable => offset = 0,
            Key::End if scrollable => {
                offset = previous_text_page_start(&content, content.len(), capacity, width);
            }
            Key::Enter | Key::Escape | Key::Char('q') | Key::Char('Q') | Key::CtrlC => {
                return Ok(());
            }
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn redraw_data_view(
    term: &Term,
    first_row: usize,
    capacity: usize,
    lines: &[String],
    width: usize,
    footer_row: usize,
    footer: &str,
    height: usize,
) -> Result<()> {
    for row in first_row..first_row.saturating_add(capacity) {
        term.move_cursor_to(0, row)
            .context("failed to position the data cursor")?;
        term.clear_line().context("failed to clear a data row")?;
    }

    let mut row = first_row;
    for line in lines {
        term.move_cursor_to(0, row)
            .context("failed to position the data cursor")?;
        term.write_str(line).context("failed to draw a data row")?;
        row = row.saturating_add(text_visual_rows(line, width));
    }

    term.move_cursor_to(0, footer_row)
        .context("failed to position the page footer")?;
    term.clear_line()
        .context("failed to clear the page footer")?;
    term.write_str(footer)
        .context("failed to draw the page footer")?;
    term.move_cursor_to(0, height.saturating_sub(1))
        .context("failed to park the page cursor")?;
    term.flush().context("failed to redraw the data page")
}

fn data_page_capacity(height: usize, fixed_rows: usize) -> usize {
    height.saturating_sub(8 + fixed_rows).max(1)
}

fn text_page_window(
    lines: &[String],
    start: usize,
    capacity: usize,
    width: usize,
) -> (Vec<String>, usize, usize) {
    let start = start.min(lines.len().saturating_sub(1));
    let mut visible = Vec::new();
    let mut used_rows = 0;
    let mut next = start;
    while let Some(line) = lines.get(next) {
        let rows = text_visual_rows(line, width);
        if !visible.is_empty() && used_rows + rows > capacity {
            break;
        }
        visible.push(line.clone());
        used_rows = used_rows.saturating_add(rows);
        next += 1;
        if used_rows >= capacity {
            break;
        }
    }
    (visible, next, used_rows.min(capacity))
}

fn previous_text_page_start(lines: &[String], end: usize, capacity: usize, width: usize) -> usize {
    let mut start = end.min(lines.len());
    let mut used_rows = 0;
    while start > 0 {
        let rows = text_visual_rows(&lines[start - 1], width);
        if used_rows > 0 && used_rows + rows > capacity {
            break;
        }
        start -= 1;
        used_rows = used_rows.saturating_add(rows);
        if used_rows >= capacity {
            break;
        }
    }
    start
}

fn text_visual_rows(line: &str, width: usize) -> usize {
    measure_text_width(line).max(1).div_ceil(width.max(1))
}

pub(crate) struct CursorGuard(Term);

impl CursorGuard {
    pub(crate) fn new(term: Term) -> Result<Self> {
        term.hide_cursor()
            .context("failed to hide terminal cursor")?;
        Ok(Self(term))
    }
}

impl Drop for CursorGuard {
    fn drop(&mut self) {
        let _ = self.0.show_cursor();
    }
}

pub(crate) fn render_page(title: &str, body: &[String]) -> Result<()> {
    render_page_on(&Term::stdout(), title, body)
}

fn render_page_on(term: &Term, title: &str, body: &[String]) -> Result<()> {
    let width = render_page_frame(term, title)?;
    if !body.is_empty() {
        println!("{}", panel_top(&accent_text("INFORMATION"), width));
        for line in body {
            for row in panel_rows(line, width) {
                println!("{row}");
            }
        }
        println!("{}", panel_bottom(width));
        println!();
    }
    Ok(())
}

fn render_plain_page_on(term: &Term, title: &str, lines: &[String]) -> Result<()> {
    render_page_frame(term, title)?;
    for line in lines {
        println!("{line}");
    }
    Ok(())
}

fn render_page_frame(term: &Term, title: &str) -> Result<usize> {
    if term.is_term() {
        term.write_str(CLEAR_APPLICATION_PAGE)
            .context("failed to clear terminal page")?;
        term.flush().context("failed to clear terminal page")?;
    } else {
        // Keeps numbered-input mode page-based on terminal-like hosts while
        // safely becoming a no-op for redirected output.
        term.clear_screen().context("failed to clear terminal")?;
    }
    let width = terminal_width(term);
    let inner_width = width - 2;
    let title_line = format!("  TGDL  /  {}", title.to_ascii_uppercase());
    let subtitle_line = "  Telegram multi-account media downloader";
    println!("{}", border_text(format!("╭{}╮", "─".repeat(width - 2))));
    println!(
        "{}{}{}",
        border_text("│"),
        accent_text(format!("{title_line:<inner_width$}")),
        border_text("│")
    );
    println!(
        "{}{}{}",
        border_text("│"),
        muted_text(format!("{subtitle_line:<inner_width$}")),
        border_text("│")
    );
    println!("{}", border_text(format!("╰{}╯", "─".repeat(width - 2))));
    println!();
    Ok(width)
}

pub(crate) fn accent_text(value: impl std::fmt::Display) -> String {
    Style::new()
        .cyan()
        .bright()
        .bold()
        .apply_to(value)
        .to_string()
}

fn border_text(value: impl std::fmt::Display) -> String {
    Style::new().blue().bright().apply_to(value).to_string()
}

pub(crate) fn terminal_width(term: &Term) -> usize {
    usize::from(term.size().1).clamp(60, 100)
}

pub(crate) fn panel_top(heading: &str, width: usize) -> String {
    let heading_width = measure_text_width(heading);
    let rule_width = width.saturating_sub(heading_width + 5);
    format!(
        "{} {} {}{}",
        muted_text("╭─"),
        heading,
        muted_text("─".repeat(rule_width)),
        muted_text("╮")
    )
}

pub(crate) fn panel_row(content: &str, width: usize) -> String {
    let inner_width = width.saturating_sub(4);
    format!(
        "{} {} {}",
        muted_text("│"),
        fixed_width_text(content, inner_width),
        muted_text("│")
    )
}

pub(crate) fn panel_rows(content: &str, width: usize) -> Vec<String> {
    let inner_width = width.saturating_sub(4);
    wrap_text_lines(content, inner_width)
        .into_iter()
        .map(|line| panel_row(&line, width))
        .collect()
}

pub(crate) fn print_wrapped(content: &str) {
    let width = terminal_width(&Term::stdout());
    for line in wrap_text_lines(content, width) {
        println!("{line}");
    }
}

pub(crate) fn wrap_text_lines(content: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![String::new()];
    }

    let mut remaining = content.replace(['\r', '\n'], " ");
    let mut lines = Vec::new();
    loop {
        if measure_text_width(&remaining) <= width {
            lines.push(remaining);
            break;
        }

        let plain = strip_ansi_codes(&remaining);
        let split = wrap_split_chars(&plain, width);
        let (line, rest) = split_ansi_at_chars(&remaining, split);
        lines.push(line);
        remaining = rest;
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

pub(crate) fn wrap_hanging_text_lines(prefix: &str, content: &str, width: usize) -> Vec<String> {
    let prefix_width = measure_text_width(prefix);
    let content_width = width.saturating_sub(prefix_width).max(1);
    wrap_text_lines(content, content_width)
        .into_iter()
        .enumerate()
        .map(|(index, line)| {
            if index == 0 {
                format!("{prefix}{line}")
            } else {
                format!("{}{line}", " ".repeat(prefix_width))
            }
        })
        .collect()
}

fn wrap_split_chars(content: &str, width: usize) -> usize {
    let mut used = 0;
    let mut fitted = 0;
    let mut last_space = None;
    for (index, character) in content.chars().enumerate() {
        let mut buffer = [0; 4];
        let character_width = measure_text_width(character.encode_utf8(&mut buffer));
        if used + character_width > width {
            break;
        }
        used += character_width;
        fitted = index + 1;
        if character.is_whitespace() && used >= width / 2 {
            last_space = Some(fitted);
        }
    }
    last_space.unwrap_or_else(|| fitted.max(1))
}

fn split_ansi_at_chars(content: &str, split: usize) -> (String, String) {
    let mut left = String::new();
    let mut right = String::new();
    let mut visible = 0;
    let mut active_style = String::new();
    let mut on_right = false;

    for (segment, is_ansi) in AnsiCodeIterator::new(content) {
        if is_ansi {
            if visible >= split && !on_right {
                start_wrapped_line(&mut left, &mut right, &active_style);
                on_right = true;
            }
            if on_right {
                right.push_str(segment);
            } else {
                left.push_str(segment);
            }
            update_active_style(&mut active_style, segment);
            continue;
        }

        for character in segment.chars() {
            if visible >= split && !on_right {
                start_wrapped_line(&mut left, &mut right, &active_style);
                on_right = true;
            }
            if on_right {
                right.push(character);
            } else {
                left.push(character);
            }
            visible += 1;
        }
    }
    if !on_right {
        start_wrapped_line(&mut left, &mut right, &active_style);
    }
    (left, right)
}

fn start_wrapped_line(left: &mut String, right: &mut String, active_style: &str) {
    if !active_style.is_empty() {
        left.push_str("\x1b[0m");
        right.push_str(active_style);
    }
}

fn update_active_style(active: &mut String, sequence: &str) {
    if !sequence.ends_with('m') {
        return;
    }
    if sequence == "\x1b[0m" || sequence == "\x1b[m" {
        active.clear();
    } else {
        active.push_str(sequence);
    }
}

pub(crate) fn fixed_width_text(content: &str, width: usize) -> String {
    let content = truncate_str(content, width, "…");
    let used = measure_text_width(&content);
    format!("{}{}", content, " ".repeat(width.saturating_sub(used)))
}

pub(crate) fn panel_bottom(width: usize) -> String {
    muted_text(format!("╰{}╯", "─".repeat(width.saturating_sub(2))))
}

pub(crate) fn normal_text(value: impl std::fmt::Display) -> String {
    Style::new().white().bright().apply_to(value).to_string()
}

pub(crate) fn muted_text(value: impl std::fmt::Display) -> String {
    Style::new().white().dim().apply_to(value).to_string()
}

pub(crate) fn success_text(value: impl std::fmt::Display) -> String {
    Style::new()
        .green()
        .bright()
        .bold()
        .apply_to(value)
        .to_string()
}

pub(crate) fn warning_text(value: impl std::fmt::Display) -> String {
    Style::new()
        .yellow()
        .bright()
        .bold()
        .apply_to(value)
        .to_string()
}

pub(crate) fn error_text(value: impl std::fmt::Display) -> String {
    Style::new()
        .red()
        .bright()
        .bold()
        .apply_to(value)
        .to_string()
}

pub(crate) fn info_text(value: impl std::fmt::Display) -> String {
    accent_text(value)
}

pub(crate) fn property_rows(
    label: &str,
    value: impl std::fmt::Display,
    width: usize,
) -> Vec<String> {
    let label = format!("{label:<20}");
    let separator = " │ ";
    let prefix_width = measure_text_width(&label) + measure_text_width(separator);
    let content_width = width.saturating_sub(4 + prefix_width).max(1);
    wrap_text_lines(&value.to_string(), content_width)
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let content = if index == 0 {
                format!(
                    "{}{}{}",
                    accent_text(&label),
                    muted_text(separator),
                    normal_text(value)
                )
            } else {
                format!("{}{}", " ".repeat(prefix_width), normal_text(value))
            };
            panel_row(&content, width)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_menu_numbers_and_rejects_out_of_range_values() {
        assert_eq!(parse_menu_selection("0", 3), Some(None));
        assert_eq!(parse_menu_selection("1", 3), Some(Some(0)));
        assert_eq!(parse_menu_selection("3", 3), Some(Some(2)));
        assert_eq!(parse_menu_selection("4", 3), None);
        assert_eq!(parse_menu_selection("not-a-number", 3), None);
    }

    #[test]
    fn remembers_parent_position_until_that_menu_is_left() {
        let title = "test menu selection memory";
        remember_menu_selection(title, 2);
        assert_eq!(remembered_menu_selection(title, 5), 2);
        assert_eq!(remembered_menu_selection(title, 2), 1);
        forget_menu_selection(title);
        assert_eq!(remembered_menu_selection(title, 5), 0);
        assert_eq!(remembered_menu_selection("unvisited test menu", 4), 0);
    }

    #[test]
    fn text_pages_count_wrapped_terminal_rows() {
        let lines = vec!["1234567890".to_owned(), "next".to_owned()];
        let (visible, next, used) = text_page_window(&lines, 0, 2, 5);

        assert_eq!(visible, vec!["1234567890".to_owned()]);
        assert_eq!(next, 1);
        assert_eq!(used, 2);
    }

    #[test]
    fn text_pages_can_find_the_previous_full_view() {
        let lines = (1..=8)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>();

        assert_eq!(previous_text_page_start(&lines, lines.len(), 3, 80), 5);
        assert_eq!(previous_text_page_start(&lines, 5, 3, 80), 2);
    }

    #[test]
    fn visual_row_count_ignores_ansi_colors() {
        assert_eq!(text_visual_rows("\x1b[31mcolored\x1b[0m", 20), 1);
    }

    #[test]
    fn panel_rows_wrap_all_long_colored_content() {
        let rows = panel_rows(
            &accent_text(
                "a deliberately long colored value that cannot fit on only one or two rows",
            ),
            36,
        );
        assert!(rows.len() > 2);
        assert!(rows.iter().all(|row| measure_text_width(row) == 36));
    }

    #[test]
    fn text_wrapping_preserves_color_across_every_line() {
        let content =
            "\x1b[31mone two three four five six seven eight nine ten eleven twelve\x1b[0m";
        let lines = wrap_text_lines(content, 18);

        assert!(lines.len() > 2);
        assert!(lines[1].starts_with("\x1b[31m"));
        assert!(!lines.iter().any(|line| line.contains('…')));
        assert!(lines.iter().all(|line| measure_text_width(line) <= 18));
    }

    #[test]
    fn text_wrapping_preserves_all_content() {
        let content = "one two three four five";
        let lines = wrap_text_lines(content, 14);

        assert_eq!(lines.len(), 2);
        assert_eq!(lines.concat(), content);
    }

    #[test]
    fn hanging_text_wraps_under_the_content_column() {
        let prefix = "#38    failed      -                   657.0 MiB  ";
        let lines = wrap_hanging_text_lines(
            prefix,
            "archive.rar — request error: rpc error -503: Timeout caused by upload.getFile",
            96,
        );

        assert!(lines.len() > 1);
        assert!(lines[0].starts_with(prefix));
        assert!(
            lines
                .iter()
                .skip(1)
                .all(|line| line.starts_with(&" ".repeat(prefix.len())))
        );
        assert!(!lines.iter().any(|line| line.contains('…')));
    }

    #[test]
    fn data_pages_leave_the_last_terminal_row_unused() {
        let height = 24;
        let fixed_rows = 11;
        let capacity = data_page_capacity(height, fixed_rows);

        assert_eq!(capacity, 5);
        assert_eq!(fixed_rows + capacity + 7, height - 1);
    }
}
