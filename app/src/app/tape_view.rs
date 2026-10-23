//! Time and sales (`TAS`): prints newest first, coloured by the side that crossed the spread,
//! with a trade-value filter, large-print highlighting and running buy/sell volume.

use std::time::Duration;

use egui::{Align2, Color32, RichText, ScrollArea, Sense, Ui, pos2, vec2};
use tr_data::{Aggressor, Print};

use super::TapePane;
use crate::congress_view::Action;
use crate::theme::{self, amber, dp, mono, muted, text, ui_font};
use crate::widgets;

/// Trade-value filters: label and the smallest notional shown.
const FILTERS: [(&str, f64); 4] = [("All", 0.0), ("$10K+", 1e4), ("$100K+", 1e5), ("$1M+", 1e6)];
/// Prints worth at least this much are tinted; blocks more strongly.
const LARGE: f64 = 1e5;
const BLOCK: f64 = 1e6;

pub(super) fn tape(ui: &mut Ui, t: &mut TapePane) -> Option<Action> {
    let mut action = None;
    let key = tr_data::security(&t.symbol);
    let live = t.status.is_none() && t.last_print.is_some_and(|x| x.elapsed() < Duration::from_secs(10));
    ui.horizontal_wrapped(|ui| {
        let r = ui.label(RichText::new(&key.ticker).font(mono(13.0)).color(amber())).interact(Sense::click());
        if r.clicked() {
            action = Some(Action::OpenChart(t.symbol.clone()));
        }
        r.on_hover_text("Open chart");
        if let Some((source, simulated)) = &t.source {
            ui.label(RichText::new(source).font(ui_font(11.5)).color(muted()));
            if *simulated {
                ui.label(
                    RichText::new(" SIMULATED ").font(ui_font(10.5)).color(Color32::BLACK).background_color(amber()),
                )
                .on_hover_text("Offline sample mode: prints are synthetic.");
            } else if live {
                ui.label(RichText::new("● LIVE").font(ui_font(10.5)).color(theme::up()));
            }
        }
    });
    ui.horizontal_wrapped(|ui| {
        let labels: Vec<&str> = FILTERS.iter().map(|f| f.0).collect();
        let sel = FILTERS.iter().position(|f| (f.1 - t.min_notional).abs() < 1e-9);
        if let Some(i) = widgets::segmented(ui, ("tape-filter", &t.symbol), &labels, sel, 20.0) {
            t.min_notional = FILTERS[i].1;
        }
        if t.count == 0 {
            return;
        }
        let delta = t.buy_volume - t.sell_volume;
        let sign = if delta >= 0.0 { "+" } else { "−" };
        let tone = if delta >= 0.0 { theme::up() } else { theme::down() };
        ui.label(RichText::new(format!("Δ {sign}{}", compact(delta.abs()))).font(mono(11.5)).color(tone))
            .on_hover_text("Buy minus sell volume since this pane opened");
        let classified = t.buy_volume + t.sell_volume;
        if classified > 0.0 {
            ui.label(
                RichText::new(format!("Buy {:.0}%", t.buy_volume / classified * 100.0)).font(mono(11.5)).color(muted()),
            )
            .on_hover_text("Share of volume where the buyer crossed the spread");
        }
        ui.label(RichText::new(format!("Vol {}", compact(t.volume))).font(mono(11.5)).color(muted()));
    });
    if let Some(s) = &t.status {
        let tone = if t.prints.is_empty() { muted() } else { amber() };
        ui.label(RichText::new(s).font(ui_font(11.5)).color(tone));
    }
    if t.prints.is_empty() {
        if t.status.is_none() {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new());
                ui.label(RichText::new("Waiting for prints…").font(ui_font(12.0)).color(muted()));
            });
        }
        return action;
    }

    let rows: Vec<usize> =
        t.prints.iter().enumerate().filter(|(_, p)| p.notional() >= t.min_notional).map(|(i, _)| i).collect();
    let w = ui.available_width();
    let row = dp(18.0);
    // Only sources that report trade conditions (US stocks) get a condition column.
    let flags = rows.iter().take(500).any(|&i| t.prints[i].flag.is_some());
    let cols = Columns::new(w, flags);
    let (hdr, _) = ui.allocate_exact_size(vec2(w, row), Sense::hover());
    let p = ui.painter();
    let y = hdr.center().y;
    p.text(pos2(hdr.left() + cols.time, y), Align2::LEFT_CENTER, "Time", ui_font(11.0), muted());
    p.text(pos2(hdr.left() + cols.price, y), Align2::RIGHT_CENTER, "Price", ui_font(11.0), muted());
    p.text(pos2(hdr.left() + cols.size, y), Align2::RIGHT_CENTER, "Size", ui_font(11.0), muted());
    if let Some(x) = cols.flag {
        p.text(pos2(hdr.left() + x, y), Align2::RIGHT_CENTER, "Cond", ui_font(11.0), muted());
    }
    if rows.is_empty() {
        ui.label(RichText::new("No prints at this size yet.").font(ui_font(12.0)).color(muted()));
        return action;
    }
    ui.spacing_mut().item_spacing.y = 0.0;
    ScrollArea::vertical().auto_shrink([false, false]).show_rows(ui, row, rows.len(), |ui, range| {
        for &i in &rows[range] {
            let pr = &t.prints[i];
            let (r, _) = ui.allocate_exact_size(vec2(w, row), Sense::hover());
            let p = ui.painter();
            let tone = match pr.side {
                Aggressor::Buy => theme::up(),
                Aggressor::Sell => theme::down(),
                Aggressor::Unknown => muted(),
            };
            let value = pr.notional();
            if value >= LARGE {
                let wash = if pr.side == Aggressor::Unknown { text() } else { tone };
                p.rect_filled(
                    r.shrink2(vec2(0.0, 0.5)),
                    2.0,
                    wash.gamma_multiply(if value >= BLOCK { 0.26 } else { 0.12 }),
                );
            }
            let font = mono(12.0);
            let y = r.center().y;
            p.text(pos2(r.left() + cols.time, y), Align2::LEFT_CENTER, time(pr, cols.millis), font.clone(), muted());
            let d = price_decimals(pr.price, key.decimals(pr.price));
            p.text(
                pos2(r.left() + cols.price, y),
                Align2::RIGHT_CENTER,
                format!("{:.*}", d, pr.price),
                font.clone(),
                tone,
            );
            p.text(pos2(r.left() + cols.size, y), Align2::RIGHT_CENTER, qty(pr.size), font.clone(), tone);
            if let (Some(x), Some(flag)) = (cols.flag, &pr.flag) {
                p.text(pos2(r.left() + x, y), Align2::RIGHT_CENTER, flag, ui_font(10.5), muted());
            }
        }
    });
    action
}

/// Column anchors (offsets from the left edge) for a pane `w` wide: time is left-aligned,
/// the rest right-aligned and packed to the left like a terminal tape. Narrow panes squeeze
/// the columns, then drop the condition column and milliseconds.
struct Columns {
    time: f32,
    price: f32,
    size: f32,
    flag: Option<f32>,
    millis: bool,
}

impl Columns {
    fn new(w: f32, flags: bool) -> Self {
        let millis = w >= dp(250.0);
        let time_w = if millis { dp(92.0) } else { dp(64.0) };
        let right = w - dp(6.0);
        let mut price = time_w + dp(100.0);
        let mut size = price + dp(100.0);
        let flag = (flags && size + dp(60.0) <= right).then_some(size + dp(60.0));
        if size > right {
            size = right;
            price = (time_w + (size - time_w) * 0.55).min(size - dp(56.0)).max(time_w + dp(40.0));
        }
        Self { time: dp(4.0), price, size, flag, millis }
    }
}

fn time(p: &Print, millis: bool) -> String {
    let fmt = if millis { "%H:%M:%S%.3f" } else { "%H:%M:%S" };
    chrono::DateTime::from_timestamp_millis(p.ts_ms)
        .map(|d| d.with_timezone(&chrono::Local).format(fmt).to_string())
        .unwrap_or_default()
}

/// Extra decimals for sub-penny prints (midpoint crosses), up to four more.
fn price_decimals(price: f64, base: usize) -> usize {
    (base..=base + 4)
        .find(|&d| {
            let scaled = price * 10f64.powi(d as i32);
            (scaled - scaled.round()).abs() < 1e-6 * scaled.abs().max(1.0)
        })
        .unwrap_or(base + 4)
        .min(10)
}

/// A print's size: whole shares with separators, fractional (crypto) sizes to five places.
fn qty(q: f64) -> String {
    if q >= 1e6 {
        format!("{:.2}M", q / 1e6)
    } else if q.fract() == 0.0 {
        let digits = format!("{}", q as u64);
        let mut out = String::with_capacity(digits.len() + digits.len() / 3);
        for (i, c) in digits.chars().enumerate() {
            if i > 0 && (digits.len() - i) % 3 == 0 {
                out.push(',');
            }
            out.push(c);
        }
        out
    } else if q >= 100.0 {
        format!("{q:.2}")
    } else if q >= 1.0 {
        format!("{q:.4}")
    } else {
        format!("{q:.5}")
    }
}

/// Running totals: 1.25M, 12.4K, or small fractional amounts.
fn compact(q: f64) -> String {
    if q >= 1e9 {
        format!("{:.2}B", q / 1e9)
    } else if q >= 1e6 {
        format!("{:.2}M", q / 1e6)
    } else if q >= 1e4 {
        format!("{:.1}K", q / 1e3)
    } else if q >= 100.0 || q == 0.0 || q.fract() == 0.0 {
        format!("{q:.0}")
    } else if q >= 1.0 {
        format!("{q:.2}")
    } else {
        format!("{q:.4}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_and_prices_read_like_a_tape() {
        assert_eq!(qty(5_000.0), "5,000");
        assert_eq!(qty(123_456.0), "123,456");
        assert_eq!(qty(100.0), "100");
        assert_eq!(qty(2_500_000.0), "2.50M");
        assert_eq!(qty(0.00089), "0.00089");
        assert_eq!(qty(1.5), "1.5000");
        assert_eq!(compact(12_400.0), "12.4K");
        assert_eq!(compact(0.0), "0");
        assert_eq!(compact(0.0312), "0.0312");
        // Sub-penny midpoint prints keep their extra digits; round prices don't grow any.
        assert_eq!(price_decimals(182.395, 2), 3);
        assert_eq!(price_decimals(182.40, 2), 2);
        assert_eq!(price_decimals(182.4025, 2), 4);
        assert_eq!(price_decimals(81_560.0, 2), 2);
    }

    #[test]
    fn tape_keeps_the_newest_and_totals_by_side() {
        let mut t = TapePane::new("AAPL US Equity", 0.0);
        let print = |i: i64, side| Print { ts_ms: i, price: 10.0, size: 100.0, side, flag: None };
        t.push((0..super::super::TAPE_CAP as i64 + 10).map(|i| print(i, Aggressor::Buy)).collect());
        t.push(vec![print(-1, Aggressor::Sell), print(-2, Aggressor::Unknown)]);
        assert_eq!(t.prints.len(), super::super::TAPE_CAP);
        assert_eq!(t.prints[0].ts_ms, -2, "newest first");
        assert_eq!(t.count, super::super::TAPE_CAP as u64 + 12);
        assert_eq!(t.sell_volume, 100.0);
        assert_eq!(t.volume - t.buy_volume - t.sell_volume, 100.0, "unclassified volume counts in the total only");
    }

    #[test]
    fn columns_fit_narrow_and_wide_panes() {
        for w in [180.0, 260.0, 400.0, 900.0] {
            let c = Columns::new(w, true);
            assert!(c.time < c.price && c.price < c.size && c.size <= w, "{w}");
            assert!(c.flag.is_none_or(|f| f > c.size && f <= w), "{w}");
            assert_eq!(c.flag.is_some(), w >= dp(380.0), "{w}");
            assert!(Columns::new(w, false).flag.is_none());
        }
    }
}
