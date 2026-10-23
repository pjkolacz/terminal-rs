//! US stock time and sales and order book depth from moomoo OpenAPI.
//!
//! moomoo's API is served by **OpenD**, a gateway the user runs and logs into with their own
//! moomoo account (`127.0.0.1:11111` by default; `MOOMOO_OPEND=host:port` overrides it,
//! `MOOMOO_OPEND=off` disables the source). Each streaming pane gets its own TCP connection:
//! `InitConnect`, subscribe to the ticker or order book with pushes registered, fetch a
//! snapshot so the pane fills at once, then forward pushes and keep the connection alive.
//! Closing the pane drops the connection, which releases the subscription. When OpenD is
//! not running the feed reports it and keeps retrying.
//!
//! Wire format: a 44-byte little-endian header (`"FT"`, protocol id, format, version, serial
//! number, body length, SHA-1 of the body, 8 reserved bytes) followed by a protobuf body.

pub mod proto;

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use prost::Message;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tr_core::{AssetClass, Security};

use crate::{Aggressor, DepthEvent, DepthFeed, OrderBook, Print, TapeEvent, TapeFeed};
use proto::Reply;

pub const DEFAULT_ADDR: &str = "127.0.0.1:11111";
pub const HEADER_LEN: usize = 44;
/// Largest body accepted from OpenD; anything bigger means the stream is out of sync.
const MAX_BODY: usize = 32 << 20;
/// Client version the official SDK reports.
const CLIENT_VER: i32 = 300;
/// Prints fetched when a tape opens.
const BACKFILL: i32 = 500;
const SOURCE: &str = "moomoo";

/// Where to reach OpenD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Moomoo {
    pub addr: String,
}

impl Moomoo {
    /// `MOOMOO_OPEND` (`host:port`, or `off`), defaulting to the local gateway.
    pub fn from_env() -> Option<Self> {
        let v = std::env::var("MOOMOO_OPEND").ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
        match v.as_deref() {
            Some(off) if ["off", "0", "none", "false"].iter().any(|o| off.eq_ignore_ascii_case(o)) => None,
            Some(addr) => Some(Self { addr: addr.to_string() }),
            None => Some(Self { addr: DEFAULT_ADDR.into() }),
        }
    }

    /// Stream time and sales for a US stock or ETF.
    pub fn tape(&self, symbol: &str, sec: &Security) -> Option<TapeFeed> {
        let target = security(sec)?;
        let (tx, rx) = mpsc::channel(256);
        let addr = self.addr.clone();
        let symbol = symbol.to_string();
        tokio::spawn(async move {
            run(&addr, &tx, |conn| tape_session(conn, &target, &tx), TapeEvent::Status).await;
            tracing::debug!("moomoo tape {symbol} stopped");
        });
        Some(rx)
    }

    /// Stream the top `depth` levels of a US stock or ETF's book.
    pub fn depth(&self, symbol: &str, sec: &Security, depth: usize) -> Option<DepthFeed> {
        let target = security(sec)?;
        let (tx, rx) = mpsc::channel(64);
        let addr = self.addr.clone();
        let symbol = symbol.to_string();
        tokio::spawn(async move {
            let key = symbol.clone();
            run(&addr, &tx, |conn| depth_session(conn, &target, &key, depth, &tx), DepthEvent::Status).await;
            tracing::debug!("moomoo depth {symbol} stopped");
        });
        Some(rx)
    }
}

/// moomoo's security for a market key: US stocks and ETFs only.
pub fn security(sec: &Security) -> Option<proto::Security> {
    matches!(sec.class, AssetClass::Equity | AssetClass::Etf)
        .then(|| proto::Security { market: proto::MARKET_US, code: sec.ticker.replace('-', ".") })
}

/// Connect, run `session` and reconnect with backoff until the receiver goes away.
async fn run<T, F, Fut>(addr: &str, tx: &mpsc::Sender<T>, mut session: F, status: fn(String) -> T)
where
    F: FnMut(Conn) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut backoff = 2;
    while !tx.is_closed() {
        if tx.send(status(format!("Connecting to moomoo OpenD at {addr}…"))).await.is_err() {
            return;
        }
        let res = match Conn::connect(addr).await {
            Ok(conn) => {
                backoff = 2;
                session(conn).await
            }
            Err(e) => Err(e),
        };
        if tx.is_closed() {
            return;
        }
        let msg = match res {
            Ok(()) => return,
            Err(e) => describe(addr, &e),
        };
        tracing::debug!("moomoo {addr}: {msg}");
        if tx.send(status(format!("{msg} Retrying in {backoff}s."))).await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

/// A user-facing explanation of a failed session.
fn describe(addr: &str, e: &anyhow::Error) -> String {
    let refused = e.chain().any(|c| {
        c.downcast_ref::<std::io::Error>().is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused)
    });
    if refused {
        format!("moomoo OpenD isn't running at {addr}. Start OpenD and log in.")
    } else {
        format!("moomoo: {e:#}.")
    }
}

async fn tape_session(mut conn: Conn, target: &proto::Security, tx: &mpsc::Sender<TapeEvent>) -> Result<()> {
    conn.subscribe(target, proto::SUB_TICKER).await?;
    let mut seen = Dedup::new(4096);
    let snap: proto::TickerResponse = conn
        .call(
            proto::QOT_GET_TICKER,
            &proto::GetTickerRequest { c2s: proto::GetTickerC2s { security: target.clone(), max_ret_num: BACKFILL } },
        )
        .await?;
    let snap = check(snap, "ticker snapshot")?;
    if !send_prints(tx, &mut seen, &snap.ticker_list).await {
        return Ok(());
    }
    let mut tick = conn.keep_alive_timer().await;
    while let Some(frame) = conn.next(&mut tick, tx).await? {
        if frame.proto_id != proto::QOT_UPDATE_TICKER {
            continue;
        }
        match decode::<proto::TickerResponse>(&frame.body).and_then(|r| check(r, "ticker push")) {
            Ok(s2c) if s2c.security.code == target.code => {
                if !send_prints(tx, &mut seen, &s2c.ticker_list).await {
                    break;
                }
            }
            Ok(_) => {}
            Err(e) => tracing::debug!("moomoo ticker push: {e:#}"),
        }
    }
    Ok(())
}

async fn depth_session(
    mut conn: Conn,
    target: &proto::Security,
    symbol: &str,
    depth: usize,
    tx: &mpsc::Sender<DepthEvent>,
) -> Result<()> {
    conn.subscribe(target, proto::SUB_ORDER_BOOK).await?;
    let req = proto::GetOrderBookRequest {
        c2s: proto::GetOrderBookC2s { security: target.clone(), num: depth.clamp(1, 60) as i32 },
    };
    // Pushes carry the full book, so a refused snapshot (e.g. more levels than the account's
    // quote right allows) is not fatal; the first push fills the pane instead.
    match conn
        .call::<proto::OrderBookResponse>(proto::QOT_GET_ORDER_BOOK, &req)
        .await
        .and_then(|r| check(r, "order book"))
    {
        Ok(s2c) => {
            if tx.send(DepthEvent::Book(book(symbol, &s2c, depth))).await.is_err() {
                return Ok(());
            }
        }
        Err(e) => tracing::debug!("moomoo order book snapshot: {e:#}"),
    }
    let mut tick = conn.keep_alive_timer().await;
    while let Some(frame) = conn.next(&mut tick, tx).await? {
        if frame.proto_id != proto::QOT_UPDATE_ORDER_BOOK {
            continue;
        }
        match decode::<proto::OrderBookResponse>(&frame.body).and_then(|r| check(r, "order book push")) {
            Ok(s2c) if s2c.security.code == target.code => {
                if tx.send(DepthEvent::Book(book(symbol, &s2c, depth))).await.is_err() {
                    break;
                }
            }
            Ok(_) => {}
            Err(e) => tracing::debug!("moomoo order book push: {e:#}"),
        }
    }
    Ok(())
}

/// Forward unseen tickers as prints. False once the receiver is gone.
async fn send_prints(tx: &mpsc::Sender<TapeEvent>, seen: &mut Dedup, tickers: &[proto::Ticker]) -> bool {
    let prints: Vec<Print> = tickers.iter().filter(|t| seen.insert(t.sequence)).map(print).collect();
    if prints.is_empty() {
        return true;
    }
    tx.send(TapeEvent::Prints { prints, source: SOURCE.into(), simulated: false }).await.is_ok()
}

/// A moomoo ticker as a print.
pub fn print(t: &proto::Ticker) -> Print {
    let ts_ms = match t.timestamp {
        Some(s) if s > 0.0 => (s * 1000.0).round() as i64,
        _ => chrono::Utc::now().timestamp_millis(),
    };
    Print {
        ts_ms,
        price: t.price,
        size: t.hp_volume.filter(|v| *v > 0.0).unwrap_or(t.volume as f64),
        side: match t.dir {
            proto::DIR_BID => Aggressor::Buy,
            proto::DIR_ASK => Aggressor::Sell,
            _ => Aggressor::Unknown,
        },
        flag: t.kind.and_then(ticker_flag).map(str::to_string),
    }
}

/// Short labels for the `Qot_Common.TickerType` values that matter on a US tape.
fn ticker_flag(kind: i32) -> Option<&'static str> {
    Some(match kind {
        2 | 14 | 30 => "LATE",
        6 | 26 => "ODD",
        7 => "AUCT",
        8 | 11 => "BULK",
        10 => "XMKT",
        13 => "127",
        16 => "NXDY",
        17 | 19 => "OPEN",
        15 | 29 => "CLOSE",
        21 | 22 => "EXT",
        24 => "AVG",
        25 | 31 => "OTC",
        27 => "DRV",
        28 => "REOP",
        _ => return None,
    })
}

/// A moomoo book as the terminal's order book, trimmed to `depth` levels a side.
pub fn book(symbol: &str, s2c: &proto::OrderBookS2c, depth: usize) -> OrderBook {
    let levels = |l: &[proto::Level]| -> Vec<crate::Level> {
        l.iter()
            .filter(|x| x.price > 0.0)
            .take(depth)
            .map(|x| (x.price, x.hp_volume.filter(|v| *v > 0.0).unwrap_or(x.volume as f64)))
            .collect()
    };
    OrderBook {
        symbol: symbol.to_string(),
        bids: levels(&s2c.order_book_bid_list),
        asks: levels(&s2c.order_book_ask_list),
        source: SOURCE.into(),
        simulated: false,
    }
}

/// Remembers the last `cap` sequence numbers, so snapshot/push overlaps print once.
struct Dedup {
    set: HashSet<i64>,
    order: VecDeque<i64>,
    cap: usize,
}

impl Dedup {
    fn new(cap: usize) -> Self {
        Self { set: HashSet::with_capacity(cap), order: VecDeque::with_capacity(cap), cap }
    }

    /// True if `seq` is new.
    fn insert(&mut self, seq: i64) -> bool {
        if !self.set.insert(seq) {
            return false;
        }
        self.order.push_back(seq);
        if self.order.len() > self.cap
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        true
    }
}

fn decode<M: Message + Default>(body: &[u8]) -> Result<M> {
    M::decode(body).context("decoding moomoo message")
}

/// The reply's body, or its `retMsg` as an error.
fn check<R: Reply>(r: R, what: &str) -> Result<R::S2c> {
    if r.ret_type() != proto::RET_OK {
        bail!("{what} refused: {} (retType {})", r.ret_msg().unwrap_or("no message"), r.ret_type());
    }
    r.into_s2c().ok_or_else(|| anyhow!("{what}: empty reply"))
}

/// One framed message.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub proto_id: u32,
    pub serial: u32,
    pub body: Vec<u8>,
}

/// Frame `body` for protocol `proto_id`.
pub fn encode_frame(proto_id: u32, serial: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(b"FT");
    out.extend_from_slice(&proto_id.to_le_bytes());
    out.push(proto::FMT_PROTOBUF as u8);
    out.push(0); // protocol version
    out.extend_from_slice(&serial.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&Sha1::digest(body));
    out.extend_from_slice(&[0; 8]);
    out.extend_from_slice(body);
    out
}

/// Parse a header into (protocol id, serial number, body length, body SHA-1).
pub fn decode_header(h: &[u8; HEADER_LEN]) -> Result<(u32, u32, usize, [u8; 20])> {
    if &h[..2] != b"FT" {
        bail!("not a moomoo OpenD frame");
    }
    let u32_at = |i: usize| u32::from_le_bytes([h[i], h[i + 1], h[i + 2], h[i + 3]]);
    if h[6] != proto::FMT_PROTOBUF as u8 {
        bail!("OpenD sent format {} (expected protobuf)", h[6]);
    }
    let len = u32_at(12) as usize;
    if len > MAX_BODY {
        bail!("OpenD frame of {len} bytes");
    }
    let mut sha = [0u8; 20];
    sha.copy_from_slice(&h[16..36]);
    Ok((u32_at(2), u32_at(8), len, sha))
}

async fn read_frame(r: &mut OwnedReadHalf) -> Result<Frame> {
    let mut head = [0u8; HEADER_LEN];
    r.read_exact(&mut head).await.context("reading from OpenD")?;
    let (proto_id, serial, len, sha) = decode_header(&head)?;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await.context("reading from OpenD")?;
    if Sha1::digest(&body).as_slice() != sha {
        bail!("OpenD frame failed its checksum (is OpenD set to encrypt API connections?)");
    }
    Ok(Frame { proto_id, serial, body })
}

/// One logged-in OpenD connection. A reader task owns the socket's read half and hands
/// frames over a channel, so waiting for a frame can be cancelled without losing bytes.
pub struct Conn {
    writer: OwnedWriteHalf,
    frames: mpsc::Receiver<Result<Frame>>,
    reader: tokio::task::JoinHandle<()>,
    /// Frames that arrived while waiting for a reply.
    pending: VecDeque<Frame>,
    serial: u32,
    keep_alive: Duration,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

static CLIENT_SEQ: AtomicU32 = AtomicU32::new(0);

impl Conn {
    /// Open a TCP connection and run `InitConnect`.
    pub async fn connect(addr: &str) -> Result<Conn> {
        let stream = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow!("timed out connecting to OpenD at {addr}"))?
            .with_context(|| format!("connecting to OpenD at {addr}"))?;
        let _ = stream.set_nodelay(true);
        let (mut read, writer) = stream.into_split();
        let (ftx, frames) = mpsc::channel(1024);
        let reader = tokio::spawn(async move {
            loop {
                let f = read_frame(&mut read).await;
                let stop = f.is_err();
                if ftx.send(f).await.is_err() || stop {
                    return;
                }
            }
        });
        let mut conn =
            Conn { writer, frames, reader, pending: VecDeque::new(), serial: 0, keep_alive: Duration::from_secs(10) };
        let id = format!("terminal-rs-{}-{}", std::process::id(), CLIENT_SEQ.fetch_add(1, Ordering::Relaxed));
        let init = proto::InitConnectRequest {
            c2s: proto::InitConnectC2s {
                client_ver: CLIENT_VER,
                client_id: id,
                recv_notify: Some(false),
                packet_enc_algo: Some(proto::ENC_NONE),
                push_proto_fmt: Some(proto::FMT_PROTOBUF),
                programming_language: Some("Rust".into()),
            },
        };
        let reply: proto::InitConnectResponse =
            tokio::time::timeout(Duration::from_secs(10), conn.call(proto::INIT_CONNECT, &init))
                .await
                .map_err(|_| anyhow!("OpenD at {addr} did not answer InitConnect"))??;
        let s2c = check(reply, "InitConnect")?;
        conn.keep_alive = Duration::from_secs(s2c.keep_alive_interval.clamp(3, 60) as u64);
        Ok(conn)
    }

    async fn send(&mut self, proto_id: u32, msg: &impl Message) -> Result<u32> {
        self.serial = self.serial.wrapping_add(1);
        let frame = encode_frame(proto_id, self.serial, &msg.encode_to_vec());
        self.writer.write_all(&frame).await.context("writing to OpenD")?;
        Ok(self.serial)
    }

    /// Send a request and wait for its reply; pushes that arrive meanwhile are kept.
    pub async fn call<R: Message + Default>(&mut self, proto_id: u32, req: &impl Message) -> Result<R> {
        let serial = self.send(proto_id, req).await?;
        loop {
            let f = self.frames.recv().await.ok_or_else(|| anyhow!("OpenD closed the connection"))??;
            if f.proto_id == proto_id && f.serial == serial {
                return decode(&f.body);
            }
            self.pending.push_back(f);
        }
    }

    /// Subscribe to one data type for `sec` and register this connection for its pushes.
    async fn subscribe(&mut self, sec: &proto::Security, sub_type: i32) -> Result<()> {
        let req = proto::SubRequest {
            c2s: proto::SubC2s {
                security_list: vec![sec.clone()],
                sub_type_list: vec![sub_type],
                is_sub_or_un_sub: true,
                is_reg_or_un_reg_push: Some(true),
                is_first_push: Some(true),
                extended_time: (sub_type == proto::SUB_TICKER).then_some(true),
            },
        };
        let reply: proto::SubResponse = self.call(proto::QOT_SUB, &req).await?;
        if reply.ret_type != proto::RET_OK {
            bail!(
                "subscribing {} refused: {}",
                sec.code,
                reply.ret_msg.as_deref().unwrap_or("no message (check the account's US quote right)")
            );
        }
        Ok(())
    }

    /// A keep-alive timer at the interval OpenD asked for, already past its first tick.
    async fn keep_alive_timer(&self) -> tokio::time::Interval {
        let mut tick = tokio::time::interval(self.keep_alive);
        tick.tick().await;
        tick
    }

    /// The next incoming frame, sending keep-alives while waiting. `None` once the receiver
    /// behind `tx` is dropped; an error when the connection fails.
    async fn next<T>(&mut self, tick: &mut tokio::time::Interval, tx: &mpsc::Sender<T>) -> Result<Option<Frame>> {
        if let Some(f) = self.pending.pop_front() {
            return Ok(Some(f));
        }
        loop {
            tokio::select! {
                f = self.frames.recv() => {
                    return Ok(Some(f.ok_or_else(|| anyhow!("OpenD closed the connection"))??));
                }
                _ = tick.tick() => {
                    let time = chrono::Utc::now().timestamp();
                    self.send(proto::KEEP_ALIVE, &proto::KeepAliveRequest { c2s: proto::KeepAliveC2s { time } }).await?;
                }
                _ = tx.closed() => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let body = proto::KeepAliveRequest { c2s: proto::KeepAliveC2s { time: 1_790_000_000 } }.encode_to_vec();
        let f = encode_frame(proto::KEEP_ALIVE, 7, &body);
        assert_eq!(f.len(), HEADER_LEN + body.len());
        assert_eq!(&f[..2], b"FT");
        let head: [u8; HEADER_LEN] = f[..HEADER_LEN].try_into().unwrap();
        let (id, serial, len, sha) = decode_header(&head).unwrap();
        assert_eq!((id, serial, len), (proto::KEEP_ALIVE, 7, body.len()));
        assert_eq!(sha.as_slice(), Sha1::digest(&body).as_slice());
        let mut bad = head;
        bad[0] = b'X';
        assert!(decode_header(&bad).is_err());
    }

    #[test]
    fn header_matches_the_sdk_layout() {
        // `struct.pack("<1s1sI2B2I20s8s", b'F', b'T', 3001, 0, 0, 5, 3, sha1(b"abc"), b"\0" * 8)`
        let f = encode_frame(3001, 5, b"abc");
        assert_eq!(&f[2..6], &3001u32.to_le_bytes());
        assert_eq!((f[6], f[7]), (0, 0));
        assert_eq!(&f[8..12], &5u32.to_le_bytes());
        assert_eq!(&f[12..16], &3u32.to_le_bytes());
        assert_eq!(
            f[16..36].iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(&f[36..44], &[0; 8]);
    }

    #[test]
    fn securities() {
        assert_eq!(security(&Security::equity("AAPL")), Some(proto::Security { market: 11, code: "AAPL".into() }));
        assert_eq!(security(&Security::equity("BRK-B")).unwrap().code, "BRK.B");
        assert_eq!(security(&Security::new("SPY", AssetClass::Etf)).unwrap().market, proto::MARKET_US);
        assert_eq!(security(&Security::new("BTCUSD", AssetClass::Crypto)), None);
        assert_eq!(security(&Security::new("EURUSD", AssetClass::Fx)), None);
    }

    fn ticker(seq: i64, dir: i32, kind: Option<i32>) -> proto::Ticker {
        proto::Ticker {
            time: "2026-10-22 10:31:07.123".into(),
            sequence: seq,
            dir,
            price: 182.45,
            volume: 300,
            turnover: 54_735.0,
            kind,
            timestamp: Some(1_792_679_467.123),
            hp_volume: None,
        }
    }

    #[test]
    fn tickers_become_prints() {
        let p = print(&ticker(1, proto::DIR_BID, Some(22)));
        assert_eq!(p.side, Aggressor::Buy);
        assert_eq!(p.ts_ms, 1_792_679_467_123);
        assert_eq!((p.price, p.size), (182.45, 300.0));
        assert_eq!(p.flag.as_deref(), Some("EXT"));
        assert_eq!(print(&ticker(2, proto::DIR_ASK, Some(1))).side, Aggressor::Sell);
        assert_eq!(print(&ticker(3, 3, None)).side, Aggressor::Unknown);
        assert_eq!(print(&ticker(3, 0, Some(1))).flag, None);
        let mut fractional = ticker(4, 1, None);
        fractional.hp_volume = Some(0.25);
        assert_eq!(print(&fractional).size, 0.25);
    }

    #[test]
    fn books_trim_and_skip_empty_levels() {
        let lvl = |price, volume| proto::Level { price, volume, order_count: 3, hp_volume: None };
        let s2c = proto::OrderBookS2c {
            security: proto::Security { market: 11, code: "AAPL".into() },
            order_book_ask_list: vec![lvl(10.01, 100), lvl(10.02, 200), lvl(10.03, 300)],
            order_book_bid_list: vec![lvl(10.00, 400), lvl(0.0, 0), lvl(9.99, 500)],
            name: None,
        };
        let b = book("AAPL US Equity", &s2c, 2);
        assert_eq!(b.asks, vec![(10.01, 100.0), (10.02, 200.0)]);
        assert_eq!(b.bids, vec![(10.00, 400.0), (9.99, 500.0)]);
        assert_eq!((b.source.as_str(), b.simulated), ("moomoo", false));
    }

    #[test]
    fn dedup_forgets_oldest() {
        let mut d = Dedup::new(2);
        assert!(d.insert(1) && d.insert(2) && !d.insert(2));
        assert!(d.insert(3));
        assert!(d.insert(1), "1 fell out of the window");
    }

    #[test]
    fn errors_name_the_problem() {
        let refused = anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused)).context("x");
        assert!(describe("127.0.0.1:11111", &refused).contains("isn't running"));
        let r = proto::SubResponse { ret_type: -1, ret_msg: Some("No quote right".into()), err_code: None, s2c: None };
        assert!(check(r, "sub").unwrap_err().to_string().contains("No quote right"));
    }
}
