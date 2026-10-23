//! The moomoo feeds against a local mock OpenD that speaks the real wire format: it checks
//! each request (InitConnect, subscription, snapshot) and answers or pushes like OpenD does.

use std::time::Duration;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tr_core::Security;
use tr_data::moomoo::{HEADER_LEN, Moomoo, decode_header, encode_frame, proto};
use tr_data::{Aggressor, DepthEvent, TapeEvent};

async fn read(s: &mut TcpStream) -> Option<(u32, u32, Vec<u8>)> {
    let mut head = [0u8; HEADER_LEN];
    s.read_exact(&mut head).await.ok()?;
    let (id, serial, len, _) = decode_header(&head).unwrap();
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).await.ok()?;
    Some((id, serial, body))
}

async fn write(s: &mut TcpStream, id: u32, serial: u32, msg: &impl Message) {
    s.write_all(&encode_frame(id, serial, &msg.encode_to_vec())).await.unwrap();
}

/// Accept one client and run the handshake; returns the socket and the subscription it asked for.
async fn handshake(l: &TcpListener, sub_ok: bool) -> (TcpStream, proto::SubC2s) {
    let (mut s, _) = l.accept().await.unwrap();
    let (id, serial, body) = read(&mut s).await.unwrap();
    assert_eq!(id, proto::INIT_CONNECT);
    let init = proto::InitConnectRequest::decode(body.as_slice()).unwrap().c2s;
    assert_eq!((init.client_ver, init.packet_enc_algo, init.push_proto_fmt), (300, Some(-1), Some(0)));
    assert!(init.client_id.starts_with("terminal-rs-"));
    let s2c = proto::InitConnectS2c {
        server_ver: 1001,
        login_user_id: 42,
        conn_id: 7,
        conn_aes_key: "0123456789abcdef".into(),
        keep_alive_interval: 10,
    };
    let reply = proto::InitConnectResponse { ret_type: 0, ret_msg: None, err_code: None, s2c: Some(s2c) };
    write(&mut s, id, serial, &reply).await;

    let (id, serial, body) = read(&mut s).await.unwrap();
    assert_eq!(id, proto::QOT_SUB);
    let sub = proto::SubRequest::decode(body.as_slice()).unwrap().c2s;
    assert!(sub.is_sub_or_un_sub && sub.is_reg_or_un_reg_push == Some(true));
    assert_eq!(sub.security_list, vec![proto::Security { market: 11, code: "AAPL".into() }]);
    let reply = if sub_ok {
        proto::SubResponse { ret_type: 0, ret_msg: None, err_code: None, s2c: Some(proto::SubS2c {}) }
    } else {
        proto::SubResponse {
            ret_type: -1,
            ret_msg: Some("No quote right for US market".into()),
            err_code: None,
            s2c: None,
        }
    };
    write(&mut s, id, serial, &reply).await;
    (s, sub)
}

fn ticker(seq: i64, dir: i32, price: f64, volume: i64) -> proto::Ticker {
    proto::Ticker {
        time: String::new(),
        sequence: seq,
        dir,
        price,
        volume,
        turnover: price * volume as f64,
        kind: Some(1),
        timestamp: Some(1_792_679_467.0 + seq as f64),
        hp_volume: None,
    }
}

fn aapl() -> proto::Security {
    proto::Security { market: 11, code: "AAPL".into() }
}

async fn prints(rx: &mut tr_data::TapeFeed) -> Vec<tr_data::Print> {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("tape event").unwrap() {
            TapeEvent::Prints { prints, source, simulated } => {
                assert_eq!((source.as_str(), simulated), ("moomoo", false));
                return prints;
            }
            TapeEvent::Status(s) => assert!(s.contains("Connecting"), "{s}"),
        }
    }
}

#[tokio::test]
async fn tape_backfills_then_streams_pushes_once() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let m = Moomoo { addr: l.local_addr().unwrap().to_string() };
    let mut rx = m.tape("AAPL US Equity", &Security::equity("AAPL")).unwrap();

    let (mut s, sub) = handshake(&l, true).await;
    assert_eq!((sub.sub_type_list, sub.extended_time), (vec![proto::SUB_TICKER], Some(true)));
    let (id, serial, body) = read(&mut s).await.unwrap();
    assert_eq!(id, proto::QOT_GET_TICKER);
    assert_eq!(proto::GetTickerRequest::decode(body.as_slice()).unwrap().c2s.security, aapl());
    let snap = proto::TickerS2c {
        security: aapl(),
        ticker_list: vec![ticker(1, proto::DIR_BID, 182.40, 100), ticker(2, proto::DIR_ASK, 182.39, 300)],
        name: None,
    };
    write(&mut s, id, serial, &proto::TickerResponse { ret_type: 0, ret_msg: None, err_code: None, s2c: Some(snap) })
        .await;

    let first = prints(&mut rx).await;
    assert_eq!(first.iter().map(|p| p.side).collect::<Vec<_>>(), [Aggressor::Buy, Aggressor::Sell]);
    assert_eq!((first[1].price, first[1].size, first[1].ts_ms), (182.39, 300.0, 1_792_679_469_000));

    // A push overlapping the snapshot: only the new print comes through. Pushes for another
    // security on the same connection are ignored.
    let other = proto::TickerS2c {
        security: proto::Security { market: 11, code: "MSFT".into() },
        ticker_list: vec![ticker(9, 1, 400.0, 1)],
        name: None,
    };
    write(
        &mut s,
        proto::QOT_UPDATE_TICKER,
        0,
        &proto::TickerResponse { ret_type: 0, ret_msg: None, err_code: None, s2c: Some(other) },
    )
    .await;
    let push = proto::TickerS2c {
        security: aapl(),
        ticker_list: vec![ticker(2, 2, 182.39, 300), ticker(3, 3, 182.395, 5_000)],
        name: None,
    };
    write(
        &mut s,
        proto::QOT_UPDATE_TICKER,
        0,
        &proto::TickerResponse { ret_type: 0, ret_msg: None, err_code: None, s2c: Some(push) },
    )
    .await;
    let next = prints(&mut rx).await;
    assert_eq!(next.len(), 1);
    assert_eq!((next[0].price, next[0].size, next[0].side), (182.395, 5_000.0, Aggressor::Unknown));

    // Closing the pane drops the receiver, which closes the connection.
    drop(rx);
    let eof = tokio::time::timeout(Duration::from_secs(5), read(&mut s)).await.expect("connection closed");
    assert!(eof.is_none());
}

#[tokio::test]
async fn depth_streams_full_snapshots() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let m = Moomoo { addr: l.local_addr().unwrap().to_string() };
    let mut rx = m.depth("AAPL US Equity", &Security::equity("AAPL"), 2).unwrap();

    let (mut s, sub) = handshake(&l, true).await;
    assert_eq!(sub.sub_type_list, vec![proto::SUB_ORDER_BOOK]);
    let (id, serial, body) = read(&mut s).await.unwrap();
    assert_eq!(id, proto::QOT_GET_ORDER_BOOK);
    assert_eq!(proto::GetOrderBookRequest::decode(body.as_slice()).unwrap().c2s.num, 2);
    let lvl = |price, volume| proto::Level { price, volume, order_count: 1, hp_volume: None };
    let book = |bids: [f64; 2], asks: [f64; 3]| proto::OrderBookS2c {
        security: aapl(),
        order_book_ask_list: vec![lvl(asks[0], 200), lvl(asks[1], 900), lvl(asks[2], 50)],
        order_book_bid_list: vec![lvl(bids[0], 300), lvl(bids[1], 700)],
        name: Some("Apple".into()),
    };
    let reply = |b| proto::OrderBookResponse { ret_type: 0, ret_msg: None, err_code: None, s2c: Some(b) };
    write(&mut s, id, serial, &reply(book([182.40, 182.39], [182.41, 182.42, 182.43]))).await;
    write(&mut s, proto::QOT_UPDATE_ORDER_BOOK, 0, &reply(book([182.41, 182.40], [182.42, 182.43, 182.44]))).await;

    let mut books = Vec::new();
    while books.len() < 2 {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("depth event").unwrap() {
            DepthEvent::Book(b) => books.push(b),
            DepthEvent::Status(st) => assert!(st.contains("Connecting"), "{st}"),
        }
    }
    assert_eq!(books[0].bids, vec![(182.40, 300.0), (182.39, 700.0)]);
    assert_eq!(books[0].asks.len(), 2, "trimmed to the requested depth");
    assert_eq!(books[1].bids[0], (182.41, 300.0));
    assert_eq!((books[1].symbol.as_str(), books[1].source.as_str()), ("AAPL US Equity", "moomoo"));
}

#[tokio::test]
async fn refused_subscriptions_and_missing_opend_are_reported() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let m = Moomoo { addr: l.local_addr().unwrap().to_string() };
    let mut rx = m.tape("AAPL US Equity", &Security::equity("AAPL")).unwrap();
    let _s = handshake(&l, false).await;
    let status = loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap() {
            TapeEvent::Status(s) if s.contains("Connecting") => continue,
            TapeEvent::Status(s) => break s,
            e => panic!("unexpected {e:?}"),
        }
    };
    assert!(status.contains("No quote right") && status.contains("Retrying"), "{status}");

    // Nothing listening: the pane is told to start OpenD.
    let port = {
        let tmp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        tmp.local_addr().unwrap().port()
    };
    let m = Moomoo { addr: format!("127.0.0.1:{port}") };
    let mut rx = m.depth("AAPL US Equity", &Security::equity("AAPL"), 10).unwrap();
    let status = loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap() {
            DepthEvent::Status(s) if s.contains("Connecting") => continue,
            DepthEvent::Status(s) => break s,
            e => panic!("unexpected {e:?}"),
        }
    };
    assert!(status.contains("isn't running") && status.contains("Start OpenD"), "{status}");
    // Only US stocks and ETFs have a moomoo feed.
    assert!(m.tape("BTCUSD Curncy", &tr_data::security("BTCUSD Curncy")).is_none());
}
