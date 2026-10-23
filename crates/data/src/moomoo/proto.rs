//! The moomoo OpenAPI messages terminal-rs uses, written by hand from the `.proto` files in
//! moomoo's Apache-2.0 SDK (`InitConnect`, `KeepAlive`, `Qot_Common`, `Qot_Sub`,
//! `Qot_GetOrderBook`, `Qot_UpdateOrderBook`, `Qot_GetTicker`, `Qot_UpdateTicker`). The protos
//! are proto2: `required` fields are plain values, `optional` ones are `Option`s, and fields
//! not listed here are skipped when decoding.

use prost::Message;

pub const INIT_CONNECT: u32 = 1001;
pub const KEEP_ALIVE: u32 = 1004;
pub const QOT_SUB: u32 = 3001;
pub const QOT_GET_TICKER: u32 = 3010;
pub const QOT_UPDATE_TICKER: u32 = 3011;
pub const QOT_GET_ORDER_BOOK: u32 = 3012;
pub const QOT_UPDATE_ORDER_BOOK: u32 = 3013;

/// `Qot_Common.QotMarket_US_Security`.
pub const MARKET_US: i32 = 11;
/// `Qot_Common.SubType_OrderBook`.
pub const SUB_ORDER_BOOK: i32 = 2;
/// `Qot_Common.SubType_Ticker`.
pub const SUB_TICKER: i32 = 4;
/// `Common.PacketEncAlgo_None`.
pub const ENC_NONE: i32 = -1;
/// `Common.ProtoFmt_Protobuf`.
pub const FMT_PROTOBUF: i32 = 0;
/// `Common.RetType_Succeed`.
pub const RET_OK: i32 = 0;

/// `Qot_Common.TickerDirection`: Bid (外盘) is a buyer-initiated trade, Ask (内盘) a seller-initiated one.
pub const DIR_BID: i32 = 1;
pub const DIR_ASK: i32 = 2;

/// `Qot_Common.Security`.
#[derive(Clone, PartialEq, Message)]
pub struct Security {
    #[prost(int32, required, tag = "1")]
    pub market: i32,
    #[prost(string, required, tag = "2")]
    pub code: String,
}

/// `Qot_Common.OrderBook`: one price level.
#[derive(Clone, PartialEq, Message)]
pub struct Level {
    #[prost(double, required, tag = "1")]
    pub price: f64,
    #[prost(int64, required, tag = "2")]
    pub volume: i64,
    /// `orederCount` in the proto.
    #[prost(int32, required, tag = "3")]
    pub order_count: i32,
    /// High-precision volume (fractional shares).
    #[prost(double, optional, tag = "5")]
    pub hp_volume: Option<f64>,
}

/// `Qot_Common.Ticker`: one print.
#[derive(Clone, PartialEq, Message)]
pub struct Ticker {
    /// Exchange-local time string.
    #[prost(string, required, tag = "1")]
    pub time: String,
    #[prost(int64, required, tag = "2")]
    pub sequence: i64,
    #[prost(int32, required, tag = "3")]
    pub dir: i32,
    #[prost(double, required, tag = "4")]
    pub price: f64,
    #[prost(int64, required, tag = "5")]
    pub volume: i64,
    #[prost(double, required, tag = "6")]
    pub turnover: f64,
    /// `Qot_Common.TickerType`.
    #[prost(int32, optional, tag = "8")]
    pub kind: Option<i32>,
    /// Unix seconds.
    #[prost(double, optional, tag = "11")]
    pub timestamp: Option<f64>,
    #[prost(double, optional, tag = "12")]
    pub hp_volume: Option<f64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InitConnectC2s {
    #[prost(int32, required, tag = "1")]
    pub client_ver: i32,
    #[prost(string, required, tag = "2")]
    pub client_id: String,
    #[prost(bool, optional, tag = "3")]
    pub recv_notify: Option<bool>,
    #[prost(int32, optional, tag = "4")]
    pub packet_enc_algo: Option<i32>,
    #[prost(int32, optional, tag = "5")]
    pub push_proto_fmt: Option<i32>,
    #[prost(string, optional, tag = "6")]
    pub programming_language: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InitConnectS2c {
    #[prost(int32, required, tag = "1")]
    pub server_ver: i32,
    #[prost(uint64, required, tag = "2")]
    pub login_user_id: u64,
    #[prost(uint64, required, tag = "3")]
    pub conn_id: u64,
    #[prost(string, required, tag = "4")]
    pub conn_aes_key: String,
    /// Seconds between keep-alives.
    #[prost(int32, required, tag = "5")]
    pub keep_alive_interval: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct KeepAliveC2s {
    /// Unix seconds.
    #[prost(int64, required, tag = "1")]
    pub time: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct KeepAliveS2c {
    #[prost(int64, required, tag = "1")]
    pub time: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct SubC2s {
    #[prost(message, repeated, tag = "1")]
    pub security_list: Vec<Security>,
    /// `Qot_Common.SubType` values. proto2 repeated scalars are unpacked.
    #[prost(int32, repeated, packed = "false", tag = "2")]
    pub sub_type_list: Vec<i32>,
    #[prost(bool, required, tag = "3")]
    pub is_sub_or_un_sub: bool,
    /// Register this connection for the pushes too.
    #[prost(bool, optional, tag = "4")]
    pub is_reg_or_un_reg_push: Option<bool>,
    /// Push the cached data once right after registering.
    #[prost(bool, optional, tag = "6")]
    pub is_first_push: Option<bool>,
    /// US pre- and post-market prints on ticker subscriptions.
    #[prost(bool, optional, tag = "9")]
    pub extended_time: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SubS2c {}

#[derive(Clone, PartialEq, Message)]
pub struct GetOrderBookC2s {
    #[prost(message, required, tag = "1")]
    pub security: Security,
    /// Levels per side.
    #[prost(int32, required, tag = "2")]
    pub num: i32,
}

/// `Qot_GetOrderBook.S2C` and `Qot_UpdateOrderBook.S2C` share these fields.
#[derive(Clone, PartialEq, Message)]
pub struct OrderBookS2c {
    #[prost(message, required, tag = "1")]
    pub security: Security,
    #[prost(message, repeated, tag = "2")]
    pub order_book_ask_list: Vec<Level>,
    #[prost(message, repeated, tag = "3")]
    pub order_book_bid_list: Vec<Level>,
    #[prost(string, optional, tag = "8")]
    pub name: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetTickerC2s {
    #[prost(message, required, tag = "1")]
    pub security: Security,
    /// At most 1000.
    #[prost(int32, required, tag = "2")]
    pub max_ret_num: i32,
}

/// `Qot_GetTicker.S2C` and `Qot_UpdateTicker.S2C` share these fields.
#[derive(Clone, PartialEq, Message)]
pub struct TickerS2c {
    #[prost(message, required, tag = "1")]
    pub security: Security,
    #[prost(message, repeated, tag = "2")]
    pub ticker_list: Vec<Ticker>,
    #[prost(string, optional, tag = "3")]
    pub name: Option<String>,
}

/// Every protocol wraps its request body as `Request { required C2S c2s = 1; }`.
macro_rules! request {
    ($name:ident, $c2s:ty) => {
        #[derive(Clone, PartialEq, Message)]
        pub struct $name {
            #[prost(message, required, tag = "1")]
            pub c2s: $c2s,
        }
    };
}

/// ...and its reply (or push) as `Response { retType, retMsg, errCode, S2C s2c }`.
macro_rules! response {
    ($name:ident, $s2c:ty) => {
        #[derive(Clone, PartialEq, Message)]
        pub struct $name {
            #[prost(int32, required, tag = "1", default = "-400")]
            pub ret_type: i32,
            #[prost(string, optional, tag = "2")]
            pub ret_msg: Option<String>,
            #[prost(int32, optional, tag = "3")]
            pub err_code: Option<i32>,
            #[prost(message, optional, tag = "4")]
            pub s2c: Option<$s2c>,
        }

        impl Reply for $name {
            type S2c = $s2c;
            fn ret_type(&self) -> i32 {
                self.ret_type
            }
            fn ret_msg(&self) -> Option<&str> {
                self.ret_msg.as_deref()
            }
            fn into_s2c(self) -> Option<$s2c> {
                self.s2c
            }
        }
    };
}

/// The common shape of every reply, so callers can check `retType` generically.
pub trait Reply: Message + Default {
    type S2c;
    fn ret_type(&self) -> i32;
    fn ret_msg(&self) -> Option<&str>;
    fn into_s2c(self) -> Option<Self::S2c>;
}

request!(InitConnectRequest, InitConnectC2s);
response!(InitConnectResponse, InitConnectS2c);
request!(KeepAliveRequest, KeepAliveC2s);
response!(KeepAliveResponse, KeepAliveS2c);
request!(SubRequest, SubC2s);
response!(SubResponse, SubS2c);
request!(GetOrderBookRequest, GetOrderBookC2s);
response!(OrderBookResponse, OrderBookS2c);
request!(GetTickerRequest, GetTickerC2s);
response!(TickerResponse, TickerS2c);
