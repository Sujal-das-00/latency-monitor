// latency_detector — multi-symbol latency arbitrage detector
//
// Measures price lag between a reference feed (Binance, combined-stream)
// and N target exchanges, for SOL/USDT, BTC/USDT and ETH/USDT simultaneously.
// Everything runs async on a single io_context / single thread.
// Adding/removing a target or symbol is a config-file change only
// (exchanges.json).
//
// Build: see CMakeLists.txt  (C++17, Boost.Asio/Beast, OpenSSL, nlohmann/json, zlib)

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <nlohmann/json.hpp>
#include <zlib.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
namespace beast     = boost::beast;
namespace websocket = boost::beast::websocket;
using tcp           = boost::asio::ip::tcp;
using json          = nlohmann::json;

// ───────────────────────────── time helper ─────────────────────────────
static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// ───────────────────────────── ANSI colors ─────────────────────────────
namespace col {
constexpr const char* reset  = "\033[0m";
constexpr const char* red    = "\033[31m";
constexpr const char* green  = "\033[32m";
constexpr const char* yellow = "\033[33m";
constexpr const char* white  = "\033[37m";
constexpr const char* cyan   = "\033[36m";
constexpr const char* bold   = "\033[1m";
}

// ───────────────────────────── core structs ────────────────────────────
struct SymbolCfg {
    std::string local;    // exchange-specific symbol identifier
    std::string display;  // human readable, e.g. "SOL/USDT"
};

struct FeedSnapshot {
    double  bid      = 0.0;
    double  ask      = 0.0;
    double  mid      = 0.0;
    int64_t exch_ts  = 0;   // exchange-reported ms
    int64_t local_ts = 0;   // our clock ms at receive time
    bool    valid    = false;
};

// Per (exchange, symbol) rolling metrics, signal state and CSV.
struct SymbolState {
    std::string display;

    FeedSnapshot latest;

    // rolling window (last N samples)
    std::deque<int64_t> lag_samples;     // exch_lag_ms per sample
    std::deque<double>  delta_samples;   // price_delta_bps per sample
    int rolling_window = 1000;

    // current (last computed) values, for live display
    double  cur_delta_bps = 0.0;
    int64_t cur_lag_ms    = 0;
    bool    cur_valid     = false;

    // signal tracking
    bool        signal_open      = false;
    int64_t     signal_open_time = 0;
    double      signal_peak_bps  = 0.0;
    int         signals_total    = 0;
    double      avg_duration_ms  = 0.0;
    int64_t     gap_start_time   = 0;     // when |delta| first crossed open
    std::string signal_dir;               // "CHEAPER" / "RICHER"

    // csv
    std::ofstream csv;
};

struct ExchangeState {
    std::string name;
    std::string type;

    // connection health
    bool connected       = false;
    int  reconnect_count = 0;

    std::vector<SymbolState> symbols;
};

// ───────────────────────── adapter / type dispatch ─────────────────────
//
// To add a new exchange "type": add an enum value, a case in feed_type(),
// symbol_key(), subscribe_messages(), and parse_message(). That is the
// whole contract — never redesign the dispatch itself.

enum class FeedType {
    Binance, OKX, Bybit, Kraken, Coinbase, Gateio, Mexc, Bitget, Phemex, Xt,
    Bingx, Unknown
};

static FeedType feed_type(const std::string& s) {
    if (s == "binance")  return FeedType::Binance;
    if (s == "okx")      return FeedType::OKX;
    if (s == "bybit")    return FeedType::Bybit;
    if (s == "kraken")   return FeedType::Kraken;
    if (s == "coinbase") return FeedType::Coinbase;
    if (s == "gateio")   return FeedType::Gateio;
    if (s == "mexc")     return FeedType::Mexc;
    if (s == "bitget")   return FeedType::Bitget;
    if (s == "phemex")   return FeedType::Phemex;
    if (s == "xt")       return FeedType::Xt;
    if (s == "bingx")    return FeedType::Bingx;
    return FeedType::Unknown;
}

// The key used to identify which configured symbol a message belongs to.
// Built once per connection (from cfg.symbols) into AdapterContext.symbol_index,
// and recomputed from each incoming message to look that map up.
static std::string symbol_key(FeedType t, const std::string& local) {
    switch (t) {
        case FeedType::Binance: return local + "@depth5@100ms";  // matches "stream"
        case FeedType::Bybit:   return "orderbook.1." + local;   // matches "topic"
        case FeedType::Bingx:   return local + "@depth5";         // matches "dataType"
        default:                return local;                     // matches as-is
    }
}

// ─────────────────────────── adapter context ───────────────────────────
//
// Mutable per-connection state needed by some adapters' parse_message().
struct AdapterContext {
    std::unordered_map<std::string, int> symbol_index;
    int num_symbols = 0;

    // Coinbase: local order book per symbol (price -> qty)
    std::vector<std::map<double, double>> cb_bids;
    std::vector<std::map<double, double>> cb_asks;

    // Phemex: have we done the one-time scale sanity check per symbol?
    std::vector<bool> phemex_checked;
    // Phemex: pointer to reference snapshots (for the sanity check)
    const std::vector<FeedSnapshot>* ref_snapshots = nullptr;
    // Phemex: id counter for server.ping requests
    int64_t ping_id = 1;
};

enum class MsgKind { Price, Pong, Ignore };

struct ParseResult {
    MsgKind      kind = MsgKind::Ignore;
    FeedSnapshot snap;            // valid when kind == Price
    int          symbol_idx = -1; // index into cfg.symbols, valid when kind == Price
    std::string  pong_payload;    // text to send when kind == Pong
    std::string  debug_warning;   // non-empty -> logged to logs/errors.log
};

// ───── per-connection keepalive (app-level ping) ─────
static int ping_interval_sec(FeedType t) {
    switch (t) {
        case FeedType::Gateio: return 25;
        case FeedType::Mexc:   return 25;
        case FeedType::Xt:     return 25;
        case FeedType::Bitget: return 30;
        case FeedType::Phemex: return 10;
        default:               return 0;
    }
}

static std::string ping_payload(FeedType t, AdapterContext& ctx) {
    switch (t) {
        case FeedType::Gateio:
            return json{{"time", now_ms() / 1000}, {"channel", "spot.ping"}}.dump();
        case FeedType::Mexc:
            return json{{"method", "PING"}}.dump();
        case FeedType::Xt:
            return json{{"method", "PING"}}.dump();
        case FeedType::Bitget:
            return "ping";
        case FeedType::Phemex:
            return json{{"id", ctx.ping_id++},
                        {"method", "server.ping"},
                        {"params", json::array()}}.dump();
        default:
            return "";
    }
}

// ───────────────────────────── small helpers ───────────────────────────

// Parse an ISO-8601 UTC timestamp with variable-length fractional seconds
// (e.g. "2026-06-13T12:34:56.789012345Z") to milliseconds since epoch.
static int64_t parse_iso8601_ms(const std::string& ts) {
    int year, mon, day, hour, min, sec;
    if (std::sscanf(ts.c_str(), "%d-%d-%dT%d:%d:%d", &year, &mon, &day, &hour,
                     &min, &sec) != 6)
        return 0;

    int64_t ms_frac = 0;
    auto dot = ts.find('.');
    if (dot != std::string::npos) {
        std::string digits;
        for (std::size_t i = dot + 1; i < ts.size() && std::isdigit((unsigned char)ts[i]); ++i)
            digits += ts[i];
        if (digits.size() > 3) digits = digits.substr(0, 3);
        while (digits.size() < 3) digits += '0';
        ms_frac = std::stoll(digits);
    }

    std::tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon  = mon - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min  = min;
    tm.tm_sec  = sec;
    std::time_t t = timegm(&tm);
    return static_cast<int64_t>(t) * 1000 + ms_frac;
}

// gzip-decompress (wbits = 16+MAX_WBITS, i.e. zlib's gzip-format mode).
static bool gunzip(const std::string& in, std::string& out) {
    z_stream zs{};
    if (inflateInit2(&zs, 16 + MAX_WBITS) != Z_OK) return false;
    zs.next_in  = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    zs.avail_in = static_cast<uInt>(in.size());

    char buf[16384];
    int ret;
    out.clear();
    do {
        zs.next_out  = reinterpret_cast<Bytef*>(buf);
        zs.avail_out = sizeof(buf);
        ret = inflate(&zs, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
            inflateEnd(&zs);
            return false;
        }
        out.append(buf, sizeof(buf) - zs.avail_out);
    } while (ret != Z_STREAM_END && zs.avail_out == 0);
    inflateEnd(&zs);
    return ret == Z_STREAM_END;
}

static std::string to_hex(const std::string& s) {
    static const char* hexd = "0123456789abcdef";
    std::string out;
    out.reserve(s.size() * 2);
    for (unsigned char c : s) {
        out += hexd[c >> 4];
        out += hexd[c & 0xf];
    }
    return out;
}

// ─────────────────────── subscribe messages per type ───────────────────
//
// Messages to send (in order) after the WS handshake. Empty = none needed
// (e.g. Binance: symbols are encoded in the combined-stream URL).
static std::vector<std::string> subscribe_messages(FeedType t,
                                                    const std::vector<SymbolCfg>& symbols) {
    std::vector<std::string> out;
    switch (t) {
        case FeedType::Binance:
            return out;

        case FeedType::OKX: {
            json args = json::array();
            for (const auto& s : symbols)
                args.push_back({{"channel", "books5"}, {"instId", s.local}});
            out.push_back(json{{"op", "subscribe"}, {"args", args}}.dump());
            return out;
        }

        case FeedType::Bybit: {
            // live Bybit v5 uses "args" (spec's "topics" is not accepted)
            json args = json::array();
            for (const auto& s : symbols) args.push_back("orderbook.1." + s.local);
            out.push_back(json{{"op", "subscribe"}, {"args", args}}.dump());
            return out;
        }

        case FeedType::Kraken: {
            json pairs = json::array();
            for (const auto& s : symbols) pairs.push_back(s.local);
            out.push_back(json{{"event", "subscribe"},
                               {"pair", pairs},
                               {"subscription", {{"name", "ticker"}}}}.dump());
            return out;
        }

        case FeedType::Coinbase: {
            json ids = json::array();
            for (const auto& s : symbols) ids.push_back(s.local);
            out.push_back(json{{"type", "subscribe"},
                               {"product_ids", ids},
                               {"channel", "level2"}}.dump());
            return out;
        }

        case FeedType::Gateio: {
            json payload = json::array();
            for (const auto& s : symbols) payload.push_back(s.local);
            out.push_back(json{{"time", now_ms() / 1000},
                               {"channel", "spot.book_ticker"},
                               {"event", "subscribe"},
                               {"payload", payload}}.dump());
            return out;
        }

        case FeedType::Mexc: {
            // per spec: one subscribe message per symbol
            for (const auto& s : symbols) {
                json params = json::array({"spot@public.bookTicker.v3.api@" + s.local});
                out.push_back(json{{"method", "SUBSCRIPTION"}, {"params", params}}.dump());
            }
            return out;
        }

        case FeedType::Bitget: {
            json args = json::array();
            for (const auto& s : symbols)
                args.push_back({{"instType", "SPOT"}, {"channel", "books5"}, {"instId", s.local}});
            out.push_back(json{{"op", "subscribe"}, {"args", args}}.dump());
            return out;
        }

        case FeedType::Phemex: {
            // per-symbol sequential subscribes
            int id = 1;
            for (const auto& s : symbols) {
                out.push_back(json{{"id", id++},
                                   {"method", "orderbook.subscribe"},
                                   {"params", json::array({s.local})}}.dump());
            }
            return out;
        }

        case FeedType::Xt: {
            json params = json::array();
            for (const auto& s : symbols) params.push_back("depth_update@" + s.local);
            out.push_back(json{{"method", "SUBSCRIBE"}, {"params", params}, {"id", 1}}.dump());
            return out;
        }

        case FeedType::Bingx: {
            // per-symbol sequential subscribes
            int id = 1;
            for (const auto& s : symbols) {
                out.push_back(json{{"id", std::to_string(id++)},
                                   {"reqType", "sub"},
                                   {"dataType", s.local + "@depth5"}}.dump());
            }
            return out;
        }

        default:
            return out;
    }
}

// ─────────────────────────────── parse_message ─────────────────────────
//
// Parse one raw WS message (already gunzipped for BingX) for the given feed
// type. Never throws: malformed messages return Ignore.
static ParseResult parse_message(FeedType t, const std::string& msg,
                                 int64_t local_ts, AdapterContext& ctx) {
    ParseResult r;
    try {
        switch (t) {
            // ─────────────────────────── Binance ──────────────────────────
            case FeedType::Binance: {
                json j = json::parse(msg);
                if (!j.contains("stream") || !j.contains("data")) return r;
                auto it = ctx.symbol_index.find(j["stream"].get<std::string>());
                if (it == ctx.symbol_index.end()) return r;
                const json& d = j["data"];
                const json* bids = nullptr;
                const json* asks = nullptr;
                if (d.contains("b") && d.contains("a")) {
                    bids = &d["b"];
                    asks = &d["a"];
                } else if (d.contains("bids") && d.contains("asks")) {
                    bids = &d["bids"];
                    asks = &d["asks"];
                } else {
                    return r;
                }
                if (bids->empty() || asks->empty()) return r;
                r.snap.bid = std::stod((*bids)[0][0].get<std::string>());
                r.snap.ask = std::stod((*asks)[0][0].get<std::string>());
                r.snap.exch_ts = d.contains("E") ? d["E"].get<int64_t>() : local_ts;
                r.symbol_idx = it->second;
                break;
            }
            // ───────────────────────────── OKX ────────────────────────────
            case FeedType::OKX: {
                if (msg == "ping") {
                    r.kind         = MsgKind::Pong;
                    r.pong_payload = "pong";
                    return r;
                }
                json j = json::parse(msg);
                if (!j.contains("arg") || !j.contains("data") ||
                    !j["data"].is_array() || j["data"].empty())
                    return r;
                auto it = ctx.symbol_index.find(j["arg"].value("instId", std::string()));
                if (it == ctx.symbol_index.end()) return r;
                const json& d = j["data"][0];
                if (d["bids"].empty() || d["asks"].empty()) return r;
                r.snap.bid = std::stod(d["bids"][0][0].get<std::string>());
                r.snap.ask = std::stod(d["asks"][0][0].get<std::string>());
                r.snap.exch_ts = std::stoll(d["ts"].get<std::string>());
                r.symbol_idx = it->second;
                break;
            }
            // ──────────────────────────── Bybit ───────────────────────────
            case FeedType::Bybit: {
                json j = json::parse(msg);
                if (j.value("op", std::string()) == "ping") {
                    r.kind         = MsgKind::Pong;
                    r.pong_payload = json{{"op", "pong"}}.dump();
                    return r;
                }
                if (!j.contains("data") || !j.contains("topic")) return r;
                auto it = ctx.symbol_index.find(j["topic"].get<std::string>());
                if (it == ctx.symbol_index.end()) return r;
                const json& d = j["data"];
                // live Bybit: bids="b", asks="a"; "s" is the symbol string
                if (!d.contains("b") || !d.contains("a")) return r;
                if (d["b"].empty() || d["a"].empty()) return r;  // one-sided delta
                r.snap.bid     = std::stod(d["b"][0][0].get<std::string>());
                r.snap.ask     = std::stod(d["a"][0][0].get<std::string>());
                r.snap.exch_ts = j.value("ts", local_ts);
                r.symbol_idx   = it->second;
                break;
            }
            // ──────────────────────────── Kraken ──────────────────────────
            case FeedType::Kraken: {
                json j = json::parse(msg);
                // heartbeat / subscriptionStatus / systemStatus are objects
                if (!j.is_array() || j.size() < 4) return r;
                if (!j[1].is_object() || !j[1].contains("a") || !j[1].contains("b")) return r;
                auto it = ctx.symbol_index.find(j[3].get<std::string>());
                if (it == ctx.symbol_index.end()) return r;
                const json& tk = j[1];
                if (tk["a"].empty() || tk["b"].empty()) return r;
                r.snap.ask     = std::stod(tk["a"][0].get<std::string>());
                r.snap.bid     = std::stod(tk["b"][0].get<std::string>());
                r.snap.exch_ts = local_ts;  // ticker carries no exchange timestamp
                r.symbol_idx   = it->second;
                break;
            }
            // ─────────────────────────── Coinbase ─────────────────────────
            case FeedType::Coinbase: {
                json j = json::parse(msg);
                if (j.value("channel", std::string()) != "l2_data") return r;
                if (!j.contains("events")) return r;
                int64_t exch_ts = j.contains("timestamp")
                                      ? parse_iso8601_ms(j["timestamp"].get<std::string>())
                                      : local_ts;
                int last_idx = -1;
                for (const auto& ev : j["events"]) {
                    std::string etype = ev.value("type", std::string());
                    if (etype != "snapshot" && etype != "update") continue;
                    auto it = ctx.symbol_index.find(ev.value("product_id", std::string()));
                    if (it == ctx.symbol_index.end()) continue;
                    int idx = it->second;
                    auto& bids = ctx.cb_bids[idx];
                    auto& asks = ctx.cb_asks[idx];
                    if (!ev.contains("updates")) continue;
                    for (const auto& u : ev["updates"]) {
                        std::string side = u.value("side", std::string());
                        double price = std::stod(u.value("price_level", std::string("0")));
                        double qty   = std::stod(u.value("new_quantity", std::string("0")));
                        auto& book = (side == "bid") ? bids : asks;
                        if (qty == 0.0) book.erase(price);
                        else            book[price] = qty;
                    }
                    while (bids.size() > 5) bids.erase(bids.begin());           // drop lowest bid
                    while (asks.size() > 5) asks.erase(std::prev(asks.end()));  // drop highest ask
                    last_idx = idx;
                }
                if (last_idx < 0) return r;
                auto& bids = ctx.cb_bids[last_idx];
                auto& asks = ctx.cb_asks[last_idx];
                if (bids.empty() || asks.empty()) return r;
                r.snap.bid     = bids.rbegin()->first;
                r.snap.ask     = asks.begin()->first;
                r.snap.exch_ts = exch_ts;
                r.symbol_idx   = last_idx;
                break;
            }
            // ──────────────────────────── Gate.io ─────────────────────────
            case FeedType::Gateio: {
                json j = json::parse(msg);
                if (j.value("channel", std::string()) != "spot.book_ticker") return r;
                if (j.value("event", std::string()) != "update") return r;
                if (!j.contains("result")) return r;
                const json& res = j["result"];
                auto it = ctx.symbol_index.find(res.value("s", std::string()));
                if (it == ctx.symbol_index.end()) return r;
                r.snap.bid     = std::stod(res.value("b", std::string("0")));
                r.snap.ask     = std::stod(res.value("a", std::string("0")));
                r.snap.exch_ts = res.value("t", local_ts);
                r.symbol_idx   = it->second;
                break;
            }
            // ───────────────────────────── MEXC ────────────────────────────
            case FeedType::Mexc: {
                json j = json::parse(msg);
                if (!j.contains("d") || !j.contains("s")) return r;
                auto it = ctx.symbol_index.find(j["s"].get<std::string>());
                if (it == ctx.symbol_index.end()) return r;
                const json& d = j["d"];
                if (!d.contains("b") || !d.contains("a")) return r;
                r.snap.bid     = std::stod(d["b"].get<std::string>());
                r.snap.ask     = std::stod(d["a"].get<std::string>());
                r.snap.exch_ts = j.value("t", local_ts);
                r.symbol_idx   = it->second;
                break;
            }
            // ──────────────────────────── Bitget ──────────────────────────
            case FeedType::Bitget: {
                if (msg == "pong") return r;
                json j = json::parse(msg);
                std::string action = j.value("action", std::string());
                if (action != "snapshot" && action != "update") return r;
                if (!j.contains("arg") || !j.contains("data") || j["data"].empty()) return r;
                auto it = ctx.symbol_index.find(j["arg"].value("instId", std::string()));
                if (it == ctx.symbol_index.end()) return r;
                const json& d = j["data"][0];
                if (d["bids"].empty() || d["asks"].empty()) return r;
                r.snap.bid     = std::stod(d["bids"][0][0].get<std::string>());
                r.snap.ask     = std::stod(d["asks"][0][0].get<std::string>());
                r.snap.exch_ts = std::stoll(d.value("ts", std::string("0")));
                r.symbol_idx   = it->second;
                break;
            }
            // ──────────────────────────── Phemex ──────────────────────────
            case FeedType::Phemex: {
                json j = json::parse(msg);
                if (!j.contains("book") || !j.contains("symbol")) return r;  // ping reply / ack
                std::string sym = j["symbol"].get<std::string>();
                auto it = ctx.symbol_index.find(sym);
                if (it == ctx.symbol_index.end()) return r;
                int idx = it->second;
                const json& book = j["book"];
                if (!book.contains("bids") || !book.contains("asks")) return r;
                if (book["bids"].empty() || book["asks"].empty()) return r;
                // live-verified: Phemex spot uses a uniform 1e8 price scale
                // (the spec's "1e5 unless BTC" heuristic was off by 1000x for
                // SOL/ETH — caught by the sanity check below).
                constexpr double scale = 1e8;
                r.snap.bid = book["bids"][0][0].get<double>() / scale;
                r.snap.ask = book["asks"][0][0].get<double>() / scale;
                int64_t ts_ns = j.value("timestamp", static_cast<int64_t>(0));
                r.snap.exch_ts = ts_ns > 0 ? ts_ns / 1000000 : local_ts;
                r.symbol_idx   = idx;

                // one-time sanity check vs reference mid (scale factor could be wrong)
                if (idx < (int)ctx.phemex_checked.size() && !ctx.phemex_checked[idx]) {
                    ctx.phemex_checked[idx] = true;
                    if (ctx.ref_snapshots && idx < (int)ctx.ref_snapshots->size()) {
                        const FeedSnapshot& refs = (*ctx.ref_snapshots)[idx];
                        if (refs.valid && refs.mid > 0) {
                            double mid  = (r.snap.bid + r.snap.ask) / 2.0;
                            double diff = std::abs(mid - refs.mid) / refs.mid;
                            if (diff > 0.20) {
                                r.debug_warning = "Phemex " + sym + " price " +
                                    std::to_string(mid) + " differs from reference " +
                                    std::to_string(refs.mid) +
                                    " by >20% — check price scale factor";
                            }
                        }
                    }
                }
                break;
            }
            // ──────────────────────────── XT.com ──────────────────────────
            case FeedType::Xt: {
                json j = json::parse(msg);
                if (j.value("topic", std::string()) != "depth_update") return r;
                if (!j.contains("data")) return r;
                const json& d = j["data"];
                auto it = ctx.symbol_index.find(d.value("s", std::string()));
                if (it == ctx.symbol_index.end()) return r;
                // depth_update is incremental: pragmatically require both sides
                // present in the same message before treating it as top-of-book.
                if (!d.contains("a") || !d.contains("b")) return r;
                if (d["a"].empty() || d["b"].empty()) return r;
                r.snap.bid     = std::stod(d["b"][0][0].get<std::string>());
                r.snap.ask     = std::stod(d["a"][0][0].get<std::string>());
                r.snap.exch_ts = d.value("t", local_ts);
                r.symbol_idx   = it->second;
                break;
            }
            // ──────────────────────────── BingX ───────────────────────────
            case FeedType::Bingx: {
                if (msg == "Ping") {
                    r.kind         = MsgKind::Pong;
                    r.pong_payload = "Pong";
                    return r;
                }
                json j = json::parse(msg);
                if (!j.contains("dataType") || !j.contains("data")) return r;
                auto it = ctx.symbol_index.find(j["dataType"].get<std::string>());
                if (it == ctx.symbol_index.end()) return r;
                const json& d = j["data"];
                if (!d.contains("bids") || !d.contains("asks")) return r;
                if (d["bids"].empty() || d["asks"].empty()) return r;
                r.snap.bid     = std::stod(d["bids"][0][0].get<std::string>());
                r.snap.ask     = std::stod(d["asks"][0][0].get<std::string>());
                r.snap.exch_ts = j.value("timestamp", local_ts);
                r.symbol_idx   = it->second;
                break;
            }
            default:
                return r;
        }
    } catch (...) {
        r.kind = MsgKind::Ignore;
        return r;
    }

    r.snap.mid      = (r.snap.bid + r.snap.ask) / 2.0;
    r.snap.local_ts = local_ts;
    r.snap.valid    = (r.snap.bid > 0.0 && r.snap.ask > 0.0);
    r.kind          = r.snap.valid ? MsgKind::Price : MsgKind::Ignore;
    return r;
}

// ─────────────────────────────── config ────────────────────────────────
struct FeedConfig {
    std::string            name;
    std::string            url;
    std::string            type;
    std::vector<SymbolCfg> symbols;
};

struct AppConfig {
    std::string            reference_name;
    std::string            reference_type;
    std::string            reference_base_url;
    std::vector<SymbolCfg> reference_symbols;

    std::vector<FeedConfig> targets;

    double signal_open_bps    = 4.0;
    double signal_close_bps   = 1.5;
    int    signal_min_ms      = 40;
    int    stats_interval_sec = 1;
    int    rolling_window     = 1000;
};

static std::vector<SymbolCfg> parse_symbols(const json& arr) {
    std::vector<SymbolCfg> out;
    for (const auto& s : arr)
        out.push_back({s.value("local", std::string()), s.value("display", std::string())});
    return out;
}

static AppConfig load_config(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot open config: " + path);
    json j;
    in >> j;

    AppConfig c;
    const json& ref      = j.at("reference");
    c.reference_name     = ref.value("name", std::string());
    c.reference_type     = ref.value("type", std::string());
    c.reference_base_url = ref.value("base_url", std::string());
    c.reference_symbols  = parse_symbols(ref.at("symbols"));

    for (const auto& t : j.at("targets")) {
        FeedConfig f;
        f.name    = t.value("name", std::string());
        f.url     = t.value("url", std::string());
        f.type    = t.value("type", std::string());
        f.symbols = parse_symbols(t.at("symbols"));
        c.targets.push_back(std::move(f));
    }

    c.signal_open_bps    = j.value("signal_open_bps", 4.0);
    c.signal_close_bps   = j.value("signal_close_bps", 1.5);
    c.signal_min_ms      = j.value("signal_min_ms", 40);
    c.stats_interval_sec = j.value("stats_interval_sec", 1);
    c.rolling_window     = j.value("rolling_window", 1000);
    return c;
}

// Build the Binance combined-stream URL for the reference feed, e.g.
// wss://stream.binance.com:9443/stream?streams=solusdt@depth5@100ms/btcusdt@depth5@100ms
static std::string build_binance_combined_url(const std::string& base_url,
                                               const std::vector<SymbolCfg>& symbols) {
    std::string streams;
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i) streams += "/";
        streams += symbols[i].local + "@depth5@100ms";
    }
    return base_url + "/stream?streams=" + streams;
}

// Parse "wss://host:port/path" into pieces.
struct WsUrl {
    std::string host;
    std::string port;
    std::string path;
};

static WsUrl parse_url(const std::string& url) {
    WsUrl u;
    std::string rest = url;
    auto scheme = rest.find("://");
    if (scheme != std::string::npos) rest = rest.substr(scheme + 3);
    auto slash = rest.find('/');
    std::string hostport = (slash == std::string::npos) ? rest : rest.substr(0, slash);
    u.path = (slash == std::string::npos) ? "/" : rest.substr(slash);
    auto colon = hostport.find(':');
    if (colon == std::string::npos) {
        u.host = hostport;
        u.port = "443";
    } else {
        u.host = hostport.substr(0, colon);
        u.port = hostport.substr(colon + 1);
    }
    return u;
}

// ───────────────────────────── forward decl ────────────────────────────
class Monitor;

// ─────────────────────────── FeedConnection ────────────────────────────
//
// One independent async WebSocket connection, covering one exchange and
// (for the reference) all configured symbols via a combined stream, or
// (for a target) all configured symbols via per-exchange multi-symbol
// subscribe. index_ == -1 means this is the reference feed; otherwise it
// indexes into Monitor's exchange vector. Owns its own reconnect loop; a
// failure here never touches other feeds.
class FeedConnection : public std::enable_shared_from_this<FeedConnection> {
public:
    using WsStream =
        websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

    FeedConnection(net::io_context& ioc, ssl::context& ctx, Monitor& mon,
                   int index, FeedConfig cfg)
        : ioc_(ioc),
          ssl_ctx_(ctx),
          mon_(mon),
          index_(index),
          cfg_(std::move(cfg)),
          type_(feed_type(cfg_.type)),
          resolver_(ioc),
          reconnect_timer_(ioc),
          ping_timer_(ioc),
          debug_timer_(ioc) {
        WsUrl u  = parse_url(cfg_.url);
        host_    = u.host;
        port_    = u.port;
        path_    = u.path;

        actx_.num_symbols = static_cast<int>(cfg_.symbols.size());
        for (int i = 0; i < actx_.num_symbols; ++i)
            actx_.symbol_index[symbol_key(type_, cfg_.symbols[i].local)] = i;
        actx_.cb_bids.resize(actx_.num_symbols);
        actx_.cb_asks.resize(actx_.num_symbols);
        actx_.phemex_checked.assign(actx_.num_symbols, false);
        init_adapter_context();
    }

    void start() { connect(); }

private:
    void fail(const std::string& where, beast::error_code ec) {
        // log to a side file so it does not corrupt the live table
        static std::ofstream errlog("logs/errors.log", std::ios::app);
        errlog << now_ms() << " " << cfg_.name << " " << where << ": "
               << ec.message() << "\n";
        errlog.flush();
        schedule_reconnect();
    }

    void schedule_reconnect();

    void connect() {
        // fresh stream every attempt; an errored stream cannot be reused
        ws_ = std::make_unique<WsStream>(ioc_, ssl_ctx_);
        ws_->set_option(websocket::stream_base::timeout::suggested(
            beast::role_type::client));
        got_price_ = false;
        write_queue_.clear();
        writing_   = false;

        auto self = shared_from_this();
        resolver_.async_resolve(
            host_, port_,
            [self](beast::error_code ec, tcp::resolver::results_type results) {
                if (ec) return self->fail("resolve", ec);
                self->on_resolve(results);
            });
    }

    void on_resolve(const tcp::resolver::results_type& results) {
        beast::get_lowest_layer(*ws_).expires_after(std::chrono::seconds(30));
        auto self = shared_from_this();
        beast::get_lowest_layer(*ws_).async_connect(
            results,
            [self](beast::error_code ec, const tcp::endpoint&) {
                if (ec) return self->fail("connect", ec);
                self->on_connect();
            });
    }

    void on_connect() {
        // SNI — required by these endpoints
        if (!SSL_set_tlsext_host_name(ws_->next_layer().native_handle(),
                                      host_.c_str())) {
            beast::error_code ec{static_cast<int>(::ERR_get_error()),
                                 net::error::get_ssl_category()};
            return fail("sni", ec);
        }
        auto self = shared_from_this();
        ws_->next_layer().async_handshake(
            ssl::stream_base::client, [self](beast::error_code ec) {
                if (ec) return self->fail("ssl_handshake", ec);
                self->on_ssl_handshake();
            });
    }

    void on_ssl_handshake() {
        // hand control of timeouts to the websocket stream from here on
        beast::get_lowest_layer(*ws_).expires_never();
        ws_->set_option(websocket::stream_base::decorator(
            [](websocket::request_type& req) {
                req.set(beast::http::field::user_agent,
                        "latency_detector/1.0");
            }));
        auto self = shared_from_this();
        ws_->async_handshake(host_, path_, [self](beast::error_code ec) {
            if (ec) return self->fail("ws_handshake", ec);
            self->on_ws_handshake();
        });
    }

    void on_ws_handshake() {
        ws_->text(true);
        mark_connected(true);
        init_debug_timer();

        for (auto& m : subscribe_messages(type_, cfg_.symbols)) enqueue_write(m);

        int pis = ping_interval_sec(type_);
        if (pis > 0) start_ping_timer(pis);

        do_read();
    }

    void do_read() {
        buffer_.consume(buffer_.size());
        auto self = shared_from_this();
        ws_->async_read(
            buffer_, [self](beast::error_code ec, std::size_t n) {
                self->on_read(ec, n);
            });
    }

    void on_read(beast::error_code ec, std::size_t) {
        const int64_t local_ts = now_ms();  // stamp immediately
        if (ec) return fail("read", ec);

        bool is_binary  = ws_->got_binary();
        std::string raw = beast::buffers_to_string(buffer_.data());
        buffer_.consume(buffer_.size());

        std::string msg;
        if (is_binary && type_ == FeedType::Bingx) {
            if (!gunzip(raw, msg)) {
                push_debug_frame("[binary, inflate failed, " +
                                 std::to_string(raw.size()) + " bytes, hex: " +
                                 to_hex(raw.substr(0, std::min<std::size_t>(raw.size(), 32))) +
                                 "]");
                do_read();
                return;
            }
        } else {
            msg = raw;
        }

        push_debug_frame(msg);

        ParseResult pr = parse_message(type_, msg, local_ts, actx_);

        if (!pr.debug_warning.empty()) {
            static std::ofstream errlog("logs/errors.log", std::ios::app);
            errlog << now_ms() << " " << cfg_.name << " " << pr.debug_warning << "\n";
            errlog.flush();
        }

        if (pr.kind == MsgKind::Pong) {
            enqueue_write(pr.pong_payload);
            do_read();
            return;
        }

        if (pr.kind == MsgKind::Price) {
            if (!got_price_) {
                got_price_ = true;
                debug_timer_.cancel();
            }
            deliver_price(pr.symbol_idx, pr.snap);
        }

        do_read();
    }

    // ───── write queue: serializes subscribe/pong/ping-timer writes ─────
    void enqueue_write(std::string payload) {
        if (payload.empty()) return;
        write_queue_.push_back(std::move(payload));
        if (!writing_) do_write();
    }

    void do_write() {
        if (write_queue_.empty()) {
            writing_ = false;
            return;
        }
        writing_  = true;
        auto self = shared_from_this();
        ws_->async_write(
            net::buffer(write_queue_.front()),
            [self](beast::error_code ec, std::size_t) {
                if (ec) return self->fail("write", ec);
                self->write_queue_.pop_front();
                self->do_write();
            });
    }

    // ───── per-connection app-level keepalive ping ─────
    void start_ping_timer(int interval_sec) {
        ping_timer_.expires_after(std::chrono::seconds(interval_sec));
        auto self = shared_from_this();
        ping_timer_.async_wait([self, interval_sec](beast::error_code ec) {
            if (ec) return;
            self->enqueue_write(ping_payload(self->type_, self->actx_));
            self->start_ping_timer(interval_sec);
        });
    }

    // ───── debug: dump last 3 raw frames if no price within 15s ─────
    void init_debug_timer() {
        debug_timer_.expires_after(std::chrono::seconds(15));
        auto self = shared_from_this();
        debug_timer_.async_wait([self](beast::error_code ec) {
            if (ec) return;  // cancelled: price arrived, or connection torn down
            if (self->got_price_) return;
            std::cout << "[DEBUG] " << self->cfg_.name
                      << ": no price data in 15s — last "
                      << self->recent_frames_.size() << " raw frames:\n";
            for (auto& f : self->recent_frames_) std::cout << "  " << f << "\n";
            std::cout.flush();
        });
    }

    void push_debug_frame(const std::string& s) {
        std::string f = s;
        if (f.size() > 400) f = f.substr(0, 400) + "...(truncated)";
        recent_frames_.push_back(std::move(f));
        while (recent_frames_.size() > 3) recent_frames_.pop_front();
    }

    void deliver_price(int symbol_idx, const FeedSnapshot& snap);  // defined after Monitor
    void mark_connected(bool up);                                  // defined after Monitor
    void init_adapter_context();                                   // defined after Monitor

    net::io_context&     ioc_;
    ssl::context&        ssl_ctx_;
    Monitor&             mon_;
    int                  index_;
    FeedConfig           cfg_;
    FeedType             type_;

    tcp::resolver        resolver_;
    net::steady_timer    reconnect_timer_;
    net::steady_timer    ping_timer_;
    net::steady_timer    debug_timer_;
    std::unique_ptr<WsStream> ws_;
    beast::flat_buffer   buffer_;

    std::deque<std::string> write_queue_;
    bool                     writing_ = false;

    bool                     got_price_ = false;
    std::deque<std::string>  recent_frames_;

    AdapterContext actx_;

    std::string host_, port_, path_;
};

// ─────────────────────────────── Monitor ───────────────────────────────
class Monitor {
public:
    Monitor(net::io_context& ioc, ssl::context& ctx, AppConfig cfg)
        : ioc_(ioc), ssl_ctx_(ctx), cfg_(std::move(cfg)), display_timer_(ioc) {
        std::filesystem::create_directories("logs");

        reference_.resize(cfg_.reference_symbols.size());

        // one ExchangeState (+ one CSV per symbol) per target
        for (const auto& t : cfg_.targets) {
            ExchangeState ex;
            ex.name = t.name;
            ex.type = t.type;
            for (const auto& sym : t.symbols) {
                SymbolState ss;
                ss.display       = sym.display;
                ss.rolling_window = cfg_.rolling_window;
                open_csv(ex.name, sym.display, ss);
                ex.symbols.push_back(std::move(ss));
            }
            exchanges_.push_back(std::move(ex));
        }
    }

    void run() {
        // reference feed: Binance combined stream over all configured symbols
        FeedConfig ref_cfg;
        ref_cfg.name    = cfg_.reference_name;
        ref_cfg.type    = cfg_.reference_type;
        ref_cfg.url     = build_binance_combined_url(cfg_.reference_base_url, cfg_.reference_symbols);
        ref_cfg.symbols = cfg_.reference_symbols;

        auto ref = std::make_shared<FeedConnection>(ioc_, ssl_ctx_, *this, -1, ref_cfg);
        conns_.push_back(ref);
        ref->start();

        // target feeds
        for (std::size_t i = 0; i < cfg_.targets.size(); ++i) {
            auto c = std::make_shared<FeedConnection>(
                ioc_, ssl_ctx_, *this, static_cast<int>(i), cfg_.targets[i]);
            conns_.push_back(c);
            c->start();
        }

        schedule_display();
    }

    // ───── called from FeedConnection ─────
    void on_price(int index, int symbol_idx, const FeedSnapshot& snap) {
        if (symbol_idx < 0) return;
        if (index < 0) {
            if (symbol_idx < (int)reference_.size()) reference_[symbol_idx] = snap;
            return;
        }
        ExchangeState& ex = exchanges_[index];
        if (symbol_idx >= (int)ex.symbols.size()) return;
        SymbolState& ss = ex.symbols[symbol_idx];
        ss.latest       = snap;
        compute_metrics(ex, ss, symbol_idx);
    }

    void on_status(int index, bool up) {
        if (index < 0) {
            reference_connected_ = up;
            if (!up) for (auto& r : reference_) r.valid = false;
            return;
        }
        ExchangeState& ex = exchanges_[index];
        if (up) {
            if (ex.connected == false) {
                // count reconnects after the first connect
                if (ex_connected_once_.count(index)) ex.reconnect_count++;
                ex_connected_once_.insert(index);
            }
            ex.connected = true;
        } else {
            ex.connected = false;
            for (auto& ss : ex.symbols) {
                ss.latest.valid = false;
                ss.cur_valid    = false;
            }
        }
    }

    const std::string& reference_name() const { return cfg_.reference_name; }
    const std::vector<FeedSnapshot>& reference() const { return reference_; }

private:
    void open_csv(const std::string& exch_name, const std::string& display, SymbolState& s) {
        std::string slug = display;
        for (auto& c : slug) if (c == '/') c = '_';
        std::string fn = "logs/" + exch_name + "_" + slug + "_latency.csv";
        bool exists = std::filesystem::exists(fn) &&
                      std::filesystem::file_size(fn) > 0;
        s.csv.open(fn, std::ios::app);
        if (!exists) {
            s.csv << "timestamp_ms,direction,peak_delta_bps,duration_ms,"
                     "binance_mid,target_mid,exch_lag_ms\n";
            s.csv.flush();
        }
    }

    bool reference_usable(int symbol_idx) const {
        return reference_connected_ && symbol_idx >= 0 &&
               symbol_idx < (int)reference_.size() && reference_[symbol_idx].valid;
    }

    void compute_metrics(ExchangeState& ex, SymbolState& ss, int symbol_idx) {
        if (!reference_usable(symbol_idx) || !ss.latest.valid) {
            ss.cur_valid = false;
            return;
        }
        const FeedSnapshot& ref = reference_[symbol_idx];
        const FeedSnapshot& tgt = ss.latest;

        double  delta_bps = ((tgt.mid - ref.mid) / ref.mid) * 10000.0;
        int64_t exch_lag  = tgt.exch_ts - ref.exch_ts;

        ss.cur_delta_bps = delta_bps;
        ss.cur_lag_ms    = exch_lag;
        ss.cur_valid     = true;

        // rolling window
        ss.lag_samples.push_back(exch_lag);
        ss.delta_samples.push_back(delta_bps);
        while ((int)ss.lag_samples.size() > ss.rolling_window)
            ss.lag_samples.pop_front();
        while ((int)ss.delta_samples.size() > ss.rolling_window)
            ss.delta_samples.pop_front();

        update_signal(ex, ss, delta_bps, tgt, ref);
    }

    void update_signal(ExchangeState& ex, SymbolState& ss, double delta_bps,
                       const FeedSnapshot& tgt, const FeedSnapshot& ref) {
        (void)ex;
        const double absd = std::abs(delta_bps);
        const int64_t t   = now_ms();

        if (!ss.signal_open) {
            if (absd > cfg_.signal_open_bps) {
                if (ss.gap_start_time == 0) {
                    ss.gap_start_time = t;
                } else if (t - ss.gap_start_time > cfg_.signal_min_ms) {
                    ss.signal_open      = true;
                    ss.signal_open_time = ss.gap_start_time;
                    ss.signal_peak_bps  = absd;
                    ss.signal_dir = (delta_bps < 0) ? "CHEAPER" : "RICHER";
                }
            } else {
                ss.gap_start_time = 0;  // require continuous excursion
            }
        } else {
            ss.signal_peak_bps = std::max(ss.signal_peak_bps, absd);
            if (delta_bps < 0)  ss.signal_dir = "CHEAPER";
            else                ss.signal_dir = "RICHER";
            if (absd < cfg_.signal_close_bps) {
                int64_t duration = t - ss.signal_open_time;
                log_signal(ss, duration, tgt, ref);
                ss.signals_total++;
                ss.avg_duration_ms +=
                    (duration - ss.avg_duration_ms) / ss.signals_total;
                ss.signal_open    = false;
                ss.gap_start_time = 0;
                ss.signal_peak_bps = 0.0;
            }
        }
    }

    void log_signal(SymbolState& ss, int64_t duration,
                    const FeedSnapshot& tgt, const FeedSnapshot& ref) {
        if (!ss.csv) return;
        ss.csv << now_ms() << ',' << ss.signal_dir << ','
               << ss.signal_peak_bps << ',' << duration << ',' << ref.mid
               << ',' << tgt.mid << ',' << (tgt.exch_ts - ref.exch_ts) << '\n';
        ss.csv.flush();
    }

    // ───────────────────────── display ─────────────────────────
    struct LagStats { int64_t median = 0, p95 = 0, p99 = 0, max = 0; };

    static int64_t pct(std::vector<int64_t>& v, double p) {
        if (v.empty()) return 0;
        std::size_t idx = std::min(v.size() - 1, (std::size_t)(p * v.size()));
        return v[idx];
    }

    LagStats lag_stats(const SymbolState& ss) {
        LagStats s;
        if (ss.lag_samples.empty()) return s;
        std::vector<int64_t> v(ss.lag_samples.begin(), ss.lag_samples.end());
        std::sort(v.begin(), v.end());
        s.median = v[v.size() / 2];
        s.p95    = pct(v, 0.95);
        s.p99    = pct(v, 0.99);
        s.max    = v.back();
        return s;
    }

    void schedule_display() {
        display_timer_.expires_after(
            std::chrono::seconds(cfg_.stats_interval_sec));
        display_timer_.async_wait([this](beast::error_code ec) {
            if (ec) return;
            render();
            schedule_display();
        });
    }

    static std::string hline(const std::vector<int>& w, const char* l,
                             const char* m, const char* r) {
        std::string s = l;
        for (std::size_t i = 0; i < w.size(); ++i) {
            for (int k = 0; k < w[i]; ++k) s += "═";
            s += (i + 1 < w.size()) ? m : r;
        }
        return s;
    }
    // pad plain text (no color codes) to width, left or right aligned
    static std::string padc(const std::string& s, int w, bool right) {
        int len = (int)s.size();
        if (len >= w) return s.substr(0, w);
        std::string pad(w - len, ' ');
        return right ? (pad + s) : (s + pad);
    }

    static bool is_usd_pair(const std::string& display) {
        return display.size() >= 4 && display.substr(display.size() - 4) == "/USD";
    }

    void render() {
        if (std::system("clear")) {}
        const std::vector<int> W = {14, 12, 11, 9, 9};

        char tbuf[16];
        std::time_t tt = std::time(nullptr);
        std::strftime(tbuf, sizeof(tbuf), "%H:%M:%S", std::localtime(&tt));

        std::cout << col::bold << col::cyan << "  LATENCY MONITOR  " << tbuf
                  << col::reset << "\n\n";

        bool any_usd_pair = false;

        for (std::size_t sidx = 0; sidx < cfg_.reference_symbols.size(); ++sidx) {
            const std::string& display = cfg_.reference_symbols[sidx].display;

            std::ostringstream refline;
            refline << "  " << col::bold << display << col::reset << "   ref: ";
            if (reference_usable((int)sidx))
                refline << col::green << std::fixed << std::setprecision(2)
                        << reference_[sidx].mid << col::reset;
            else
                refline << col::red << "NO REFERENCE" << col::reset;
            std::cout << refline.str() << "\n";

            std::cout << hline(W, "╔", "╦", "╗") << "\n";
            std::cout << "║" << padc(" Exchange", W[0], false)
                      << "║" << padc(" Mid", W[1], false)
                      << "║" << padc(" Delta bps", W[2], false)
                      << "║" << padc(" Lag med", W[3], false)
                      << "║" << padc(" Signals", W[4], false) << "║\n";
            std::cout << hline(W, "╠", "╬", "╣") << "\n";

            for (auto& ex : exchanges_) {
                if (sidx >= ex.symbols.size()) continue;
                SymbolState& ss = ex.symbols[sidx];

                std::string label = ex.name;
                if (is_usd_pair(ss.display)) {
                    label += "*";
                    any_usd_pair = true;
                }

                if (!ex.connected) {
                    std::cout << "║" << col::red
                              << padc(" " + label, W[0], false) << col::reset
                              << "║" << padc(" -", W[1], false)
                              << "║" << padc(" -", W[2], false)
                              << "║" << padc(" -", W[3], false)
                              << "║" << padc(" " + std::to_string(ss.signals_total), W[4], false)
                              << "║\n";
                    continue;
                }

                LagStats ls = lag_stats(ss);
                std::ostringstream mid, delta, lm, sg;
                mid << std::fixed << std::setprecision(2) << ss.latest.mid;
                if (ss.cur_valid)
                    delta << std::showpos << std::fixed << std::setprecision(1)
                          << ss.cur_delta_bps;
                else
                    delta << "-";
                lm << ls.median << "ms";
                sg << ss.signals_total;

                const char* dcol = col::white;
                double absd = std::abs(ss.cur_delta_bps);
                if (ss.cur_valid) {
                    if (absd > cfg_.signal_open_bps)        dcol = col::green;
                    else if (absd >= cfg_.signal_close_bps) dcol = col::yellow;
                }

                std::cout << "║" << padc(" " + label, W[0], false)
                          << "║" << padc(" " + mid.str(), W[1], false)
                          << "║" << dcol << padc(" " + delta.str(), W[2], false)
                          << col::reset
                          << "║" << padc(" " + lm.str(), W[3], false)
                          << "║" << padc(" " + sg.str(), W[4], false) << "║\n";
            }
            std::cout << hline(W, "╚", "╩", "╝") << "\n\n";
        }

        if (any_usd_pair)
            std::cout << "  * USD pair (not USDT) — delta vs the USDT reference "
                         "may include a stablecoin basis spread\n\n";

        // active signals
        std::cout << "  Active signals:\n";
        bool any = false;
        for (auto& ex : exchanges_) {
            for (std::size_t sidx = 0; sidx < ex.symbols.size(); ++sidx) {
                SymbolState& ss = ex.symbols[sidx];
                if (!ss.signal_open) continue;
                any              = true;
                int64_t open_ms  = now_ms() - ss.signal_open_time;
                int     fill     = std::min(10, (int)(open_ms / 50));
                std::string bar;
                for (int i = 0; i < 10; ++i) bar += (i < fill) ? "█" : "░";
                std::cout << "    " << col::green << ex.name << col::reset << " "
                          << ss.display << "  → " << ss.signal_dir << " by "
                          << std::fixed << std::setprecision(1)
                          << ss.signal_peak_bps << " bps  open " << open_ms
                          << "ms  [" << bar << "]\n";
            }
        }
        if (!any) std::cout << "    (none)\n";
        std::cout << "\n";

        // connections line(s)
        std::cout << "  Connections: " << cfg_.reference_name << " "
                  << (reference_connected_ ? "✓" : "✗") << "   ";
        int n = 0;
        for (auto& ex : exchanges_) {
            std::cout << ex.name << " ";
            if (ex.connected)
                std::cout << col::green << "✓" << col::reset;
            else
                std::cout << col::red << "✗(attempt " << ex.reconnect_count << ")"
                          << col::reset;
            std::cout << "   ";
            if (++n % 4 == 0) std::cout << "\n  ";
        }
        std::cout << "\n";

        if (!reference_connected_)
            std::cout << "\n  " << col::red << col::bold
                      << "*** NO REFERENCE — deltas invalid, signals paused ***"
                      << col::reset << "\n";
        std::cout.flush();
    }

    net::io_context&  ioc_;
    ssl::context&     ssl_ctx_;
    AppConfig         cfg_;
    net::steady_timer display_timer_;

    std::vector<FeedSnapshot> reference_;
    bool                      reference_connected_ = false;

    std::vector<ExchangeState>                   exchanges_;
    std::vector<std::shared_ptr<FeedConnection>> conns_;
    std::set<int>                                ex_connected_once_;
};

// ───── FeedConnection methods that depend on Monitor ─────
void FeedConnection::deliver_price(int symbol_idx, const FeedSnapshot& snap) {
    mon_.on_price(index_, symbol_idx, snap);
}
void FeedConnection::mark_connected(bool up) { mon_.on_status(index_, up); }

void FeedConnection::init_adapter_context() {
    actx_.ref_snapshots = &mon_.reference();
}

void FeedConnection::schedule_reconnect() {
    mark_connected(false);
    auto self = shared_from_this();
    reconnect_timer_.expires_after(std::chrono::seconds(2));
    reconnect_timer_.async_wait([self](beast::error_code ec) {
        if (ec) return;
        self->connect();
    });
}

// ─────────────────────────────── main ──────────────────────────────────
int main(int argc, char** argv) {
    std::string config_path = (argc > 1) ? argv[1] : "exchanges.json";
    try {
        AppConfig cfg = load_config(config_path);

        net::io_context ioc;
        ssl::context    ctx(ssl::context::tls_client);
        ctx.set_default_verify_paths();
        ctx.set_verify_mode(ssl::verify_none);  // monitoring tool: no mTLS

        Monitor mon(ioc, ctx, cfg);
        mon.run();

        net::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&](const beast::error_code&, int) {
            if (std::system("clear")) {}
            std::cout << "shutting down...\n";
            ioc.stop();
        });

        ioc.run();
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
