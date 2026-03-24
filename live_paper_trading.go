package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// РўРµСЃС‚РѕРІС‹Рµ С„Р»Р°РіРё РѕРєСЂСѓР¶РµРЅРёСЏ (РґР»СЏ РѕС‚Р»Р°РґРєРё, РЅРµ РґР»СЏ В«Р±РѕРµРІРѕР№В» РѕС…РѕС‚С‹ Р·Р° РєР°С‡РµСЃС‚РІРѕРј).
func envSkipVelocity() bool { return strings.TrimSpace(os.Getenv("SKIP_VELOCITY")) == "1" }
func envSkipVelocityValue() (val bool, set bool) {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("SKIP_VELOCITY")))
	switch s {
	case "1", "true", "yes":
		return true, true
	case "0", "false", "no":
		return false, true
	default:
		return false, false
	}
}
func envSkipAntiScam() bool { return strings.TrimSpace(os.Getenv("SKIP_ANTI_SCAM")) == "1" }
func turboModeEnabled() bool {
	if !(liveTradingEnabled() && envSignalProfile() == "aggressive") {
		return false
	}
	s := strings.TrimSpace(strings.ToLower(os.Getenv("AUTO_TURBO_MODE")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return true
}
func shouldSkipVelocity() bool {
	if v, set := envSkipVelocityValue(); set {
		return v
	}
	// В turbo-mode режем velocity-check ради скорости входа.
	return turboModeEnabled()
}
func ultraFastEntryMode() bool {
	if !liveTradingEnabled() {
		return false
	}
	s := strings.TrimSpace(strings.ToLower(os.Getenv("ULTRA_FAST_ENTRY")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	if s == "1" || s == "true" || s == "yes" {
		return true
	}
	// РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РІ live+aggressive turbo РІРєР»СЋС‡Р°РµРј РјР°РєСЃРёРјР°Р»СЊРЅРѕ Р±С‹СЃС‚СЂС‹Р№ РІС…РѕРґ.
	return turboModeEnabled()
}
func shouldSkipAntiScam() bool {
	if envSkipAntiScam() {
		return true
	}
	// Р’ live+aggressive РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ СЂРµР¶РµРј Р°РЅС‚Рё-СЃРєР°Рј СЂР°РґРё СЃРєРѕСЂРѕСЃС‚Рё РІС…РѕРґР°.
	if liveTradingEnabled() && envSignalProfile() == "aggressive" {
		s := strings.TrimSpace(strings.ToLower(os.Getenv("AUTO_SKIP_ANTI_SCAM")))
		if s == "0" || s == "false" || s == "no" {
			return false
		}
		return true
	}
	return false
}
func velocityPause() time.Duration {
	if s := strings.TrimSpace(os.Getenv("VELOCITY_PAUSE_MS")); s != "" {
		if ms, err := strconv.Atoi(s); err == nil && ms >= 150 && ms <= 800 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return VELOCITY_PAUSE
}
func envSignalProfile() string {
	p := strings.TrimSpace(strings.ToLower(os.Getenv("SIGNAL_PROFILE")))
	switch p {
	case "strict", "balanced", "aggressive":
		return p
	default:
		return "balanced"
	}
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  РљРћРќР¤РР“
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

const (
	HELIUS_API_KEY = "1859e3e5-5d82-476e-a121-8d5f57705cf7"
	PAPER_BALANCE  = 7.0
	// Р”РѕР»СЏ Р±Р°Р»Р°РЅСЃР° РЅР° РѕРґРЅСѓ СЃРґРµР»РєСѓ: РїСЂРё $7 в†’ ~$1; РїРѕСЃР»Рµ РїР»СЋСЃР° СЃС‚Р°РІРєР° СЃС‡РёС‚Р°РµС‚СЃСЏ РѕС‚ РЅРѕРІРѕРіРѕ Р±Р°РЅРєР°
	BET_PCT_OF_BALANCE = 1.0 / 7.0
	MIN_STAKE_USD      = 0.25 // РЅРµ РѕС‚РєСЂС‹РІР°РµРј РїРѕР·РёС†РёСЋ РјРµР»СЊС‡Рµ (РїС‹Р»СЊ / С€СѓРј)
	MAX_POSITIONS      = 1    // СЃРЅР°Р№РїРµСЂ: РѕРґРЅР° РїРѕР·РёС†РёСЏ вЂ” С„РѕРєСѓСЃ Рё РјРµРЅСЊС€Рµ С€СѓРјР°

	// Pump.fun: Global fee_basis_points = 100 в†’ 1% СЃ РїРѕРєСѓРїРєРё Рё СЃ РїСЂРѕРґР°Р¶Рё (РґРѕРєСѓРјРµРЅС‚Р°С†РёСЏ РїСЂРѕРіСЂР°РјРјС‹)
	PUMP_FEE_BPS = 100
	// РџСЂРѕСЃРєР°Р»СЊР·С‹РІР°РЅРёРµ РґР»СЏ paper-РѕС†РµРЅРєРё (live РґР»СЏ pump Р±РµСЂС‘С‚СЃСЏ РёР· pump_direct.go РѕС‚РґРµР»СЊРЅРѕ).
	SLIPPAGE_BPS = 49
	// РћС†РµРЅРєР° СЃРµС‚Рё РЅР° РѕРґРЅСѓ РїРѕРґРїРёСЃСЊ (Р±СѓРјР°РіР°); live вЂ” РїРѕ С„Р°РєС‚Сѓ RPC / pump
	SOLANA_TX_LAMPORTS = 12_000.0

	PRICE_TICK         = 1500 * time.Millisecond // 1.5s вЂ” С‡Р°С‰Рµ Р»РѕРІРёРј РїР°РјРї
	MAX_HOLD           = 300 * time.Second       // 5 РјРёРЅСѓС‚ вЂ” РґР°С‘Рј С‚РѕРєРµРЅСѓ В«РїРѕРґС‹С€Р°С‚СЊВ»
	FAST_EXIT_AFTER    = 300 * time.Second       // РµСЃР»Рё Р·Р° 300СЃ РЅРµС‚ СЂРѕСЃС‚Р° вЂ” РІС‹С…РѕРґРёРј
	FAST_EXIT_MIN_MULT = 1.00                    // 0% (Р±РµР· РїР»СЋСЃР°)
	// РЎРµСЂРІРёСЃРЅС‹Рµ РёРЅС‚РµСЂРІР°Р»С‹/Р»РёРјРёС‚С‹ RPC РґР»СЏ Р·Р°С‰РёС‚С‹ РѕС‚ -32429.
	BALANCE_CHECK_INTERVAL = 30 * time.Second
	RPC_RETRY_BASE_DELAY   = 250 * time.Millisecond
	RPC_MAX_RETRIES        = 4
	RPC_MAX_CONCURRENT     = 8

	// Recovery Mode ($3.9): СѓР·РєРѕРµ РѕРєРЅРѕ + Р»РёРєРІРёРґРЅРѕСЃС‚СЊ, С‡С‚РѕР±С‹ РЅРµ Р±СЂР°С‚СЊ В«РїСѓСЃС‚С‹РµВ» РјС‘СЂС‚РІС‹Рµ РїСѓР»С‹.
	SNIPER_CURVE_MIN           = 0.0  // 0.0%
	SNIPER_CURVE_MAX           = 0.25 // 25%
	MIN_REAL_SOL               = 0.15 // РјРёРЅРёРјСѓРј 0.15 SOL РІ РєСЂРёРІРѕР№
	FAST_HEAVY_CHECK_CURVE_MAX = 0.05 // С‚СЏР¶С‘Р»С‹Рµ RPC-С„РёР»СЊС‚СЂС‹ С‚РѕР»СЊРєРѕ РґРѕ 5% РєСЂРёРІРѕР№
	CREATOR_BALANCE_CACHE_TTL  = 5 * time.Minute

	// РђРЅС‚Рё-СЃРєР°Рј
	CREATOR_SOL_MIN       = 2.0 // min 2 SOL Сѓ РґРµРІР° вЂ” С‚РѕР»СЊРєРѕ В«Р¶РёСЂРЅС‹РµВ», РЅРµ rug
	CREATOR_SOL_SUSPECT   = 80.0
	MAX_NONCURVE_PCT      = 0.12
	TOP10_HOLDERS_MAX_PCT = 0.30 // С‚РѕРї-10 (excl curve) >30% вЂ” РєР»Р°СЃС‚РµСЂ, СЃРѕР»СЊСЋС‚
	DEV_MAX_TXS_HOUR      = 7    // dev >7 tx/С‡Р°СЃ вЂ” serial rugger (СЃРѕР·РґР°С‘С‚ 4+ С‚РѕРєРµРЅРѕРІ)

	// Р’С‹С…РѕРґС‹ Final Recovery: hard SL -30%; TP С‚РѕР»СЊРєРѕ РѕС‚ +150%.
	STOP_LOSS_HARD      = 0.70 // -30%
	STOP_CONFIRM_LVL    = 0.70 // -30%
	STOP_CONFIRM_N      = 1
	SELL_SLIPPAGE_GUARD = 0.10            // >10% РѕР¶РёРґР°РµРјРѕРіРѕ slip РЅР° РІС‹С…РѕРґРµ вЂ” РїРѕРґРѕР¶РґР°С‚СЊ СЃР»РµРґСѓСЋС‰РёР№ С‚РёРє
	TAKE_PROFIT         = 1.75            // +75% вЂ” С„РёРєСЃРёСЂСѓРµРј РІРµСЃСЊ РѕР±СЉС‘Рј
	TRAIL_ACTIVATE      = 1.30            // С‚СЂРµР№Р»РёРЅРі РїРѕСЃР»Рµ +30%
	TRAILING            = 0.12            // РѕС‚РєР°С‚ 12% РѕС‚ РїРёРєР° (Р±С‹Р»Рѕ 16%)
	TRAIL_MIN_AGE       = 5 * time.Second // Р±С‹Р» 10s вЂ” С‚СЂРµР№Р» СЂР°РЅСЊС€Рµ
	TRAIL_MIN_PROFIT    = 1.05            // РїРѕР» +5% (Р±С‹Р»Рѕ +10%)
	BREAKEVEN_ARM       = 1.30            // РїРѕСЃР»Рµ +30% СЃС‚Р°РІРёРј СЃС‚РѕРї РІ РЅРѕР»СЊ
	SCRATCH_AFTER       = 2 * time.Minute // РЅРµ Р·Р°РІРёСЃР°РµРј РІ С„Р»СЌС‚Рµ СЃР»РёС€РєРѕРј РґРѕР»РіРѕ
	SCRATCH_IF_BELOW    = 0.97            // СЃРєСЂРµС‚С‡ С‚РѕР»СЊРєРѕ РµСЃР»Рё СЃРѕРІСЃРµРј РїР»РѕСЃРєРѕ
	NO_IMPULSE_AFTER    = 4 * time.Minute // В«РЅРµС‚ РёРјРїСѓР»СЊСЃР°В» вЂ” РїРѕСЃР»Рµ СѓРјРµСЂРµРЅРЅРѕР№ РєРѕРЅСЃРѕР»РёРґР°С†РёРё
	NO_IMPULSE_NEED     = 1.04            // РїРёРє РґРѕР»Р¶РµРЅ С…РѕС‚СЏ Р±С‹ +4% Рє РІС…РѕРґСѓ, РёРЅР°С‡Рµ РІС‹С…РѕРґ

	// Р•СЃР»Рё create-С‚СЂР°РЅР·Р°РєС†РёСЏ СЃС‚Р°СЂС€Рµ вЂ” РЅРµ СЃС‡РёС‚Р°РµРј В«С‚РѕР»СЊРєРѕ С‡С‚Рѕ Р·Р°Р»РёСЃС‚РёР»РёСЃСЊВ» (Р·Р°С‰РёС‚Р° РѕС‚ РєСЂРёРІС‹С… СЃРёРіРЅР°Р»РѕРІ)
	MAX_CREATE_TX_AGE       = 30 * time.Minute
	MAX_READY_TO_SEND_DELAY = 4000 * time.Millisecond

	VELOCITY_PAUSE         = 150 * time.Millisecond // Р±С‹СЃС‚СЂРµРµ СЂРµР°РєС†РёСЏ РЅР° РёРјРїСѓР»СЊСЃ
	VELOCITY_MIN_DPROGRESS = 0.02                   // min +2% Р·Р° РїР°СѓР·Сѓ вЂ” С‚РѕР»СЊРєРѕ РїРµСЂРІР°СЏ РІРѕР»РЅР°
	VELOCITY_MIN_DREALSOL  = 0.0
	VELOCITY_MIN_DELTA_DP  = -0.0001 // -0.01% (СЂР°Р·СЂРµС€Р°РµРј РјРёРєСЂРѕ-РѕС‚РєР°С‚ РЅР° Р·Р°РјРµСЂРµ)
	LIVE_FIXED_BUY_SOL     = 0.04    // С„РёРєСЃРёСЂРѕРІР°РЅРЅР°СЏ СЃС‚Р°РІРєР° РІ live
	LIVE_BUY_BALANCE_SHARE = 0.85    // legacy (РЅРµ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РІ fixed live buy)
	ACTIVE_POSITIONS_FILE  = "current_trades.json"

	// Р›РѕРіРё: false = РЅРµ РїРµС‡Р°С‚Р°С‚СЊ РєР°Р¶РґС‹Р№ РѕС‚СЃРµРІ (С‚РѕР»СЊРєРѕ СЃРІРѕРґРєР° СЂР°Р· РІ РјРёРЅСѓС‚Сѓ + СѓСЃРїРµС€РЅС‹Р№ Р’РҐРћР”)
	VERBOSE_REJECT_LOGS = false
)

// Wrapped SOL mint (wrap/СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚СЊ)
const WSOL_MINT = "So11111111111111111111111111111111111111112"

// Р§Р°СЃС‚С‹Рµ SPL РІ tx вЂ” РЅРµ mint pump-РјРѕРЅРµС‚С‹ (СЂР°РЅСЊС€Рµ С†РµРїР»СЏР»Рё РїРµСЂРІС‹Р№ Р±Р°Р»Р°РЅСЃ в†’ USDC Рё С‚.Рґ.)
var ignoredTokenMints = map[string]bool{
	WSOL_MINT: true,
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": true, // USDC
	"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": true, // USDT
}

// РЎС‡С‘С‚С‡РёРєРё РѕС‚СЃРµРІР° (Р±РµР· СЃРїР°РјР° РІ РєРѕРЅСЃРѕР»СЊ)
var (
	rejectMu           sync.Mutex
	rejectCounts       = map[string]int64{}
	rejectPrevSnapshot map[string]int64 // РґР»СЏ РґРµР»СЊС‚С‹ Р·Р° РјРёРЅСѓС‚Сѓ
	// Р•РґРёРЅС‹Р№ РІС‹РІРѕРґ: stats Рё РѕС‚СЃРµРІ РёРґСѓС‚ РёР· СЂР°Р·РЅС‹С… РіРѕСЂСѓС‚РёРЅ вЂ” Р±РµР· РјСЊСЋС‚РµРєСЃР° СЃС‚СЂРѕРєРё РїРµСЂРµРјРµС€РёРІР°СЋС‚СЃСЏ
	consoleMu sync.Mutex
)

// Р’РѕСЂРѕРЅРєР° РІС…РѕРґР° (РЅР°РєРѕРїРёС‚РµР»СЊРЅРѕ СЃ Р·Р°РїСѓСЃРєР°) вЂ” РІРёРґРЅРѕ, РґРѕ РєР°РєРѕРіРѕ С€Р°РіР° РґРѕС…РѕРґСЏС‚ С‚РѕРєРµРЅС‹
var (
	funnelInWindow  int64 // РїСЂРѕС€Р»Рё РєСЂРёРІСѓСЋ+Р»РёРєРІРёРґРЅРѕСЃС‚СЊ, РґРѕ velocity
	funnelPassVel   int64 // РїСЂРѕС€Р»Рё velocity
	funnelPassScam  int64 // РїСЂРѕС€Р»Рё Р°РЅС‚Рё-СЃРєР°Рј
	funnelOpenOK    int64 // open() = true
	funnelOpenFail  int64 // open() = false (СЂРµРґРєРѕ: Р±Р°Р»Р°РЅСЃ/Р»РёРјРёС‚)
	liveBuyInFlight int32 // Р·Р°С‰РёС‚Р° РѕС‚ РїР°СЂР°Р»Р»РµР»СЊРЅС‹С… buy РІ live
	feeGuardState   struct {
		mu    sync.Mutex
		until time.Time
	}
)

// РџРѕРґРїРёСЃРё РєР»СЋС‡РµР№ РІ РјРёРЅСѓС‚РЅРѕР№ СЃРІРѕРґРєРµ (СЂСѓСЃ./РєСЂР°С‚РєРѕ)
var rejectLabelRU = map[string]string{
	"no_mint":  "РЅРµС‚_mint",
	"not_pump": "РЅРµ_pump",
	"stale_tx": "СЃС‚Р°СЂС‹Р№_tx",
	"no_price": "РЅРµС‚_С†РµРЅС‹",
	"complete": "graduated",
	"empty":    "РїСѓСЃС‚Рѕ",
	"late":     "РїРѕР·РґРЅРѕ",
	"low_sol":  "РјР°Р»Рѕ_SOL",
	"velocity": "velocity",
	"vel_rpc":  "vel_RPC",
	"vel_low":  "vel_РјР°Р»Рѕ",
	"vel_late": "vel_РїРѕР·РґРЅРѕ",
	"scam":     "СЃРєР°Рј",
	"pda_err":  "PDA",
}

func rejectKeyLabel(k string) string {
	if s, ok := rejectLabelRU[k]; ok {
		return s
	}
	return k
}

func rejectBump(key string) {
	rejectMu.Lock()
	rejectCounts[key]++
	rejectMu.Unlock()
}

func logRejectLine(key, sym, mint, detail string) {
	rejectBump(key)
	if !VERBOSE_REJECT_LOGS {
		return
	}
	if detail != "" {
		fmt.Printf("%s %-18s %s\n", gray("вЂ”"), sym, detail)
	} else {
		fmt.Printf("%s %-18s %s\n", gray("вЂ”"), sym, key)
	}
	if mint != "" {
		fmt.Printf("   %s %s\n", gray("DEX"), cyan("https://dexscreener.com/solana/"+mint))
	}
}

func printRejectSummary() {
	rejectMu.Lock()
	defer rejectMu.Unlock()
	if rejectPrevSnapshot == nil {
		rejectPrevSnapshot = make(map[string]int64)
	}
	order := []string{"no_mint", "not_pump", "stale_tx", "no_price", "complete", "empty", "late", "low_sol", "velocity", "vel_rpc", "vel_low", "vel_late", "scam", "pda_err"}
	seen := make(map[string]bool, len(order))

	var deltaParts []string
	for _, k := range order {
		seen[k] = true
		v := rejectCounts[k]
		prev := rejectPrevSnapshot[k]
		if d := v - prev; d > 0 {
			deltaParts = append(deltaParts, fmt.Sprintf("%s +%d", rejectKeyLabel(k), d))
		}
	}
	for k, v := range rejectCounts {
		if seen[k] {
			continue
		}
		if d := v - rejectPrevSnapshot[k]; d > 0 {
			deltaParts = append(deltaParts, fmt.Sprintf("%s +%d", rejectKeyLabel(k), d))
		}
	}

	var totalParts []string
	for _, k := range order {
		if v := rejectCounts[k]; v > 0 {
			totalParts = append(totalParts, fmt.Sprintf("%s=%d", rejectKeyLabel(k), v))
		}
	}
	for k, v := range rejectCounts {
		if !seen[k] && v > 0 {
			totalParts = append(totalParts, fmt.Sprintf("%s=%d", rejectKeyLabel(k), v))
		}
	}

	for k, v := range rejectCounts {
		rejectPrevSnapshot[k] = v
	}

	if len(totalParts) == 0 {
		return
	}
	deltaStr := strings.Join(deltaParts, " В· ")
	if deltaStr == "" {
		deltaStr = "С‚РёС…Рѕ"
	}
	consoleMu.Lock()
	defer consoleMu.Unlock()
	fmt.Printf("%s Р·Р° РјРёРЅ: %s  |  РІСЃРµРіРѕ: %s\n", gray("рџ“Љ РѕС‚СЃРµРІ"), deltaStr, strings.Join(totalParts, " В· "))
}

func printFunnelLine() {
	consoleMu.Lock()
	defer consoleMu.Unlock()
	iw := atomic.LoadInt64(&funnelInWindow)
	v := atomic.LoadInt64(&funnelPassVel)
	sc := atomic.LoadInt64(&funnelPassScam)
	ok := atomic.LoadInt64(&funnelOpenOK)
	bad := atomic.LoadInt64(&funnelOpenFail)
	fmt.Printf("%s РІРѕСЂРѕРЅРєР° (СЃ Р·Р°РїСѓСЃРєР°): РІ_РѕРєРЅРµ %d в†’ velocity %d в†’ СЃРєР°Рј %d в†’ РІС…РѕРґ ok %d",
		gray("в—Ћ"), iw, v, sc, ok)
	if bad > 0 {
		fmt.Printf(" В· РІС…РѕРґ fail %d", bad)
	}
	fmt.Println()
	if iw == 0 {
		fmt.Println(gray("   (РµСЃР»Рё В«РІ_РѕРєРЅРµВ»=0 вЂ” РїРѕС‡С‚Рё РІСЃС‘ РѕС‚СЃРµРєР°РµС‚СЃСЏ РґРѕ velocity: no_price / РїСѓСЃС‚Рѕ / РїРѕР·РґРЅРѕ / low_sol)"))
	} else if v == 0 {
		fmt.Println(gray(fmt.Sprintf("   (Р±С‹Р»Рё РІ РѕРєРЅРµ РєСЂРёРІРѕР№, РЅРѕ velocity РµС‰С‘ РЅРё СЂР°Р·Сѓ РЅРµ РїСЂРѕС€С‘Р» вЂ” СѓР·РєРѕРµ РјРµСЃС‚Рѕ; РїСЂРѕС„=%s)", envSignalProfile())))
	} else if sc == 0 {
		fmt.Println(gray("   (velocity Р±С‹Р», Р°РЅС‚Рё-СЃРєР°Рј РїРѕРєР° РЅРµ РїСЂРѕРїСѓСЃС‚РёР» РЅРё РѕРґРЅРѕРіРѕ)"))
	}
}

func printEntryPace() {
	recentEntryMu.Lock()
	times := append([]time.Time(nil), recentEntryTimes...)
	recentEntryMu.Unlock()
	consoleMu.Lock()
	defer consoleMu.Unlock()
	if len(times) < 2 {
		fmt.Printf("%s РІС…РѕРґРѕРІ РїРѕРєР° РјР°Р»Рѕ РґР»СЏ СЃСЂРµРґРЅРµРіРѕ РёРЅС‚РµСЂРІР°Р»Р° (РЅСѓР¶РЅРѕ в‰Ґ2)\n", gray("в—Ћ"))
		return
	}
	lastGap := times[len(times)-1].Sub(times[len(times)-2])
	n := len(times) - 1
	if n > 25 {
		n = 25
	}
	startIdx := len(times) - 1 - n
	if startIdx < 0 {
		startIdx = 0
	}
	var sum time.Duration
	intervals := 0
	for i := startIdx + 1; i < len(times); i++ {
		sum += times[i].Sub(times[i-1])
		intervals++
	}
	if intervals <= 0 {
		return
	}
	avg := sum / time.Duration(intervals)
	fmt.Printf("%s СЃРґРµР»РєРё: РІС…РѕРґРѕРІ %d В· РїРѕСЃР»РµРґРЅРёР№ РёРЅС‚РµСЂРІР°Р» %v В· СЃСЂРµРґРЅРёР№ %v (С†РµР»СЊ ~2 РјРёРЅ РІ paper вЂ” РЅРµ РіР°СЂР°РЅС‚РёСЏ, Р·Р°РІРёСЃРёС‚ РѕС‚ СЃРµС‚Рё)\n",
		gray("в—Ћ"), len(times), lastGap.Round(time.Second), avg.Round(time.Second))
}

const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

// LAUNCHLAB_PROGRAM вЂ” Raydium LaunchLab (mainnet), СЃРј. docs.raydium.io
const LAUNCHLAB_PROGRAM = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"

// Pump.fun: decimals Сѓ РјРѕРЅРµС‚ РЅР° РєСЂРёРІРѕР№ = 6 (РєР°Рє РІ Global / bonding curve)
const pumpTokenDecimals = 6

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  Р¦Р’Р•РўРђ
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

var (
	green  = func(s string) string { return "\033[32m" + s + "\033[0m" }
	red    = func(s string) string { return "\033[31m" + s + "\033[0m" }
	yellow = func(s string) string { return "\033[33m" + s + "\033[0m" }
	cyan   = func(s string) string { return "\033[36m" + s + "\033[0m" }
	bold   = func(s string) string { return "\033[1m" + s + "\033[0m" }
	gray   = func(s string) string { return "\033[90m" + s + "\033[0m" }
)

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  РЎРўР РЈРљРўРЈР Р«
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

type NewToken struct {
	Mint            string
	BondingCurve    string // pump: bonding curve PDA | launchlab: pool state PDA
	Sig             string
	Source          string // "pump" | "launchlab" (РїСѓСЃС‚Рѕ = pump)
	DetectedAt      time.Time
	FiltersPassedAt time.Time
}

// postTokenBal вЂ” СЌР»РµРјРµРЅС‚ meta.postTokenBalances РІ getTransaction (jsonParsed)
type postTokenBal struct {
	Mint string `json:"mint"`
}

type BondingCurve struct {
	VirtualSolReserves   uint64
	VirtualTokenReserves uint64
	RealSolReserves      uint64
	RealTokenReserves    uint64
	TokenTotalSupply     uint64
	Complete             bool
}

type Position struct {
	mu             sync.Mutex
	Mint           string
	BondingCurve   string
	Symbol         string
	EntryPrice     float64 // СЌС„С„РµРєС‚РёРІРЅР°СЏ $/С‚РѕРєРµРЅ РЅР° РІС…РѕРґРµ (РїРѕСЃР»Рµ slip)
	Tokens         float64 // СѓСЃР»РѕРІРЅРѕРµ РєРѕР»-РІРѕ С‚РѕРєРµРЅРѕРІ: USD РІ РїСѓР» / EntryPrice
	PeakPrice      float64
	CapitalUSD     float64 // СЃРєРѕР»СЊРєРѕ USD СЃРЅСЏС‚Рѕ СЃ Р±Р°Р»Р°РЅСЃР° (РіСЂРѕСЃСЃ)
	OpenedAt       time.Time
	BreakevenArmed bool
	// Р‘РѕРµРІРѕР№ СЂРµР¶РёРј: С„Р°РєС‚РёС‡РµСЃРєРёРµ Р»Р°РјРїРѕСЂС‚С‹ РЅР° РІС…РѕРґРµ Рё raw-Р±Р°Р»Р°РЅСЃ SPL РґР»СЏ РїСЂРѕРґР°Р¶Рё
	Live        bool
	TokenRaw    uint64
	BuyLamports uint64
	HalfTaken   bool
	Source      string // pump | launchlab
}

// snapshotPosition вЂ” РєРѕРїРёСЏ РїРѕР»РµР№ Р±РµР· mutex (РґР»СЏ closePos РёР· РіРѕСЂСѓС‚РёРЅС‹ РјРѕРЅРёС‚РѕСЂРёРЅРіР°).
func snapshotPosition(p *Position) *Position {
	return &Position{
		Mint:           p.Mint,
		BondingCurve:   p.BondingCurve,
		Symbol:         p.Symbol,
		EntryPrice:     p.EntryPrice,
		Tokens:         p.Tokens,
		PeakPrice:      p.PeakPrice,
		CapitalUSD:     p.CapitalUSD,
		OpenedAt:       p.OpenedAt,
		BreakevenArmed: p.BreakevenArmed,
		Live:           p.Live,
		TokenRaw:       p.TokenRaw,
		BuyLamports:    p.BuyLamports,
		HalfTaken:      p.HalfTaken,
		Source:         p.Source,
	}
}

func tokenSource(tok NewToken) string {
	if tok.Source == "launchlab" {
		return "launchlab"
	}
	return "pump"
}

type ClosedTrade struct {
	Symbol     string
	Mint       string
	CapitalUSD float64
	ExitNetUSD float64 // С‡С‚Рѕ РІРµСЂРЅСѓР»РѕСЃСЊ РЅР° РєРѕС€РµР»С‘Рє РїРѕСЃР»Рµ РІСЃРµС… РєРѕРјРёСЃСЃРёР№
	FeesUSD    float64 // СЃСѓРјРјР°СЂРЅРѕ РєРѕРјРёСЃСЃРёРё (РѕС†РµРЅРєР°)
	PnL        float64
	PnLPct     float64
	Reason     string
	Dur        time.Duration
}

type Wallet struct {
	mu      sync.Mutex
	Balance float64
	Start   float64
	Pos     map[string]*Position
	Closed  []ClosedTrade
	// РЎРІРѕРґРєР° РїРѕ С‚РёРїСѓ РІС‹С…РѕРґР° (СѓС‡РёРјСЃСЏ РЅР° РјРёРЅСѓСЃР°С…)
	ExitWin  map[string]int
	ExitLoss map[string]int
}

type persistedPosition struct {
	Mint           string    `json:"mint"`
	BondingCurve   string    `json:"bonding_curve"`
	Symbol         string    `json:"symbol"`
	EntryPrice     float64   `json:"entry_price"`
	Tokens         float64   `json:"tokens"`
	PeakPrice      float64   `json:"peak_price"`
	CapitalUSD     float64   `json:"capital_usd"`
	OpenedAt       time.Time `json:"opened_at"`
	BreakevenArmed bool      `json:"breakeven_armed"`
	Live           bool      `json:"live"`
	TokenRaw       uint64    `json:"token_raw"`
	BuyLamports    uint64    `json:"buy_lamports"`
	HalfTaken      bool      `json:"half_taken"`
	Source         string    `json:"source"`
}

func (w *Wallet) saveActivePositionsLocked() {
	if !liveTradingEnabled() {
		return
	}
	list := make([]persistedPosition, 0, len(w.Pos))
	for _, p := range w.Pos {
		if p == nil || !p.Live {
			continue
		}
		list = append(list, persistedPosition{
			Mint:           p.Mint,
			BondingCurve:   p.BondingCurve,
			Symbol:         p.Symbol,
			EntryPrice:     p.EntryPrice,
			Tokens:         p.Tokens,
			PeakPrice:      p.PeakPrice,
			CapitalUSD:     p.CapitalUSD,
			OpenedAt:       p.OpenedAt,
			BreakevenArmed: p.BreakevenArmed,
			Live:           p.Live,
			TokenRaw:       p.TokenRaw,
			BuyLamports:    p.BuyLamports,
			HalfTaken:      p.HalfTaken,
			Source:         p.Source,
		})
	}
	raw, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(ACTIVE_POSITIONS_FILE, raw, 0o644)
}

func (w *Wallet) loadActivePositions() {
	if !liveTradingEnabled() {
		return
	}
	raw, err := os.ReadFile(ACTIVE_POSITIONS_FILE)
	if err != nil || len(raw) == 0 {
		legacyRaw, legacyErr := os.ReadFile("active_positions.json")
		if legacyErr != nil || len(legacyRaw) == 0 {
			return
		}
		raw = legacyRaw
	}
	var list []persistedPosition
	if json.Unmarshal(raw, &list) != nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, pp := range list {
		if pp.Mint == "" || pp.BondingCurve == "" || !pp.Live {
			continue
		}
		// РќРµ РїРѕРґРЅРёРјР°РµРј СЃРёР»СЊРЅРѕ СЃС‚Р°СЂС‹Рµ СЃРґРµР»РєРё РїРѕСЃР»Рµ СЂРµСЃС‚Р°СЂС‚Р°: РѕРЅРё Р±Р»РѕРєРёСЂСѓСЋС‚ РЅРѕРІС‹Рµ РІС…РѕРґС‹ РїСЂРё MAX_POSITIONS=1.
		if pp.OpenedAt.IsZero() || time.Since(pp.OpenedAt) > (maxHoldDuration()+90*time.Second) {
			continue
		}
		// Р•СЃР»Рё РІ РєРѕС€РµР»СЊРєРµ СѓР¶Рµ РЅРµС‚ С‚РѕРєРµРЅРѕРІ вЂ” РїРѕР·РёС†РёСЏ Р·Р°РєСЂС‹С‚Р°, РЅРѕ РјРѕРіР»Р° РѕСЃС‚Р°С‚СЊСЃСЏ РІ С„Р°Р№Р»Рµ РїРѕСЃР»Рµ Р°РІР°СЂРёР№РЅРѕРіРѕ СЂРµСЃС‚Р°СЂС‚Р°.
		if strings.TrimSpace(pp.Source) != "launchlab" {
			if raw, err := PumpDirectTokenRawBalance(pp.Mint); err == nil {
				if raw == 0 {
					continue
				}
				pp.TokenRaw = raw
			}
		}
		w.Pos[pp.Mint] = &Position{
			Mint:           pp.Mint,
			BondingCurve:   pp.BondingCurve,
			Symbol:         pp.Symbol,
			EntryPrice:     pp.EntryPrice,
			Tokens:         pp.Tokens,
			PeakPrice:      pp.PeakPrice,
			CapitalUSD:     pp.CapitalUSD,
			OpenedAt:       pp.OpenedAt,
			BreakevenArmed: pp.BreakevenArmed,
			Live:           pp.Live,
			TokenRaw:       pp.TokenRaw,
			BuyLamports:    pp.BuyLamports,
			HalfTaken:      pp.HalfTaken,
			Source:         pp.Source,
		}
	}
}

func (w *Wallet) activePositionSnapshots() []*Position {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*Position, 0, len(w.Pos))
	for _, p := range w.Pos {
		if p == nil {
			continue
		}
		out = append(out, snapshotPosition(p))
	}
	return out
}

func newWallet() *Wallet {
	w := &Wallet{
		Balance:  PAPER_BALANCE,
		Start:    PAPER_BALANCE,
		Pos:      make(map[string]*Position),
		ExitWin:  make(map[string]int),
		ExitLoss: make(map[string]int),
	}
	if liveTradingEnabled() {
		syncWalletBalanceUSDFresh(w)
		w.Start = w.Balance
		w.loadActivePositions()
	}
	return w
}

// bucketExitReason вЂ” РєРѕСЂРѕС‚РєРёР№ СЏСЂР»С‹Рє РґР»СЏ СЃС‚Р°С‚РёСЃС‚РёРєРё
func bucketExitReason(reason string) string {
	switch {
	case strings.HasPrefix(reason, "РўР•Р™Рљ"):
		return "С‚РµР№Рє"
	case strings.Contains(reason, "РЎРўРћРџ"):
		return "СЃС‚РѕРї"
	case strings.Contains(reason, "РўР Р•Р™Р›РРќР“"):
		return "С‚СЂРµР№Р»"
	case strings.Contains(reason, "Р‘Р Р•Р™Рљ"):
		return "Р±СЂРµР№РєРёРІРЅ"
	case strings.Contains(reason, "РЎРљР Р•РўР§"):
		return "СЃРєСЂРµС‚С‡"
	case strings.Contains(reason, "РќР•Рў РРњРџРЈР›Р¬РЎРђ"):
		return "РЅРµС‚_РёРјРїСѓР»СЊСЃР°"
	case strings.Contains(reason, "РўРђР™РњРђРЈРў"):
		return "С‚Р°Р№РјР°СѓС‚"
	case strings.Contains(reason, "РњРР“Р РђР¦РРЇ"):
		return "РјРёРіСЂР°С†РёСЏ"
	case strings.Contains(reason, "РЈРњР•Р "):
		return "РЅРµС‚_РґР°РЅРЅС‹С…"
	default:
		return "РїСЂРѕС‡РµРµ"
	}
}

func lossLearningHint(reason string) string {
	switch bucketExitReason(reason) {
	case "СЃС‚РѕРї":
		return "Р§Р°СЃС‚Рѕ СЃС‚РѕРї: СѓР¶РµСЃС‚РѕС‡РёС‚СЊ РІС…РѕРґ (РІС‹С€Рµ curve min / СЃРёР»СЊРЅРµРµ velocity) РёР»Рё РЅРµ РѕСЃР»Р°Р±Р»СЏС‚СЊ СЃРєР°Рј."
	case "СЃРєСЂРµС‚С‡", "РЅРµС‚_РёРјРїСѓР»СЊСЃР°":
		return "РЎР»Р°Р±С‹Р№ СЂР°Р·РіРѕРЅ: РїРѕРґРЅСЏС‚СЊ SNIPER_CURVE_MIN РёР»Рё velocity, С‡С‚РѕР±С‹ РЅРµ Р»РѕРІРёС‚СЊ В«РїСѓСЃС‚С‹РµВ» РёРјРїСѓР»СЊСЃС‹."
	case "Р±СЂРµР№РєРёРІРЅ":
		return "РћС‚РєР°С‚ РїРѕСЃР»Рµ РїРёРєР°: РЅРѕСЂРј Р·Р°С‰РёС‚Р° РєР°РїРёС‚Р°Р»Р°; РїСЂРё С‡Р°СЃС‚С‹С… вЂ” СЃРјРѕС‚СЂРµС‚СЊ TRAIL/РІС…РѕРґ РїРѕР·Р¶Рµ РїРѕ РєСЂРёРІРѕР№."
	case "С‚Р°Р№РјР°СѓС‚", "РЅРµС‚_РґР°РЅРЅС‹С…":
		return "Р”РѕР»РіРѕ Р±РµР· РґРІРёР¶РµРЅРёСЏ / RPC: РїСЂРѕРІРµСЂРёС‚СЊ СЃРµС‚СЊ; РїСЂРё С‡Р°СЃС‚С‹С… С‚Р°Р№РјР°СѓС‚Р°С… СѓРјРµРЅСЊС€РёС‚СЊ MAX_HOLD."
	case "РјРёРіСЂР°С†РёСЏ":
		return "РЈС€Р»Рѕ РІ Raydium: С„РёРєСЃ РїРѕ РїСЂР°РІРёР»Р°Рј; РЅРµ РѕС€РёР±РєР° СЃС‚СЂР°С‚РµРіРёРё."
	default:
		return "РЎРјРѕС‚СЂРё РїРѕР»РЅС‹Р№ Reason РІ Р»РѕРіРµ РІС‹С…РѕРґР° Рё СЃРІРѕРґРєСѓ ExitLoss РІ РєРѕС€РµР»СЊРєРµ."
	}
}

// Р’СЂРµРјРµРЅР° СѓСЃРїРµС€РЅС‹С… РІС…РѕРґРѕРІ вЂ” РѕС†РµРЅРєР° СЃСЂРµРґРЅРµРіРѕ РёРЅС‚РµСЂРІР°Р»Р° РјРµР¶РґСѓ СЃРґРµР»РєР°РјРё
var (
	recentEntryMu    sync.Mutex
	recentEntryTimes []time.Time
)

func recordSuccessfulEntry() {
	recentEntryMu.Lock()
	defer recentEntryMu.Unlock()
	recentEntryTimes = append(recentEntryTimes, time.Now())
	if len(recentEntryTimes) > 100 {
		recentEntryTimes = recentEntryTimes[len(recentEntryTimes)-100:]
	}
}

// stakeFromBalance вЂ” РіСЂРѕСЃСЃ USD РЅР° РІС…РѕРґ: n% РѕС‚ С‚РµРєСѓС‰РµРіРѕ Р±Р°Р»Р°РЅСЃР° (СЃР»РѕР¶РЅС‹Р№ РїСЂРѕС†РµРЅС‚ РїРѕ СЃРґРµР»РєР°Рј).
func stakeFromBalance(balance float64) float64 {
	if balance <= 0 {
		return 0
	}
	s := balance * BET_PCT_OF_BALANCE
	if s < MIN_STAKE_USD {
		return 0
	}
	if s > balance {
		return balance
	}
	return s
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  RPC РљР›РР•РќРў
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

var httpClient = &http.Client{Timeout: 8 * time.Second}
var rpcLimiter = make(chan struct{}, RPC_MAX_CONCURRENT)
var rpcEndpointCursor uint64

type balCacheEntry struct {
	sol float64
	ts  time.Time
}

var balanceCache struct {
	mu sync.Mutex
	m  map[string]balCacheEntry
}

type metadataCacheEntry struct {
	ok     bool
	detail string
	ts     time.Time
}

var metadataCache struct {
	mu sync.Mutex
	m  map[string]metadataCacheEntry
}

// РљСѓСЂСЃ SOL/USD (CoinGecko, РёРЅР°С‡Рµ fallback РІ getSolUSD)
var solUSDPrice struct {
	mu  sync.RWMutex
	USD float64
}

func getSolUSD() float64 {
	solUSDPrice.mu.RLock()
	v := solUSDPrice.USD
	solUSDPrice.mu.RUnlock()
	if v >= 1 {
		return v
	}
	return 170
}

func fetchSolUSDPrice() (float64, error) {
	return fetchCoinGeckoSOL()
}

func fetchCoinGeckoSOL() (float64, error) {
	url := "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
	resp, err := httpClient.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var out map[string]struct {
		Usd float64 `json:"usd"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, err
	}
	if row, ok := out["solana"]; ok && row.Usd > 0 {
		return row.Usd, nil
	}
	return 0, fmt.Errorf("no sol price")
}

func refreshSolPriceUSD() {
	p, err := fetchSolUSDPrice()
	if err != nil {
		return
	}
	if p < 40 || p > 600 {
		return // РѕС‚СЃРµРєР°РµРј СЏРІРЅС‹Р№ РјСѓСЃРѕСЂ API
	}
	solUSDPrice.mu.Lock()
	solUSDPrice.USD = p
	solUSDPrice.mu.Unlock()
}

func pumpFeePct() float64 { return float64(PUMP_FEE_BPS) / 10000.0 }
func slipPct() float64    { return float64(SLIPPAGE_BPS) / 10000.0 }

func solanaTxFeeUSD() float64 {
	return (SOLANA_TX_LAMPORTS / 1e9) * getSolUSD()
}

// Р“СЂРѕСЃСЃ USD в†’ СЃРєРѕР»СЊРєРѕ РґРѕС€Р»Рѕ РІ РєСЂРёРІСѓСЋ РїРѕСЃР»Рµ 1% pump + РєРѕРјРёСЃСЃРёРё СЃРµС‚Рё РЅР° РїРѕРєСѓРїРєСѓ
func usdToPoolAfterBuy(grossUSD float64) float64 {
	return grossUSD - grossUSD*pumpFeePct() - solanaTxFeeUSD()
}

func effectiveBuyPrice(spot float64) float64 { return spot * (1 + slipPct()) }

// Р§С‚Рѕ РІРµСЂРЅС‘С‚СЃСЏ РЅР° РєРѕС€РµР»С‘Рє РїРѕСЃР»Рµ РїСЂРѕРґР°Р¶Рё (1% pump + slip + СЃРµС‚СЊ)
func exitNetAfterSell(tokens float64, spot float64) float64 {
	if tokens <= 0 || spot <= 0 {
		return 0
	}
	exitMark := spot * (1 - slipPct())
	gross := tokens * exitMark
	return gross*(1-pumpFeePct()) - solanaTxFeeUSD()
}

func heliusRPCURLs() []string {
	return []string{
		"https://mainnet.helius-rpc.com/?api-key=" + HELIUS_API_KEY,
		"https://atlas-mainnet.helius-rpc.com/?api-key=" + HELIUS_API_KEY,
	}
}

func heliusURL() string {
	urls := heliusRPCURLs()
	if len(urls) == 0 {
		return "https://mainnet.helius-rpc.com/?api-key=" + HELIUS_API_KEY
	}
	i := atomic.AddUint64(&rpcEndpointCursor, 1)
	return urls[int(i)%len(urls)]
}

func apiReady() bool {
	return HELIUS_API_KEY != "Р’РЎРўРђР’Р¬_РљР›Р®Р§_РЎР®Р”Рђ" && len(HELIUS_API_KEY) > 10
}

func rpc(method string, params []interface{}) ([]byte, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": method, "params": params,
	})
	rpcLimiter <- struct{}{}
	defer func() { <-rpcLimiter }()

	backoff := RPC_RETRY_BASE_DELAY
	for attempt := 0; attempt < RPC_MAX_RETRIES; attempt++ {
		resp, err := httpClient.Post(heliusURL(), "application/json", bytes.NewReader(body))
		if err != nil {
			if attempt == RPC_MAX_RETRIES-1 {
				return nil, err
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			if attempt == RPC_MAX_RETRIES-1 {
				return nil, readErr
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		rateLimited := resp.StatusCode == 429 || bytes.Contains(raw, []byte(`"code":-32429`))
		if rateLimited {
			if attempt == RPC_MAX_RETRIES-1 {
				return nil, fmt.Errorf("rpc rate-limited after retries (-32429)")
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		return raw, nil
	}
	return nil, fmt.Errorf("rpc failed after retries")
}

func isRateLimitErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "429") ||
		strings.Contains(s, "-32429") ||
		strings.Contains(s, "rate limit")
}

func isSignatureConfirmed(sig string) (bool, string, error) {
	data, err := rpc("getSignatureStatuses", []interface{}{
		[]string{sig},
		map[string]bool{"searchTransactionHistory": true},
	})
	if err != nil {
		return false, "", err
	}
	var out struct {
		Result struct {
			Value []struct {
				ConfirmationStatus string      `json:"confirmationStatus"`
				Err                interface{} `json:"err"`
			} `json:"value"`
		} `json:"result"`
	}
	if json.Unmarshal(data, &out) != nil || len(out.Result.Value) == 0 {
		return false, "", fmt.Errorf("bad getSignatureStatuses")
	}
	v := out.Result.Value[0]
	if v.Err != nil {
		return false, "rejected", nil
	}
	if v.ConfirmationStatus == "confirmed" || v.ConfirmationStatus == "finalized" {
		return true, v.ConfirmationStatus, nil
	}
	return false, v.ConfirmationStatus, nil
}

func waitBuySettlement(sig, mint string, timeout time.Duration) (ok bool, reason string) {
	deadline := time.Now().Add(timeout)
	seenInNetwork := false
	for time.Now().Before(deadline) {
		confirmed, status, err := isSignatureConfirmed(sig)
		if err == nil {
			if status != "" && status != "rejected" {
				seenInNetwork = true
			}
			if confirmed {
				if raw, berr := PumpDirectTokenRawBalance(mint); berr == nil && raw > 0 {
					return true, "confirmed+" + status
				}
			}
		}
		if raw, berr := PumpDirectTokenRawBalance(mint); berr == nil && raw > 0 {
			return true, "token_balance_seen"
		}
		time.Sleep(500 * time.Millisecond)
	}
	if seenInNetwork {
		return true, "pending_confirmation"
	}
	return false, "no_confirmation_or_zero_balance"
}

func (w *Wallet) verifyBuyAsync(mint, sym, sig string, detectedAt time.Time) {
	ok, reason := waitBuySettlement(sig, mint, 5*time.Second)
	if !ok {
		ok, reason = waitBuySettlement(sig, mint, 25*time.Second)
	}
	if ok {
		if reason == "pending_confirmation" {
			consoleMu.Lock()
			fmt.Printf("%s BUY PENDING %s | sig=%s | waiting network confirmation\n", yellow("вЏі"), sym, gray(sig))
			consoleMu.Unlock()
		} else if !detectedAt.IsZero() {
			consoleMu.Lock()
			fmt.Printf("%s Transaction Confirmed | %s | +%dms\n",
				gray("вЏ±"), "$"+short(mint), time.Since(detectedAt).Milliseconds())
			consoleMu.Unlock()
		}
		return
	}
	consoleMu.Lock()
	fmt.Printf("%s BUY FAILED %s | sig=%s | reason=%s\n", red("вќЊ"), sym, gray(sig), reason)
	consoleMu.Unlock()
	w.mu.Lock()
	if _, exists := w.Pos[mint]; exists {
		delete(w.Pos, mint)
		w.saveActivePositionsLocked()
	}
	w.mu.Unlock()
	syncWalletBalanceUSDFresh(w)
}

func hotPathSilent() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("HOT_PATH_SILENT")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return true
}

func fastAntiScamMode() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("FAST_ANTI_SCAM")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return true
}

func hotPathTraceEnabled() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("HOT_PATH_TRACE")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return true
}

func monitorMaxPriceFails() int {
	if s := strings.TrimSpace(os.Getenv("MONITOR_MAX_PRICE_FAILS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 3 && v <= 60 {
			return v
		}
	}
	if turboModeEnabled() {
		return 14
	}
	return 8
}

func monitorFailLogEvery() int {
	if s := strings.TrimSpace(os.Getenv("MONITOR_FAIL_LOG_EVERY")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 1 && v <= 20 {
			return v
		}
	}
	return 3
}

func noPriceProfitLockFails() int {
	if s := strings.TrimSpace(os.Getenv("NO_PRICE_PROFIT_LOCK_FAILS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 1 && v <= 30 {
			return v
		}
	}
	if ultraFastEntryMode() {
		return 3
	}
	return 5
}

func noPriceProfitLockMinMult() float64 {
	if s := strings.TrimSpace(os.Getenv("NO_PRICE_PROFIT_LOCK_MIN_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0.1 && v <= 50 {
			return 1 + v/100.0
		}
	}
	if ultraFastEntryMode() {
		return 1.004 // +0.4% РґРѕСЃС‚Р°С‚РѕС‡РЅРѕ, С‡С‚РѕР±С‹ РЅРµ РѕС‚РґР°РІР°С‚СЊ СѓР¶Рµ РїРѕР№РјР°РЅРЅС‹Р№ РјРёРєСЂРѕ-РїР»СЋСЃ
	}
	return 1.01
}

func noPricePanicSellPeakMult() float64 {
	if s := strings.TrimSpace(os.Getenv("NO_PRICE_PANIC_SELL_PEAK_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 1 && v <= 300 {
			return 1 + v/100.0
		}
	}
	if ultraFastEntryMode() {
		return 1.10 // РµСЃР»Рё СѓР¶Рµ Р±С‹Р»Рѕ +10% Рё С†РµРЅР° РїСЂРѕРїР°Р»Р° вЂ” РІС‹С…РѕРґРёРј РјРіРЅРѕРІРµРЅРЅРѕ
	}
	return 1.20
}

func ultraQualityFilterEnabled() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("ULTRA_QUALITY_FILTER")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return ultraFastEntryMode()
}

func ultraMinRealSOL() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_MIN_REAL_SOL")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0.01 && v <= 20 {
			return v
		}
	}
	return 0.4
}

func ultraMinProgress() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_MIN_PROGRESS_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0 && v <= 100 {
			return v / 100.0
		}
	}
	return 0.02
}

func ultraMaxProgress() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_MAX_PROGRESS_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 1 && v <= 100 {
			return v / 100.0
		}
	}
	return 0.70
}

func ultraMomentumPause() time.Duration {
	if s := strings.TrimSpace(os.Getenv("ULTRA_MOMENTUM_PAUSE_MS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 20 && v <= 500 {
			return time.Duration(v) * time.Millisecond
		}
	}
	return 90 * time.Millisecond
}

func ultraMinDeltaProgress() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_MIN_DP_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0 && v <= 20 {
			return v / 100.0
		}
	}
	return 0.002 // 0.2%
}

func ultraMinDeltaSOL() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_MIN_DSOL")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0 && v <= 5 {
			return v
		}
	}
	return 0.01
}

func ultraSkipMomentumIfParseMs() int64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_SKIP_MOMENTUM_IF_PARSE_MS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 100 && v <= 2000 {
			return int64(v)
		}
	}
	return 550
}

func ultraSkipMomentumIfFirstSnapMs() int64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_SKIP_MOMENTUM_IF_FIRST_SNAP_MS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 50 && v <= 1500 {
			return int64(v)
		}
	}
	return 260
}

func ultraSkipMomentumMinProgress() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_SKIP_MOMENTUM_MIN_PROGRESS_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0 && v <= 100 {
			return v / 100.0
		}
	}
	return 0.08
}

func ultraSkipMomentumMinRealSOL() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_SKIP_MOMENTUM_MIN_REAL_SOL")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0.01 && v <= 50 {
			return v
		}
	}
	return 1.2
}

func ultraSkipMomentumMaxProgress() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_SKIP_MOMENTUM_MAX_PROGRESS_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 1 && v <= 100 {
			return v / 100.0
		}
	}
	return 0.18
}

func curveSnapshotRetryTries() int {
	if s := strings.TrimSpace(os.Getenv("CURVE_SNAPSHOT_TRIES")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 1 && v <= 6 {
			return v
		}
	}
	if turboModeEnabled() {
		return 3
	}
	return 3
}

func flatExitAfter() time.Duration {
	if s := strings.TrimSpace(os.Getenv("FLAT_EXIT_AFTER_SEC")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v == 0 {
			return 0
		} else if err == nil && v >= 5 && v <= 300 {
			return time.Duration(v) * time.Second
		}
	}
	if ultraFastEntryMode() {
		return 25 * time.Second
	}
	return 40 * time.Second
}

func flatExitMinMult() float64 {
	if s := strings.TrimSpace(os.Getenv("FLAT_EXIT_MIN_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= -50 && v <= 50 {
			return 1 + v/100.0
		}
	}
	return 0.985
}

func flatExitMaxMult() float64 {
	if s := strings.TrimSpace(os.Getenv("FLAT_EXIT_MAX_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= -50 && v <= 50 {
			return 1 + v/100.0
		}
	}
	return 1.01
}

func earlyStopWindow() time.Duration {
	if s := strings.TrimSpace(os.Getenv("EARLY_STOP_WINDOW_SEC")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v == 0 {
			return 0
		} else if err == nil && v >= 5 && v <= 180 {
			return time.Duration(v) * time.Second
		}
	}
	return 45 * time.Second
}

func earlyStopLossMult() float64 {
	if s := strings.TrimSpace(os.Getenv("EARLY_STOP_LOSS_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 1 && v <= 50 {
			return 1 - v/100.0
		}
	}
	return 0.90 // -10%
}

func impulseProtectPeakMult() float64 {
	if s := strings.TrimSpace(os.Getenv("IMPULSE_PROTECT_PEAK_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v == 0 {
			return 0
		} else if err == nil && v >= 0.5 && v <= 50 {
			return 1 + v/100.0
		}
	}
	return 1.02 // РїРѕСЃР»Рµ +2%
}

func impulseProtectFloorMult() float64 {
	if s := strings.TrimSpace(os.Getenv("IMPULSE_PROTECT_FLOOR_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= -20 && v <= 10 {
			return 1 + v/100.0
		}
	}
	return 0.995 // РЅРµ РѕС‚РґР°С‘Рј РЅРёР¶Рµ -0.5% РїРѕСЃР»Рµ Р·Р°С„РёРєСЃРёСЂРѕРІР°РЅРЅРѕРіРѕ РёРјРїСѓР»СЊСЃР°
}

func ultraDevFilterEnabled() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("ULTRA_DEV_FILTER")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return ultraFastEntryMode()
}

func ultraDevMinSOL() float64 {
	if s := strings.TrimSpace(os.Getenv("ULTRA_DEV_MIN_SOL")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0 {
			return v
		}
	}
	return 0.000001
}

func monitorTickInterval() time.Duration {
	if s := strings.TrimSpace(os.Getenv("MONITOR_TICK_MS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 200 && v <= 3000 {
			return time.Duration(v) * time.Millisecond
		}
	}
	if ultraFastEntryMode() {
		return 400 * time.Millisecond
	}
	return PRICE_TICK
}

func quickPumpWindow() time.Duration {
	if s := strings.TrimSpace(os.Getenv("QUICK_PUMP_WINDOW_MS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 300 && v <= 10000 {
			return time.Duration(v) * time.Millisecond
		}
	}
	if ultraFastEntryMode() {
		return 1500 * time.Millisecond
	}
	return 0
}

func quickPumpTakeProfitMult() float64 {
	if s := strings.TrimSpace(os.Getenv("QUICK_PUMP_TP_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 10 && v <= 300 {
			return 1 + v/100.0
		}
	}
	if ultraFastEntryMode() {
		return 1.35 // +35% РІ РїРµСЂРІС‹Рµ ~1.5СЃ, РїРѕС‚РѕРј СЃС‚Р°РЅРґР°СЂС‚РЅС‹Р№ TP
	}
	return takeProfitMult()
}

func maxHoldDuration() time.Duration {
	if s := strings.TrimSpace(os.Getenv("MAX_HOLD_SEC")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 15 && v <= 1800 {
			return time.Duration(v) * time.Second
		}
	}
	return MAX_HOLD
}

func fastExitAfterDuration() time.Duration {
	if s := strings.TrimSpace(os.Getenv("FAST_EXIT_AFTER_SEC")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 10 && v <= 1800 {
			return time.Duration(v) * time.Second
		}
	}
	return FAST_EXIT_AFTER
}

func takeProfitMult() float64 {
	if s := strings.TrimSpace(os.Getenv("TAKE_PROFIT_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 10 && v <= 400 {
			return 1 + v/100.0
		}
	}
	return TAKE_PROFIT
}

func stopLossHardMult() float64 {
	if s := strings.TrimSpace(os.Getenv("STOP_LOSS_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 1 && v <= 90 {
			return 1 - v/100.0
		}
	}
	return STOP_LOSS_HARD
}

func stopConfirmLvlMult() float64 {
	if s := strings.TrimSpace(os.Getenv("STOP_CONFIRM_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 1 && v <= 90 {
			return 1 - v/100.0
		}
	}
	return STOP_CONFIRM_LVL
}

func liveFixedBuySOLValue() float64 {
	// РЎРѕРІРјРµСЃС‚РёРјРѕСЃС‚СЊ: РЅРµСЃРєРѕР»СЊРєРѕ РёРјС‘РЅ РїРµСЂРµРјРµРЅРЅС‹С… РґР»СЏ С„РёРєСЃ-СЃС‚Р°РІРєРё.
	for _, k := range []string{"FIXED_STAKE_SOL", "FIXED_STAKE", "LIVE_FIXED_BUY_SOL", "BUY_SOL"} {
		if s := strings.TrimSpace(os.Getenv(k)); s != "" {
			if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0.001 && v <= 1.0 {
				return v
			}
		}
	}
	// Safety fallback: РІ ultra-fast СЂРµР¶РёРјРµ РёСЃРїРѕР»СЊР·СѓРµРј РјРµР»РєСѓСЋ СЃС‚Р°РІРєСѓ РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ.
	if ultraFastEntryMode() {
		return 0.005
	}
	return LIVE_FIXED_BUY_SOL
}

func minFeeReserveSOLValue() float64 {
	if s := strings.TrimSpace(os.Getenv("MIN_FEE_RESERVE_SOL")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0.001 && v <= 0.05 {
			return v
		}
	}
	if ultraFastEntryMode() {
		return 0.004
	}
	return 0.003
}

func isInsufficientFeeErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "insufficient funds for fee") ||
		strings.Contains(s, "insufficientfundsforfee")
}

func feeGuardActive() bool {
	feeGuardState.mu.Lock()
	defer feeGuardState.mu.Unlock()
	return time.Now().Before(feeGuardState.until)
}

func activateFeeGuard(d time.Duration, reason string) {
	feeGuardState.mu.Lock()
	feeGuardState.until = time.Now().Add(d)
	feeGuardState.mu.Unlock()
	consoleMu.Lock()
	fmt.Printf("%s fee-guard %ds: %s\n", yellow("вљ "), int(d.Seconds()), reason)
	consoleMu.Unlock()
}

func maxDrawdownPct() float64 {
	if s := strings.TrimSpace(os.Getenv("MAX_DRAWDOWN_PCT")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 1 && v <= 90 {
			return v
		}
	}
	return 20
}

func printHotPathTrace(sym, src, status, detail string, parseMs, curveMs, checksMs int64, totalMs int64) {
	if !hotPathTraceEnabled() {
		return
	}
	consoleMu.Lock()
	fmt.Printf("%s HotPath | %s | %s | parse=%dms curve=%dms checks=%dms total=%dms | %s\n",
		gray("вЏ±"), sym, src+"/"+status, parseMs, curveMs, checksMs, totalMs, detail)
	consoleMu.Unlock()
}

func abortIfTooLate(tok NewToken, stage string) bool {
	if tok.DetectedAt.IsZero() {
		return false
	}
	d := time.Since(tok.DetectedAt)
	if d <= MAX_READY_TO_SEND_DELAY {
		return false
	}
	consoleMu.Lock()
	if stage == "velocity_check" {
		fmt.Printf("вљ пёЏ LATENCY WARNING: High latency (%d ms) stage=%s, continuing\n", d.Milliseconds(), stage)
		consoleMu.Unlock()
		return false
	}
	fmt.Printf("вќЊ TRADE ABORTED: Latency too high (%d ms) stage=%s\n", d.Milliseconds(), stage)
	consoleMu.Unlock()
	return true
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  PUMP.FUN BONDING CURVE вЂ” СЂРµР°Р»СЊРЅР°СЏ С†РµРЅР° on-chain
//
//  Pump.fun С…СЂР°РЅРёС‚ РІРёСЂС‚СѓР°Р»СЊРЅС‹Рµ СЂРµР·РµСЂРІС‹ РІ bonding curve Р°РєРєР°СѓРЅС‚Рµ.
//  Р¦РµРЅР° С‚РѕРєРµРЅР° = virtualSolReserves / virtualTokenReserves * SOL_PRICE
//  Р”Р°РЅРЅС‹Рµ: bytes 8-56 Р°РєРєР°СѓРЅС‚Р° (РїРѕСЃР»Рµ 8-Р±Р°Р№С‚РЅРѕРіРѕ discriminator)
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func parseBondingCurve(data []byte) (*BondingCurve, error) {
	// Pump.fun bonding curve layout (borsh):
	// [0:8]   discriminator (8 bytes)
	// [8:16]  virtualTokenReserves (u64)
	// [16:24] virtualSolReserves (u64)
	// [24:32] realTokenReserves (u64)
	// [32:40] realSolReserves (u64)
	// [40:48] tokenTotalSupply (u64)
	// [48]    complete (bool)
	if len(data) < 49 {
		return nil, fmt.Errorf("data too short: %d", len(data))
	}
	bc := &BondingCurve{
		VirtualTokenReserves: binary.LittleEndian.Uint64(data[8:16]),
		VirtualSolReserves:   binary.LittleEndian.Uint64(data[16:24]),
		RealTokenReserves:    binary.LittleEndian.Uint64(data[24:32]),
		RealSolReserves:      binary.LittleEndian.Uint64(data[32:40]),
		TokenTotalSupply:     binary.LittleEndian.Uint64(data[40:48]),
		Complete:             data[48] == 1,
	}
	return bc, nil
}

// curveSnap вЂ” СЃРЅРёРјРѕРє РєСЂРёРІРѕР№ РґР»СЏ РІС…РѕРґР° Рё РјРѕРЅРёС‚РѕСЂРёРЅРіР°
type curveSnap struct {
	PriceUSD   float64
	Progress   float64
	RealSolSOL float64
	Complete   bool
}

func getCurveSnapshot(bcAddr string) (*curveSnap, error) {
	data, err := rpc("getAccountInfo", []interface{}{
		bcAddr,
		map[string]string{
			"encoding":   "base64",
			"commitment": "processed",
		},
	})
	if err != nil {
		return nil, err
	}

	var r struct {
		Result *struct {
			Value struct {
				Data []interface{} `json:"data"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.Result == nil {
		return nil, fmt.Errorf("account missing")
	}

	dataArr := r.Result.Value.Data
	if len(dataArr) < 1 {
		return nil, fmt.Errorf("no data")
	}

	b64str, ok := dataArr[0].(string)
	if !ok {
		return nil, fmt.Errorf("not string")
	}

	raw, err := base64.StdEncoding.DecodeString(b64str)
	if err != nil {
		return nil, err
	}

	bc, err := parseBondingCurve(raw)
	if err != nil {
		return nil, err
	}

	if bc.VirtualTokenReserves == 0 {
		return nil, fmt.Errorf("empty curve")
	}

	rawPerToken := math.Pow10(pumpTokenDecimals)
	priceInSol := (float64(bc.VirtualSolReserves) / 1e9) / (float64(bc.VirtualTokenReserves) / rawPerToken)
	sol := getSolUSD()
	priceInUSD := priceInSol * sol

	targetSol := 85.0
	realSolSOL := float64(bc.RealSolReserves) / 1e9
	progress := realSolSOL / targetSol
	if progress > 1 {
		progress = 1
	}
	if bc.Complete {
		progress = 1
	}

	return &curveSnap{
		PriceUSD:   priceInUSD,
		Progress:   progress,
		RealSolSOL: realSolSOL,
		Complete:   bc.Complete,
	}, nil
}

func getCurveSnapshotUnified(bcAddr, source string) (*curveSnap, error) {
	if source == "launchlab" {
		return getLaunchLabSnapshot(bcAddr)
	}
	return getCurveSnapshot(bcAddr)
}

// РќРµСЃРєРѕР»СЊРєРѕ РїРѕРїС‹С‚РѕРє: create в†’ РєСЂРёРІР°СЏ/pool РёРЅРѕРіРґР° РїРѕСЏРІР»СЏРµС‚СЃСЏ РІ RPC СЃ Р·Р°РґРµСЂР¶РєРѕР№.
func getCurveSnapshotWithRetry(bcAddr string, source string) (*curveSnap, error) {
	var last *curveSnap
	var lastErr error
	tries := curveSnapshotRetryTries()
	for attempt := 0; attempt < tries; attempt++ {
		if attempt > 0 {
			pause := 50 * attempt
			if turboModeEnabled() {
				pause = 25 * attempt
			}
			time.Sleep(time.Duration(pause) * time.Millisecond)
		}
		s, err := getCurveSnapshotUnified(bcAddr, source)
		last, lastErr = s, err
		if err == nil && s != nil && s.PriceUSD > 0 {
			return s, nil
		}
	}
	return last, lastErr
}

// curveVelocityOK вЂ” РІС‚РѕСЂРѕР№ Р·Р°РјРµСЂ РїРѕСЃР»Рµ РїР°СѓР·С‹: РЅСѓР¶РµРЅ Р·Р°РјРµС‚РЅС‹Р№ РїСЂРёС‚РѕРє (РїРѕРєСѓРїРєРё/Р±РѕС‚С‹).
// rejectKey РїСѓСЃС‚РѕР№ РїСЂРё ok; РёРЅР°С‡Рµ vel_rpc / vel_low / vel_late вЂ” РґР»СЏ СЃРІРѕРґРєРё РѕС‚СЃРµРІР°.
func curveVelocityOK(bc string, snap0 *curveSnap, source string, createAt *time.Time) (snap1 *curveSnap, ok bool, detail string, rejectKey string) {
	if snap0 == nil || snap0.Complete {
		return nil, false, "РЅРµС‚ СЃРЅРёРјРєР°", "velocity"
	}
	if shouldSkipVelocity() {
		return snap0, true, "SKIP_VELOCITY/AUTO (Р±РµР· РїР°СѓР·С‹ Рё РІС‚РѕСЂРѕРіРѕ Р·Р°РјРµСЂР°)", ""
	}
	pause, minDP, minDSol, mode := adaptiveVelocityParams(createAt, snap0)
	time.Sleep(pause)
	s1, err := getCurveSnapshotWithRetry(bc, source)
	if err != nil || s1 == nil || s1.PriceUSD <= 0 {
		return nil, false, "РІС‚РѕСЂРѕР№ СЃРЅРёРјРѕРє РєСЂРёРІРѕР№", "vel_rpc"
	}
	if s1.Complete {
		return nil, false, "РєСЂРёРІР°СЏ complete РЅР° РІС‚РѕСЂРѕРј Р·Р°РјРµСЂРµ", "complete"
	}
	dP := s1.Progress - snap0.Progress
	dSol := s1.RealSolSOL - snap0.RealSolSOL
	// Recovery logic: РґРѕРїСѓСЃРєР°РµРј РЅРµР±РѕР»СЊС€РѕР№ РѕС‚СЂРёС†Р°С‚РµР»СЊРЅС‹Р№ С€СѓРј РґРѕ -0.01%.
	if dP < VELOCITY_MIN_DELTA_DP {
		return s1, false, fmt.Sprintf("micro-velocity РЅРёР¶Рµ РїРѕСЂРѕРіР° (О”%.3f%% < %.3f%% Р·Р° %v)", dP*100, VELOCITY_MIN_DELTA_DP*100, pause), "vel_low"
	}
	if dP < minDP && dSol < minDSol {
		return s1, false, fmt.Sprintf("РјР°Р»Рѕ РїСЂРёС‚РѕРєР° (О”%.2f%% / +%.3f SOL Р·Р° %v; РїСЂРѕС„=%s, needв‰€О”%.2f%% РёР»Рё +%.3f SOL)",
			dP*100, dSol, pause, mode, minDP*100, minDSol), "vel_low"
	}
	if s1.Progress > SNIPER_CURVE_MAX+0.06 {
		return s1, false, fmt.Sprintf("РєСЂРёРІР°СЏ СѓР¶Рµ %.1f%% вЂ” РїРѕР·РґРЅРѕ", s1.Progress*100), "vel_late"
	}
	return s1, true, fmt.Sprintf("О”%.2f%% / +%.3f SOL Р·Р° %v (%s)", dP*100, dSol, pause, mode), ""
}

func adaptiveVelocityParams(createAt *time.Time, snap0 *curveSnap) (pause time.Duration, minDP, minDSol float64, tag string) {
	pause = velocityPause()
	minDP = VELOCITY_MIN_DPROGRESS
	minDSol = VELOCITY_MIN_DREALSOL

	switch envSignalProfile() {
	case "strict":
		minDP *= 1.55
		minDSol *= 1.55
		tag = "strict"
	case "aggressive":
		minDP *= 0.65
		minDSol *= 0.65
		tag = "aggressive"
	default:
		tag = "balanced"
	}

	// РђРґР°РїС‚РёРІРЅРѕСЃС‚СЊ РїРѕ РІРѕР·СЂР°СЃС‚Сѓ create: РЅР° СЃС‚Р°СЂС‚Рµ РїСѓР»С‹ Р¶РёРІСѓС‚ Р±С‹СЃС‚СЂРµРµ, РјРѕР¶РЅРѕ РґР°С‚СЊ С‡СѓС‚СЊ РјСЏРіС‡Рµ РїРѕСЂРѕРі;
	// РµСЃР»Рё СѓР¶Рµ РЅРµ В«СЃРІРµР¶Р°РєВ», РЅР°РѕР±РѕСЂРѕС‚ СѓР¶РµСЃС‚РѕС‡Р°РµРј, С‡С‚РѕР±С‹ РЅРµ Р»РµР·С‚СЊ РІ Р·Р°СЃС‚РѕР№.
	if createAt != nil {
		age := time.Since(*createAt)
		if age <= 15*time.Second {
			minDP *= 0.8
			minDSol *= 0.8
			tag += "+fresh"
		} else if age >= 90*time.Second {
			minDP *= 1.25
			minDSol *= 1.25
			tag += "+stale"
		}
	}
	if snap0 != nil {
		if snap0.Progress < 0.03 {
			minDP *= 0.9
			minDSol *= 0.9
		} else if snap0.Progress > 0.08 {
			minDP *= 1.15
			minDSol *= 1.15
		}
	}
	minDP = math.Max(minDP, 0.02) // РЅРµ РЅРёР¶Рµ +2% вЂ” quality over quantity
	return pause, minDP, minDSol, tag
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  BONDING CURVE PDA вЂ” РїРѕ РѕС„РёС†РёР°Р»СЊРЅС‹Рј seeds pump.fun
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func pumpBondingCurvePDA(mintAddr string) (string, error) {
	mint, err := solana.PublicKeyFromBase58(mintAddr)
	if err != nil {
		return "", err
	}
	program := solana.MustPublicKeyFromBase58(PUMP_PROGRAM)
	pda, _, err := solana.FindProgramAddress(
		[][]byte{[]byte("bonding-curve"), mint.Bytes()},
		program,
	)
	if err != nil {
		return "", err
	}
	return pda.String(), nil
}

// pickMintFromPostBalances вЂ” РїСЂРёРѕСЂРёС‚РµС‚ mint вЂ¦pump; РЅРµ USDC/wSOL РёР· С‚РѕРіРѕ Р¶Рµ tx
func pickMintFromPostBalances(balances []postTokenBal) string {
	var fallback string
	for _, b := range balances {
		m := b.Mint
		if m == "" || ignoredTokenMints[m] {
			continue
		}
		if strings.HasSuffix(m, "pump") {
			return m
		}
		if fallback == "" {
			fallback = m
		}
	}
	if fallback != "" {
		return fallback
	}
	for _, b := range balances {
		if b.Mint != "" && !ignoredTokenMints[b.Mint] {
			return b.Mint
		}
	}
	return ""
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  CREATE TX: mint + СЃРѕР·РґР°С‚РµР»СЊ (РїРµСЂРІС‹Р№ signer / fee payer)
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func parseCreateTx(sig string) (mint, creator string, createBlockTime *time.Time) {
	data, err := getTransactionJSONParsedFast(sig)
	if err != nil {
		return "", "", nil
	}
	mint, creator, createBlockTime, isCreate := parseTxMintCreator(data, func(logs []string) bool {
		for _, l := range logs {
			if contains(l, "Instruction: Create") {
				return true
			}
		}
		return false
	})
	if !isCreate {
		if mint == "" {
			mint, creator, createBlockTime = refillMintFromConfirmed(sig, creator, createBlockTime, func(logs []string) bool {
				for _, l := range logs {
					if contains(l, "Instruction: Create") {
						return true
					}
				}
				return false
			})
		}
		return mint, creator, createBlockTime
	}
	if mint == "" {
		mint, creator, createBlockTime = refillMintFromConfirmed(sig, creator, createBlockTime, func(logs []string) bool {
			for _, l := range logs {
				if contains(l, "Instruction: Create") {
					return true
				}
			}
			return false
		})
	}
	return mint, creator, createBlockTime
}

func getTransactionJSONParsedFast(sig string) ([]byte, error) {
	params := func(commitment string) []interface{} {
		return []interface{}{
			sig,
			map[string]interface{}{
				// Р”Р»СЏ hot-path Р±РµСЂС‘Рј json (Р»РµРіС‡Рµ payload, Р±С‹СЃС‚СЂРµРµ РѕС‚РІРµС‚Р°), РїР°СЂСЃРµСЂ РЅРёР¶Рµ РїРѕРґРґРµСЂР¶РёРІР°РµС‚ РѕР±Р° С„РѕСЂРјР°С‚Р°.
				"encoding":                       "json",
				"maxSupportedTransactionVersion": 0,
				"commitment":                     commitment,
			},
		}
	}

	// Р‘С‹СЃС‚СЂС‹Р№ РїСѓС‚СЊ РґР»СЏ hot-path: СЃРЅР°С‡Р°Р»Р° processed, С‡С‚РѕР±С‹ РЅРµ Р¶РґР°С‚СЊ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёРµ.
	tries := 3
	if turboModeEnabled() {
		tries = 1
	}
	for attempt := 0; attempt < tries; attempt++ {
		data, err := rpc("getTransaction", params("processed"))
		if err == nil {
			var wrap struct {
				Result json.RawMessage `json:"result"`
			}
			if json.Unmarshal(data, &wrap) == nil && string(wrap.Result) != "null" {
				return data, nil
			}
		}
		if attempt < tries-1 {
			time.Sleep(time.Duration(60*(attempt+1)) * time.Millisecond)
		}
	}

	// Fallback РґР»СЏ СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚Рё СЃРѕ СЃС‚Р°СЂС‹РјРё/РјРµРґР»РµРЅРЅС‹РјРё СѓР·Р»Р°РјРё.
	return rpc("getTransaction", params("confirmed"))
}

func parseTxMintCreator(data []byte, wantLogs func([]string) bool) (mint, creator string, createBlockTime *time.Time, wanted bool) {
	var wrap struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(data, &wrap) != nil || string(wrap.Result) == "null" {
		return "", "", nil, false
	}
	var r struct {
		BlockTime *int64 `json:"blockTime"`
		Meta      struct {
			PostTokenBalances []postTokenBal `json:"postTokenBalances"`
			LogMessages       []string       `json:"logMessages"`
		} `json:"meta"`
		Transaction struct {
			Message struct {
				AccountKeys []json.RawMessage `json:"accountKeys"`
			} `json:"message"`
		} `json:"transaction"`
	}
	if err := json.Unmarshal(wrap.Result, &r); err != nil {
		return "", "", nil, false
	}
	if r.BlockTime != nil && *r.BlockTime > 0 {
		t := time.Unix(*r.BlockTime, 0)
		createBlockTime = &t
	}
	creator = firstSignerFromKeys(r.Transaction.Message.AccountKeys)
	mint = pickMintFromPostBalances(r.Meta.PostTokenBalances)
	if mint == "" {
		mint = pickMintFromAccountKeys(r.Transaction.Message.AccountKeys)
	}
	if !wantLogs(r.Meta.LogMessages) {
		return mint, creator, createBlockTime, false
	}
	return mint, creator, createBlockTime, true
}

func refillMintFromConfirmed(sig, creator string, createBlockTime *time.Time, wantLogs func([]string) bool) (mintOut, creatorOut string, blockTimeOut *time.Time) {
	creatorOut = creator
	blockTimeOut = createBlockTime
	for attempt := 0; attempt < 3; attempt++ {
		data, err := rpc("getTransaction", []interface{}{
			sig,
			map[string]interface{}{
				"encoding":                       "json",
				"maxSupportedTransactionVersion": 0,
				"commitment":                     "confirmed",
			},
		})
		if err == nil {
			mint, parsedCreator, bt, ok := parseTxMintCreator(data, wantLogs)
			if creatorOut == "" && parsedCreator != "" {
				creatorOut = parsedCreator
			}
			if blockTimeOut == nil && bt != nil {
				blockTimeOut = bt
			}
			if mint != "" {
				return mint, creatorOut, blockTimeOut
			}
			_ = ok
		}
		if attempt < 2 {
			time.Sleep(time.Duration(80*(attempt+1)) * time.Millisecond)
		}
	}
	return "", creatorOut, blockTimeOut
}

func pickMintFromAccountKeys(keys []json.RawMessage) string {
	var fallback string
	for _, rawK := range keys {
		var o struct {
			Pubkey string `json:"pubkey"`
		}
		if json.Unmarshal(rawK, &o) == nil && o.Pubkey != "" {
			k := o.Pubkey
			if ignoredTokenMints[k] {
				continue
			}
			if strings.HasSuffix(k, "pump") {
				return k
			}
			if fallback == "" {
				fallback = k
			}
			continue
		}
		var s string
		if json.Unmarshal(rawK, &s) == nil && s != "" {
			if ignoredTokenMints[s] {
				continue
			}
			if strings.HasSuffix(s, "pump") {
				return s
			}
			if fallback == "" {
				fallback = s
			}
		}
	}
	return fallback
}

func formatCreateAge(t *time.Time) string {
	if t == nil {
		return "РІСЂРµРјСЏ Р±Р»РѕРєР° n/a"
	}
	age := time.Since(*t).Round(time.Second)
	return fmt.Sprintf("%v РЅР°Р·Р°Рґ В· Р±Р»РѕРє %s UTC", age, t.UTC().Format("02.01 15:04:05"))
}

func firstSignerFromKeys(keys []json.RawMessage) string {
	for _, rawK := range keys {
		var o struct {
			Pubkey string `json:"pubkey"`
			Signer bool   `json:"signer"`
		}
		if json.Unmarshal(rawK, &o) == nil && o.Signer && o.Pubkey != "" {
			return o.Pubkey
		}
	}
	for _, rawK := range keys {
		var s string
		if json.Unmarshal(rawK, &s) == nil && len(s) >= 32 {
			return s
		}
	}
	return ""
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  РђРќРўР-РЎРљРђРњ: RPC
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func rpcGetBalanceSOL(pub string) (float64, error) {
	data, err := rpc("getBalance", []interface{}{pub, map[string]string{"commitment": "confirmed"}})
	if err != nil {
		return 0, err
	}
	var env struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return 0, err
	}
	if env.Error != nil {
		return 0, fmt.Errorf("%s", env.Error.Message)
	}
	if len(env.Result) == 0 || string(env.Result) == "null" {
		return 0, fmt.Errorf("null result")
	}
	var lamF float64
	if json.Unmarshal(env.Result, &lamF) == nil {
		return lamF / 1e9, nil
	}
	var wrapped struct {
		Value float64 `json:"value"`
	}
	if json.Unmarshal(env.Result, &wrapped) == nil {
		return wrapped.Value / 1e9, nil
	}
	return 0, fmt.Errorf("balance parse")
}

func rpcGetBalanceSOLCached(pub string, ttl time.Duration) (float64, error) {
	if ttl <= 0 {
		return rpcGetBalanceSOL(pub)
	}
	now := time.Now()
	balanceCache.mu.Lock()
	if balanceCache.m == nil {
		balanceCache.m = make(map[string]balCacheEntry)
	}
	if e, ok := balanceCache.m[pub]; ok && now.Sub(e.ts) <= ttl {
		balanceCache.mu.Unlock()
		return e.sol, nil
	}
	balanceCache.mu.Unlock()

	sol, err := rpcGetBalanceSOL(pub)
	if err != nil {
		return 0, err
	}
	balanceCache.mu.Lock()
	balanceCache.m[pub] = balCacheEntry{sol: sol, ts: now}
	balanceCache.mu.Unlock()
	return sol, nil
}

func rpcRefreshBalanceSOLCached(pub string) (float64, error) {
	sol, err := rpcGetBalanceSOL(pub)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	balanceCache.mu.Lock()
	if balanceCache.m == nil {
		balanceCache.m = make(map[string]balCacheEntry)
	}
	balanceCache.m[pub] = balCacheEntry{sol: sol, ts: now}
	balanceCache.mu.Unlock()
	return sol, nil
}

// freezeAuthority в‰  null вЂ” РѕРїР°СЃРЅРѕ. mintAuthority С‡Р°СЃС‚Рѕ = bonding curve / pool (РЅРѕСЂРјР°) РёР»Рё null.
func rpcMintAuthorities(mint, bondingCurve string, extraAllow ...string) (badMintAuth, badFreeze bool, err error) {
	data, err := rpc("getAccountInfo", []interface{}{
		mint, map[string]string{"encoding": "jsonParsed"},
	})
	if err != nil {
		return false, false, err
	}
	var out struct {
		Result *struct {
			Value *struct {
				Data *struct {
					Parsed *struct {
						Info *struct {
							MintAuthority   interface{} `json:"mintAuthority"`
							FreezeAuthority interface{} `json:"freezeAuthority"`
						} `json:"info"`
					} `json:"parsed"`
				} `json:"data"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return false, false, err
	}
	if out.Result == nil || out.Result.Value == nil || out.Result.Value.Data == nil || out.Result.Value.Data.Parsed == nil || out.Result.Value.Data.Parsed.Info == nil {
		return false, false, fmt.Errorf("mint parse")
	}
	info := out.Result.Value.Data.Parsed.Info
	if s, ok := info.MintAuthority.(string); ok && s != "" {
		valid := s == bondingCurve
		for _, a := range extraAllow {
			if a != "" && s == a {
				valid = true
				break
			}
		}
		if !valid {
			badMintAuth = true
		}
	}
	if !jsonIsNull(info.FreezeAuthority) {
		badFreeze = true
	}
	return badMintAuth, badFreeze, nil
}

func jsonIsNull(v interface{}) bool {
	if v == nil {
		return true
	}
	switch s := v.(type) {
	case string:
		return s == "" || s == "null"
	default:
		return false
	}
}

func rpcGetTokenSupplyRaw(mint string) (string, error) {
	data, err := rpc("getTokenSupply", []interface{}{mint, map[string]string{"commitment": "confirmed"}})
	if err != nil {
		return "", err
	}
	var out struct {
		Result struct {
			Value struct {
				Amount string `json:"amount"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	return out.Result.Value.Amount, nil
}

func rpcParsedTokenAccountOwner(tokenAccAddr string) (owner string, err error) {
	data, err := rpc("getAccountInfo", []interface{}{
		tokenAccAddr, map[string]string{"encoding": "jsonParsed"},
	})
	if err != nil {
		return "", err
	}
	var out struct {
		Result *struct {
			Value *struct {
				Data *struct {
					Parsed *struct {
						Type string `json:"type"`
						Info *struct {
							Owner string `json:"owner"`
						} `json:"info"`
					} `json:"parsed"`
				} `json:"data"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	if out.Result == nil || out.Result.Value == nil || out.Result.Value.Data == nil || out.Result.Value.Data.Parsed == nil || out.Result.Value.Data.Parsed.Info == nil {
		return "", fmt.Errorf("no token acc")
	}
	return out.Result.Value.Data.Parsed.Info.Owner, nil
}

func rpcTokenMetadataURI(mint string) (string, error) {
	mintPK, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return "", err
	}
	metadataProgram := solana.MustPublicKeyFromBase58("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
	pda, _, err := solana.FindProgramAddress(
		[][]byte{[]byte("metadata"), metadataProgram.Bytes(), mintPK.Bytes()},
		metadataProgram,
	)
	if err != nil {
		return "", err
	}
	data, err := rpc("getAccountInfo", []interface{}{
		pda.String(), map[string]interface{}{"encoding": "base64"},
	})
	if err != nil {
		return "", err
	}
	var out struct {
		Result struct {
			Value *struct {
				Data []interface{} `json:"data"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	if out.Result.Value == nil || len(out.Result.Value.Data) < 1 {
		return "", fmt.Errorf("no metadata account")
	}
	rawB64, _ := out.Result.Value.Data[0].(string)
	if rawB64 == "" {
		return "", fmt.Errorf("empty metadata data")
	}
	raw, err := base64.StdEncoding.DecodeString(rawB64)
	if err != nil {
		return "", err
	}
	// Metaplex Metadata V1 layout: key(1) + updateAuth(32) + mint(32) + name + symbol + uri (borsh string/u32-len).
	off := 1 + 32 + 32
	readStr := func() (string, bool) {
		if off+4 > len(raw) {
			return "", false
		}
		n := int(binary.LittleEndian.Uint32(raw[off : off+4]))
		off += 4
		if n < 0 || off+n > len(raw) {
			return "", false
		}
		s := strings.TrimSpace(strings.Trim(string(raw[off:off+n]), "\x00"))
		off += n
		return s, true
	}
	if _, ok := readStr(); !ok { // name
		return "", fmt.Errorf("metadata parse name")
	}
	if _, ok := readStr(); !ok { // symbol
		return "", fmt.Errorf("metadata parse symbol")
	}
	uri, ok := readStr()
	if !ok || uri == "" {
		return "", fmt.Errorf("metadata uri missing")
	}
	return strings.TrimSpace(uri), nil
}

func hasSocialLinksInMetadata(mint string) (bool, string) {
	metadataCache.mu.Lock()
	if metadataCache.m == nil {
		metadataCache.m = make(map[string]metadataCacheEntry)
	}
	if e, ok := metadataCache.m[mint]; ok && time.Since(e.ts) <= 20*time.Minute {
		metadataCache.mu.Unlock()
		return e.ok, e.detail
	}
	metadataCache.mu.Unlock()

	uri, err := rpcTokenMetadataURI(mint)
	if err != nil {
		metadataCache.mu.Lock()
		metadataCache.m[mint] = metadataCacheEntry{ok: false, detail: "metadata URI РЅРµРґРѕСЃС‚СѓРїРµРЅ", ts: time.Now()}
		metadataCache.mu.Unlock()
		return false, "metadata URI РЅРµРґРѕСЃС‚СѓРїРµРЅ"
	}
	resp, err := httpClient.Get(uri)
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		metadataCache.mu.Lock()
		metadataCache.m[mint] = metadataCacheEntry{ok: false, detail: "metadata JSON РЅРµРґРѕСЃС‚СѓРїРµРЅ", ts: time.Now()}
		metadataCache.mu.Unlock()
		return false, "metadata JSON РЅРµРґРѕСЃС‚СѓРїРµРЅ"
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		metadataCache.mu.Lock()
		metadataCache.m[mint] = metadataCacheEntry{ok: false, detail: "metadata JSON read error", ts: time.Now()}
		metadataCache.mu.Unlock()
		return false, "metadata JSON read error"
	}
	s := strings.ToLower(string(body))
	hasTg := strings.Contains(s, "t.me/") || strings.Contains(s, "telegram")
	hasTw := strings.Contains(s, "twitter.com/") || strings.Contains(s, "x.com/")
	hasWeb := strings.Contains(s, "http://") || strings.Contains(s, "https://") || strings.Contains(s, "\"website\"")
	if !hasTg && !hasTw && !hasWeb {
		metadataCache.mu.Lock()
		metadataCache.m[mint] = metadataCacheEntry{ok: false, detail: "РЅРµС‚ social (tg/twitter/website) РІ metadata", ts: time.Now()}
		metadataCache.mu.Unlock()
		return false, "РЅРµС‚ social (tg/twitter/website) РІ metadata"
	}
	metadataCache.mu.Lock()
	metadataCache.m[mint] = metadataCacheEntry{ok: true, detail: "social ok", ts: time.Now()}
	metadataCache.mu.Unlock()
	return true, "social ok"
}

func socialFromCacheOnly(mint string) (bool, string, bool) {
	metadataCache.mu.Lock()
	defer metadataCache.mu.Unlock()
	if metadataCache.m == nil {
		return false, "", false
	}
	e, ok := metadataCache.m[mint]
	if !ok || time.Since(e.ts) > 20*time.Minute {
		return false, "", false
	}
	return e.ok, e.detail, true
}

func devSoldInFirstMinute(creator string, createAt *time.Time) (bool, string) {
	if creator == "" || createAt == nil {
		return false, "skip dev-sold check"
	}
	data, err := rpc("getSignaturesForAddress", []interface{}{
		creator, map[string]interface{}{"limit": 20},
	})
	if err != nil {
		return false, "dev sigs RPC"
	}
	var out struct {
		Result []struct {
			Signature string `json:"signature"`
			BlockTime int64  `json:"blockTime"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return false, "dev sigs parse"
	}
	until := createAt.Add(1 * time.Minute).Unix()
	start := createAt.Unix()
	for _, s := range out.Result {
		if s.BlockTime < start || s.BlockTime > until {
			continue
		}
		txRaw, err := rpc("getTransaction", []interface{}{
			s.Signature, map[string]interface{}{"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
		})
		if err != nil {
			continue
		}
		var tx struct {
			Result *struct {
				Meta *struct {
					LogMessages []string `json:"logMessages"`
				} `json:"meta"`
			} `json:"result"`
		}
		if json.Unmarshal(txRaw, &tx) != nil || tx.Result == nil || tx.Result.Meta == nil {
			continue
		}
		for _, l := range tx.Result.Meta.LogMessages {
			if strings.Contains(l, "Instruction: Sell") {
				return true, "dev sold in first minute"
			}
		}
	}
	return false, "dev holding"
}

func devCreatedTooMany(creator string) bool {
	if creator == "" {
		return false
	}
	data, err := rpc("getSignaturesForAddress", []interface{}{
		creator, map[string]interface{}{"limit": 30},
	})
	if err != nil {
		return false
	}
	var out struct {
		Result []struct {
			BlockTime int64 `json:"blockTime"`
		} `json:"result"`
	}
	if json.Unmarshal(data, &out) != nil {
		return false
	}
	now := time.Now().Unix()
	hourAgo := now - 3600
	count := 0
	for _, s := range out.Result {
		if s.BlockTime >= hourAgo && s.BlockTime <= now {
			count++
			if count > DEV_MAX_TXS_HOUR {
				return true
			}
		}
	}
	return false
}

func topHolderOwners(mint string, limit int) ([]string, error) {
	data, err := rpc("getTokenLargestAccounts", []interface{}{
		mint, map[string]string{"commitment": "confirmed"},
	})
	if err != nil {
		return nil, err
	}
	var out struct {
		Result struct {
			Value []struct {
				Address string `json:"address"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if limit > len(out.Result.Value) {
		limit = len(out.Result.Value)
	}
	owners := make([]string, 0, limit)
	seen := map[string]bool{}
	for _, row := range out.Result.Value[:limit] {
		owner, err := rpcParsedTokenAccountOwner(row.Address)
		if err != nil || owner == "" || seen[owner] {
			continue
		}
		seen[owner] = true
		owners = append(owners, owner)
	}
	return owners, nil
}

func bundledBuyersCheck(mint, creator string, createAt *time.Time) (bool, string) {
	if createAt == nil {
		return true, "no create time"
	}
	owners, err := topHolderOwners(mint, 8)
	if err != nil {
		return false, "bundle RPC"
	}
	blockTs := createAt.Unix()
	sameBlock := 0
	for _, o := range owners {
		if o == creator {
			continue
		}
		data, err := rpc("getSignaturesForAddress", []interface{}{
			o, map[string]interface{}{"limit": 1},
		})
		if err != nil {
			continue
		}
		var out struct {
			Result []struct {
				BlockTime int64 `json:"blockTime"`
			} `json:"result"`
		}
		if json.Unmarshal(data, &out) != nil || len(out.Result) == 0 {
			continue
		}
		if out.Result[0].BlockTime == blockTs {
			sameBlock++
		}
	}
	if sameBlock > 5 {
		return false, fmt.Sprintf("bundled attack: %d top holders РІ Р±Р»РѕРєРµ dev", sameBlock)
	}
	return true, fmt.Sprintf("bundle ok (%d)", sameBlock)
}

// РўРѕРї-10 С…РѕР»РґРµСЂРѕРІ (excl curve) >30% вЂ” РєР»Р°СЃС‚РµСЂ СЃРѕР»СЊС‘С‚ РІ РїРµСЂРІСѓСЋ СЃРµРєСѓРЅРґСѓ
func rpcTop10HoldersClusterOK(mint, bondingCurve string) (ok bool, detail string) {
	data, err := rpc("getTokenLargestAccounts", []interface{}{
		mint, map[string]string{"commitment": "confirmed"},
	})
	if err != nil {
		return true, "" // РїСЂРё РѕС€РёР±РєРµ РїСЂРѕРїСѓСЃРєР°РµРј
	}
	var out struct {
		Result struct {
			Value []struct {
				Address string `json:"address"`
				Amount  string `json:"amount"`
			} `json:"value"`
		} `json:"result"`
	}
	if json.Unmarshal(data, &out) != nil || len(out.Result.Value) == 0 {
		return true, ""
	}
	totalStr, err := rpcGetTokenSupplyRaw(mint)
	if err != nil {
		return true, ""
	}
	total, _ := strconv.ParseFloat(totalStr, 64)
	if total <= 0 {
		return true, ""
	}
	var top10NonCurve float64
	n := 0
	for _, row := range out.Result.Value {
		if n >= 10 {
			break
		}
		owner, err := rpcParsedTokenAccountOwner(row.Address)
		if err != nil {
			continue
		}
		if owner == bondingCurve {
			continue
		}
		amt, _ := strconv.ParseFloat(row.Amount, 64)
		top10NonCurve += amt
		n++
	}
	if total > 0 && top10NonCurve/total > TOP10_HOLDERS_MAX_PCT {
		return false, fmt.Sprintf("С‚РѕРї-10 С…РѕР»РґРµСЂРѕРІ %.0f%% > %.0f%% вЂ” РєР»Р°СЃС‚РµСЂ СЃРѕР»СЊС‘С‚", 100*top10NonCurve/total, 100*TOP10_HOLDERS_MAX_PCT)
	}
	return true, ""
}

// РўРѕРї-С…РѕР»РґРµСЂС‹: РєСЂРёРІР°СЏ РґРѕР»Р¶РЅР° РґРµСЂР¶Р°С‚СЊ Р»СЊРІРёРЅСѓСЋ РґРѕР»СЋ; РёРЅР°С‡Рµ вЂ” СЂР°Р·РґР°С‡Р°/СЃРєР°Рј-РїР°С‚С‚РµСЂРЅ
func antiScamThresholds() (creatorMinSOL, creatorMaxSOL, minCurveShare, maxNonCurveShare float64) {
	creatorMinSOL = CREATOR_SOL_MIN
	creatorMaxSOL = CREATOR_SOL_SUSPECT
	minCurveShare = 0.55
	maxNonCurveShare = MAX_NONCURVE_PCT

	switch envSignalProfile() {
	case "strict":
		creatorMinSOL = math.Max(creatorMinSOL, 0.06)
		minCurveShare = 0.62
		maxNonCurveShare = math.Min(maxNonCurveShare, 0.10)
	case "aggressive":
		creatorMinSOL = math.Min(creatorMinSOL, 2.0) // РЅРµ РЅРёР¶Рµ 2 SOL
		minCurveShare = 0.45
		maxNonCurveShare = math.Max(maxNonCurveShare, 0.18)
	}
	return
}

func rpcHolderDistributionOK(mint, bondingCurve, creator string, minCurveShare, maxNonCurveShare float64) (ok bool, detail string) {
	data, err := rpc("getTokenLargestAccounts", []interface{}{
		mint, map[string]string{"commitment": "confirmed"},
	})
	if err != nil {
		return false, "RPC largest"
	}
	var out struct {
		Result struct {
			Value []struct {
				Address string `json:"address"`
				Amount  string `json:"amount"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil || len(out.Result.Value) == 0 {
		return false, "РЅРµС‚ largest accounts"
	}
	totalStr, err := rpcGetTokenSupplyRaw(mint)
	if err != nil {
		return false, "supply"
	}
	total, _ := strconv.ParseFloat(totalStr, 64)
	if total <= 0 {
		return false, "supply=0"
	}

	var curveAmt float64
	var nonCurve float64
	lim := len(out.Result.Value)
	if lim > 12 {
		lim = 12
	}
	for _, row := range out.Result.Value[:lim] {
		owner, err := rpcParsedTokenAccountOwner(row.Address)
		if err != nil {
			continue
		}
		amt, _ := strconv.ParseFloat(row.Amount, 64)
		if owner == bondingCurve {
			curveAmt += amt
		} else {
			nonCurve += amt
			// СЃРѕР·РґР°С‚РµР»СЊ РЅР°РїСЂСЏРјСѓСЋ РґРµСЂР¶РёС‚ РєСЂСѓРїРЅС‹Р№ РѕС„С„-РєСЂРёРІРѕР№ СЃС‚РµРє вЂ” РїРѕРґРѕР·СЂРёС‚РµР»СЊРЅРѕ
			if owner == creator && amt/total > 0.03 {
				return false, fmt.Sprintf("СЃРѕР·РґР°С‚РµР»СЊ РґРµСЂР¶РёС‚ %.1f%% РІРЅРµ РєСЂРёРІРѕР№", 100*amt/total)
			}
		}
	}
	if okTop10, detailTop10 := rpcTop10HoldersClusterOK(mint, bondingCurve); !okTop10 {
		return false, detailTop10
	}
	if curveAmt/total < minCurveShare {
		return false, fmt.Sprintf("РІ РєСЂРёРІРѕР№ С‚РѕР»СЊРєРѕ %.0f%% СЃР°РїР»Р°СЏ (РЅСѓР¶РЅРѕ в‰Ґ%.0f%%)", 100*curveAmt/total, 100*minCurveShare)
	}
	if nonCurve/total > maxNonCurveShare {
		return false, fmt.Sprintf("%.0f%% С‚РѕРєРµРЅРѕРІ РІРЅРµ РєСЂРёРІРѕР№ (РјР°РєСЃ %.0f%%)", 100*nonCurve/total, 100*maxNonCurveShare)
	}
	return true, fmt.Sprintf("РєСЂРёРІР°СЏ ~%.0f%% supply", 100*curveAmt/total)
}

// mintAuthorityRef вЂ” pump: bonding curve PDA; launchlab: pool PDA (РґР»СЏ СЃСЂР°РІРЅРµРЅРёСЏ СЃ mintAuthority).
// liquidityVault вЂ” Р°РєРєР°СѓРЅС‚, РіРґРµ Р»РµР¶РёС‚ РѕСЃРЅРѕРІРЅР°СЏ Р»РёРєРІРёРґРЅРѕСЃС‚СЊ (pump: С‚Р° Р¶Рµ РєСЂРёРІР°СЏ; launchlab: pool_vault base).
func antiScamCheck(mint, mintAuthorityRef, liquidityVault, creator string, createAt *time.Time, extraMintAuth ...string) (ok bool, detail string) {
	if creator == "" {
		return false, "РЅРµС‚ pubkey СЃРѕР·РґР°С‚РµР»СЏ"
	}
	sol, err := rpcGetBalanceSOLCached(creator, CREATOR_BALANCE_CACHE_TTL)
	if err != nil {
		return false, "balance RPC"
	}
	creatorMinSOL, creatorMaxSOL, minCurveShare, maxNonCurveShare := antiScamThresholds()
	if sol < creatorMinSOL {
		return false, fmt.Sprintf("SOL СЃРѕР·РґР°С‚РµР»СЏ %.3f < %.2f", sol, creatorMinSOL)
	}
	if sol > creatorMaxSOL {
		return false, fmt.Sprintf("SOL СЃРѕР·РґР°С‚РµР»СЏ %.1f > %.0f (РїРѕРґРѕР·СЂ.)", sol, creatorMaxSOL)
	}
	// РћР±СЏР·Р°С‚РµР»СЊРЅС‹Р№ anti-scam: Сѓ С‚РѕРєРµРЅР° РґРѕР»Р¶РЅР° Р±С‹С‚СЊ С…РѕС‚СЏ Р±С‹ РѕРґРЅР° social-СЃСЃС‹Р»РєР°.
	okSocial, socialDetail := hasSocialLinksInMetadata(mint)
	if !okSocial {
		return false, socialDetail
	}
	if fastAntiScamMode() {
		badMint, badFreeze, err := rpcMintAuthorities(mint, mintAuthorityRef, extraMintAuth...)
		if err != nil {
			return false, "mint RPC"
		}
		if badMint {
			return false, "mintAuthority РЅРµ РєСЂРёРІР°СЏ (С‡СѓР¶Р°СЏ С‡РµРєР°РЅРєР°)"
		}
		if badFreeze {
			return false, "freezeAuthority (Р·Р°РјРѕСЂРѕР·РєР° СЃС‡РµС‚РѕРІ)"
		}
		if okTop10, detailTop10 := rpcTop10HoldersClusterOK(mint, mintAuthorityRef); !okTop10 {
			return false, detailTop10
		}
		if devCreatedTooMany(creator) {
			return false, "dev serial rugger (>7 tx/С‡Р°СЃ)"
		}
		return true, fmt.Sprintf("fast anti-scam | %s | dev %.2f SOL", socialDetail, sol)
	}

	type filterResult struct {
		key    string
		ok     bool
		detail string
	}
	results := make(chan filterResult, 5)
	var wg sync.WaitGroup
	wg.Add(5)

	// mint check
	go func() {
		defer wg.Done()
		badMint, badFreeze, err := rpcMintAuthorities(mint, mintAuthorityRef, extraMintAuth...)
		if err != nil {
			results <- filterResult{key: "mint", ok: false, detail: "mint RPC"}
			return
		}
		if badMint {
			results <- filterResult{key: "mint", ok: false, detail: "mintAuthority РЅРµ РєСЂРёРІР°СЏ (С‡СѓР¶Р°СЏ С‡РµРєР°РЅРєР°)"}
			return
		}
		if badFreeze {
			results <- filterResult{key: "mint", ok: false, detail: "freezeAuthority (Р·Р°РјРѕСЂРѕР·РєР° СЃС‡РµС‚РѕРІ)"}
			return
		}
		results <- filterResult{key: "mint", ok: true, detail: "mint ok"}
	}()

	// creator history
	go func() {
		defer wg.Done()
		if devCreatedTooMany(creator) {
			results <- filterResult{key: "creator", ok: false, detail: "dev serial rugger (>7 tx/С‡Р°СЃ)"}
			return
		}
		if sold, soldDetail := devSoldInFirstMinute(creator, createAt); sold {
			results <- filterResult{key: "creator", ok: false, detail: soldDetail}
			return
		}
		results <- filterResult{key: "creator", ok: true, detail: "creator ok"}
	}()

	// liquidity / holders check
	go func() {
		defer wg.Done()
		ok2, hd := rpcHolderDistributionOK(mint, liquidityVault, creator, minCurveShare, maxNonCurveShare)
		results <- filterResult{key: "liquidity", ok: ok2, detail: hd}
	}()

	// social metadata check (СѓР¶Рµ РїСЂРѕРІРµСЂРµРЅ РІС‹С€Рµ, РѕСЃС‚Р°РІР»СЏРµРј СЃС‚Р°С‚СѓСЃ РґР»СЏ РґРµС‚Р°Р»РёР·Р°С†РёРё)
	go func() {
		defer wg.Done()
		results <- filterResult{key: "social", ok: true, detail: socialDetail}
	}()

	// bundled buyers check
	go func() {
		defer wg.Done()
		okBundle, bundleDetail := bundledBuyersCheck(mint, creator, createAt)
		results <- filterResult{key: "bundle", ok: okBundle, detail: bundleDetail}
	}()

	wg.Wait()
	close(results)

	hd := ""
	socialInfo := ""
	bundleDetail := ""
	for r := range results {
		if !r.ok {
			return false, r.detail
		}
		switch r.key {
		case "liquidity":
			hd = r.detail
		case "social":
			socialInfo = r.detail
		case "bundle":
			bundleDetail = r.detail
		}
	}
	return true, hd + fmt.Sprintf(" | %s | %s | dev %.2f SOL", socialInfo, bundleDetail, sol)
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  WEBSOCKET
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func pumpCreateFromLogs(logs []string) bool {
	for _, l := range logs {
		if contains(l, "Instruction: Create") {
			return true
		}
	}
	return false
}

// listenProgram вЂ” РїРѕРґРїРёСЃРєР° РЅР° Р»РѕРіРё РѕРґРЅРѕР№ РїСЂРѕРіСЂР°РјРјС‹ (Pump.fun РёР»Рё Raydium LaunchLab).
func listenProgram(programID, prettyLabel string, wantLogs func([]string) bool, ch chan<- NewToken, tokenSrc string) {
	endpoints := []string{
		"wss://mainnet.helius-rpc.com/?api-key=" + HELIUS_API_KEY,
		"wss://atlas-mainnet.helius-rpc.com/?api-key=" + HELIUS_API_KEY,
	}
	headers := http.Header{}
	headers.Set("User-Agent", "Mozilla/5.0")
	headers.Set("Origin", "https://solana.com")
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	backoff := 3 * time.Second
	ei := 0

	for {
		url := endpoints[ei%len(endpoints)]
		ei++
		fmt.Printf("%s WS [%s] в†’ %s\n", cyan("рџ”Њ"), prettyLabel, url[:52]+"...")
		conn, resp, err := dialer.Dial(url, headers)
		if err != nil {
			code := 0
			if resp != nil {
				code = resp.StatusCode
			}
			if code == 403 {
				fmt.Println(red("вќЊ HTTP 403 вЂ” РїСЂРѕР±СѓСЋ РґСЂСѓРіРѕР№ endpoint..."))
			} else {
				fmt.Printf("%s [%s] %v\n", red("вќЊ"), prettyLabel, err)
			}
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(backoff*2), float64(30*time.Second)))
			continue
		}
		backoff = 3 * time.Second
		fmt.Printf("%s WebSocket вЂ” %s\n", green("вњ“"), prettyLabel)

		conn.WriteJSON(map[string]interface{}{
			"jsonrpc": "2.0", "id": 1,
			"method": "logsSubscribe",
			"params": []interface{}{
				map[string]interface{}{"mentions": []string{programID}},
				map[string]string{"commitment": "processed"},
			},
		})

		stop := make(chan struct{})
		go func() {
			t := time.NewTicker(20 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					conn.WriteMessage(websocket.PingMessage, nil)
				}
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("%s WS [%s] СЂР°Р·РѕСЂРІР°РЅ: %s\n", yellow("вљ "), prettyLabel, err.Error())
				close(stop)
				conn.Close()
				break
			}
			var m struct {
				Params struct {
					Result struct {
						Value struct {
							Signature string      `json:"signature"`
							Logs      []string    `json:"logs"`
							Err       interface{} `json:"err"`
						} `json:"value"`
					} `json:"result"`
				} `json:"params"`
			}
			if json.Unmarshal(msg, &m) != nil {
				continue
			}
			v := m.Params.Result.Value
			if v.Signature == "" || v.Err != nil {
				continue
			}
			if wantLogs(v.Logs) {
				ch <- NewToken{Sig: v.Signature, Source: tokenSrc, DetectedAt: time.Now()}
			}
		}
	}
}

// listenPumpWSS вЂ” РѕС‚РґРµР»СЊРЅС‹Р№ WSS-СЃР»СѓС€Р°С‚РµР»СЊ Р»РѕРіРѕРІ Pump.fun РґР»СЏ РјРёРЅРёРјР°Р»СЊРЅРѕР№ Р·Р°РґРµСЂР¶РєРё РЅР° РґРµС‚РµРєС‚Рµ.
func listenPumpWSS(ch chan<- NewToken) {
	listenProgram(PUMP_PROGRAM, "Pump.fun", pumpCreateFromLogs, ch, "pump")
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  WALLET
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func (w *Wallet) open(tok NewToken, sym string, spot float64) bool {
	if liveTradingEnabled() {
		w.mu.Lock()
		ddPct := maxDrawdownPct()
		stopBal := w.Start * (1 - ddPct/100.0)
		if w.Balance <= stopBal {
			w.mu.Unlock()
			consoleMu.Lock()
			fmt.Printf("%s open reject %s | drawdown_stop (bal=%.2f <= %.2f, max_dd=%.0f%%)\n",
				yellow("вљ "), sym, w.Balance, stopBal, ddPct)
			consoleMu.Unlock()
			return false
		}
		if len(w.Pos) >= MAX_POSITIONS {
			w.mu.Unlock()
			consoleMu.Lock()
			fmt.Printf("%s open reject %s | position_limit=%d\n", yellow("вљ "), sym, MAX_POSITIONS)
			consoleMu.Unlock()
			return false
		}
		w.mu.Unlock()
		return w.openLive(tok, sym, spot, 0)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.Pos) >= MAX_POSITIONS {
		return false
	}
	capital := stakeFromBalance(w.Balance)
	if capital <= 0 {
		return false
	}
	pool := usdToPoolAfterBuy(capital)
	if pool < 0.02 {
		return false
	}
	entry := effectiveBuyPrice(spot)
	tokens := pool / entry
	if tokens <= 0 {
		return false
	}
	w.Balance -= capital
	w.Pos[tok.Mint] = &Position{
		Mint:         tok.Mint,
		BondingCurve: tok.BondingCurve,
		Symbol:       sym,
		EntryPrice:   entry,
		Tokens:       tokens,
		PeakPrice:    entry,
		CapitalUSD:   capital,
		OpenedAt:     time.Now(),
		Source:       tokenSource(tok),
	}
	tx := solanaTxFeeUSD()
	fmt.Printf("\n%s Р’РҐРћР”  %-18s | РіСЂРѕСЃСЃ $%.2f | РІ РєСЂРёРІСѓСЋ ~$%.2f | pump %.0f%% + СЃРµС‚СЊ ~$%.3f | eff $%.10f | Р±Р°Р»Р°РЅСЃ $%.2f\n",
		cyan("в†’"), sym, capital, pool, pumpFeePct()*100, tx, entry, w.Balance)
	return true
}

// openLive вЂ” СЂРµР°Р»СЊРЅС‹Р№ СЃРІР°Рї SOLв†’С‚РѕРєРµРЅ С‚РѕР»СЊРєРѕ С‡РµСЂРµР· Pump.fun bonding curve (pump_direct).
func (w *Wallet) openLive(tok NewToken, sym string, spot float64, capitalUSD float64) bool {
	if abortIfTooLate(tok, "open_live_start") {
		consoleMu.Lock()
		fmt.Printf("%s open reject %s | latency_guard\n", yellow("вљ "), sym)
		consoleMu.Unlock()
		return false
	}
	if feeGuardActive() {
		consoleMu.Lock()
		fmt.Printf("%s open reject %s | fee_guard_active\n", yellow("вљ "), sym)
		consoleMu.Unlock()
		return false
	}
	if !liveUsePumpDirect(tok) {
		consoleMu.Lock()
		fmt.Println(yellow("вљ  LIVE: С‚РѕР»СЊРєРѕ Pump.fun РЅР° РєСЂРёРІРѕР№ вЂ” LaunchLab/РґСЂСѓРіРёРµ РёСЃС‚РѕС‡РЅРёРєРё РІ live РѕС‚РєР»СЋС‡РµРЅС‹ (Р±СѓРјР°РіР° Р±РµР· РёР·РјРµРЅРµРЅРёР№)."))
		consoleMu.Unlock()
		return false
	}
	solPrice := getSolUSD()
	if solPrice < 1 {
		return false
	}
	solBal, balErr := rpcRefreshBalanceSOLCached(livePub.String())
	if balErr != nil {
		w.mu.Lock()
		memBalanceUSD := w.Balance
		w.mu.Unlock()
		solBal = memBalanceUSD / solPrice
	}
	if solBal <= 0 {
		consoleMu.Lock()
		fmt.Printf("%s open reject %s | zero_balance\n", yellow("вљ "), sym)
		consoleMu.Unlock()
		return false
	}
	reserve := math.Max(liveReserveSOLValue(), minFeeReserveSOLValue())
	_ = capitalUSD
	availableAfterReserve := solBal - reserve
	solForSwap := liveFixedBuySOLValue()
	if availableAfterReserve < solForSwap {
		solForSwap = availableAfterReserve
	}
	if solForSwap <= 0.001 {
		consoleMu.Lock()
		fmt.Printf("%s open reject %s | РјР°Р»Рѕ SOL РїРѕСЃР»Рµ СЂРµР·РµСЂРІР° (bal=%.4f SOL reserve=%.4f)\n",
			yellow("вљ "), sym, solBal, reserve)
		consoleMu.Unlock()
		return false
	}
	lamports := uint64(solForSwap * 1e9)
	if lamports < 50_000 {
		consoleMu.Lock()
		fmt.Printf("%s open reject %s | lamports_too_low=%d\n", yellow("вљ "), sym, lamports)
		consoleMu.Unlock()
		return false
	}
	if !atomic.CompareAndSwapInt32(&liveBuyInFlight, 0, 1) {
		consoleMu.Lock()
		fmt.Printf("%s open reject %s | buy_in_flight\n", yellow("вљ "), sym)
		consoleMu.Unlock()
		return false
	}
	defer atomic.StoreInt32(&liveBuyInFlight, 0)
	var tokenRaw uint64
	var sig string
	var solIn uint64
	var sentAt time.Time
	var err error
	if !hotPathSilent() {
		fmt.Println(gray("вЏі Pump.fun: РїСЂСЏРјР°СЏ РїРѕРєСѓРїРєР° РЅР° РєСЂРёРІРѕР№вЂ¦"))
	}
	tokenRaw, sig, solIn, sentAt, err = PumpDirectBuy(tok.Mint, lamports)
	if err != nil && isRateLimitErr(err) {
		time.Sleep(1 * time.Second)
		tokenRaw, sig, solIn, sentAt, err = PumpDirectBuy(tok.Mint, lamports)
	}
	if err != nil {
		consoleMu.Lock()
		fmt.Printf("%s Pump buy %s | %v\n", red("вќЊ"), sym, err)
		consoleMu.Unlock()
		if isInsufficientFeeErr(err) {
			activateFeeGuard(45*time.Second, "insufficient funds for fee on buy")
		}
		syncWalletBalanceUSDFresh(w)
		return false
	}
	if !tok.DetectedAt.IsZero() && !sentAt.IsZero() {
		delayMs := sentAt.Sub(tok.DetectedAt).Milliseconds()
		consoleMu.Lock()
		fmt.Printf("%s Transaction Sent | %s | %s | delay=%dms\n",
			gray("вЏ±"), "$"+short(tok.Mint), sentAt.Format(time.RFC3339Nano), delayMs)
		lat := getLastBuyLatency()
		fmt.Printf("%s latency breakdown | filters=%dms | blockhash(cache)=%dms | signing=%dms | sending=%dms\n",
			gray("вЏ±"), lat.FiltersMs, lat.BlockhashMs, lat.SigningMs, lat.SendingMs)
		if !tok.FiltersPassedAt.IsZero() {
			d1 := tok.FiltersPassedAt.Sub(tok.DetectedAt).Milliseconds()
			d2 := sentAt.Sub(tok.FiltersPassedAt).Milliseconds()
			if !lat.SignedAt.IsZero() {
				d2 = lat.SignedAt.Sub(tok.FiltersPassedAt).Milliseconds()
			}
			d3 := int64(lat.SendingMs)
			fmt.Printf("%s dt_detect_to_filter=%dms | dt_filter_to_sign=%dms | dt_sign_to_send=%dms\n",
				gray("вЏ±"), d1, d2, d3)
		}
		consoleMu.Unlock()
	}
	syncWalletBalanceUSDFresh(w)
	tokens := float64(tokenRaw) / 1e6 // pump: 6 decimals
	entry := spot
	if tokens > 0 {
		entry = (float64(solIn) / 1e9 * solPrice) / tokens
	}
	capitalEff := float64(solIn) / 1e9 * solPrice

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.Pos) >= MAX_POSITIONS {
		consoleMu.Lock()
		fmt.Printf("%s open reject %s | position_limit=%d\n", yellow("вљ "), sym, MAX_POSITIONS)
		consoleMu.Unlock()
		return false
	}
	w.Pos[tok.Mint] = &Position{
		Mint:         tok.Mint,
		BondingCurve: tok.BondingCurve,
		Symbol:       sym,
		EntryPrice:   entry,
		Tokens:       tokens,
		PeakPrice:    entry,
		CapitalUSD:   capitalEff,
		OpenedAt:     time.Now(),
		Live:         true,
		TokenRaw:     tokenRaw,
		BuyLamports:  solIn,
		Source:       tokenSource(tok),
	}
	go w.verifyBuyAsync(tok.Mint, sym, sig, tok.DetectedAt)
	w.saveActivePositionsLocked()
	bal := w.Balance
	fmt.Printf("\n%s Р’РҐРћР” LIVE %-18s | ~$%.2f SOLв†’С‚РѕРєРµРЅ | eff $%.10f | raw %d | %s | Р±Р°Р»Р°РЅСЃ $%.2f\n",
		cyan("в†’"), sym, capitalEff, entry, tokenRaw, gray(sig), bal)
	return true
}

func (w *Wallet) closePos(mint, reason string, spot float64) {
	w.mu.Lock()
	pos, ok := w.Pos[mint]
	if !ok {
		w.mu.Unlock()
		return
	}
	live := pos.Live
	snap := snapshotPosition(pos)
	delete(w.Pos, mint)
	w.mu.Unlock()

	if live {
		w.closePosLive(snap, reason, spot)
		return
	}

	net := exitNetAfterSell(snap.Tokens, spot)
	pnl := net - snap.CapitalUSD
	pct := 0.0
	if snap.CapitalUSD > 0 {
		pct = pnl / snap.CapitalUSD * 100
	}
	feesEst := snap.CapitalUSD*pumpFeePct() + snap.Tokens*spot*pumpFeePct() + 2*solanaTxFeeUSD()
	dur := time.Since(snap.OpenedAt).Round(time.Second)

	bk := bucketExitReason(reason)
	w.mu.Lock()
	w.Balance += net
	w.Closed = append(w.Closed, ClosedTrade{
		Symbol: snap.Symbol, Mint: snap.Mint, CapitalUSD: snap.CapitalUSD,
		ExitNetUSD: net, FeesUSD: feesEst, PnL: pnl, PnLPct: pct,
		Reason: reason, Dur: dur,
	})
	if pnl < 0 {
		w.ExitLoss[bk]++
	} else {
		w.ExitWin[bk]++
	}
	w.saveActivePositionsLocked()
	bal := w.Balance
	w.mu.Unlock()

	icon := green("вњ“")
	ps := green(fmt.Sprintf("+$%.2f (+%.0f%%)", pnl, pct))
	if pnl < 0 {
		icon = red("вњ—")
		ps = red(fmt.Sprintf("$%.2f (%.0f%%)", pnl, pct))
	}
	fmt.Printf("\n%s Р’Р«РҐРћР” %-18s | %s | РЅРµС‚С‚Рѕ $%.2f (~РєРѕРјРёСЃСЃРёРё ~$%.2f) | %-24s | %s | Р±Р°Р»: $%.2f\n",
		icon, snap.Symbol, ps, net, feesEst, reason, dur, bal)
	fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(snap.Mint)))
	if pnl < 0 {
		fmt.Printf("   %s [%s] %s\n", yellow("в“ СѓС‡С‘С‚"), bk, lossLearningHint(reason))
	}
}

func (w *Wallet) closePosLive(pos *Position, reason string, spot float64) {
	if !liveUsePumpDirectClose(pos) {
		consoleMu.Lock()
		fmt.Println(yellow("вљ  LIVE РІС‹С…РѕРґ: С‚РѕР»СЊРєРѕ Pump.fun РЅР° РєСЂРёРІРѕР№ вЂ” СЌС‚Р° РїРѕР·РёС†РёСЏ РЅРµ pump; Р·Р°РєСЂРѕР№ РІСЂСѓС‡РЅСѓСЋ РЅР° DEX."))
		consoleMu.Unlock()
		syncWalletBalanceUSDFresh(w)
		w.mu.Lock()
		w.Pos[pos.Mint] = pos
		w.saveActivePositionsLocked()
		w.mu.Unlock()
		return
	}
	// РќРµ РїСЂРѕРґР°С‘Рј РІ СЃР»РёС€РєРѕРј РїР»РѕС…РѕР№ С‚РёРє: РµСЃР»Рё РѕР¶РёРґР°РµРјРѕРµ РїСЂРѕСЃРєР°Р»СЊР·С‹РІР°РЅРёРµ > 10%, Р¶РґС‘Рј СЃР»РµРґСѓСЋС‰РёР№ С†РёРєР».
	if spot > 0 {
		if estSlip, err := PumpDirectEstimateSellSlippage(pos.Mint, pos.TokenRaw, spot); err == nil && estSlip > SELL_SLIPPAGE_GUARD {
			consoleMu.Lock()
			fmt.Printf("%s wait better tick: est sell slippage %.1f%% > %.0f%%\n",
				yellow("вЏё"), estSlip*100, SELL_SLIPPAGE_GUARD*100)
			consoleMu.Unlock()
			w.mu.Lock()
			w.Pos[pos.Mint] = pos
			w.saveActivePositionsLocked()
			w.mu.Unlock()
			return
		}
	}
	var sig string
	var solOut uint64
	fmt.Println(gray("вЏі Pump.fun: РїСЂРѕРґР°Р¶Р° РЅР° РєСЂРёРІРѕР№вЂ¦"))
	sig, solOut, err := PumpDirectSellAll(pos.Mint)
	solUSD := getSolUSD()
	if err != nil {
		consoleMu.Lock()
		fmt.Printf("%s %v\n", red("вќЊ Pump sell:"), err)
		consoleMu.Unlock()
		if isInsufficientFeeErr(err) {
			activateFeeGuard(60*time.Second, "insufficient funds for fee on sell (need SOL for exit tx)")
		}
		syncWalletBalanceUSD(w)
		w.mu.Lock()
		w.Pos[pos.Mint] = pos
		w.saveActivePositionsLocked()
		w.mu.Unlock()
		return
	}
	syncWalletBalanceUSDFresh(w)

	net := float64(solOut) / 1e9 * solUSD
	pnl := net - pos.CapitalUSD
	pct := 0.0
	if pos.CapitalUSD > 0 {
		pct = pnl / pos.CapitalUSD * 100
	}
	feesEst := 2 * solanaTxFeeUSD()
	dur := time.Since(pos.OpenedAt).Round(time.Second)
	bk := bucketExitReason(reason)

	w.mu.Lock()
	w.Closed = append(w.Closed, ClosedTrade{
		Symbol: pos.Symbol, Mint: pos.Mint, CapitalUSD: pos.CapitalUSD,
		ExitNetUSD: net, FeesUSD: feesEst, PnL: pnl, PnLPct: pct,
		Reason: reason, Dur: dur,
	})
	if pnl < 0 {
		w.ExitLoss[bk]++
	} else {
		w.ExitWin[bk]++
	}
	w.saveActivePositionsLocked()
	bal := w.Balance
	w.mu.Unlock()

	icon := green("вњ“")
	ps := green(fmt.Sprintf("+$%.2f (+%.0f%%)", pnl, pct))
	if pnl < 0 {
		icon = red("вњ—")
		ps = red(fmt.Sprintf("$%.2f (%.0f%%)", pnl, pct))
	}
	fmt.Printf("\n%s Р’Р«РҐРћР” LIVE %-18s | %s | РЅРµС‚С‚Рѕ ~$%.2f | %s | %s | Р±Р°Р»: $%.2f\n",
		icon, pos.Symbol, ps, net, reason, dur, bal)
	fmt.Printf("   %s %s\n", gray("sig"), gray(sig))
	fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(pos.Mint)))
	if pnl < 0 {
		fmt.Printf("   %s [%s] %s\n", yellow("в“ СѓС‡С‘С‚"), bk, lossLearningHint(reason))
	}
}

func (w *Wallet) stats() {
	if liveTradingEnabled() {
		syncWalletBalanceUSD(w)
	}
	w.mu.Lock()
	wins, total := 0, 0.0
	for _, t := range w.Closed {
		total += t.PnL
		if t.PnL > 0 {
			wins++
		}
	}
	wr := 0.0
	n := len(w.Closed)
	if n > 0 {
		wr = float64(wins) / float64(n) * 100
	}
	pct := total / w.Start * 100
	nOpen := len(w.Pos)
	bal := w.Balance
	start := w.Start
	lossBk := make(map[string]int)
	winBk := make(map[string]int)
	for k, v := range w.ExitLoss {
		lossBk[k] = v
	}
	for k, v := range w.ExitWin {
		winBk[k] = v
	}
	bs := green(fmt.Sprintf("$%.2f", bal))
	if bal < start {
		bs = red(fmt.Sprintf("$%.2f", bal))
	}
	ps := green(fmt.Sprintf("+$%.2f (+%.1f%%)", total, pct))
	if total < 0 {
		ps = red(fmt.Sprintf("$%.2f (%.1f%%)", total, pct))
	}
	w.mu.Unlock()

	consoleMu.Lock()
	defer consoleMu.Unlock()
	title := "PAPER WALLET вЂ” Р Р•РђР›Р¬РќР«Р• Р”РђРќРќР«Р•"
	if liveTradingEnabled() {
		title = "LIVE WALLET вЂ” MAINNET (Pump.fun)"
	}
	fmt.Println("\n" + bold("в”Њв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”ђ"))
	fmt.Println(bold("в”‚  " + title + "                    в”‚"))
	fmt.Println(bold("в”њв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”¤"))
	fmt.Printf("в”‚  Р‘Р°Р»Р°РЅСЃ:   %-33sв”‚\n", bs)
	fmt.Printf("в”‚  PnL:      %-33sв”‚\n", ps)
	fmt.Printf("в”‚  РЎРґРµР»РѕРє:   %-33sв”‚\n", fmt.Sprintf("%d Р·Р°РєСЂС‹С‚Рѕ | %d РѕС‚РєСЂС‹С‚Рѕ", n, nOpen))
	fmt.Printf("в”‚  Win/Loss: %-33sв”‚\n",
		fmt.Sprintf("%s/%s  WR: %.0f%%",
			green(fmt.Sprintf("%d", wins)),
			red(fmt.Sprintf("%d", n-wins)), wr))
	if len(lossBk) > 0 || len(winBk) > 0 {
		fmt.Println(bold("в”њв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”¤"))
		if len(winBk) > 0 {
			fmt.Printf("в”‚  РџР»СЋСЃС‹ РїРѕ РІС‹С…РѕРґСѓ: %-26sв”‚\n", gray(formatExitBuckets(winBk)))
		}
		if len(lossBk) > 0 {
			fmt.Printf("в”‚  РњРёРЅСѓСЃС‹ РїРѕ РІС‹С…РѕРґСѓ: %-25sв”‚\n", yellow(formatExitBuckets(lossBk)))
		}
	}
	fmt.Println(bold("в””в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”"))
	if bal >= 21 {
		fmt.Println(green("рџЋ‰ Р¦Р•Р›Р¬ $21 Р‘Р«Р›Рђ Р‘Р« Р”РћРЎРўРР“РќРЈРўРђ!"))
	}
}

func formatExitBuckets(m map[string]int) string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}

// runPaperSelfTest вЂ” РѕРґРёРЅ РІРёСЂС‚СѓР°Р»СЊРЅС‹Р№ РІС…РѕРґ Рё РІС‹С…РѕРґ (Р±РµР· mainnet), С‡С‚РѕР±С‹ РїСЂРѕРІРµСЂРёС‚СЊ PnL Рё РєРѕРјРёСЃСЃРёРё Р·Р° СЃРµРєСѓРЅРґС‹.
func runPaperSelfTest() {
	dw := newWallet()
	mint := "SelfTestMintSelfTestMintSelfTestMintPuump"
	tok := NewToken{Mint: mint, BondingCurve: "SelfTestBC111111111111111111111111111111111111", Sig: "selftest"}
	sym := "$SELF..pump"
	spot := 0.00012345

	consoleMu.Lock()
	defer consoleMu.Unlock()

	fmt.Println(bold("\nв•ђв•ђв•ђ PAPER SELF-TEST (РєРѕС€РµР»С‘Рє + РєРѕРјРёСЃСЃРёРё, Р±РµР· WebSocket) в•ђв•ђв•ђ"))
	fmt.Printf("РЎС‚Р°СЂС‚РѕРІС‹Р№ Р±Р°Р»Р°РЅСЃ: $%.2f В· СЃС‚Р°РІРєР° РІ live: %.3f SOL (fixed)\n\n",
		dw.Start, liveFixedBuySOLValue())

	if !dw.open(tok, sym, spot) {
		fmt.Println(red("open() РЅРµ РїСЂРѕС€С‘Р» вЂ” РјР°Р»Рѕ Р±Р°Р»Р°РЅСЃР° РёР»Рё СЃС‚Р°РІРєР°"))
		return
	}

	exitSpot := spot * 1.10 // +10% Рє spot вЂ” РѕР¶РёРґР°РµРј РїР»СЋСЃ РїРѕСЃР»Рµ РєРѕРјРёСЃСЃРёР№
	dw.closePos(mint, "SELFTEST +10% spot", exitSpot)

	dw.mu.Lock()
	bal := dw.Balance
	nClosed := len(dw.Closed)
	var lastPnL float64
	if nClosed > 0 {
		lastPnL = dw.Closed[nClosed-1].PnL
	}
	dw.mu.Unlock()

	fmt.Printf("\n%s РС‚РѕРі: Р±Р°Р»Р°РЅСЃ $%.2f В· РїРѕСЃР»РµРґРЅСЏСЏ СЃРґРµР»РєР° PnL %+.2f USD\n", bold("в—Џ"), bal, lastPnL)
	if lastPnL > 0 {
		fmt.Println(green("вњ“ Р¦РµРїРѕС‡РєР° РІС…РѕРґ в†’ РІС‹С…РѕРґ Рё СѓС‡С‘С‚ РєРѕРјРёСЃСЃРёР№ СЂР°Р±РѕС‚Р°РµС‚ (РїР»СЋСЃ РѕР¶РёРґР°РµРј РїСЂРё +10% spot)."))
	} else {
		fmt.Println(yellow("в“ PnL РїРѕСЃР»Рµ РєРѕРјРёСЃСЃРёР№ РЅРµ РІ РїР»СЋСЃРµ вЂ” С‚Р°Рє Р±С‹РІР°РµС‚ РїСЂРё СѓР·РєРѕР№ РјР°СЂР¶Рµ; Р»РѕРіРёРєР° РєРѕС€РµР»СЊРєР° РІСЃС‘ СЂР°РІРЅРѕ РѕС‚СЂР°Р±РѕС‚Р°Р»Р°."))
	}
	fmt.Println(gray("Live: go run . (Р±РµР· -selftest) вЂ” Р¶РґС‘С‚ СЂРµР°Р»СЊРЅС‹Рµ create-С‚РѕРєРµРЅС‹; live_wallet + pump_direct."))
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  РњРћРќРРўРћР РРќР“ вЂ” С†РµРЅР° РёР· bonding curve РЅР°РїСЂСЏРјСѓСЋ
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func monitor(w *Wallet, mint, bcAddr, sym, source string) {
	ticker := time.NewTicker(monitorTickInterval())
	maxHold := maxHoldDuration()
	fastExitAfter := fastExitAfterDuration()
	takeProfit := takeProfitMult()
	stopLossHard := stopLossHardMult()
	stopConfirmLvl := stopConfirmLvlMult()
	timeout := time.NewTimer(maxHold)
	defer ticker.Stop()
	defer timeout.Stop()

	fmt.Printf("  %s %s\n", gray("DEX"), cyan(dexScreenerURL(mint)))

	consecutiveFails := 0
	confirmedStopTicks := 0
	maxPriceFails := monitorMaxPriceFails()
	failLogEvery := monitorFailLogEvery()
	lockFails := noPriceProfitLockFails()
	lockMinMult := noPriceProfitLockMinMult()
	panicNoPricePeakMult := noPricePanicSellPeakMult()
	quickWindow := quickPumpWindow()
	quickTP := quickPumpTakeProfitMult()
	flatAfter := flatExitAfter()
	flatMin := flatExitMinMult()
	flatMax := flatExitMaxMult()
	earlyStopWin := earlyStopWindow()
	earlyStopMult := earlyStopLossMult()
	impulsePeak := impulseProtectPeakMult()
	impulseFloor := impulseProtectFloorMult()
	var lastMult float64
	var lastPrint time.Time
	var lastFailPrint time.Time
	var lastGoodPrice float64
	var lastGoodAt time.Time
	const monitorPrintMinMove = 0.0025 // ~0.25% Рє С†РµРЅРµ РІС…РѕРґР° вЂ” РЅРѕРІР°СЏ СЃС‚СЂРѕРєР°
	monitorHeartbeat := 12 * time.Second

	for {
		select {
		case <-timeout.C:
			snap, _ := getCurveSnapshotWithRetry(bcAddr, source)
			px := 0.0
			if snap != nil {
				px = snap.PriceUSD
			}
			holdStr := fmt.Sprintf("%.0fСЃ", maxHold.Seconds())
			if maxHold >= time.Minute {
				holdStr = fmt.Sprintf("%.0f РјРёРЅ", maxHold.Minutes())
			}
			w.closePos(mint, fmt.Sprintf("РўРђР™РњРђРЈРў %s", holdStr), px)
			return

		case <-ticker.C:
			w.mu.Lock()
			pos, open := w.Pos[mint]
			w.mu.Unlock()
			if !open {
				return
			}

			snap, err := getCurveSnapshotWithRetry(bcAddr, source)
			if err != nil || snap == nil || snap.PriceUSD <= 0 {
				consecutiveFails++
				pos.mu.Lock()
				openedFor := time.Since(pos.OpenedAt)
				livePos := pos.Live
				entry := pos.EntryPrice
				peak := pos.PeakPrice
				pos.mu.Unlock()
				// Р•СЃР»Рё СѓР¶Рµ Р±С‹Р» Р·Р°С„РёРєСЃРёСЂРѕРІР°РЅ РїР»СЋСЃ Рё РІРЅРµР·Р°РїРЅРѕ РїСЂРѕРїР°Р»Р° С†РµРЅР° вЂ” РІС‹С…РѕРґРёРј СЃСЂР°Р·Сѓ, РЅРµ РѕС‚РґР°С‘Рј РїСЂРёР±С‹Р»СЊ.
				if livePos && consecutiveFails >= lockFails && entry > 0 {
					lastGoodMult := 0.0
					if lastGoodPrice > 0 {
						lastGoodMult = lastGoodPrice / entry
					}
					if lastGoodMult >= lockMinMult || peak >= entry*lockMinMult {
						stale := time.Since(lastGoodAt).Round(100 * time.Millisecond)
						w.closePos(mint, fmt.Sprintf("LOCK PROFIT (РЅРµС‚ С†РµРЅС‹ x%d, stale=%v)", consecutiveFails, stale), 0)
						return
					}
				}
				if livePos && entry > 0 && peak >= entry*panicNoPricePeakMult {
					w.closePos(mint, fmt.Sprintf("PANIC SELL (РЅРµС‚ С†РµРЅС‹ РїРѕСЃР»Рµ РїРёРєР° +%.0f%%)", (panicNoPricePeakMult-1)*100), 0)
					return
				}
				// Р’ live/turbo РїРµСЂРІС‹Рµ СЃРµРєСѓРЅРґС‹ С‡Р°СЃС‚Рѕ РґР°СЋС‚ RPC-РїСѓСЃС‚РѕС‚Сѓ; РЅРµ Р·Р°РєСЂС‹РІР°РµРј СЃР»РёС€РєРѕРј СЂР°РЅРѕ.
				if consecutiveFails > maxPriceFails && !(livePos && openedFor < 20*time.Second) {
					w.closePos(mint, "РўРћРљР•Рќ РЈРњР•Р  (РЅРµС‚ РґР°РЅРЅС‹С…)", 0)
					return
				}
				if livePos && openedFor < 20*time.Second && consecutiveFails > maxPriceFails {
					consecutiveFails = maxPriceFails
				}
				now := time.Now()
				shouldPrint := consecutiveFails <= 2 || (consecutiveFails%failLogEvery == 0)
				if now.Sub(lastFailPrint) >= 1200*time.Millisecond && shouldPrint {
					fmt.Printf("  %s %-18s | РѕС€РёР±РєР° С†РµРЅС‹ (%d/%d)\n", gray("?"), sym, consecutiveFails, maxPriceFails)
					lastFailPrint = now
				}
				continue
			}
			consecutiveFails = 0
			lastGoodPrice = snap.PriceUSD
			lastGoodAt = time.Now()

			price := snap.PriceUSD
			progress := snap.Progress

			if snap.Complete {
				fmt.Printf("  %s %-18s | РљР РР’РђРЇ Р—РђР’Р•Р РЁР•РќРђ в†’ РјРёРіСЂР°С†РёСЏ\n", yellow("рџљЂ"), sym)
				w.closePos(mint, "РњРР“Р РђР¦РРЇ (complete)", price)
				return
			}

			pos.mu.Lock()
			if price > pos.PeakPrice {
				pos.PeakPrice = price
			}
			if pos.PeakPrice >= pos.EntryPrice*BREAKEVEN_ARM {
				pos.BreakevenArmed = true
			}
			entry := pos.EntryPrice
			peak := pos.PeakPrice
			breakeven := pos.BreakevenArmed
			opened := pos.OpenedAt
			livePos := pos.Live
			halfTaken := pos.HalfTaken
			tokenRaw := pos.TokenRaw
			pos.mu.Unlock()

			mult := price / entry
			pct := (mult - 1) * 100
			ps := green(fmt.Sprintf("+%.1f%%", pct))
			if pct < 0 {
				ps = red(fmt.Sprintf("%.1f%%", pct))
			}
			// РџРµС‡Р°С‚СЊ С‚РѕР»СЊРєРѕ РїСЂРё Р·Р°РјРµС‚РЅРѕРј РґРІРёР¶РµРЅРёРё mult РёР»Рё СЂР°Р· РІ heartbeat (Р±РµР· РґСѓР±Р»РµР№)
			now := time.Now()
			if lastPrint.IsZero() || math.Abs(mult-lastMult) >= monitorPrintMinMove || now.Sub(lastPrint) >= monitorHeartbeat {
				lastMult = mult
				lastPrint = now
				fmt.Printf("  %s %-18s | spot: %s | $%.10f | x%.3f | curve: %.1f%%\n",
					cyan("в—€"), sym, ps, price, mult, progress*100)
			}

			age := time.Since(opened)
			if earlyStopWin > 0 && age <= earlyStopWin && mult <= earlyStopMult {
				w.closePos(mint, fmt.Sprintf("EARLY STOP -%.0f%% (%.0fs)", (1-earlyStopMult)*100, earlyStopWin.Seconds()), price)
				return
			}
			if impulsePeak > 1 && peak >= entry*impulsePeak && mult <= impulseFloor {
				w.closePos(mint, fmt.Sprintf("IMPULSE LOST (Р±С‹Р»Рѕ +%.1f%%, now %.1f%%)", (impulsePeak-1)*100, (mult-1)*100), price)
				return
			}
			// Dynamic exit: РїРѕСЃР»Рµ +15% С„РёРєСЃРёСЂСѓРµРј РјРёРЅРёРјСѓРј +5%; С‚Р°РєР¶Рµ РІС‹С…РѕРґРёРј РїСЂРё РїСЂРѕСЃР°РґРєРµ 15% РѕС‚ РїРёРєР°.
			if peak >= entry*1.15 {
				if price <= entry*1.05 {
					w.closePos(mint, "PROFIT LOCK (РїРѕСЃР»Рµ +15% РЅРµ РѕС‚РґР°С‘Рј РЅРёР¶Рµ +5%)", price)
					return
				}
				if price <= peak*0.85 {
					w.closePos(mint, fmt.Sprintf("DRAWDOWN EXIT (-15%% РѕС‚ РїРёРєР° x%.2f)", peak/entry), price)
					return
				}
			}
			if quickWindow > 0 && livePos && age <= quickWindow {
				if mult >= quickTP {
					w.closePos(mint, fmt.Sprintf("QUICK PUMP РўР•Р™Рљ ~+%.0f%% (%.1fs)", (quickTP-1)*100, quickWindow.Seconds()), price)
					return
				}
			} else if mult >= takeProfit {
				w.closePos(mint, fmt.Sprintf("РўР•Р™Рљ ~+%.0f%% spot", (takeProfit-1)*100), price)
				return
			}
			// Recovery: РЅР° +100% С„РёРєСЃРёСЂСѓРµРј РїРѕР»РѕРІРёРЅСѓ, РѕСЃС‚Р°С‚РѕРє РІРµРґС‘Рј С‚СЂРµР№Р»РёРЅРіРѕРј.
			if livePos && !halfTaken && mult >= 2.0 && tokenRaw > 10 {
				if sig, soldRaw, solOut, err := PumpDirectSellFraction(mint, 0.5); err == nil && soldRaw > 0 {
					w.mu.Lock()
					if p2, ok := w.Pos[mint]; ok {
						if p2.TokenRaw > soldRaw {
							p2.TokenRaw -= soldRaw
						} else {
							p2.TokenRaw = 0
						}
						p2.Tokens = p2.Tokens * 0.5
						p2.CapitalUSD = p2.CapitalUSD * 0.5
						p2.HalfTaken = true
					}
					w.saveActivePositionsLocked()
					w.mu.Unlock()
					fmt.Printf("  %s %-18s | partial +100%%: sold 50%% | out %.4f SOL | %s\n",
						green("в†—"), sym, float64(solOut)/1e9, gray(sig))
				}
			}

			// РўСЂРµР№Р»РёРЅРі: С‚РѕР»СЊРєРѕ РїРѕСЃР»Рµ TRAIL_MIN_AGE; СЃС‚РѕРї = max(РѕС‚РєР°С‚ РѕС‚ РїРёРєР°, РјРёРЅ. +4% Рє РІС…РѕРґСѓ)
			if time.Since(opened) >= TRAIL_MIN_AGE && peak >= entry*TRAIL_ACTIVATE {
				trailLine := peak * (1 - TRAILING)
				floorLine := entry * TRAIL_MIN_PROFIT
				stopLine := math.Max(trailLine, floorLine)
				if price <= stopLine {
					w.closePos(mint, fmt.Sprintf("РўР Р•Р™Р›РРќР“ (РїРёРє x%.2f, РїРѕР» в‰Ґ+%.0f%%)", peak/entry, (TRAIL_MIN_PROFIT-1)*100), price)
					return
				}
			}

			if breakeven && price < entry {
				w.closePos(mint, "Р‘Р Р•Р™Рљ-РР’Рќ РїРѕСЃР»Рµ РёРјРїСѓР»СЊСЃР°", price)
				return
			}
			if flatAfter > 0 && time.Since(opened) >= flatAfter && mult >= flatMin && mult <= flatMax {
				w.closePos(mint, fmt.Sprintf("FLAT EXIT (%.0fs, x%.3f)", flatAfter.Seconds(), mult), price)
				return
			}
			// Fast Exit: Р·Р° 30СЃ РЅРµС‚ +5% вЂ” РІС‹С…РѕРґРёРј, РЅРµ Р¶РґС‘Рј РїРѕРєР° СЃРѕР»СЊС‘С‚
			if time.Since(opened) >= fastExitAfter && mult < FAST_EXIT_MIN_MULT {
				w.closePos(mint, fmt.Sprintf("FAST EXIT (РЅРµС‚ +%.0f%% Р·Р° %.0fСЃ, spot %.1f%%)", (FAST_EXIT_MIN_MULT-1)*100, fastExitAfter.Seconds(), (mult-1)*100), price)
				return
			}
			// SCRATCH: С„Р»СЌС‚ >2 РјРёРЅ вЂ” РѕСЃРІРѕР±РѕР¶РґР°РµРј РєР°РїРёС‚Р°Р» (РµСЃР»Рё РЅРµ РІС‹Р»РµС‚РµР»Рё РїРѕ FAST EXIT)
			if time.Since(opened) >= SCRATCH_AFTER && mult < SCRATCH_IF_BELOW {
				w.closePos(mint, fmt.Sprintf("РЎРљР Р•РўР§ (С„Р»СЌС‚ %.0f РјРёРЅ, spot %.1f%%)", SCRATCH_AFTER.Minutes(), (mult-1)*100), price)
				return
			}
			// Final Recovery: РЅРµ РІС‹С…РѕРґРёРј РїРѕ "СЃР»Р°Р±РѕРјСѓ РёРјРїСѓР»СЊСЃСѓ"/"РЅРµС‚ РёРјРїСѓР»СЊСЃР°", РґР°С‘Рј РїРѕР·РёС†РёРё СЂР°Р·С‹РіСЂР°С‚СЊСЃСЏ.
			if price <= entry*stopConfirmLvl {
				confirmedStopTicks++
			} else {
				confirmedStopTicks = 0
			}
			// "Fake stop-out" Р·Р°С‰РёС‚Р°: РїРѕРґС‚РІРµСЂР¶РґРµРЅРёРµ/РїРѕСЂРѕРі РїРѕ recovery РЅР°СЃС‚СЂРѕР№РєР°Рј.
			if price <= entry*stopLossHard || confirmedStopTicks >= STOP_CONFIRM_N {
				pct := (1 - stopLossHard) * 100
				if price <= entry*stopLossHard {
					w.closePos(mint, fmt.Sprintf("РЎРўРћРџ -%.0f%% (hard)", pct), price)
				} else {
					w.closePos(mint, fmt.Sprintf("РЎРўРћРџ РїРѕРґС‚РІРµСЂР¶РґС‘РЅ (-%.0f%%)", pct), price)
				}
				return
			}
		}
	}
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  Р’РЎРџРћРњРћР“РђРўР•Р›Р¬РќР«Р•
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func contains(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func short(a string) string {
	if len(a) > 12 {
		return a[:6] + ".." + a[len(a)-4:]
	}
	return a
}

// DexScreener вЂ” РіСЂР°С„РёРє/РїР°СЂР° РїРѕ mint РЅР° Solana
func dexScreenerURL(mint string) string {
	return "https://dexscreener.com/solana/" + mint
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  MAIN
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func main() {
	_ = godotenv.Load()
	selftest := flag.Bool("selftest", false, "РїСЂРѕРІРµСЂРєР° РІРёСЂС‚СѓР°Р»СЊРЅРѕРіРѕ РєРѕС€РµР»СЊРєР° Рё РєРѕРјРёСЃСЃРёР№ Р·Р° СЃРµРєСѓРЅРґС‹ (Р±РµР· WebSocket), Р·Р°С‚РµРј РІС‹С…РѕРґ")
	flag.Parse()
	if *selftest {
		refreshSolPriceUSD()
		runPaperSelfTest()
		os.Exit(0)
	}

	if !apiReady() {
		fmt.Println(red("вќЊ Р’СЃС‚Р°РІСЊ Helius API РєР»СЋС‡ РІ HELIUS_API_KEY"))
		fmt.Println(yellow("   dev.helius.xyz в†’ Sign Up в†’ Create App"))
		os.Exit(1)
	}
	if err := initLiveTrading(); err != nil {
		fmt.Println(red("вќЊ " + err.Error()))
		os.Exit(1)
	}

	fmt.Println(bold("в•”в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•—"))
	if liveTradingEnabled() {
		fmt.Println(bold("в•‘   PUMP.FUN вЂ” Р‘РћР•Р’РћР™ Р Р•Р–РРњ (MAINNET)                      в•‘"))
		fmt.Println(bold("в•‘   Р РµР°Р»СЊРЅС‹Рµ SOL В· Pump.fun curve В· СЂРёСЃРє РїРѕС‚РµСЂРё РєР°РїРёС‚Р°Р»Р°    в•‘"))
	} else {
		fmt.Println(bold("в•‘   PUMP.FUN LIVE PAPER TRADING                            в•‘"))
		fmt.Println(bold("в•‘   Р РµР°Р»СЊРЅС‹Рµ С‚РѕРєРµРЅС‹ В· Р РµР°Р»СЊРЅС‹Рµ С†РµРЅС‹ В· Р’РёСЂС‚СѓР°Р»СЊРЅС‹Рµ РґРµРЅСЊРіРё  в•‘"))
	}
	fmt.Println(bold("в•љв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ќ"))
	if liveTradingEnabled() {
		fmt.Println(green("вњ“ Helius WebSocket вЂ” Pump.fun (live РїРѕС‚РѕРє)"))
	} else {
		fmt.Println(green("вњ“ Helius WebSocket вЂ” Pump.fun + Raydium LaunchLab (РґРІР° РїРѕС‚РѕРєР°)"))
	}
	fmt.Println(green("вњ“ Bonding Curve Price вЂ” РїСЂСЏРјРѕ РёР· on-chain РґР°РЅРЅС‹С…"))
	if liveTradingEnabled() {
		fmt.Printf("%s LIVE РєРѕС€РµР»С‘Рє %s | %s\n", green("вњ“"), cyan(short(livePub.String())),
			yellow("РўРѕР»СЊРєРѕ mint вЂ¦pump РЅР° РєСЂРёРІРѕР№; LaunchLab РІ live РЅРµ С‚РѕСЂРіСѓРµС‚СЃСЏ."))
		if useJitoEnabled() {
			fmt.Println(green("вњ“ Jito bundles: Р’РљР› (sendBundle)"))
		} else {
			fmt.Println(yellow("вљ  Jito bundles: Р’Р«РљР› (РёРґРµС‚ РѕР±С‹С‡РЅС‹Р№ RPC sendTransaction)"))
		}
	}
	if shouldSkipVelocity() {
		fmt.Println(yellow("вљ  Velocity-check РѕС‚РєР»СЋС‡С‘РЅ (SKIP/AUTO) вЂ” Р±РѕР»СЊС€Рµ РІС…РѕРґРѕРІ, Р±РѕР»СЊС€Рµ С€СѓРјР°."))
	}
	if shouldSkipAntiScam() {
		fmt.Println(red("вљ  Anti-scam С„РёР»СЊС‚СЂС‹ РѕС‚РєР»СЋС‡РµРЅС‹ (SKIP/AUTO) вЂ” РІ live РјРѕР¶РЅРѕ СЃР»РёС‚СЊ SOL РЅР° РјСѓСЃРѕСЂ."))
	}
	if turboModeEnabled() {
		fmt.Println(yellow("вљ  TURBO mode (live+aggressive): СѓСЃРєРѕСЂРµРЅРЅС‹Р№ hot-path СЃ РјРёРЅРёРјР°Р»СЊРЅС‹РјРё РїСЂРѕРІРµСЂРєР°РјРё."))
	}
	if ultraFastEntryMode() {
		fmt.Println(red("вљ  ULTRA_FAST_ENTRY: РІС…РѕРґ СЃСЂР°Р·Сѓ РїРѕСЃР»Рµ parse mint (Р±РµР· curve/velocity/scam-РіРµР№С‚РѕРІ)."))
		fmt.Printf("%s ULTRA monitor: tick=%v | quick_tp=~+%.0f%% РІ РїРµСЂРІС‹Рµ %.1fs\n",
			yellow("вљ "), monitorTickInterval(), (quickPumpTakeProfitMult()-1)*100, quickPumpWindow().Seconds())
	}

	refreshSolPriceUSD()
	refreshDynamicPriorityFeeFromRPC()
	startPumpHotCaches()
	sp := getSolUSD()
	fmt.Printf("%s SOL/USD: $%.2f (CoinGecko, Р°РІС‚РѕРѕР±РЅРѕРІР»РµРЅРёРµ ~90 СЃ)\n", green("вњ“"), sp)
	go func() {
		t := time.NewTicker(90 * time.Second)
		for range t.C {
			refreshSolPriceUSD()
		}
	}()
	go func() {
		t := time.NewTicker(20 * time.Second)
		for range t.C {
			refreshDynamicPriorityFeeFromRPC()
		}
	}()
	fmt.Printf("\n%s Р РµР¶РёРј: %s | РєСЂРёРІР°СЏ: %.1f%%вЂ“%.1f%% | min SOL: %.2f | velocity(base): %v (О”в‰Ґ%.2f%% РёР»Рё +%.3f SOL) | profile=%s | РѕРЅС‡РµР№РЅ-С„РёР»СЊС‚СЂС‹\n",
		bold("в–¶"), cyan("SNIPER"), SNIPER_CURVE_MIN*100, SNIPER_CURVE_MAX*100, MIN_REAL_SOL,
		velocityPause(), VELOCITY_MIN_DPROGRESS*100, VELOCITY_MIN_DREALSOL, envSignalProfile())
	wallet := newWallet()
	if liveTradingEnabled() {
		go func() {
			t := time.NewTicker(BALANCE_CHECK_INTERVAL)
			defer t.Stop()
			for range t.C {
				syncWalletBalanceUSDFresh(wallet)
			}
		}()
	}
	restored := wallet.activePositionSnapshots()
	if liveTradingEnabled() && len(restored) > 0 {
		fmt.Printf("%s Р’РѕСЃСЃС‚Р°РЅРѕРІР»РµРЅРѕ Р°РєС‚РёРІРЅС‹С… РїРѕР·РёС†РёР№ РёР· %s: %d\n",
			yellow("в†є"), ACTIVE_POSITIONS_FILE, len(restored))
		for _, p := range restored {
			go monitor(wallet, p.Mint, p.BondingCurve, p.Symbol, p.Source)
		}
	}
	if liveTradingEnabled() {
		fmt.Printf("%s Р‘Р°Р»Р°РЅСЃ: %s (РѕРЅС‡РµР№РЅ) | РЎС‚Р°РІРєР°: FIXED %.3f SOL (РїРѕСЃР»Рµ СЂРµР·РµСЂРІР°) | РњР°РєСЃ РїРѕР·РёС†РёР№: %d\n",
			bold("в–¶"), green(fmt.Sprintf("$%.2f", wallet.Balance)), liveFixedBuySOLValue(), MAX_POSITIONS)
	} else {
		fmt.Printf("%s Р‘Р°Р»Р°РЅСЃ: %s | РЎС‚Р°РІРєР°: %.2f%% РѕС‚ Р±Р°РЅРєР° (min $%.2f) в†’ СЃРµР№С‡Р°СЃ ~$%.2f РЅР° СЃРґРµР»РєСѓ | РњР°РєСЃ РїРѕР·РёС†РёР№: %d\n",
			bold("в–¶"), green(fmt.Sprintf("$%.2f", PAPER_BALANCE)), BET_PCT_OF_BALANCE*100, MIN_STAKE_USD,
			stakeFromBalance(PAPER_BALANCE), MAX_POSITIONS)
	}
	fmt.Printf("%s DexScreener вЂ” РїРѕ mint РїРѕРєР°Р·С‹РІР°РµС‚СЃСЏ РІСЃСЏ РёСЃС‚РѕСЂРёСЏ С‚РѕСЂРіРѕРІ; РґР°С‚С‹ РЅР° РѕСЃРё вЂ” РєР°Р»РµРЅРґР°СЂСЊ СЃРІРµС‡РµР№, РЅРµ РґР°С‚Р° В«СЃРѕР·РґР°РЅРёСЏ СЃСЃС‹Р»РєРёВ». РЎРІРµР¶РµСЃС‚СЊ Р»РёСЃС‚РёРЅРіР° СЃРјРѕС‚СЂРё РІ СЃС‚СЂРѕРєРµ вЏ± (РІСЂРµРјСЏ Р±Р»РѕРєР° create-С‚x).\n",
		gray("в“"))
	fmt.Printf("%s Р‘С‹СЃС‚СЂР°СЏ РїСЂРѕРІРµСЂРєР° РєРѕС€РµР»СЊРєР°: %s\n",
		gray("в“"), cyan("go run . -selftest"))
	fmt.Printf("%s Р¦РµР»СЊ В«~1 РІС…РѕРґ / 2 РјРёРЅВ» вЂ” РѕСЂРёРµРЅС‚РёСЂ: РѕРґРЅР° РїРѕР·РёС†РёСЏ + РїРѕС‚РѕРє Pump.fun; СЃРјРѕС‚СЂРё СЃС‚СЂРѕРєСѓ в—Ћ В«СЃСЂРµРґРЅРёР№ РёРЅС‚РµСЂРІР°Р»В» СЂР°Р· РІ РјРёРЅСѓС‚Сѓ.\n\n",
		gray("в“"))

	tokenCh := make(chan NewToken, 200)
	var seen sync.Map
	// Р‘РѕР»СЊС€Рµ СЃР»РѕС‚РѕРІ: РїРѕРєР° РѕРґРёРЅ С‚РѕРєРµРЅ РІ sleep(velocity), РѕСЃС‚Р°Р»СЊРЅС‹Рµ create РѕР±СЂР°Р±Р°С‚С‹РІР°СЋС‚СЃСЏ
	sem := make(chan struct{}, RPC_MAX_CONCURRENT)

	go listenPumpWSS(tokenCh)
	// Р’ live-СЂРµР¶РёРјРµ С‚РѕСЂРіСѓРµРј С‚РѕР»СЊРєРѕ Pump.fun curve: LaunchLab РїРѕС‚РѕРє СЃРѕР·РґР°С‘С‚ Р»РёС€РЅРёР№ С€СѓРј/Р·Р°РґРµСЂР¶РєРё.
	if !liveTradingEnabled() {
		go listenProgram(LAUNCHLAB_PROGRAM, "Raydium LaunchLab", launchLabInitFromLogs, tokenCh, "launchlab")
	}

	go func() {
		for tok := range tokenCh {
			tok := tok
			select {
			case sem <- struct{}{}:
			default:
				rejectBump("busy")
				continue
			}
			go func() {
				defer func() { <-sem }()
				traceStart := time.Now()
				parseDone := traceStart
				curveDone := traceStart

				src := tokenSource(tok)
				var mint, creator string
				var createAt *time.Time
				var bc string
				var err error
				if src == "launchlab" {
					mint, creator, createAt = parseLaunchLabCreateTx(tok.Sig)
					if abortIfTooLate(tok, "parse_launchlab_create_tx") {
						return
					}
					if mint == "" {
						logRejectLine("no_mint", "?", "", "launchlab: РЅРµС‚ mint РІ tx")
						return
					}
					bc, err = launchLabPoolPDA(mint)
					if err != nil {
						logRejectLine("pda_err", "$"+short(mint), mint, err.Error())
						return
					}
				} else {
					mint, creator, createAt = parseCreateTx(tok.Sig)
					if abortIfTooLate(tok, "parse_pump_create_tx") {
						return
					}
					if mint == "" {
						logRejectLine("no_mint", "?", "", "РЅРµС‚ mint РІ create tx")
						return
					}
					if !strings.HasSuffix(mint, "pump") {
						sym := "$" + short(mint)
						logRejectLine("not_pump", sym, mint, "mint РЅРµ вЂ¦pump вЂ” РЅРµ pump.fun С‚РѕРєРµРЅ")
						return
					}
					bc, err = pumpBondingCurvePDA(mint)
					if err != nil {
						logRejectLine("pda_err", "$"+short(mint), mint, err.Error())
						if !tok.DetectedAt.IsZero() {
							total := time.Since(tok.DetectedAt).Milliseconds()
							printHotPathTrace("$"+short(mint), src, "reject:pda_err", err.Error(), 0, 0, 0, total)
						}
						return
					}
				}
				tok.Mint = mint
				tok.BondingCurve = bc
				parseDone = time.Now()

				if _, loaded := seen.LoadOrStore(mint, true); loaded {
					return
				}
				if tok.DetectedAt.IsZero() {
					tok.DetectedAt = time.Now()
				}

				wallet.mu.Lock()
				cnt := len(wallet.Pos)
				_, has := wallet.Pos[mint]
				wallet.mu.Unlock()
				if has || cnt >= MAX_POSITIONS {
					return
				}

				sym := "$" + short(mint)
				if !hotPathSilent() {
					consoleMu.Lock()
					fmt.Printf("%s Token Detected | %s | %s | %s\n",
						gray("вЏ±"), sym, src, tok.DetectedAt.Format(time.RFC3339Nano))
					consoleMu.Unlock()
				}
				if src == "pump" && ultraFastEntryMode() {
					var snapFast *curveSnap
					if ultraQualityFilterEnabled() {
						parseMs := parseDone.Sub(traceStart).Milliseconds()
						snap0, err := getCurveSnapshotWithRetry(bc, src)
						curveDone = time.Now()
						if err != nil || snap0 == nil || snap0.PriceUSD <= 0 {
							logRejectLine("no_price", sym, mint, "ultra-quality: РЅРµС‚ С†РµРЅС‹")
							if !tok.DetectedAt.IsZero() {
								total := time.Since(tok.DetectedAt).Milliseconds()
								printHotPathTrace(sym, src, "reject:ultra_no_price", "curve snapshot unavailable", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), 0, total)
							}
							return
						}
						if snap0.Complete {
							logRejectLine("complete", sym, mint, "ultra-quality: curve complete")
							if !tok.DetectedAt.IsZero() {
								total := time.Since(tok.DetectedAt).Milliseconds()
								printHotPathTrace(sym, src, "reject:ultra_complete", "bonding curve complete", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), 0, total)
							}
							return
						}
						if snap0.RealSolSOL < ultraMinRealSOL() {
							logRejectLine("low_sol", sym, mint, fmt.Sprintf("ultra-quality: SOL %.2f < %.2f", snap0.RealSolSOL, ultraMinRealSOL()))
							if !tok.DetectedAt.IsZero() {
								total := time.Since(tok.DetectedAt).Milliseconds()
								printHotPathTrace(sym, src, "reject:ultra_low_sol", "real SOL below threshold", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), 0, total)
							}
							return
						}
						if snap0.Progress < ultraMinProgress() {
							logRejectLine("empty", sym, mint, fmt.Sprintf("ultra-quality: curve %.1f%% < %.1f%%", snap0.Progress*100, ultraMinProgress()*100))
							if !tok.DetectedAt.IsZero() {
								total := time.Since(tok.DetectedAt).Milliseconds()
								printHotPathTrace(sym, src, "reject:ultra_empty", "too early / empty curve", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), 0, total)
							}
							return
						}
						if snap0.Progress > ultraMaxProgress() {
							logRejectLine("late", sym, mint, fmt.Sprintf("ultra-quality: curve %.1f%% > %.1f%%", snap0.Progress*100, ultraMaxProgress()*100))
							if !tok.DetectedAt.IsZero() {
								total := time.Since(tok.DetectedAt).Milliseconds()
								printHotPathTrace(sym, src, "reject:ultra_late", "curve too late", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), 0, total)
							}
							return
						}
						skipMomentum := parseMs >= ultraSkipMomentumIfParseMs() ||
							curveDone.Sub(parseDone).Milliseconds() >= ultraSkipMomentumIfFirstSnapMs()
						pause := ultraMomentumPause()
						if pause > 0 && !skipMomentum {
							time.Sleep(pause)
							snap1, err := getCurveSnapshotWithRetry(bc, src)
							if err != nil || snap1 == nil || snap1.PriceUSD <= 0 {
								logRejectLine("vel_rpc", sym, mint, "ultra-quality: РІС‚РѕСЂРѕР№ Р·Р°РјРµСЂ РЅРµРґРѕСЃС‚СѓРїРµРЅ")
								if !tok.DetectedAt.IsZero() {
									total := time.Since(tok.DetectedAt).Milliseconds()
									printHotPathTrace(sym, src, "reject:ultra_vel_rpc", "second snapshot unavailable", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
								}
								return
							}
							dP := snap1.Progress - snap0.Progress
							dSol := snap1.RealSolSOL - snap0.RealSolSOL
							if dP < ultraMinDeltaProgress() && dSol < ultraMinDeltaSOL() {
								logRejectLine("vel_low", sym, mint, fmt.Sprintf("ultra-quality: СЃР»Р°Р±С‹Р№ РёРјРїСѓР»СЊСЃ (О”%.2f%% / +%.3f SOL)", dP*100, dSol))
								if !tok.DetectedAt.IsZero() {
									total := time.Since(tok.DetectedAt).Milliseconds()
									printHotPathTrace(sym, src, "reject:ultra_vel_low", "weak momentum", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
								}
								return
							}
							snapFast = snap1
							curveDone = time.Now()
						} else {
							if skipMomentum {
								if snap0.Progress > ultraSkipMomentumMaxProgress() {
									logRejectLine("late", sym, mint, fmt.Sprintf("ultra-quality: skip-momentum max curve %.1f%% > %.1f%%", snap0.Progress*100, ultraSkipMomentumMaxProgress()*100))
									if !tok.DetectedAt.IsZero() {
										total := time.Since(tok.DetectedAt).Milliseconds()
										printHotPathTrace(sym, src, "reject:ultra_skip_late", "skip-momentum at late curve", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), 0, total)
									}
									return
								}
								if snap0.Progress < ultraSkipMomentumMinProgress() || snap0.RealSolSOL < ultraSkipMomentumMinRealSOL() {
									logRejectLine("vel_low", sym, mint, fmt.Sprintf("ultra-quality: skip-momentum gate (curve %.1f%% / SOL %.2f)", snap0.Progress*100, snap0.RealSolSOL))
									if !tok.DetectedAt.IsZero() {
										total := time.Since(tok.DetectedAt).Milliseconds()
										printHotPathTrace(sym, src, "reject:ultra_skip_gate", "skip-momentum quality too low", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), 0, total)
									}
									return
								}
							}
							snapFast = snap0
						}
					}
					if ultraDevFilterEnabled() {
						if creator == "" {
							logRejectLine("scam", sym, mint, "ultra-dev-filter: creator missing")
							if !tok.DetectedAt.IsZero() {
								total := time.Since(tok.DetectedAt).Milliseconds()
								printHotPathTrace(sym, src, "reject:ultra_dev", "creator missing", parseDone.Sub(traceStart).Milliseconds(), 0, 0, total)
							}
							return
						}
						if sol, err := rpcGetBalanceSOLCached(creator, CREATOR_BALANCE_CACHE_TTL); err == nil {
							if sol <= ultraDevMinSOL() {
								logRejectLine("scam", sym, mint, fmt.Sprintf("ultra-dev-filter: creator SOL %.6f <= %.6f", sol, ultraDevMinSOL()))
								if !tok.DetectedAt.IsZero() {
									total := time.Since(tok.DetectedAt).Milliseconds()
									printHotPathTrace(sym, src, "reject:ultra_dev", "creator balance too low", parseDone.Sub(traceStart).Milliseconds(), 0, 0, total)
								}
								return
							}
						}
					}
					atomic.AddInt64(&funnelInWindow, 1)
					atomic.AddInt64(&funnelPassVel, 1)
					atomic.AddInt64(&funnelPassScam, 1)
					tok.FiltersPassedAt = time.Now()
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						detail := "skip curve/velocity/scam"
						curveMs := int64(0)
						if ultraQualityFilterEnabled() {
							curveMs = curveDone.Sub(parseDone).Milliseconds()
							if snapFast != nil {
								detail = fmt.Sprintf("ultra-quality ok | curve %.1f%% | SOL %.2f", snapFast.Progress*100, snapFast.RealSolSOL)
							} else {
								detail = "ultra-quality ok"
							}
						}
						printHotPathTrace(sym, src, "pass:ultra_fast", detail, parseDone.Sub(traceStart).Milliseconds(), curveMs, 0, total)
					}
					price := 0.0
					if snapFast != nil {
						price = snapFast.PriceUSD
					}
					if wallet.open(tok, sym, price) {
						atomic.AddInt64(&funnelOpenOK, 1)
						if !hotPathSilent() {
							consoleMu.Lock()
							msg := "skip curve/velocity/scam"
							if ultraQualityFilterEnabled() && snapFast != nil {
								msg = fmt.Sprintf("ultra-quality: curve %.1f%% | SOL %.2f", snapFast.Progress*100, snapFast.RealSolSOL)
							}
							fmt.Printf("\n%s %-18s | %s | ULTRA_FAST_ENTRY в†’ Р’РҐРћР”\n",
								green("вњ“"), sym, gray(msg))
							if createAt != nil {
								fmt.Printf("   %s %s\n", gray("вЏ±"), formatCreateAge(createAt))
							}
							fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(mint)))
							consoleMu.Unlock()
						}
						go monitor(wallet, mint, bc, sym, src)
					} else {
						atomic.AddInt64(&funnelOpenFail, 1)
						consoleMu.Lock()
						fmt.Println(yellow("вљ  Р’РҐРћР” РѕС‚РєР»РѕРЅС‘РЅ open(): Р±Р°Р»Р°РЅСЃ, Р»РёРјРёС‚, РЅРµ pump РІ live, РёР»Рё RPC"))
						consoleMu.Unlock()
					}
					return
				}

				if createAt != nil && time.Since(*createAt) > MAX_CREATE_TX_AGE {
					logRejectLine("stale_tx", sym, mint, fmt.Sprintf(
						"create-С‚x СЃС‚Р°СЂС€Рµ %v (%s) вЂ” РЅРµ СЃРІРµР¶РёР№ Р»РёСЃС‚РёРЅРі", MAX_CREATE_TX_AGE, formatCreateAge(createAt)))
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:stale_tx", "stale create tx", parseDone.Sub(traceStart).Milliseconds(), 0, 0, total)
					}
					return
				}

				snap0, err := getCurveSnapshotWithRetry(bc, src)
				if abortIfTooLate(tok, "curve_snapshot") {
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:late_curve_snapshot", "latency guard", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
					}
					return
				}
				if err != nil || snap0 == nil || snap0.PriceUSD <= 0 {
					logRejectLine("no_price", sym, mint, "РЅРµС‚ С†РµРЅС‹ (РєСЂРёРІР°СЏ / pool)")
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:no_price", "curve snapshot unavailable", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
					}
					return
				}
				if snap0.Complete {
					logRejectLine("complete", sym, mint, "bonding curve complete")
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:complete", "bonding curve complete", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
					}
					return
				}
				if snap0.Progress < SNIPER_CURVE_MIN {
					logRejectLine("empty", sym, mint, fmt.Sprintf("curve %.2f%% < %.1f%% вЂ” РїСѓСЃС‚Рѕ",
						snap0.Progress*100, SNIPER_CURVE_MIN*100))
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:empty", "curve too early", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
					}
					return
				}
				if snap0.Progress > SNIPER_CURVE_MAX {
					logRejectLine("late", sym, mint, fmt.Sprintf("curve %.1f%% > %.1f%% вЂ” РїРѕР·РґРЅРѕ",
						snap0.Progress*100, SNIPER_CURVE_MAX*100))
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:late", "curve too late", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
					}
					return
				}
				if snap0.RealSolSOL < MIN_REAL_SOL {
					logRejectLine("low_sol", sym, mint, fmt.Sprintf("real SOL %.2f < %.2f",
						snap0.RealSolSOL, MIN_REAL_SOL))
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:low_sol", "real SOL below threshold", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
					}
					return
				}

				atomic.AddInt64(&funnelInWindow, 1)
				liqVault := bc
				if src == "launchlab" {
					vault, err := launchLabBaseVault(bc, mint)
					if err != nil {
						logRejectLine("pda_err", sym, mint, "launchlab vault: "+err.Error())
						if !tok.DetectedAt.IsZero() {
							total := time.Since(tok.DetectedAt).Milliseconds()
							printHotPathTrace(sym, src, "reject:pda_err", "launchlab vault pda", parseDone.Sub(traceStart).Milliseconds(), time.Since(parseDone).Milliseconds(), 0, total)
						}
						return
					}
					liqVault = vault
				}
				curveDone = time.Now()
				var extraMint []string
				if src == "launchlab" {
					extraMint = []string{LAUNCHLAB_PROGRAM}
				}
				// velocity Рё antiScam РїР°СЂР°Р»Р»РµР»СЊРЅРѕ вЂ” СЌРєРѕРЅРѕРјРёСЏ ~200ms
				var snap1 *curveSnap
				var vOK bool
				var vDetail, vKey string
				var scamOK bool
				var scamMeta string
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					snap1, vOK, vDetail, vKey = curveVelocityOK(bc, snap0, src, createAt)
				}()
				go func() {
					defer wg.Done()
					if shouldSkipAntiScam() {
						scamOK, scamMeta = true, "[anti-scam skip (SKIP/AUTO) вЂ” РЅРµ РґР»СЏ Р±РµР·РѕРїР°СЃРЅРѕР№ С‚РѕСЂРіРѕРІР»Рё]"
					} else {
						scamOK, scamMeta = antiScamCheck(mint, bc, liqVault, creator, createAt, extraMint...)
					}
				}()
				wg.Wait()
				if abortIfTooLate(tok, "velocity_check") {
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "warn:velocity_latency", "velocity stage over delay", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), time.Since(curveDone).Milliseconds(), total)
					}
					return
				}
				if !vOK {
					if vKey == "" {
						vKey = "velocity"
					}
					logRejectLine(vKey, sym, mint, "velocity: "+vDetail)
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:"+vKey, vDetail, parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), time.Since(curveDone).Milliseconds(), total)
					}
					return
				}
				atomic.AddInt64(&funnelPassVel, 1)
				if src == "pump" && !turboModeEnabled() && snap1.Progress > FAST_HEAVY_CHECK_CURVE_MAX {
					logRejectLine("late", sym, mint, fmt.Sprintf("fast-gate: curve %.1f%% > %.1f%% РґРѕ С‚СЏР¶С‘Р»С‹С… RPC-РїСЂРѕРІРµСЂРѕРє",
						snap1.Progress*100, FAST_HEAVY_CHECK_CURVE_MAX*100))
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:late_fast_gate", "curve progressed during checks", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), time.Since(curveDone).Milliseconds(), total)
					}
					return
				}
				if abortIfTooLate(tok, "anti_scam_check") {
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:late_anti_scam", "latency guard", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), time.Since(curveDone).Milliseconds(), total)
					}
					return
				}
				if !scamOK {
					logRejectLine("scam", sym, mint, "С„РёР»СЊС‚СЂ: "+scamMeta)
					if !tok.DetectedAt.IsZero() {
						total := time.Since(tok.DetectedAt).Milliseconds()
						printHotPathTrace(sym, src, "reject:scam", scamMeta, parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), time.Since(curveDone).Milliseconds(), total)
					}
					return
				}
				if !tok.DetectedAt.IsZero() {
					delta := time.Since(tok.DetectedAt)
					if !hotPathSilent() {
						consoleMu.Lock()
						fmt.Printf("%s Filters Passed | %s | +%dms\n",
							gray("вЏ±"), sym, delta.Milliseconds())
						consoleMu.Unlock()
					}
					if delta > MAX_READY_TO_SEND_DELAY {
						abortIfTooLate(tok, "before_open")
						printHotPathTrace(sym, src, "reject:before_open", "over max ready-to-send delay", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), time.Since(curveDone).Milliseconds(), delta.Milliseconds())
						return
					}
				}
				tok.FiltersPassedAt = time.Now()
				if !tok.DetectedAt.IsZero() {
					total := time.Since(tok.DetectedAt).Milliseconds()
					printHotPathTrace(sym, src, "pass:filters", "ready to open position", parseDone.Sub(traceStart).Milliseconds(), curveDone.Sub(parseDone).Milliseconds(), time.Since(curveDone).Milliseconds(), total)
				}

				atomic.AddInt64(&funnelPassScam, 1)
				price := snap1.PriceUSD
				progress := snap1.Progress
				realSol := snap1.RealSolSOL

				ageInfo := formatCreateAge(createAt)
				consoleMu.Lock()
				srcTag := gray("pump")
				if src == "launchlab" {
					srcTag = cyan("LaunchLab")
				}
				fmt.Printf("\n%s %-18s | %s | $%.10f | curve %.1f%% | SOL %.2f | %s | %s в†’ Р’РҐРћР”\n",
					green("вњ“"), sym, srcTag, price, progress*100, realSol, gray(vDetail), gray(scamMeta))
				fmt.Printf("   %s %s\n", gray("вЏ±"), cyan(ageInfo))
				fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(mint)))
				if liveTradingEnabled() {
					fmt.Println(gray("   вЏі LIVE: РїРѕРґРїРёСЃСЊ С‚СЂР°РЅР·Р°РєС†РёРё Pump.fun (RPC) вЂ” РѕР±С‹С‡РЅРѕ РЅРµСЃРєРѕР»СЊРєРѕ СЃРµРєСѓРЅРґ."))
				}
				consoleMu.Unlock()

				opened := wallet.open(tok, sym, price)

				if opened {
					atomic.AddInt64(&funnelOpenOK, 1)
					recordSuccessfulEntry()
					go monitor(wallet, mint, bc, sym, src)
				} else {
					atomic.AddInt64(&funnelOpenFail, 1)
					consoleMu.Lock()
					fmt.Println(yellow("вљ  Р’РҐРћР” РѕС‚РєР»РѕРЅС‘РЅ open(): Р±Р°Р»Р°РЅСЃ, Р»РёРјРёС‚, РЅРµ pump РІ live, РёР»Рё RPC"))
					consoleMu.Unlock()
				}
			}()
		}
	}()

	// РЎС‚Р°С‚РёСЃС‚РёРєР° РєР°Р¶РґС‹Рµ 3 РјРёРЅСѓС‚С‹
	go func() {
		t := time.NewTicker(3 * time.Minute)
		for range t.C {
			wallet.stats()
		}
	}()

	// РЎРІРѕРґРєР° РѕС‚СЃРµРІР° СЂР°Р· РІ РјРёРЅСѓС‚Сѓ (Р±РµР· СЃРїР°РјР° РїРѕ РєР°Р¶РґРѕРјСѓ С‚РѕРєРµРЅСѓ)
	go func() {
		t := time.NewTicker(1 * time.Minute)
		for range t.C {
			printRejectSummary()
			printFunnelLine()
			printEntryPace()
		}
	}()

	fmt.Println(gray("РќР°Р¶РјРё Ctrl+C РґР»СЏ РѕСЃС‚Р°РЅРѕРІРєРё\n"))
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	fmt.Println(yellow("\nвЏ№  РћСЃС‚Р°РЅРѕРІРєР°..."))
	wallet.stats()
	wallet.mu.Lock()
	if len(wallet.Closed) > 0 {
		fmt.Println("\n" + bold("в”Ђв”Ђ РСЃС‚РѕСЂРёСЏ СЃРґРµР»РѕРє в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ"))
		for _, t := range wallet.Closed {
			ps := green(fmt.Sprintf("+$%.2f", t.PnL))
			if t.PnL < 0 {
				ps = red(fmt.Sprintf("$%.2f", t.PnL))
			}
			dex := ""
			if t.Mint != "" {
				dex = dexScreenerURL(t.Mint)
			}
			fmt.Printf("  %-20s %s  %-28s %s\n", t.Symbol, ps, t.Reason, t.Dur)
			if dex != "" {
				fmt.Printf("      %s\n", cyan(dex))
			}
		}
	}
	wallet.mu.Unlock()
	if liveTradingEnabled() {
		fmt.Println(yellow("\nвљ  LIVE: СЃРґРµР»РєРё Р±С‹Р»Рё РІ mainnet вЂ” РїСЂРѕРІРµСЂСЊ Р±Р°Р»Р°РЅСЃ РІ РєРѕС€РµР»СЊРєРµ."))
	} else {
		fmt.Println(bold("\nвњ“ Р РµР°Р»СЊРЅС‹С… РґРµРЅРµРі РЅРµ РїРѕС‚СЂР°С‡РµРЅРѕ."))
	}
}

// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
//  RAYDIUM LAUNCHLAB (РІ СЌС‚РѕРј Р¶Рµ С„Р°Р№Р»Рµ вЂ” go run Р±РµР· launchlab.go)
// в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

func launchLabBaseVault(poolAddr, mint string) (string, error) {
	pool, err := solana.PublicKeyFromBase58(poolAddr)
	if err != nil {
		return "", err
	}
	m, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return "", err
	}
	prog := solana.MustPublicKeyFromBase58(LAUNCHLAB_PROGRAM)
	pda, _, err := solana.FindProgramAddress(
		[][]byte{[]byte("pool_vault"), pool.Bytes(), m.Bytes()},
		prog,
	)
	if err != nil {
		return "", err
	}
	return pda.String(), nil
}

func launchLabPoolPDA(mint string) (string, error) {
	m, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return "", err
	}
	wsol, err := solana.PublicKeyFromBase58(WSOL_MINT)
	if err != nil {
		return "", err
	}
	prog := solana.MustPublicKeyFromBase58(LAUNCHLAB_PROGRAM)
	pda, _, err := solana.FindProgramAddress(
		[][]byte{[]byte("pool"), m.Bytes(), wsol.Bytes()},
		prog,
	)
	if err != nil {
		return "", err
	}
	return pda.String(), nil
}

func parseLaunchLabPoolData(raw []byte) (virtualA, virtualB, realA, realB, totalFund uint64, status, mintDecA uint8, err error) {
	if len(raw) < 8+77 {
		return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("РєРѕСЂРѕС‚РєРёР№ Р°РєРєР°СѓРЅС‚ pool")
	}
	body := raw[8:]
	if len(body) < 77 {
		return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("РєРѕСЂРѕС‚РєРѕРµ С‚РµР»Рѕ")
	}
	status = body[17]
	mintDecA = body[18]
	virtualA = binary.LittleEndian.Uint64(body[37:45])
	virtualB = binary.LittleEndian.Uint64(body[45:53])
	realA = binary.LittleEndian.Uint64(body[53:61])
	realB = binary.LittleEndian.Uint64(body[61:69])
	totalFund = binary.LittleEndian.Uint64(body[69:77])
	return virtualA, virtualB, realA, realB, totalFund, status, mintDecA, nil
}

func getLaunchLabSnapshot(poolAddr string) (*curveSnap, error) {
	data, err := rpc("getAccountInfo", []interface{}{
		poolAddr,
		map[string]string{
			"encoding":   "base64",
			"commitment": "processed",
		},
	})
	if err != nil {
		return nil, err
	}
	var r struct {
		Result *struct {
			Value *struct {
				Data  []interface{} `json:"data"`
				Owner string        `json:"owner"`
			} `json:"value"`
		} `json:"result"`
	}
	if json.Unmarshal(data, &r) != nil || r.Result == nil || r.Result.Value == nil {
		return nil, fmt.Errorf("РЅРµС‚ pool Р°РєРєР°СѓРЅС‚Р°")
	}
	if r.Result.Value.Owner != LAUNCHLAB_PROGRAM {
		return nil, fmt.Errorf("owner РЅРµ LaunchLab")
	}
	arr := r.Result.Value.Data
	if len(arr) < 1 {
		return nil, fmt.Errorf("РЅРµС‚ data")
	}
	b64str, _ := arr[0].(string)
	raw, err := base64.StdEncoding.DecodeString(b64str)
	if err != nil {
		return nil, err
	}
	va, vb, _, rb, totalFund, status, mintDecA, err := parseLaunchLabPoolData(raw)
	if err != nil {
		return nil, err
	}
	if va == 0 {
		return nil, fmt.Errorf("virtualA=0")
	}
	decA := int(mintDecA)
	if decA <= 0 {
		decA = 6
	}
	rawPerToken := math.Pow10(decA)
	priceInSol := (float64(vb) / 1e9) / (float64(va) / rawPerToken)
	sol := getSolUSD()
	priceUSD := priceInSol * sol

	realSol := float64(rb) / 1e9
	var progress float64
	if totalFund > 0 {
		progress = float64(rb) / float64(totalFund)
	}
	if progress > 1 {
		progress = 1
	}
	complete := status >= 2 || (totalFund > 0 && rb >= totalFund)
	if complete {
		progress = 1
	}

	return &curveSnap{
		PriceUSD:   priceUSD,
		Progress:   progress,
		RealSolSOL: realSol,
		Complete:   complete,
	}, nil
}

func launchLabInitFromLogs(logs []string) bool {
	for _, l := range logs {
		if strings.Contains(l, "Instruction: InitializeV2") ||
			strings.Contains(l, "Instruction: InitializeWithToken2022") {
			return true
		}
	}
	return false
}

func parseLaunchLabCreateTx(sig string) (mint, creator string, createBlockTime *time.Time) {
	data, err := getTransactionJSONParsedFast(sig)
	if err != nil {
		return "", "", nil
	}
	mint, creator, createBlockTime, isInit := parseTxMintCreator(data, launchLabInitFromLogs)
	if !isInit {
		return "", "", createBlockTime
	}
	if mint == "" {
		mint, creator, createBlockTime = refillMintFromConfirmed(sig, creator, createBlockTime, launchLabInitFromLogs)
	}
	return mint, creator, createBlockTime
}

