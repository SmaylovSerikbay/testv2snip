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

// Тестовые флаги окружения (для отладки, не для «боевой» охоты за качеством).
func envSkipVelocity() bool   { return strings.TrimSpace(os.Getenv("SKIP_VELOCITY")) == "1" }
func envSkipAntiScam() bool   { return strings.TrimSpace(os.Getenv("SKIP_ANTI_SCAM")) == "1" }
func envSignalProfile() string {
	p := strings.TrimSpace(strings.ToLower(os.Getenv("SIGNAL_PROFILE")))
	switch p {
	case "strict", "balanced", "aggressive":
		return p
	default:
		return "balanced"
	}
}

// ════════════════════════════════════════════════════
//  КОНФИГ
// ════════════════════════════════════════════════════

const (
	HELIUS_API_KEY = "1859e3e5-5d82-476e-a121-8d5f57705cf7"
	PAPER_BALANCE  = 7.0
	// Доля баланса на одну сделку: при $7 → ~$1; после плюса ставка считается от нового банка
	BET_PCT_OF_BALANCE = 1.0 / 7.0
	MIN_STAKE_USD      = 0.25 // не открываем позицию мельче (пыль / шум)
	MAX_POSITIONS      = 1    // снайпер: одна позиция — фокус и меньше шума

	// Pump.fun: Global fee_basis_points = 100 → 1% с покупки и с продажи (документация программы)
	PUMP_FEE_BPS = 100
	// Проскальзывание для paper-оценки (live для pump берётся из pump_direct.go отдельно).
	SLIPPAGE_BPS = 49
	// Оценка сети на одну подпись (бумага); live — по факту RPC / pump
	SOLANA_TX_LAMPORTS = 12_000.0

	PRICE_TICK = 3 * time.Second
	MAX_HOLD   = 7 * time.Minute

	// Recovery Mode ($3.9): узкое окно + ликвидность, чтобы не брать «пустые» мёртвые пулы.
	SNIPER_CURVE_MIN = 0.002 // 0.2%
	SNIPER_CURVE_MAX = 0.15  // 15%
	MIN_REAL_SOL     = 1.00  // минимум 1.0 SOL в кривой (ранние импульсы)

	// Анти-скам
	CREATOR_SOL_MIN     = 0.04 // не слишком режем поток, но отсеиваем совсем пустых
	CREATOR_SOL_SUSPECT = 80.0
	MAX_NONCURVE_PCT    = 0.12

	// Выходы Final Recovery: hard SL -30%; TP только от +150%.
	STOP_LOSS_HARD   = 0.70 // -30%
	STOP_CONFIRM_LVL = 0.70 // -30%
	STOP_CONFIRM_N   = 1
	TAKE_PROFIT      = 2.5 // +150%
	TRAIL_ACTIVATE   = 1.40 // трейлинг стартует после +40%
	TRAILING         = 0.16
	TRAIL_MIN_AGE    = 10 * time.Second
	TRAIL_MIN_PROFIT = 1.10
	BREAKEVEN_ARM    = 1.10
	SCRATCH_AFTER    = 2 * time.Minute  // не зависаем в флэте слишком долго
	SCRATCH_IF_BELOW = 0.97             // скретч только если совсем плоско
	NO_IMPULSE_AFTER = 4 * time.Minute // «нет импульса» — после умеренной консолидации
	NO_IMPULSE_NEED  = 1.04            // пик должен хотя бы +4% к входу, иначе выход

	// Если create-транзакция старше — не считаем «только что залистились» (защита от кривых сигналов)
	MAX_CREATE_TX_AGE = 30 * time.Minute

	VELOCITY_PAUSE         = 500 * time.Millisecond // микро-velocity: быстрее вход
	VELOCITY_MIN_DPROGRESS = 0.0
	VELOCITY_MIN_DREALSOL  = 0.0
	VELOCITY_MIN_DELTA_DP  = -0.0001 // -0.01% (разрешаем микро-откат на замере)
	LIVE_FIXED_BUY_SOL     = 0.013 // fixed buy for final recovery mode

	// Логи: false = не печатать каждый отсев (только сводка раз в минуту + успешный ВХОД)
	VERBOSE_REJECT_LOGS = false
)

// Wrapped SOL mint (wrap/совместимость)
const WSOL_MINT = "So11111111111111111111111111111111111111112"

// Частые SPL в tx — не mint pump-монеты (раньше цепляли первый баланс → USDC и т.д.)
var ignoredTokenMints = map[string]bool{
	WSOL_MINT: true,
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": true, // USDC
	"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": true, // USDT
}

// Счётчики отсева (без спама в консоль)
var (
	rejectMu           sync.Mutex
	rejectCounts       = map[string]int64{}
	rejectPrevSnapshot map[string]int64 // для дельты за минуту
	// Единый вывод: stats и отсев идут из разных горутин — без мьютекса строки перемешиваются
	consoleMu sync.Mutex
)

// Воронка входа (накопительно с запуска) — видно, до какого шага доходят токены
var (
	funnelInWindow int64 // прошли кривую+ликвидность, до velocity
	funnelPassVel  int64 // прошли velocity
	funnelPassScam int64 // прошли анти-скам
	funnelOpenOK   int64 // open() = true
	funnelOpenFail int64 // open() = false (редко: баланс/лимит)
)

// Подписи ключей в минутной сводке (рус./кратко)
var rejectLabelRU = map[string]string{
	"no_mint":  "нет_mint",
	"not_pump": "не_pump",
	"stale_tx": "старый_tx",
	"no_price": "нет_цены",
	"complete": "graduated",
	"empty":    "пусто",
	"late":     "поздно",
	"low_sol":  "мало_SOL",
	"velocity": "velocity",
	"vel_rpc":  "vel_RPC",
	"vel_low":  "vel_мало",
	"vel_late": "vel_поздно",
	"scam":     "скам",
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
		fmt.Printf("%s %-18s %s\n", gray("—"), sym, detail)
	} else {
		fmt.Printf("%s %-18s %s\n", gray("—"), sym, key)
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
	deltaStr := strings.Join(deltaParts, " · ")
	if deltaStr == "" {
		deltaStr = "тихо"
	}
	consoleMu.Lock()
	defer consoleMu.Unlock()
	fmt.Printf("%s за мин: %s  |  всего: %s\n", gray("📊 отсев"), deltaStr, strings.Join(totalParts, " · "))
}

func printFunnelLine() {
	consoleMu.Lock()
	defer consoleMu.Unlock()
	iw := atomic.LoadInt64(&funnelInWindow)
	v := atomic.LoadInt64(&funnelPassVel)
	sc := atomic.LoadInt64(&funnelPassScam)
	ok := atomic.LoadInt64(&funnelOpenOK)
	bad := atomic.LoadInt64(&funnelOpenFail)
	fmt.Printf("%s воронка (с запуска): в_окне %d → velocity %d → скам %d → вход ok %d",
		gray("◎"), iw, v, sc, ok)
	if bad > 0 {
		fmt.Printf(" · вход fail %d", bad)
	}
	fmt.Println()
	if iw == 0 {
		fmt.Println(gray("   (если «в_окне»=0 — почти всё отсекается до velocity: no_price / пусто / поздно / low_sol)"))
	} else if v == 0 {
		fmt.Println(gray(fmt.Sprintf("   (были в окне кривой, но velocity ещё ни разу не прошёл — узкое место; проф=%s)", envSignalProfile())))
	} else if sc == 0 {
		fmt.Println(gray("   (velocity был, анти-скам пока не пропустил ни одного)"))
	}
}

func printEntryPace() {
	recentEntryMu.Lock()
	times := append([]time.Time(nil), recentEntryTimes...)
	recentEntryMu.Unlock()
	consoleMu.Lock()
	defer consoleMu.Unlock()
	if len(times) < 2 {
		fmt.Printf("%s входов пока мало для среднего интервала (нужно ≥2)\n", gray("◎"))
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
	fmt.Printf("%s сделки: входов %d · последний интервал %v · средний %v (цель ~2 мин в paper — не гарантия, зависит от сети)\n",
		gray("◎"), len(times), lastGap.Round(time.Second), avg.Round(time.Second))
}

const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

// LAUNCHLAB_PROGRAM — Raydium LaunchLab (mainnet), см. docs.raydium.io
const LAUNCHLAB_PROGRAM = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"

// Pump.fun: decimals у монет на кривой = 6 (как в Global / bonding curve)
const pumpTokenDecimals = 6

// ════════════════════════════════════════════════════
//  ЦВЕТА
// ════════════════════════════════════════════════════

var (
	green  = func(s string) string { return "\033[32m" + s + "\033[0m" }
	red    = func(s string) string { return "\033[31m" + s + "\033[0m" }
	yellow = func(s string) string { return "\033[33m" + s + "\033[0m" }
	cyan   = func(s string) string { return "\033[36m" + s + "\033[0m" }
	bold   = func(s string) string { return "\033[1m" + s + "\033[0m" }
	gray   = func(s string) string { return "\033[90m" + s + "\033[0m" }
)

// ════════════════════════════════════════════════════
//  СТРУКТУРЫ
// ════════════════════════════════════════════════════

type NewToken struct {
	Mint         string
	BondingCurve string // pump: bonding curve PDA | launchlab: pool state PDA
	Sig          string
	Source       string // "pump" | "launchlab" (пусто = pump)
}

// postTokenBal — элемент meta.postTokenBalances в getTransaction (jsonParsed)
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
	EntryPrice     float64 // эффективная $/токен на входе (после slip)
	Tokens         float64 // условное кол-во токенов: USD в пул / EntryPrice
	PeakPrice      float64
	CapitalUSD     float64 // сколько USD снято с баланса (гросс)
	OpenedAt       time.Time
	BreakevenArmed bool
	// Боевой режим: фактические лампорты на входе и raw-баланс SPL для продажи
	Live          bool
	TokenRaw      uint64
	BuyLamports   uint64
	HalfTaken     bool
	Source        string // pump | launchlab
}

// snapshotPosition — копия полей без mutex (для closePos из горутины мониторинга).
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
	ExitNetUSD float64 // что вернулось на кошелёк после всех комиссий
	FeesUSD    float64 // суммарно комиссии (оценка)
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
	// Сводка по типу выхода (учимся на минусах)
	ExitWin  map[string]int
	ExitLoss map[string]int
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
		syncWalletBalanceUSD(w)
		w.Start = w.Balance
	}
	return w
}

// bucketExitReason — короткий ярлык для статистики
func bucketExitReason(reason string) string {
	switch {
	case strings.HasPrefix(reason, "ТЕЙК"):
		return "тейк"
	case strings.Contains(reason, "СТОП"):
		return "стоп"
	case strings.Contains(reason, "ТРЕЙЛИНГ"):
		return "трейл"
	case strings.Contains(reason, "БРЕЙК"):
		return "брейкивн"
	case strings.Contains(reason, "СКРЕТЧ"):
		return "скретч"
	case strings.Contains(reason, "НЕТ ИМПУЛЬСА"):
		return "нет_импульса"
	case strings.Contains(reason, "ТАЙМАУТ"):
		return "таймаут"
	case strings.Contains(reason, "МИГРАЦИЯ"):
		return "миграция"
	case strings.Contains(reason, "УМЕР"):
		return "нет_данных"
	default:
		return "прочее"
	}
}

func lossLearningHint(reason string) string {
	switch bucketExitReason(reason) {
	case "стоп":
		return "Часто стоп: ужесточить вход (выше curve min / сильнее velocity) или не ослаблять скам."
	case "скретч", "нет_импульса":
		return "Слабый разгон: поднять SNIPER_CURVE_MIN или velocity, чтобы не ловить «пустые» импульсы."
	case "брейкивн":
		return "Откат после пика: норм защита капитала; при частых — смотреть TRAIL/вход позже по кривой."
	case "таймаут", "нет_данных":
		return "Долго без движения / RPC: проверить сеть; при частых таймаутах уменьшить MAX_HOLD."
	case "миграция":
		return "Ушло в Raydium: фикс по правилам; не ошибка стратегии."
	default:
		return "Смотри полный Reason в логе выхода и сводку ExitLoss в кошельке."
	}
}

// Времена успешных входов — оценка среднего интервала между сделками
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

// stakeFromBalance — гросс USD на вход: n% от текущего баланса (сложный процент по сделкам).
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

// ════════════════════════════════════════════════════
//  RPC КЛИЕНТ
// ════════════════════════════════════════════════════

var httpClient = &http.Client{Timeout: 8 * time.Second}

// Курс SOL/USD (CoinGecko, иначе fallback в getSolUSD)
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
		return // отсекаем явный мусор API
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

// Гросс USD → сколько дошло в кривую после 1% pump + комиссии сети на покупку
func usdToPoolAfterBuy(grossUSD float64) float64 {
	return grossUSD - grossUSD*pumpFeePct() - solanaTxFeeUSD()
}

func effectiveBuyPrice(spot float64) float64 { return spot * (1 + slipPct()) }

// Что вернётся на кошелёк после продажи (1% pump + slip + сеть)
func exitNetAfterSell(tokens float64, spot float64) float64 {
	if tokens <= 0 || spot <= 0 {
		return 0
	}
	exitMark := spot * (1 - slipPct())
	gross := tokens * exitMark
	return gross*(1-pumpFeePct()) - solanaTxFeeUSD()
}

func heliusURL() string {
	return "https://mainnet.helius-rpc.com/?api-key=" + HELIUS_API_KEY
}

func apiReady() bool {
	return HELIUS_API_KEY != "ВСТАВЬ_КЛЮЧ_СЮДА" && len(HELIUS_API_KEY) > 10
}

func rpc(method string, params []interface{}) ([]byte, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": method, "params": params,
	})
	resp, err := httpClient.Post(heliusURL(), "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ════════════════════════════════════════════════════
//  PUMP.FUN BONDING CURVE — реальная цена on-chain
//
//  Pump.fun хранит виртуальные резервы в bonding curve аккаунте.
//  Цена токена = virtualSolReserves / virtualTokenReserves * SOL_PRICE
//  Данные: bytes 8-56 аккаунта (после 8-байтного discriminator)
// ════════════════════════════════════════════════════

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

// curveSnap — снимок кривой для входа и мониторинга
type curveSnap struct {
	PriceUSD   float64
	Progress   float64
	RealSolSOL float64
	Complete   bool
}

func getCurveSnapshot(bcAddr string) (*curveSnap, error) {
	data, err := rpc("getAccountInfo", []interface{}{
		bcAddr,
		map[string]string{"encoding": "base64"},
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

// Несколько попыток: create → кривая/pool иногда появляется в RPC с задержкой.
func getCurveSnapshotWithRetry(bcAddr string, source string) (*curveSnap, error) {
	var last *curveSnap
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(120*attempt) * time.Millisecond)
		}
		s, err := getCurveSnapshotUnified(bcAddr, source)
		last, lastErr = s, err
		if err == nil && s != nil && s.PriceUSD > 0 {
			return s, nil
		}
	}
	return last, lastErr
}

// curveVelocityOK — второй замер после паузы: нужен заметный приток (покупки/боты).
// rejectKey пустой при ok; иначе vel_rpc / vel_low / vel_late — для сводки отсева.
func curveVelocityOK(bc string, snap0 *curveSnap, source string, createAt *time.Time) (snap1 *curveSnap, ok bool, detail string, rejectKey string) {
	if snap0 == nil || snap0.Complete {
		return nil, false, "нет снимка", "velocity"
	}
	if envSkipVelocity() {
		return snap0, true, "SKIP_VELOCITY=1 (без паузы и второго замера)", ""
	}
	pause, minDP, minDSol, mode := adaptiveVelocityParams(createAt, snap0)
	time.Sleep(pause)
	s1, err := getCurveSnapshotWithRetry(bc, source)
	if err != nil || s1 == nil || s1.PriceUSD <= 0 {
		return nil, false, "второй снимок кривой", "vel_rpc"
	}
	if s1.Complete {
		return nil, false, "кривая complete на втором замере", "complete"
	}
	dP := s1.Progress - snap0.Progress
	dSol := s1.RealSolSOL - snap0.RealSolSOL
	// Recovery logic: допускаем небольшой отрицательный шум до -0.01%.
	if dP < VELOCITY_MIN_DELTA_DP {
		return s1, false, fmt.Sprintf("micro-velocity ниже порога (Δ%.3f%% < %.3f%% за %v)", dP*100, VELOCITY_MIN_DELTA_DP*100, pause), "vel_low"
	}
	if dP < minDP && dSol < minDSol {
		return s1, false, fmt.Sprintf("мало притока (Δ%.2f%% / +%.3f SOL за %v; проф=%s, need≈Δ%.2f%% или +%.3f SOL)",
			dP*100, dSol, pause, mode, minDP*100, minDSol), "vel_low"
	}
	if s1.Progress > SNIPER_CURVE_MAX+0.06 {
		return s1, false, fmt.Sprintf("кривая уже %.1f%% — поздно", s1.Progress*100), "vel_late"
	}
	return s1, true, fmt.Sprintf("Δ%.2f%% / +%.3f SOL за %v (%s)", dP*100, dSol, pause, mode), ""
}

func adaptiveVelocityParams(createAt *time.Time, snap0 *curveSnap) (pause time.Duration, minDP, minDSol float64, tag string) {
	pause = VELOCITY_PAUSE
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

	// Адаптивность по возрасту create: на старте пулы живут быстрее, можно дать чуть мягче порог;
	// если уже не «свежак», наоборот ужесточаем, чтобы не лезть в застой.
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
	return pause, minDP, minDSol, tag
}

// ════════════════════════════════════════════════════
//  BONDING CURVE PDA — по официальным seeds pump.fun
// ════════════════════════════════════════════════════

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

// pickMintFromPostBalances — приоритет mint …pump; не USDC/wSOL из того же tx
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

// ════════════════════════════════════════════════════
//  CREATE TX: mint + создатель (первый signer / fee payer)
// ════════════════════════════════════════════════════

func parseCreateTx(sig string) (mint, creator string, createBlockTime *time.Time) {
	data, err := rpc("getTransaction", []interface{}{
		sig,
		map[string]interface{}{
			"encoding":                       "jsonParsed",
			"maxSupportedTransactionVersion": 0,
			"commitment":                     "confirmed",
		},
	})
	if err != nil {
		return "", "", nil
	}

	var wrap struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(data, &wrap) != nil || string(wrap.Result) == "null" {
		return "", "", nil
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
		return "", "", nil
	}
	if r.BlockTime != nil && *r.BlockTime > 0 {
		t := time.Unix(*r.BlockTime, 0)
		createBlockTime = &t
	}

	isCreate := false
	for _, l := range r.Meta.LogMessages {
		// Только Create pump.fun — не InitializeMint чужих SPL (SOL/USDC и т.д.)
		if contains(l, "Instruction: Create") {
			isCreate = true
			break
		}
	}
	if !isCreate {
		return "", "", createBlockTime
	}

	mint = pickMintFromPostBalances(r.Meta.PostTokenBalances)

	creator = firstSignerFromKeys(r.Transaction.Message.AccountKeys)
	return mint, creator, createBlockTime
}

func formatCreateAge(t *time.Time) string {
	if t == nil {
		return "время блока n/a"
	}
	age := time.Since(*t).Round(time.Second)
	return fmt.Sprintf("%v назад · блок %s UTC", age, t.UTC().Format("02.01 15:04:05"))
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

// ════════════════════════════════════════════════════
//  АНТИ-СКАМ: RPC
// ════════════════════════════════════════════════════

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

// freezeAuthority ≠ null — опасно. mintAuthority часто = bonding curve / pool (норма) или null.
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
	uri, err := rpcTokenMetadataURI(mint)
	if err != nil {
		return false, "metadata URI недоступен"
	}
	resp, err := httpClient.Get(uri)
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		return false, "metadata JSON недоступен"
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, "metadata JSON read error"
	}
	s := strings.ToLower(string(body))
	hasTg := strings.Contains(s, "t.me/") || strings.Contains(s, "telegram")
	hasTw := strings.Contains(s, "twitter.com/") || strings.Contains(s, "x.com/")
	if !hasTg && !hasTw {
		return false, "нет Telegram/Twitter в metadata"
	}
	return true, "social ok"
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
		return false, fmt.Sprintf("bundled attack: %d top holders в блоке dev", sameBlock)
	}
	return true, fmt.Sprintf("bundle ok (%d)", sameBlock)
}

// Топ-холдеры: кривая должна держать львиную долю; иначе — раздача/скам-паттерн
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
		creatorMinSOL = math.Min(creatorMinSOL, 0.02)
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
		return false, "нет largest accounts"
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
	if lim > 6 {
		lim = 6
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
			// создатель напрямую держит крупный офф-кривой стек — подозрительно
			if owner == creator && amt/total > 0.03 {
				return false, fmt.Sprintf("создатель держит %.1f%% вне кривой", 100*amt/total)
			}
		}
	}
	if curveAmt/total < minCurveShare {
		return false, fmt.Sprintf("в кривой только %.0f%% саплая (нужно ≥%.0f%%)", 100*curveAmt/total, 100*minCurveShare)
	}
	if nonCurve/total > maxNonCurveShare {
		return false, fmt.Sprintf("%.0f%% токенов вне кривой (макс %.0f%%)", 100*nonCurve/total, 100*maxNonCurveShare)
	}
	return true, fmt.Sprintf("кривая ~%.0f%% supply", 100*curveAmt/total)
}

// mintAuthorityRef — pump: bonding curve PDA; launchlab: pool PDA (для сравнения с mintAuthority).
// liquidityVault — аккаунт, где лежит основная ликвидность (pump: та же кривая; launchlab: pool_vault base).
func antiScamCheck(mint, mintAuthorityRef, liquidityVault, creator string, createAt *time.Time, extraMintAuth ...string) (ok bool, detail string) {
	if creator == "" {
		return false, "нет pubkey создателя"
	}
	sol, err := rpcGetBalanceSOL(creator)
	if err != nil {
		return false, "balance RPC"
	}
	creatorMinSOL, creatorMaxSOL, minCurveShare, maxNonCurveShare := antiScamThresholds()
	if sol < creatorMinSOL {
		return false, fmt.Sprintf("SOL создателя %.3f < %.2f", sol, creatorMinSOL)
	}
	if sol > creatorMaxSOL {
		return false, fmt.Sprintf("SOL создателя %.1f > %.0f (подозр.)", sol, creatorMaxSOL)
	}
	badMint, badFreeze, err := rpcMintAuthorities(mint, mintAuthorityRef, extraMintAuth...)
	if err != nil {
		return false, "mint RPC"
	}
	if badMint {
		return false, "mintAuthority не кривая (чужая чеканка)"
	}
	if badFreeze {
		return false, "freezeAuthority (заморозка счетов)"
	}
	okSocial, socialDetail := hasSocialLinksInMetadata(mint)
	if !okSocial {
		return false, socialDetail
	}
	if sold, soldDetail := devSoldInFirstMinute(creator, createAt); sold {
		return false, soldDetail
	}
	okBundle, bundleDetail := bundledBuyersCheck(mint, creator, createAt)
	if !okBundle {
		return false, bundleDetail
	}
	ok2, hd := rpcHolderDistributionOK(mint, liquidityVault, creator, minCurveShare, maxNonCurveShare)
	if !ok2 {
		return false, hd
	}
	return true, hd + fmt.Sprintf(" | %s | %s | dev %.2f SOL", socialDetail, bundleDetail, sol)
}

// ════════════════════════════════════════════════════
//  WEBSOCKET
// ════════════════════════════════════════════════════

func pumpCreateFromLogs(logs []string) bool {
	for _, l := range logs {
		if contains(l, "Instruction: Create") {
			return true
		}
	}
	return false
}

// listenProgram — подписка на логи одной программы (Pump.fun или Raydium LaunchLab).
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
		fmt.Printf("%s WS [%s] → %s\n", cyan("🔌"), prettyLabel, url[:52]+"...")
		conn, resp, err := dialer.Dial(url, headers)
		if err != nil {
			code := 0
			if resp != nil {
				code = resp.StatusCode
			}
			if code == 403 {
				fmt.Println(red("❌ HTTP 403 — пробую другой endpoint..."))
			} else {
				fmt.Printf("%s [%s] %v\n", red("❌"), prettyLabel, err)
			}
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(backoff*2), float64(30*time.Second)))
			continue
		}
		backoff = 3 * time.Second
		fmt.Printf("%s WebSocket — %s\n", green("✓"), prettyLabel)

		conn.WriteJSON(map[string]interface{}{
			"jsonrpc": "2.0", "id": 1,
			"method": "logsSubscribe",
			"params": []interface{}{
				map[string]interface{}{"mentions": []string{programID}},
				map[string]string{"commitment": "confirmed"},
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
				fmt.Printf("%s WS [%s] разорван: %s\n", yellow("⚠"), prettyLabel, err.Error())
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
				ch <- NewToken{Sig: v.Signature, Source: tokenSrc}
			}
		}
	}
}

// ════════════════════════════════════════════════════
//  WALLET
// ════════════════════════════════════════════════════

func (w *Wallet) open(tok NewToken, sym string, spot float64) bool {
	if liveTradingEnabled() {
		w.mu.Lock()
		if len(w.Pos) >= MAX_POSITIONS {
			w.mu.Unlock()
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
	fmt.Printf("\n%s ВХОД  %-18s | гросс $%.2f | в кривую ~$%.2f | pump %.0f%% + сеть ~$%.3f | eff $%.10f | баланс $%.2f\n",
		cyan("→"), sym, capital, pool, pumpFeePct()*100, tx, entry, w.Balance)
	return true
}

// openLive — реальный свап SOL→токен только через Pump.fun bonding curve (pump_direct).
func (w *Wallet) openLive(tok NewToken, sym string, spot float64, capitalUSD float64) bool {
	if !liveUsePumpDirect(tok) {
		consoleMu.Lock()
		fmt.Println(yellow("⚠ LIVE: только Pump.fun на кривой — LaunchLab/другие источники в live отключены (бумага без изменений)."))
		consoleMu.Unlock()
		return false
	}
	solPrice := getSolUSD()
	if solPrice < 1 {
		return false
	}
	solBal, err := rpcGetBalanceSOL(livePub.String())
	if err != nil {
		consoleMu.Lock()
		fmt.Println(red("❌ RPC баланс: " + err.Error()))
		consoleMu.Unlock()
		return false
	}
	reserve := liveReserveSOL
	_ = capitalUSD
	solForSwap := LIVE_FIXED_BUY_SOL
	if solForSwap > solBal-reserve {
		solForSwap = solBal - reserve
	}
	if solForSwap <= 0.001 {
		consoleMu.Lock()
		fmt.Println(yellow("⚠ LIVE: мало SOL после резерва под комиссии"))
		consoleMu.Unlock()
		return false
	}
	lamports := uint64(solForSwap * 1e9)
	if lamports < 50_000 {
		return false
	}
	var tokenRaw uint64
	var sig string
	var solIn uint64
	fmt.Println(gray("⏳ Pump.fun: прямая покупка на кривой…"))
	tokenRaw, sig, solIn, err = PumpDirectBuy(tok.Mint, lamports)
	if err != nil {
		consoleMu.Lock()
		fmt.Printf("%s %v\n", red("❌ Pump buy:"), err)
		consoleMu.Unlock()
		syncWalletBalanceUSD(w)
		return false
	}
	syncWalletBalanceUSD(w)
	tokens := float64(tokenRaw) / 1e6 // pump: 6 decimals
	entry := spot
	if tokens > 0 {
		entry = (float64(solIn) / 1e9 * solPrice) / tokens
	}
	capitalEff := float64(solIn) / 1e9 * solPrice

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.Pos) >= MAX_POSITIONS {
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
	bal := w.Balance
	fmt.Printf("\n%s ВХОД LIVE %-18s | ~$%.2f SOL→токен | eff $%.10f | raw %d | %s | баланс $%.2f\n",
		cyan("→"), sym, capitalEff, entry, tokenRaw, gray(sig), bal)
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
	bal := w.Balance
	w.mu.Unlock()

	icon := green("✓")
	ps := green(fmt.Sprintf("+$%.2f (+%.0f%%)", pnl, pct))
	if pnl < 0 {
		icon = red("✗")
		ps = red(fmt.Sprintf("$%.2f (%.0f%%)", pnl, pct))
	}
	fmt.Printf("\n%s ВЫХОД %-18s | %s | нетто $%.2f (~комиссии ~$%.2f) | %-24s | %s | бал: $%.2f\n",
		icon, snap.Symbol, ps, net, feesEst, reason, dur, bal)
	fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(snap.Mint)))
	if pnl < 0 {
		fmt.Printf("   %s [%s] %s\n", yellow("ⓘ учёт"), bk, lossLearningHint(reason))
	}
}

func (w *Wallet) closePosLive(pos *Position, reason string, spot float64) {
	_ = spot
	if !liveUsePumpDirectClose(pos) {
		consoleMu.Lock()
		fmt.Println(yellow("⚠ LIVE выход: только Pump.fun на кривой — эта позиция не pump; закрой вручную на DEX."))
		consoleMu.Unlock()
		syncWalletBalanceUSD(w)
		w.mu.Lock()
		w.Pos[pos.Mint] = pos
		w.mu.Unlock()
		return
	}
	var sig string
	var solOut uint64
	fmt.Println(gray("⏳ Pump.fun: продажа на кривой…"))
	sig, solOut, err := PumpDirectSellAll(pos.Mint)
	solUSD := getSolUSD()
	if err != nil {
		consoleMu.Lock()
		fmt.Printf("%s %v\n", red("❌ Pump sell:"), err)
		consoleMu.Unlock()
		syncWalletBalanceUSD(w)
		w.mu.Lock()
		w.Pos[pos.Mint] = pos
		w.mu.Unlock()
		return
	}
	syncWalletBalanceUSD(w)

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
	bal := w.Balance
	w.mu.Unlock()

	icon := green("✓")
	ps := green(fmt.Sprintf("+$%.2f (+%.0f%%)", pnl, pct))
	if pnl < 0 {
		icon = red("✗")
		ps = red(fmt.Sprintf("$%.2f (%.0f%%)", pnl, pct))
	}
	fmt.Printf("\n%s ВЫХОД LIVE %-18s | %s | нетто ~$%.2f | %s | %s | бал: $%.2f\n",
		icon, pos.Symbol, ps, net, reason, dur, bal)
	fmt.Printf("   %s %s\n", gray("sig"), gray(sig))
	fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(pos.Mint)))
	if pnl < 0 {
		fmt.Printf("   %s [%s] %s\n", yellow("ⓘ учёт"), bk, lossLearningHint(reason))
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
	title := "PAPER WALLET — РЕАЛЬНЫЕ ДАННЫЕ"
	if liveTradingEnabled() {
		title = "LIVE WALLET — MAINNET (Pump.fun)"
	}
	fmt.Println("\n" + bold("┌──────────────────────────────────────────────┐"))
	fmt.Println(bold("│  "+title+"                    │"))
	fmt.Println(bold("├──────────────────────────────────────────────┤"))
	fmt.Printf("│  Баланс:   %-33s│\n", bs)
	fmt.Printf("│  PnL:      %-33s│\n", ps)
	fmt.Printf("│  Сделок:   %-33s│\n", fmt.Sprintf("%d закрыто | %d открыто", n, nOpen))
	fmt.Printf("│  Win/Loss: %-33s│\n",
		fmt.Sprintf("%s/%s  WR: %.0f%%",
			green(fmt.Sprintf("%d", wins)),
			red(fmt.Sprintf("%d", n-wins)), wr))
	if len(lossBk) > 0 || len(winBk) > 0 {
		fmt.Println(bold("├──────────────────────────────────────────────┤"))
		if len(winBk) > 0 {
			fmt.Printf("│  Плюсы по выходу: %-26s│\n", gray(formatExitBuckets(winBk)))
		}
		if len(lossBk) > 0 {
			fmt.Printf("│  Минусы по выходу: %-25s│\n", yellow(formatExitBuckets(lossBk)))
		}
	}
	fmt.Println(bold("└──────────────────────────────────────────────┘"))
	if bal >= 21 {
		fmt.Println(green("🎉 ЦЕЛЬ $21 БЫЛА БЫ ДОСТИГНУТА!"))
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

// runPaperSelfTest — один виртуальный вход и выход (без mainnet), чтобы проверить PnL и комиссии за секунды.
func runPaperSelfTest() {
	dw := newWallet()
	mint := "SelfTestMintSelfTestMintSelfTestMintPuump"
	tok := NewToken{Mint: mint, BondingCurve: "SelfTestBC111111111111111111111111111111111111", Sig: "selftest"}
	sym := "$SELF..pump"
	spot := 0.00012345

	consoleMu.Lock()
	defer consoleMu.Unlock()

	fmt.Println(bold("\n═══ PAPER SELF-TEST (кошелёк + комиссии, без WebSocket) ═══"))
	fmt.Printf("Стартовый баланс: $%.2f · ставка в live: %.3f SOL (fixed)\n\n",
		dw.Start, LIVE_FIXED_BUY_SOL)

	if !dw.open(tok, sym, spot) {
		fmt.Println(red("open() не прошёл — мало баланса или ставка"))
		return
	}

	exitSpot := spot * 1.10 // +10% к spot — ожидаем плюс после комиссий
	dw.closePos(mint, "SELFTEST +10% spot", exitSpot)

	dw.mu.Lock()
	bal := dw.Balance
	nClosed := len(dw.Closed)
	var lastPnL float64
	if nClosed > 0 {
		lastPnL = dw.Closed[nClosed-1].PnL
	}
	dw.mu.Unlock()

	fmt.Printf("\n%s Итог: баланс $%.2f · последняя сделка PnL %+.2f USD\n", bold("●"), bal, lastPnL)
	if lastPnL > 0 {
		fmt.Println(green("✓ Цепочка вход → выход и учёт комиссий работает (плюс ожидаем при +10% spot)."))
	} else {
		fmt.Println(yellow("ⓘ PnL после комиссий не в плюсе — так бывает при узкой марже; логика кошелька всё равно отработала."))
	}
	fmt.Println(gray("Live: go run . (без -selftest) — ждёт реальные create-токены; live_wallet + pump_direct."))
}

// ════════════════════════════════════════════════════
//  МОНИТОРИНГ — цена из bonding curve напрямую
// ════════════════════════════════════════════════════

func monitor(w *Wallet, mint, bcAddr, sym, source string) {
	ticker := time.NewTicker(PRICE_TICK)
	timeout := time.NewTimer(MAX_HOLD)
	defer ticker.Stop()
	defer timeout.Stop()

	fmt.Printf("  %s %s\n", gray("DEX"), cyan(dexScreenerURL(mint)))

	consecutiveFails := 0
	confirmedStopTicks := 0
	var lastMult float64
	var lastPrint time.Time
	const monitorPrintMinMove = 0.0025 // ~0.25% к цене входа — новая строка
	monitorHeartbeat := 12 * time.Second

	for {
		select {
		case <-timeout.C:
			snap, _ := getCurveSnapshotUnified(bcAddr, source)
			px := 0.0
			if snap != nil {
				px = snap.PriceUSD
			}
			w.closePos(mint, fmt.Sprintf("ТАЙМАУТ %.0f мин", MAX_HOLD.Minutes()), px)
			return

		case <-ticker.C:
			w.mu.Lock()
			pos, open := w.Pos[mint]
			w.mu.Unlock()
			if !open {
				return
			}

			snap, err := getCurveSnapshotUnified(bcAddr, source)
			if err != nil || snap == nil || snap.PriceUSD <= 0 {
				consecutiveFails++
				if consecutiveFails > 5 {
					w.closePos(mint, "ТОКЕН УМЕР (нет данных)", pos.EntryPrice*0.5)
					return
				}
				fmt.Printf("  %s %-18s | ошибка цены (%d/5)\n", gray("?"), sym, consecutiveFails)
				continue
			}
			consecutiveFails = 0

			price := snap.PriceUSD
			progress := snap.Progress

			if snap.Complete {
				fmt.Printf("  %s %-18s | КРИВАЯ ЗАВЕРШЕНА → миграция\n", yellow("🚀"), sym)
				w.closePos(mint, "МИГРАЦИЯ (complete)", price)
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
			// Печать только при заметном движении mult или раз в heartbeat (без дублей)
			now := time.Now()
			if lastPrint.IsZero() || math.Abs(mult-lastMult) >= monitorPrintMinMove || now.Sub(lastPrint) >= monitorHeartbeat {
				lastMult = mult
				lastPrint = now
				fmt.Printf("  %s %-18s | spot: %s | $%.10f | x%.3f | curve: %.1f%%\n",
					cyan("◈"), sym, ps, price, mult, progress*100)
			}

			if mult >= TAKE_PROFIT {
				w.closePos(mint, fmt.Sprintf("ТЕЙК ~+%.0f%% spot", (TAKE_PROFIT-1)*100), price)
				return
			}
			// Recovery: на +100% фиксируем половину, остаток ведём трейлингом.
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
					w.mu.Unlock()
					fmt.Printf("  %s %-18s | partial +100%%: sold 50%% | out %.4f SOL | %s\n",
						green("↗"), sym, float64(solOut)/1e9, gray(sig))
				}
			}

			// Трейлинг: только после TRAIL_MIN_AGE; стоп = max(откат от пика, мин. +4% к входу)
			if time.Since(opened) >= TRAIL_MIN_AGE && peak >= entry*TRAIL_ACTIVATE {
				trailLine := peak * (1 - TRAILING)
				floorLine := entry * TRAIL_MIN_PROFIT
				stopLine := math.Max(trailLine, floorLine)
				if price <= stopLine {
					w.closePos(mint, fmt.Sprintf("ТРЕЙЛИНГ (пик x%.2f, пол ≥+%.0f%%)", peak/entry, (TRAIL_MIN_PROFIT-1)*100), price)
					return
				}
			}

			if breakeven && price < entry {
				w.closePos(mint, "БРЕЙК-ИВН после импульса", price)
				return
			}
			// Final Recovery: не выходим по "слабому импульсу"/"нет импульса", даём позиции разыграться.
			if price <= entry*STOP_CONFIRM_LVL {
				confirmedStopTicks++
			} else {
				confirmedStopTicks = 0
			}
			// "Fake stop-out" защита: требуется 2 подряд тика ниже -20%,
			// либо мгновенный hard-stop при -25%.
			if price <= entry*STOP_LOSS_HARD || confirmedStopTicks >= STOP_CONFIRM_N {
				if price <= entry*STOP_LOSS_HARD {
					w.closePos(mint, "СТОП -25% (hard)", price)
				} else {
					w.closePos(mint, "СТОП подтверждён (-20% x2)", price)
				}
				return
			}
		}
	}
}

// ════════════════════════════════════════════════════
//  ВСПОМОГАТЕЛЬНЫЕ
// ════════════════════════════════════════════════════

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

// DexScreener — график/пара по mint на Solana
func dexScreenerURL(mint string) string {
	return "https://dexscreener.com/solana/" + mint
}

// ════════════════════════════════════════════════════
//  MAIN
// ════════════════════════════════════════════════════

func main() {
	_ = godotenv.Load()
	selftest := flag.Bool("selftest", false, "проверка виртуального кошелька и комиссий за секунды (без WebSocket), затем выход")
	flag.Parse()
	if *selftest {
		refreshSolPriceUSD()
		runPaperSelfTest()
		os.Exit(0)
	}

	if !apiReady() {
		fmt.Println(red("❌ Вставь Helius API ключ в HELIUS_API_KEY"))
		fmt.Println(yellow("   dev.helius.xyz → Sign Up → Create App"))
		os.Exit(1)
	}
	if err := initLiveTrading(); err != nil {
		fmt.Println(red("❌ " + err.Error()))
		os.Exit(1)
	}

	fmt.Println(bold("╔══════════════════════════════════════════════════════════╗"))
	if liveTradingEnabled() {
		fmt.Println(bold("║   PUMP.FUN — БОЕВОЙ РЕЖИМ (MAINNET)                      ║"))
		fmt.Println(bold("║   Реальные SOL · Pump.fun curve · риск потери капитала    ║"))
	} else {
		fmt.Println(bold("║   PUMP.FUN LIVE PAPER TRADING                            ║"))
		fmt.Println(bold("║   Реальные токены · Реальные цены · Виртуальные деньги  ║"))
	}
	fmt.Println(bold("╚══════════════════════════════════════════════════════════╝"))
	fmt.Println(green("✓ Helius WebSocket — Pump.fun + Raydium LaunchLab (два потока)"))
	fmt.Println(green("✓ Bonding Curve Price — прямо из on-chain данных"))
	if liveTradingEnabled() {
		fmt.Printf("%s LIVE кошелёк %s | %s\n", green("✓"), cyan(short(livePub.String())),
			yellow("Только mint …pump на кривой; LaunchLab в live не торгуется."))
	}
	if envSkipVelocity() {
		fmt.Println(yellow("⚠ SKIP_VELOCITY=1 — нет 2.6с паузы и второго замера; больше входов, больше шума."))
	}
	if envSkipAntiScam() {
		fmt.Println(red("⚠ SKIP_ANTI_SCAM=1 — фильтр скама отключён; в live можно слить SOL на мусор."))
	}

	refreshSolPriceUSD()
	sp := getSolUSD()
	fmt.Printf("%s SOL/USD: $%.2f (CoinGecko, автообновление ~90 с)\n", green("✓"), sp)
	go func() {
		t := time.NewTicker(90 * time.Second)
		for range t.C {
			refreshSolPriceUSD()
		}
	}()

	fmt.Printf("\n%s Режим: %s | кривая: %.1f%%–%.1f%% | min SOL: %.2f | velocity(base): %v (Δ≥%.2f%% или +%.3f SOL) | profile=%s | ончейн-фильтры\n",
		bold("▶"), cyan("SNIPER"), SNIPER_CURVE_MIN*100, SNIPER_CURVE_MAX*100, MIN_REAL_SOL,
		VELOCITY_PAUSE, VELOCITY_MIN_DPROGRESS*100, VELOCITY_MIN_DREALSOL, envSignalProfile())
	wallet := newWallet()
	if liveTradingEnabled() {
		fixedUSD := LIVE_FIXED_BUY_SOL * getSolUSD()
		fmt.Printf("%s Баланс: %s (ончейн) | Ставка: fixed %.3f SOL (~$%.2f) | Макс позиций: %d\n",
			bold("▶"), green(fmt.Sprintf("$%.2f", wallet.Balance)), LIVE_FIXED_BUY_SOL, fixedUSD, MAX_POSITIONS)
	} else {
		fmt.Printf("%s Баланс: %s | Ставка: %.2f%% от банка (min $%.2f) → сейчас ~$%.2f на сделку | Макс позиций: %d\n",
			bold("▶"), green(fmt.Sprintf("$%.2f", PAPER_BALANCE)), BET_PCT_OF_BALANCE*100, MIN_STAKE_USD,
			stakeFromBalance(PAPER_BALANCE), MAX_POSITIONS)
	}
	fmt.Printf("%s DexScreener — по mint показывается вся история торгов; даты на оси — календарь свечей, не дата «создания ссылки». Свежесть листинга смотри в строке ⏱ (время блока create-тx).\n",
		gray("ⓘ"))
	fmt.Printf("%s Быстрая проверка кошелька: %s\n",
		gray("ⓘ"), cyan("go run . -selftest"))
	fmt.Printf("%s Цель «~1 вход / 2 мин» — ориентир: одна позиция + поток Pump.fun; смотри строку ◎ «средний интервал» раз в минуту.\n\n",
		gray("ⓘ"))

	tokenCh := make(chan NewToken, 200)
	var seen sync.Map
	// Больше слотов: пока один токен в sleep(velocity), остальные create обрабатываются
	sem := make(chan struct{}, 12)

	go listenProgram(PUMP_PROGRAM, "Pump.fun", pumpCreateFromLogs, tokenCh, "pump")
	go listenProgram(LAUNCHLAB_PROGRAM, "Raydium LaunchLab", launchLabInitFromLogs, tokenCh, "launchlab")

	go func() {
		for tok := range tokenCh {
			tok := tok
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()

				src := tokenSource(tok)
				var mint, creator string
				var createAt *time.Time
				var bc string
				var err error
				if src == "launchlab" {
					mint, creator, createAt = parseLaunchLabCreateTx(tok.Sig)
					if mint == "" {
						logRejectLine("no_mint", "?", "", "launchlab: нет mint в tx")
						return
					}
					bc, err = launchLabPoolPDA(mint)
					if err != nil {
						logRejectLine("pda_err", "$"+short(mint), mint, err.Error())
						return
					}
				} else {
					mint, creator, createAt = parseCreateTx(tok.Sig)
					if mint == "" {
						logRejectLine("no_mint", "?", "", "нет mint в create tx")
						return
					}
					if !strings.HasSuffix(mint, "pump") {
						sym := "$" + short(mint)
						logRejectLine("not_pump", sym, mint, "mint не …pump — не pump.fun токен")
						return
					}
					bc, err = pumpBondingCurvePDA(mint)
					if err != nil {
						logRejectLine("pda_err", "$"+short(mint), mint, err.Error())
						return
					}
				}
				tok.Mint = mint
				tok.BondingCurve = bc

				if _, loaded := seen.LoadOrStore(mint, true); loaded {
					return
				}

				wallet.mu.Lock()
				cnt := len(wallet.Pos)
				_, has := wallet.Pos[mint]
				wallet.mu.Unlock()
				if has || cnt >= MAX_POSITIONS {
					return
				}

				sym := "$" + short(mint)

				if createAt != nil && time.Since(*createAt) > MAX_CREATE_TX_AGE {
					logRejectLine("stale_tx", sym, mint, fmt.Sprintf(
						"create-тx старше %v (%s) — не свежий листинг", MAX_CREATE_TX_AGE, formatCreateAge(createAt)))
					return
				}

				snap0, err := getCurveSnapshotWithRetry(bc, src)
				if err != nil || snap0 == nil || snap0.PriceUSD <= 0 {
					logRejectLine("no_price", sym, mint, "нет цены (кривая / pool)")
					return
				}
				if snap0.Complete {
					logRejectLine("complete", sym, mint, "bonding curve complete")
					return
				}
				if snap0.Progress < SNIPER_CURVE_MIN {
					logRejectLine("empty", sym, mint, fmt.Sprintf("curve %.2f%% < %.1f%% — пусто",
						snap0.Progress*100, SNIPER_CURVE_MIN*100))
					return
				}
				if snap0.Progress > SNIPER_CURVE_MAX {
					logRejectLine("late", sym, mint, fmt.Sprintf("curve %.1f%% > %.1f%% — поздно",
						snap0.Progress*100, SNIPER_CURVE_MAX*100))
					return
				}
				if snap0.RealSolSOL < MIN_REAL_SOL {
					logRejectLine("low_sol", sym, mint, fmt.Sprintf("real SOL %.2f < %.2f",
						snap0.RealSolSOL, MIN_REAL_SOL))
					return
				}

				atomic.AddInt64(&funnelInWindow, 1)
				snap1, vOK, vDetail, vKey := curveVelocityOK(bc, snap0, src, createAt)
				if !vOK {
					if vKey == "" {
						vKey = "velocity"
					}
					logRejectLine(vKey, sym, mint, "velocity: "+vDetail)
					return
				}

				atomic.AddInt64(&funnelPassVel, 1)
				liqVault := bc
				if src == "launchlab" {
					vault, err := launchLabBaseVault(bc, mint)
					if err != nil {
						logRejectLine("pda_err", sym, mint, "launchlab vault: "+err.Error())
						return
					}
					liqVault = vault
				}
				var extraMint []string
				if src == "launchlab" {
					extraMint = []string{LAUNCHLAB_PROGRAM}
				}
				var ok bool
				var scamMeta string
				if envSkipAntiScam() {
					ok, scamMeta = true, "[SKIP_ANTI_SCAM тест — не для реальной торговли]"
				} else {
					ok, scamMeta = antiScamCheck(mint, bc, liqVault, creator, createAt, extraMint...)
				}
				if !ok {
					logRejectLine("scam", sym, mint, "фильтр: "+scamMeta)
					return
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
				fmt.Printf("\n%s %-18s | %s | $%.10f | curve %.1f%% | SOL %.2f | %s | %s → ВХОД\n",
					green("✓"), sym, srcTag, price, progress*100, realSol, gray(vDetail), gray(scamMeta))
				fmt.Printf("   %s %s\n", gray("⏱"), cyan(ageInfo))
				fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(mint)))
				if liveTradingEnabled() {
					fmt.Println(gray("   ⏳ LIVE: подпись транзакции Pump.fun (RPC) — обычно несколько секунд."))
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
					fmt.Println(yellow("⚠ ВХОД отклонён open(): баланс, лимит, не pump в live, или RPC"))
					consoleMu.Unlock()
				}
			}()
		}
	}()

	// Статистика каждые 3 минуты
	go func() {
		t := time.NewTicker(3 * time.Minute)
		for range t.C {
			wallet.stats()
		}
	}()

	// Сводка отсева раз в минуту (без спама по каждому токену)
	go func() {
		t := time.NewTicker(1 * time.Minute)
		for range t.C {
			printRejectSummary()
			printFunnelLine()
			printEntryPace()
		}
	}()

	fmt.Println(gray("Нажми Ctrl+C для остановки\n"))
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	fmt.Println(yellow("\n⏹  Остановка..."))
	wallet.stats()
	wallet.mu.Lock()
	if len(wallet.Closed) > 0 {
		fmt.Println("\n" + bold("── История сделок ─────────────────────────────"))
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
		fmt.Println(yellow("\n⚠ LIVE: сделки были в mainnet — проверь баланс в кошельке."))
	} else {
		fmt.Println(bold("\n✓ Реальных денег не потрачено."))
	}
}

// ════════════════════════════════════════════════════
//  RAYDIUM LAUNCHLAB (в этом же файле — go run без launchlab.go)
// ════════════════════════════════════════════════════

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
		return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("короткий аккаунт pool")
	}
	body := raw[8:]
	if len(body) < 77 {
		return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("короткое тело")
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
		map[string]string{"encoding": "base64"},
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
		return nil, fmt.Errorf("нет pool аккаунта")
	}
	if r.Result.Value.Owner != LAUNCHLAB_PROGRAM {
		return nil, fmt.Errorf("owner не LaunchLab")
	}
	arr := r.Result.Value.Data
	if len(arr) < 1 {
		return nil, fmt.Errorf("нет data")
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
	data, err := rpc("getTransaction", []interface{}{
		sig,
		map[string]interface{}{
			"encoding":                       "jsonParsed",
			"maxSupportedTransactionVersion": 0,
			"commitment":                     "confirmed",
		},
	})
	if err != nil {
		return "", "", nil
	}
	var wrap struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(data, &wrap) != nil || string(wrap.Result) == "null" {
		return "", "", nil
	}
	var r struct {
		BlockTime *int64 `json:"blockTime"`
		Meta      struct {
			PostTokenBalances []postTokenBal `json:"postTokenBalances"`
			LogMessages       []string         `json:"logMessages"`
		} `json:"meta"`
		Transaction struct {
			Message struct {
				AccountKeys []json.RawMessage `json:"accountKeys"`
			} `json:"message"`
		} `json:"transaction"`
	}
	if err := json.Unmarshal(wrap.Result, &r); err != nil {
		return "", "", nil
	}
	if r.BlockTime != nil && *r.BlockTime > 0 {
		t := time.Unix(*r.BlockTime, 0)
		createBlockTime = &t
	}
	if !launchLabInitFromLogs(r.Meta.LogMessages) {
		return "", "", createBlockTime
	}
	mint = pickMintFromPostBalances(r.Meta.PostTokenBalances)
	creator = firstSignerFromKeys(r.Transaction.Message.AccountKeys)
	return mint, creator, createBlockTime
}
