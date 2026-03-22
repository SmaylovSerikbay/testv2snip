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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gorilla/websocket"
)

// ════════════════════════════════════════════════════
//  КОНФИГ
// ════════════════════════════════════════════════════

const (
	HELIUS_API_KEY = "1859e3e5-5d82-476e-a121-8d5f57705cf7"
	PAPER_BALANCE  = 7.0
	FIXED_BET      = 1.0
	MAX_POSITIONS  = 1 // снайпер: одна позиция — фокус и меньше шума

	// Pump.fun: Global fee_basis_points = 100 → 1% с покупки и с продажи (документация программы)
	PUMP_FEE_BPS = 100
	// Проскальзывание/хуже исполнение относительно spot (консервативно)
	SLIPPAGE_BPS = 60
	// Оценка сети: base + priority ~N лампортов на подпись (бумага; на mainnet меняется)
	SOLANA_TX_LAMPORTS = 18_000.0

	PRICE_TICK = 3 * time.Second
	MAX_HOLD   = 7 * time.Minute

	// ── Снайпер: баланс «чаще сигнал» (velocity) vs «меньше мусора» (скам + верх кривой)
	SNIPER_CURVE_MIN = 0.010 // ≥1.0%
	SNIPER_CURVE_MAX = 0.075 // ≤7.5% — не догоняем сильно растянутую кривую
	MIN_REAL_SOL     = 0.15  // минимум ликвидности в SOL

	// Анти-скам — чуть строже ончейн-фильтры при ослабленном velocity
	CREATOR_SOL_MIN     = 0.06 // минимум SOL у создателя (было 0.04)
	CREATOR_SOL_SUSPECT = 80.0
	MAX_NONCURVE_PCT    = 0.10 // меньше токенов вне кривой у топов (было 0.12)

	// Выходы: быстрый скальп
	STOP_LOSS        = 0.90
	TAKE_PROFIT      = 1.16 // +16% spot — быстрее фикс
	TRAIL_ACTIVATE   = 1.08
	TRAILING         = 0.07
	TRAIL_MIN_AGE    = 18 * time.Second
	TRAIL_MIN_PROFIT = 1.035
	BREAKEVEN_ARM    = 1.05
	SCRATCH_AFTER    = 90 * time.Second
	SCRATCH_IF_BELOW = 0.985
	NO_IMPULSE_AFTER = 2*time.Minute + 30*time.Second
	NO_IMPULSE_NEED  = 1.025

	// Если create-транзакция старше — не считаем «только что залистились» (защита от кривых сигналов)
	MAX_CREATE_TX_AGE = 45 * time.Minute

	// Velocity: OR — Δprogress или ΔrealSol; realSol чувствительнее на ранней кривой (мелкие покупки)
	VELOCITY_PAUSE         = 3500 * time.Millisecond
	VELOCITY_MIN_DPROGRESS = 0.0015 // +0.15 «п.п.» progress (realSol/85)
	VELOCITY_MIN_DREALSOL  = 0.010  // или +0.010 SOL в кривую за окно (типичный мелкий приток)

	// Логи: false = не печатать каждый отсев (только сводка раз в минуту + успешный ВХОД)
	VERBOSE_REJECT_LOGS = false
)

// Wrapped SOL mint — для Jupiter Price API
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
	funnelInWindow  int64 // прошли кривую+ликвидность, до velocity
	funnelPassVel   int64 // прошли velocity
	funnelPassScam  int64 // прошли анти-скам
	funnelOpenOK    int64 // open() = true
	funnelOpenFail  int64 // open() = false (редко: баланс/лимит)
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
	order := []string{"no_mint", "not_pump", "stale_tx", "no_price", "complete", "empty", "late", "low_sol", "velocity", "scam", "pda_err"}
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
		fmt.Println(gray(fmt.Sprintf("   (были в окне кривой, но velocity за %v ещё ни разу не прошёл — узкое место)", VELOCITY_PAUSE)))
	} else if sc == 0 {
		fmt.Println(gray("   (velocity был, анти-скам пока не пропустил ни одного)"))
	}
}

const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

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
	BondingCurve string // адрес bonding curve аккаунта
	Sig          string
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
}

func newWallet() *Wallet {
	return &Wallet{
		Balance: PAPER_BALANCE,
		Start:   PAPER_BALANCE,
		Pos:     make(map[string]*Position),
	}
}

// ════════════════════════════════════════════════════
//  RPC КЛИЕНТ
// ════════════════════════════════════════════════════

var httpClient = &http.Client{Timeout: 8 * time.Second}

// Курс SOL/USD (обновляется с Jupiter, иначе fallback)
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

func fetchJupiterSOLPrice() (float64, error) {
	type jupV6 struct {
		Data map[string]struct {
			Price float64 `json:"price"`
		} `json:"data"`
	}
	try := func(url string) (float64, bool) {
		resp, err := httpClient.Get(url)
		if err != nil || resp.StatusCode != 200 {
			if resp != nil {
				resp.Body.Close()
			}
			return 0, false
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, false
		}
		var out jupV6
		if json.Unmarshal(body, &out) != nil || len(out.Data) == 0 {
			return 0, false
		}
		for _, row := range out.Data {
			if row.Price > 0 {
				return row.Price, true
			}
		}
		return 0, false
	}
	if p, ok := try("https://price.jup.ag/v6/price?ids=" + WSOL_MINT); ok {
		return p, nil
	}
	if p, ok := try("https://lite-api.jup.ag/price/v2?ids=" + WSOL_MINT); ok {
		return p, nil
	}
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
	p, err := fetchJupiterSOLPrice()
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
//  PUMP.FUN BONDING CURVE — реальная цена без Jupiter
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

// Повтор при гонке create → аккаунт кривой ещё не в RPC
func getCurveSnapshotWithRetry(bcAddr string) (*curveSnap, error) {
	s, err := getCurveSnapshot(bcAddr)
	if err == nil && s != nil && s.PriceUSD > 0 {
		return s, nil
	}
	time.Sleep(180 * time.Millisecond)
	return getCurveSnapshot(bcAddr)
}

// curveVelocityOK — второй замер после паузы: нужен заметный приток (покупки/боты).
func curveVelocityOK(bc string, snap0 *curveSnap) (snap1 *curveSnap, ok bool, detail string) {
	if snap0 == nil || snap0.Complete {
		return nil, false, "нет снимка"
	}
	time.Sleep(VELOCITY_PAUSE)
	s1, err := getCurveSnapshotWithRetry(bc)
	if err != nil || s1 == nil || s1.PriceUSD <= 0 || s1.Complete {
		return nil, false, "второй снимок кривой"
	}
	dP := s1.Progress - snap0.Progress
	dSol := s1.RealSolSOL - snap0.RealSolSOL
	if dP < VELOCITY_MIN_DPROGRESS && dSol < VELOCITY_MIN_DREALSOL {
		return s1, false, fmt.Sprintf("мало притока (Δ%.2f%% / +%.3f SOL за %v)", dP*100, dSol, VELOCITY_PAUSE)
	}
	// Чуть шире запас: за паузу кривая может уехать вверх без «ложного поздно»
	if s1.Progress > SNIPER_CURVE_MAX+0.045 {
		return s1, false, fmt.Sprintf("кривая уже %.1f%% — поздно", s1.Progress*100)
	}
	return s1, true, fmt.Sprintf("Δ%.2f%% / +%.3f SOL за %v", dP*100, dSol, VELOCITY_PAUSE)
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
		Error  *struct{ Message string `json:"message"` } `json:"error"`
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

// freezeAuthority ≠ null — опасно. mintAuthority часто = bonding curve (норма) или null.
func rpcMintAuthorities(mint, bondingCurve string) (badMintAuth, badFreeze bool, err error) {
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
	if s, ok := info.MintAuthority.(string); ok && s != "" && s != bondingCurve {
		badMintAuth = true
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

// Топ-холдеры: кривая должна держать львиную долю; иначе — раздача/скам-паттерн
func rpcHolderDistributionOK(mint, bondingCurve, creator string) (ok bool, detail string) {
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
	if curveAmt/total < 0.55 {
		return false, fmt.Sprintf("в кривой только %.0f%% саплая", 100*curveAmt/total)
	}
	if nonCurve/total > MAX_NONCURVE_PCT {
		return false, fmt.Sprintf("%.0f%% токенов вне кривой (макс %.0f%%)", 100*nonCurve/total, 100*MAX_NONCURVE_PCT)
	}
	return true, fmt.Sprintf("кривая ~%.0f%% supply", 100*curveAmt/total)
}

func antiScamCheck(mint, bondingCurve, creator string) (ok bool, detail string) {
	if creator == "" {
		return false, "нет pubkey создателя"
	}
	sol, err := rpcGetBalanceSOL(creator)
	if err != nil {
		return false, "balance RPC"
	}
	if sol < CREATOR_SOL_MIN {
		return false, fmt.Sprintf("SOL создателя %.3f < %.2f", sol, CREATOR_SOL_MIN)
	}
	if sol > CREATOR_SOL_SUSPECT {
		return false, fmt.Sprintf("SOL создателя %.1f > %.0f (подозр.)", sol, CREATOR_SOL_SUSPECT)
	}
	badMint, badFreeze, err := rpcMintAuthorities(mint, bondingCurve)
	if err != nil {
		return false, "mint RPC"
	}
	if badMint {
		return false, "mintAuthority не кривая (чужая чеканка)"
	}
	if badFreeze {
		return false, "freezeAuthority (заморозка счетов)"
	}
	ok2, hd := rpcHolderDistributionOK(mint, bondingCurve, creator)
	if !ok2 {
		return false, hd
	}
	return true, hd + fmt.Sprintf(" | dev %.2f SOL", sol)
}

// ════════════════════════════════════════════════════
//  WEBSOCKET
// ════════════════════════════════════════════════════

func listenWS(ch chan<- NewToken) {
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
		fmt.Printf("%s WS → %s\n", cyan("🔌"), url[:52]+"...")
		conn, resp, err := dialer.Dial(url, headers)
		if err != nil {
			code := 0
			if resp != nil {
				code = resp.StatusCode
			}
			if code == 403 {
				fmt.Println(red("❌ HTTP 403 — пробую другой endpoint..."))
			} else {
				fmt.Printf("%s %v\n", red("❌"), err)
			}
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(backoff*2), float64(30*time.Second)))
			continue
		}
		backoff = 3 * time.Second
		fmt.Println(green("✓ WebSocket подключён — слушаем Pump.fun!"))

		conn.WriteJSON(map[string]interface{}{
			"jsonrpc": "2.0", "id": 1,
			"method": "logsSubscribe",
			"params": []interface{}{
				map[string]interface{}{"mentions": []string{PUMP_PROGRAM}},
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
				fmt.Println(yellow("⚠ WS разорван: " + err.Error()))
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
			for _, l := range v.Logs {
				if contains(l, "Instruction: Create") {
					ch <- NewToken{Sig: v.Signature}
					break
				}
			}
		}
	}
}

// ════════════════════════════════════════════════════
//  WALLET
// ════════════════════════════════════════════════════

func (w *Wallet) open(tok NewToken, sym string, spot float64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.Pos) >= MAX_POSITIONS {
		return false
	}
	capital := math.Min(FIXED_BET, w.Balance)
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
	}
	tx := solanaTxFeeUSD()
	fmt.Printf("\n%s ВХОД  %-18s | гросс $%.2f | в кривую ~$%.2f | pump %.0f%% + сеть ~$%.3f | eff $%.10f | баланс $%.2f\n",
		cyan("→"), sym, capital, pool, pumpFeePct()*100, tx, entry, w.Balance)
	return true
}

func (w *Wallet) closePos(mint, reason string, spot float64) {
	w.mu.Lock()
	pos, ok := w.Pos[mint]
	if !ok {
		w.mu.Unlock()
		return
	}
	delete(w.Pos, mint)
	w.mu.Unlock()

	net := exitNetAfterSell(pos.Tokens, spot)
	pnl := net - pos.CapitalUSD
	pct := 0.0
	if pos.CapitalUSD > 0 {
		pct = pnl / pos.CapitalUSD * 100
	}
	// Оценка всех комиссий по сделке (для лога)
	feesEst := pos.CapitalUSD*pumpFeePct() + pos.Tokens*spot*pumpFeePct() + 2*solanaTxFeeUSD()
	dur := time.Since(pos.OpenedAt).Round(time.Second)

	w.mu.Lock()
	w.Balance += net
	w.Closed = append(w.Closed, ClosedTrade{
		Symbol: pos.Symbol, Mint: pos.Mint, CapitalUSD: pos.CapitalUSD,
		ExitNetUSD: net, FeesUSD: feesEst, PnL: pnl, PnLPct: pct,
		Reason: reason, Dur: dur,
	})
	bal := w.Balance
	w.mu.Unlock()

	icon := green("✓")
	ps := green(fmt.Sprintf("+$%.2f (+%.0f%%)", pnl, pct))
	if pnl < 0 {
		icon = red("✗")
		ps = red(fmt.Sprintf("$%.2f (%.0f%%)", pnl, pct))
	}
	fmt.Printf("\n%s ВЫХОД %-18s | %s | нетто $%.2f (~комиссии ~$%.2f) | %-24s | %s | бал: $%.2f\n",
		icon, pos.Symbol, ps, net, feesEst, reason, dur, bal)
	fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(pos.Mint)))
}

func (w *Wallet) stats() {
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
	fmt.Println("\n" + bold("┌──────────────────────────────────────────────┐"))
	fmt.Println(bold("│  PAPER WALLET — РЕАЛЬНЫЕ ДАННЫЕ              │"))
	fmt.Println(bold("├──────────────────────────────────────────────┤"))
	fmt.Printf("│  Баланс:   %-33s│\n", bs)
	fmt.Printf("│  PnL:      %-33s│\n", ps)
	fmt.Printf("│  Сделок:   %-33s│\n", fmt.Sprintf("%d закрыто | %d открыто", n, nOpen))
	fmt.Printf("│  Win/Loss: %-33s│\n",
		fmt.Sprintf("%s/%s  WR: %.0f%%",
			green(fmt.Sprintf("%d", wins)),
			red(fmt.Sprintf("%d", n-wins)), wr))
	fmt.Println(bold("└──────────────────────────────────────────────┘"))
	if bal >= 21 {
		fmt.Println(green("🎉 ЦЕЛЬ $21 БЫЛА БЫ ДОСТИГНУТА!"))
	}
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
	fmt.Printf("Стартовый баланс: $%.2f · ставка как в live: $%.2f\n\n", dw.Start, FIXED_BET)

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
	fmt.Println(gray("Live: go run live_paper_trading.go (без -selftest) — ждёт реальные create-токены."))
}

// ════════════════════════════════════════════════════
//  МОНИТОРИНГ — цена из bonding curve напрямую
// ════════════════════════════════════════════════════

func monitor(w *Wallet, mint, bcAddr, sym string) {
	ticker := time.NewTicker(PRICE_TICK)
	timeout := time.NewTimer(MAX_HOLD)
	defer ticker.Stop()
	defer timeout.Stop()

	fmt.Printf("  %s %s\n", gray("DEX"), cyan(dexScreenerURL(mint)))

	consecutiveFails := 0
	var lastMult float64
	var lastPrint time.Time
	const monitorPrintMinMove = 0.0025 // ~0.25% к цене входа — новая строка
	monitorHeartbeat := 12 * time.Second

	for {
		select {
		case <-timeout.C:
			snap, _ := getCurveSnapshot(bcAddr)
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

			snap, err := getCurveSnapshot(bcAddr)
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
			if time.Since(opened) >= SCRATCH_AFTER && mult < SCRATCH_IF_BELOW {
				w.closePos(mint, "СКРЕТЧ (слабый импульс)", price)
				return
			}
			if time.Since(opened) >= NO_IMPULSE_AFTER && peak < entry*NO_IMPULSE_NEED {
				w.closePos(mint, "НЕТ ИМПУЛЬСА", price)
				return
			}
			if price <= entry*STOP_LOSS {
				w.closePos(mint, "СТОП -12%", price)
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

	fmt.Println(bold("╔══════════════════════════════════════════════════════════╗"))
	fmt.Println(bold("║   PUMP.FUN LIVE PAPER TRADING                            ║"))
	fmt.Println(bold("║   Реальные токены · Реальные цены · Виртуальные деньги  ║"))
	fmt.Println(bold("╚══════════════════════════════════════════════════════════╝"))
	fmt.Println(green("✓ Helius WebSocket — реальные транзакции Pump.fun"))
	fmt.Println(green("✓ Bonding Curve Price — прямо из on-chain данных"))

	refreshSolPriceUSD()
	sp := getSolUSD()
	fmt.Printf("%s SOL/USD: $%.2f (Jupiter Price API, автообновление)\n", green("✓"), sp)
	go func() {
		t := time.NewTicker(90 * time.Second)
		for range t.C {
			refreshSolPriceUSD()
		}
	}()

	fmt.Printf("\n%s Режим: %s | кривая: %.1f%%–%.1f%% | min SOL: %.2f | velocity: %v (Δ≥%.2f%% прогресса или +%.3f SOL) | ончейн-фильтры\n",
		bold("▶"), cyan("SNIPER"), SNIPER_CURVE_MIN*100, SNIPER_CURVE_MAX*100, MIN_REAL_SOL,
		VELOCITY_PAUSE, VELOCITY_MIN_DPROGRESS*100, VELOCITY_MIN_DREALSOL)
	fmt.Printf("%s Баланс: %s | Ставка: $%.2f | Макс позиций: %d\n",
		bold("▶"), green(fmt.Sprintf("$%.2f", PAPER_BALANCE)), FIXED_BET, MAX_POSITIONS)
	fmt.Printf("%s DexScreener — по mint показывается вся история торгов; даты на оси — календарь свечей, не дата «создания ссылки». Свежесть листинга смотри в строке ⏱ (время блока create-тx).\n",
		gray("ⓘ"))
	fmt.Printf("%s Быстрая проверка кошелька: %s\n\n",
		gray("ⓘ"), cyan("go run live_paper_trading.go -selftest"))

	wallet := newWallet()
	tokenCh := make(chan NewToken, 200)
	var seen sync.Map
	sem := make(chan struct{}, 3) // макс 3 параллельных запроса

	go listenWS(tokenCh)

	go func() {
		for tok := range tokenCh {
			tok := tok
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()

				mint, creator, createAt := parseCreateTx(tok.Sig)
				if mint == "" {
					logRejectLine("no_mint", "?", "", "нет mint в create tx")
					return
				}
				if !strings.HasSuffix(mint, "pump") {
					sym := "$" + short(mint)
					logRejectLine("not_pump", sym, mint, "mint не …pump — не pump.fun токен")
					return
				}
				bc, err := pumpBondingCurvePDA(mint)
				if err != nil {
					logRejectLine("pda_err", "$"+short(mint), mint, err.Error())
					return
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

				snap0, err := getCurveSnapshotWithRetry(bc)
				if err != nil || snap0 == nil || snap0.PriceUSD <= 0 {
					logRejectLine("no_price", sym, mint, "нет цены в bonding curve")
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
				snap1, vOK, vDetail := curveVelocityOK(bc, snap0)
				if !vOK {
					logRejectLine("velocity", sym, mint, "velocity: "+vDetail)
					return
				}

				atomic.AddInt64(&funnelPassVel, 1)
				ok, scamMeta := antiScamCheck(mint, bc, creator)
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
				fmt.Printf("\n%s %-18s | $%.10f | curve %.1f%% | SOL %.2f | %s | %s → ВХОД\n",
					green("✓"), sym, price, progress*100, realSol, gray(vDetail), gray(scamMeta))
				fmt.Printf("   %s %s\n", gray("⏱"), cyan(ageInfo))
				fmt.Printf("   %s %s\n", gray("DEX"), cyan(dexScreenerURL(mint)))
				opened := wallet.open(tok, sym, price)
				consoleMu.Unlock()
				if opened {
					atomic.AddInt64(&funnelOpenOK, 1)
					go monitor(wallet, mint, bc, sym)
				} else {
					atomic.AddInt64(&funnelOpenFail, 1)
					consoleMu.Lock()
					fmt.Println(yellow("⚠ ВХОД отклонён open(): мало баланса или лимит позиций"))
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
	fmt.Println(bold("\n✓ Реальных денег не потрачено."))
}
