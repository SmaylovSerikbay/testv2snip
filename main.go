package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// ═══════════════════════════════════════════════════════════════════════════════
//  PUMP.FUN CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════

var (
	PumpProgram   = solana.MustPublicKeyFromBase58("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
	PumpGlobal    = solana.MustPublicKeyFromBase58("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf")
	PumpFee       = solana.MustPublicKeyFromBase58("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV")
	PumpEventAuth solana.PublicKey
	CUBudget      = solana.MustPublicKeyFromBase58("ComputeBudget111111111111111111111111111111")
	Token2022     = solana.MustPublicKeyFromBase58("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
	FeeProgram    = solana.MustPublicKeyFromBase58("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
	FeeConfig     solana.PublicKey
)

var (
	buyDisc   [8]byte
	sellDisc  [8]byte
	tradeDisc [8]byte
)

func init() {
	h := sha256.Sum256([]byte("global:buy"))
	copy(buyDisc[:], h[:8])
	h = sha256.Sum256([]byte("global:sell"))
	copy(sellDisc[:], h[:8])
	h = sha256.Sum256([]byte("event:TradeEvent"))
	copy(tradeDisc[:], h[:8])
	ea, _, _ := solana.FindProgramAddress([][]byte{[]byte("__event_authority")}, PumpProgram)
	PumpEventAuth = ea
	fc, _, _ := solana.FindProgramAddress([][]byte{[]byte("fee_config"), PumpProgram.Bytes()}, FeeProgram)
	FeeConfig = fc
}

// ═══════════════════════════════════════════════════════════════════════════════
//  TYPES
// ═══════════════════════════════════════════════════════════════════════════════

type Config struct {
	Key          solana.PrivateKey
	RPC          string
	WSS          string
	BuyLamp      uint64
	Slip         uint64
	PrioLamp     uint64
	TP           float64
	SL           float64
	TimeKillSec  int
	TimeKillMin  float64
	MaxTargets   int
	MaxPositions int
	ScrapeIvl    time.Duration
	MonitorMs    int
	Live         bool
}

type Signal struct {
	Mint   solana.PublicKey
	Wallet string
}

type Position struct {
	Mint    solana.PublicKey
	Tokens  uint64
	Spent   uint64 // lamports
	Entry   time.Time
	Wallet  string
	HiPnl   float64 // peak PnL for trailing stop
	TokProg solana.PublicKey
}

type BondingCurve struct {
	VTK, VSR, RTK, RSR, Supply uint64
	Done                       bool
	Creator                    solana.PublicKey
}

type TradeEvent struct {
	Mint      solana.PublicKey
	Sol       uint64
	Tokens    uint64
	IsBuy     bool
	User      solana.PublicKey
	Timestamp int64
}

type genericIx struct {
	pid  solana.PublicKey
	accs []*solana.AccountMeta
	dat  []byte
}

func (g *genericIx) ProgramID() solana.PublicKey     { return g.pid }
func (g *genericIx) Accounts() []*solana.AccountMeta { return g.accs }
func (g *genericIx) Data() ([]byte, error)           { return g.dat, nil }

// ═══════════════════════════════════════════════════════════════════════════════
//  GLOBAL STATE
// ═══════════════════════════════════════════════════════════════════════════════

var (
	cfg   Config
	rpcCl *rpc.Client

	// Token-bucket rate limiter: allows bursts, ~8 req/s sustained
	rpcBucket chan struct{}

	signalCh   = make(chan Signal, 64)
	wsReconnCh = make(chan struct{}, 1)

	tgtMu   sync.RWMutex
	targets = map[string]time.Time{} // wallet → lastSeen

	posMu sync.RWMutex
	pos   = map[string]*Position{} // mint_str → position

	// Dedup: prevents concurrent buys of the same mint
	buyingMu sync.Mutex
	buying   = map[string]bool{}

	// Per-target consecutive loss tracking
	tgtLossMu sync.Mutex
	tgtLosses = map[string]int{}

	bhMu    sync.RWMutex
	bhHash  solana.Hash
	bhStale int64 // unix seconds when fetched

	statBuys  atomic.Int64
	statSells atomic.Int64
)

func rpcWait() { <-rpcBucket }

// ═══════════════════════════════════════════════════════════════════════════════
//  MAIN
// ═══════════════════════════════════════════════════════════════════════════════

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	_ = godotenv.Load()
	cfg = loadCfg()
	rpcCl = rpc.New(cfg.RPC)

	// Token bucket: 10 burst, refill at ~8/s
	rpcBucket = make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		rpcBucket <- struct{}{}
	}
	go func() {
		t := time.NewTicker(125 * time.Millisecond)
		for range t.C {
			select {
			case rpcBucket <- struct{}{}:
			default:
			}
		}
	}()

	// Read fee_recipient from on-chain Global account
	{
		rpcWait()
		gInfo, gErr := rpcCl.GetAccountInfoWithOpts(context.Background(), PumpGlobal,
			&rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64})
		if gErr == nil && gInfo != nil && gInfo.Value != nil {
			gd := gInfo.Value.Data.GetBinary()
			if len(gd) >= 73 {
				var fr solana.PublicKey
				copy(fr[:], gd[41:73])
				if !fr.IsZero() {
					PumpFee = fr
					log.Printf("[INIT] Fee recipient (on-chain): %s", PumpFee)
				}
			}
		}
	}

	hasKey := len(cfg.Key) == 64
	mode := "PAPER"
	if cfg.Live {
		mode = "LIVE"
	}
	if hasKey {
		log.Printf("[INIT] Кошелёк: %s | Режим: %s", cfg.Key.PublicKey(), mode)
	} else {
		log.Printf("[INIT] Ключ не задан — режим наблюдения | %s", mode)
	}
	log.Printf("[INIT] Ставка %.4f SOL | TP +%.0f%% SL -%.0f%% | TimeKill %ds<%+.0f%%",
		float64(cfg.BuyLamp)/1e9, cfg.TP*100, cfg.SL*100, cfg.TimeKillSec, cfg.TimeKillMin*100)
	log.Printf("[INIT] MaxTargets %d | MaxPos %d | Scrape %v | Monitor %dms",
		cfg.MaxTargets, cfg.MaxPositions, cfg.ScrapeIvl, cfg.MonitorMs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if hasKey {
		rpcWait()
		b, err := rpcCl.GetBalance(ctx, cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
		if err == nil {
			log.Printf("[INIT] Баланс: %.6f SOL", float64(b.Value)/1e9)
		}
	}

	var wg sync.WaitGroup
	launch := func(f func(context.Context)) { wg.Add(1); go func() { defer wg.Done(); f(ctx) }() }

	launch(runBlockhashLoop)
	launch(runScraper)
	launch(runWSListener)
	launch(runExecutor)
	launch(runMonitor)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Printf("[EXIT] Buys=%d Sells=%d", statBuys.Load(), statSells.Load())
	cancel()
	wg.Wait()
}

func loadCfg() Config {
	c := Config{
		BuyLamp:      5_000_000,
		Slip:         2000,
		PrioLamp:     100_000,
		TP:           0.40,
		SL:           0.20,
		TimeKillSec:  45,
		TimeKillMin:  0.05,
		MaxTargets:   20,
		MaxPositions: 5,
		ScrapeIvl:    3 * time.Minute,
		MonitorMs:    1000,
	}
	if v := os.Getenv("HELIUS_API_KEY"); v != "" {
		c.RPC = "https://mainnet.helius-rpc.com/?api-key=" + v
		c.WSS = "wss://mainnet.helius-rpc.com/?api-key=" + v
	} else {
		c.RPC = ev("RPC_URL", "https://api.mainnet-beta.solana.com")
		c.WSS = ev("WSS_URL", "wss://api.mainnet-beta.solana.com")
	}
	if pk := os.Getenv("SOLANA_PRIVATE_KEY"); pk != "" {
		k, err := solana.PrivateKeyFromBase58(pk)
		if err != nil {
			log.Fatalf("[CFG] Bad key: %v", err)
		}
		c.Key = k
	}
	c.Live = ev("LIVE_TRADING", "0") == "1"
	if v := os.Getenv("BUY_AMOUNT_SOL"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.BuyLamp = uint64(f * 1e9)
		}
	}
	c.Slip = evU("SLIPPAGE_BPS", c.Slip)
	c.PrioLamp = evU("PRIORITY_FEE_LAMPORTS", c.PrioLamp)
	c.TP = evF("TAKE_PROFIT_PCT", c.TP*100) / 100
	c.SL = evF("STOP_LOSS_PCT", c.SL*100) / 100
	c.TimeKillSec = int(evU("TIMEKILL_SEC", uint64(c.TimeKillSec)))
	c.TimeKillMin = evF("TIMEKILL_MIN_PCT", c.TimeKillMin*100) / 100
	c.MaxTargets = int(evU("MAX_TARGETS", uint64(c.MaxTargets)))
	c.MaxPositions = int(evU("MAX_POSITIONS", uint64(c.MaxPositions)))
	if m := evU("SCRAPE_INTERVAL_MIN", 0); m > 0 {
		c.ScrapeIvl = time.Duration(m) * time.Minute
	}
	c.MonitorMs = int(evU("MONITOR_MS", uint64(c.MonitorMs)))
	return c
}

func ev(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func evU(k string, d uint64) uint64 {
	v, err := strconv.ParseUint(os.Getenv(k), 10, 64)
	if err != nil {
		return d
	}
	return v
}
func evF(k string, d float64) float64 {
	v, err := strconv.ParseFloat(os.Getenv(k), 64)
	if err != nil {
		return d
	}
	return v
}

// ═══════════════════════════════════════════════════════════════════════════════
//  BLOCKHASH CACHE — обновляется каждые 20с, hot-path не ждёт RPC
// ═══════════════════════════════════════════════════════════════════════════════

func runBlockhashLoop(ctx context.Context) {
	refresh := func() {
		rpcWait()
		r, err := rpcCl.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
		if err != nil {
			return
		}
		bhMu.Lock()
		bhHash = r.Value.Blockhash
		bhStale = time.Now().Unix()
		bhMu.Unlock()
	}
	refresh()
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			refresh()
		}
	}
}

func cachedBH(ctx context.Context) solana.Hash {
	bhMu.RLock()
	h := bhHash
	age := time.Now().Unix() - bhStale
	bhMu.RUnlock()
	if age < 40 {
		return h
	}
	rpcWait()
	r, err := rpcCl.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return h
	}
	bhMu.Lock()
	bhHash = r.Value.Blockhash
	bhStale = time.Now().Unix()
	bhMu.Unlock()
	return r.Value.Blockhash
}

// ═══════════════════════════════════════════════════════════════════════════════
//  SCRAPER — 3 источника: RPC live + DexScreener profiles + DexScreener boosts
// ═══════════════════════════════════════════════════════════════════════════════

func runScraper(ctx context.Context) {
	log.Printf("[SCRAPE] Запуск. Интервал %v", cfg.ScrapeIvl)
	scrape(ctx)

	fast := time.NewTicker(45 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case <-ctx.Done():
			fast.Stop()
			return
		case <-fast.C:
			scrape(ctx)
		}
	}
	fast.Stop()

	t := time.NewTicker(cfg.ScrapeIvl)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			scrape(ctx)
		}
	}
}

func scrape(ctx context.Context) {
	start := time.Now()
	log.Println("[SCRAPE] Сканируем активных покупателей...")

	myKey := ""
	if len(cfg.Key) == 64 {
		myKey = cfg.Key.PublicKey().String()
	}
	freq := map[string]int{}

	scrapeRPCLive(ctx, freq, myKey)
	scrapeDexEndpoint(ctx, freq, myKey,
		"https://api.dexscreener.com/token-profiles/latest/v1", 15, "profiles")
	scrapeDexEndpoint(ctx, freq, myKey,
		"https://api.dexscreener.com/token-boosts/latest/v1", 10, "boosts")

	now := time.Now()
	tgtMu.Lock()
	added := 0
	for w, cnt := range freq {
		if cnt < 1 {
			continue
		}
		if _, ok := targets[w]; !ok && len(targets) < cfg.MaxTargets {
			targets[w] = now
			added++
			log.Printf("[SCRAPE] +Цель: %s (%d покупок)", short(w), cnt)
		} else if ok {
			targets[w] = now
		}
	}
	stale := 0
	for w, seen := range targets {
		if now.Sub(seen) > 15*time.Minute {
			delete(targets, w)
			stale++
		}
	}
	total := len(targets)
	tgtMu.Unlock()

	if stale > 0 || added > 0 {
		notifyWSReconn()
	}
	log.Printf("[SCRAPE] %v | unique_wallets=%d +%d -%d = %d целей",
		time.Since(start).Round(time.Millisecond), len(freq), added, stale, total)
}

func scrapeRPCLive(ctx context.Context, freq map[string]int, myKey string) {
	for _, addr := range []solana.PublicKey{PumpEventAuth, PumpFee} {
		rpcWait()
		lim := 50
		sigs, err := rpcCl.GetSignaturesForAddressWithOpts(ctx, addr,
			&rpc.GetSignaturesForAddressOpts{Limit: &lim})
		if err != nil {
			log.Printf("[SCRAPE] RPC %s: err=%v", short(addr.String()), err)
			continue
		}
		if len(sigs) == 0 {
			log.Printf("[SCRAPE] RPC %s: 0 сигнатур", short(addr.String()))
			continue
		}

		ok, errs, buys := 0, 0, 0
		for _, s := range sigs {
			if s.Err != nil {
				errs++
				continue
			}
			if ok >= 15 {
				break
			}
			ok++
			ev := parseTxEvent(ctx, s.Signature)
			if ev == nil || !ev.IsBuy {
				continue
			}
			buys++
			w := ev.User.String()
			if w != myKey {
				freq[w]++
			}
		}
		log.Printf("[SCRAPE] RPC %s: total=%d ok=%d err=%d buys=%d",
			short(addr.String()), len(sigs), ok, errs, buys)
		if buys > 0 {
			return
		}
	}
}

func scrapeDexEndpoint(ctx context.Context, freq map[string]int, myKey, url string, maxCheck int, tag string) {
	body, err := httpGet(url)
	if err != nil {
		log.Printf("[SCRAPE] Dex/%s: %v", tag, err)
		return
	}
	var items []struct {
		ChainID string `json:"chainId"`
		Token   string `json:"tokenAddress"`
	}
	if json.Unmarshal(body, &items) != nil {
		return
	}

	checked, totalBuyers := 0, 0
	for _, it := range items {
		if it.ChainID != "solana" || it.Token == "" || checked >= maxCheck {
			continue
		}
		mint, err := solana.PublicKeyFromBase58(it.Token)
		if err != nil {
			continue
		}
		bc, _, _ := solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, PumpProgram)

		rpcWait()
		lim := 20
		sigs, err := rpcCl.GetSignaturesForAddressWithOpts(ctx, bc, &rpc.GetSignaturesForAddressOpts{Limit: &lim})
		if err != nil || len(sigs) == 0 {
			continue
		}
		checked++

		parsed := 0
		for _, s := range sigs {
			if s.Err != nil || parsed >= 5 {
				continue
			}
			ev := parseTxEvent(ctx, s.Signature)
			if ev == nil {
				continue
			}
			parsed++
			if !ev.IsBuy {
				continue
			}
			totalBuyers++
			w := ev.User.String()
			if w != myKey {
				freq[w]++
			}
		}
	}
	if checked > 0 || totalBuyers > 0 {
		log.Printf("[SCRAPE] Dex/%s: %d pump.fun токенов, %d покупателей", tag, checked, totalBuyers)
	}
}

func notifyWSReconn() {
	select {
	case wsReconnCh <- struct{}{}:
	default:
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  WS LISTENER — logsSubscribe на каждый target, сигналы → signalCh
// ═══════════════════════════════════════════════════════════════════════════════

type wsMsg struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int            `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Params  *struct {
		Sub    int             `json:"subscription"`
		Result json.RawMessage `json:"result"`
	} `json:"params,omitempty"`
}

func runWSListener(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := wsLoop(ctx); err != nil {
			log.Printf("[WS] Ошибка: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func wsLoop(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.WSS, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	subToWallet := map[int]string{}
	reqToWallet := map[int]string{}
	var writeMu sync.Mutex

	tgtMu.RLock()
	id := 0
	for addr := range targets {
		id++
		reqToWallet[id] = addr
		_ = conn.WriteJSON(map[string]any{
			"jsonrpc": "2.0", "id": id,
			"method": "logsSubscribe",
			"params": []any{
				map[string]any{"mentions": []string{addr}},
				map[string]any{"commitment": "confirmed"},
			},
		})
	}
	count := len(targets)
	tgtMu.RUnlock()
	log.Printf("[WS] Подключён, подписки: %d", count)

	// Ping + reconnect trigger
	go func() {
		ping := time.NewTicker(30 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				conn.Close()
				return
			case <-ping.C:
				writeMu.Lock()
				_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
				writeMu.Unlock()
			case <-wsReconnCh:
				log.Println("[WS] Targets изменились — реконнект")
				conn.Close()
				return
			}
		}
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var m wsMsg
		if json.Unmarshal(raw, &m) != nil {
			continue
		}

		// Subscription confirmation
		if m.ID != nil && m.Result != nil {
			var subID int
			if json.Unmarshal(m.Result, &subID) == nil && subID > 0 {
				if w, ok := reqToWallet[*m.ID]; ok {
					subToWallet[subID] = w
				}
			}
			continue
		}

		// Log notification
		if m.Method != "logsNotification" || m.Params == nil {
			continue
		}
		wallet := subToWallet[m.Params.Sub]
		if wallet == "" {
			continue
		}

		// Update lastSeen
		tgtMu.Lock()
		targets[wallet] = time.Now()
		tgtMu.Unlock()

		var lr struct {
			Value struct {
				Signature string   `json:"signature"`
				Err       any      `json:"err"`
				Logs      []string `json:"logs"`
			} `json:"value"`
		}
		if json.Unmarshal(m.Params.Result, &lr) != nil || lr.Value.Err != nil {
			continue
		}

		// HOT PATH — парсим inline, не блокируя reader
		go parseBuySignal(wallet, lr.Value.Logs)
	}
}

func parseBuySignal(wallet string, logs []string) {
	hasPump, isBuy := false, false
	var evData []byte

	for _, l := range logs {
		if strings.Contains(l, PumpProgram.String()) && strings.Contains(l, "invoke") {
			hasPump = true
		}
		if l == "Program log: Instruction: Buy" {
			isBuy = true
		}
		if strings.HasPrefix(l, "Program data: ") {
			raw, _ := base64.StdEncoding.DecodeString(l[14:])
			if len(raw) >= 8 && bytes.Equal(raw[:8], tradeDisc[:]) {
				evData = raw
			}
		}
	}
	if !hasPump || !isBuy || evData == nil {
		return
	}
	ev := parseTE(evData)
	if ev == nil || ev.User.String() != wallet {
		return
	}

	select {
	case signalCh <- Signal{Mint: ev.Mint, Wallet: wallet}:
	default:
		log.Println("[WS] signalCh full — пропуск")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  EXECUTOR — читает signalCh, мгновенно покупает
// ═══════════════════════════════════════════════════════════════════════════════

func runExecutor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-signalCh:
			go doBuy(ctx, sig)
		}
	}
}

func doBuy(ctx context.Context, sig Signal) {
	mint := sig.Mint.String()

	// ── DEDUP: atomic claim на минт (блокирует повторные горутины) ──
	buyingMu.Lock()
	if buying[mint] {
		buyingMu.Unlock()
		return
	}
	buying[mint] = true
	buyingMu.Unlock()
	defer func() { buyingMu.Lock(); delete(buying, mint); buyingMu.Unlock() }()

	// ── Быстрые проверки без RPC ──
	posMu.RLock()
	_, dup := pos[mint]
	cnt := len(pos)
	posMu.RUnlock()
	if dup {
		return
	}
	if cnt >= cfg.MaxPositions {
		log.Printf("[BUY] Слотов нет (%d/%d) — пропуск %s", cnt, cfg.MaxPositions, short(mint))
		return
	}
	if len(cfg.Key) != 64 {
		return
	}

	log.Printf("[BUY] Сигнал: %s от %s", short(mint), short(sig.Wallet))

	tokProg := getMintTokenProgram(ctx, sig.Mint)
	if tokProg == Token2022 {
		log.Printf("[BUY] Token-2022: %s", short(mint))
	}

	state, bc, err := readBC(ctx, sig.Mint)
	if err != nil || state.Done {
		log.Printf("[BUY] BC недоступна или завершена: %s", short(mint))
		return
	}

	if state.VSR < 1_500_000_000 {
		log.Printf("[BUY] Мало ликвидности: %.2f SOL — пропуск %s", float64(state.VSR)/1e9, short(mint))
		return
	}

	fee := cfg.BuyLamp / 100
	tokOut := calcTokOut(state.VTK, state.VSR, cfg.BuyLamp-fee)
	if tokOut == 0 {
		return
	}
	maxSol := cfg.BuyLamp * (10000 + cfg.Slip) / 10000

	user := cfg.Key.PublicKey()
	assocBC := findATA(bc, sig.Mint, tokProg)
	assocUser := findATA(user, sig.Mint, tokProg)

	creatorVault, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("creator-vault"), state.Creator.Bytes()}, PumpProgram)
	globalVolAcc, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("global_volume_accumulator")}, PumpProgram)
	userVolAcc, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("user_volume_accumulator"), user.Bytes()}, PumpProgram)

	data := make([]byte, 25)
	copy(data[:8], buyDisc[:])
	binary.LittleEndian.PutUint64(data[8:], tokOut)
	binary.LittleEndian.PutUint64(data[16:], maxSol)
	data[24] = 0 // track_volume = None

	cuLimit := uint32(250_000)
	ixs := []solana.Instruction{
		cuLimitIx(cuLimit),
		cuPriceIx(cfg.PrioLamp * 1_000_000 / uint64(cuLimit)),
		ataIx(user, user, sig.Mint, tokProg),
		&genericIx{pid: PumpProgram, dat: data, accs: []*solana.AccountMeta{
			{PublicKey: PumpGlobal},
			{PublicKey: PumpFee, IsWritable: true},
			{PublicKey: sig.Mint},
			{PublicKey: bc, IsWritable: true},
			{PublicKey: assocBC, IsWritable: true},
			{PublicKey: assocUser, IsWritable: true},
			{PublicKey: user, IsSigner: true, IsWritable: true},
			{PublicKey: solana.SystemProgramID},
			{PublicKey: tokProg},
			{PublicKey: creatorVault, IsWritable: true},
			{PublicKey: PumpEventAuth},
			{PublicKey: PumpProgram},
			{PublicKey: globalVolAcc},
			{PublicKey: userVolAcc, IsWritable: true},
			{PublicKey: FeeConfig},
			{PublicKey: FeeProgram},
		}},
	}

	bh := cachedBH(ctx)
	tx, err := solana.NewTransaction(ixs, bh, solana.TransactionPayer(user))
	if err != nil {
		log.Printf("[BUY] Build: %v", err)
		return
	}
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key == user {
			k := cfg.Key
			return &k
		}
		return nil
	})
	if err != nil {
		log.Printf("[BUY] Sign: %v", err)
		return
	}

	if !cfg.Live {
		log.Printf("[BUY][PAPER] %s | %.4f SOL → %d tok", short(mint), float64(cfg.BuyLamp)/1e9, tokOut)
		addPos(sig.Mint, tokOut, cfg.BuyLamp, sig.Wallet, tokProg)
		statBuys.Add(1)
		return
	}

	rpcWait()
	txSig, err := rpcCl.SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{SkipPreflight: false})
	if err != nil {
		errMsg := fmt.Sprintf("%v", err)
		if len(errMsg) > 200 {
			errMsg = errMsg[:200] + "…"
		}
		log.Printf("[BUY] ✗ %s | %s", errMsg, short(mint))
		return
	}
	log.Printf("[BUY] TX: %s | %s | %.4f SOL", txSig.String()[:12], short(mint), float64(cfg.BuyLamp)/1e9)
	addPos(sig.Mint, tokOut, cfg.BuyLamp, sig.Wallet, tokProg)
	statBuys.Add(1)

	go confirmTx(ctx, txSig, "BUY", mint, func() {
		removePos(mint)
		log.Printf("[BUY] Удалена фантомная позиция: %s", short(mint))
	})
}

func addPos(mint solana.PublicKey, tok, spent uint64, wallet string, tokProg solana.PublicKey) {
	posMu.Lock()
	pos[mint.String()] = &Position{Mint: mint, Tokens: tok, Spent: spent, Entry: time.Now(), Wallet: wallet, TokProg: tokProg}
	posMu.Unlock()
}

func removePos(mint string) {
	posMu.Lock()
	delete(pos, mint)
	posMu.Unlock()
}

func confirmTx(ctx context.Context, txSig solana.Signature, tag, mintStr string, onFail func()) {
	time.Sleep(4 * time.Second)

	for attempt := 0; attempt < 12; attempt++ {
		rpcWait()
		statuses, err := rpcCl.GetSignatureStatuses(ctx, false, txSig)
		if err != nil || len(statuses.Value) == 0 || statuses.Value[0] == nil {
			time.Sleep(2 * time.Second)
			continue
		}
		st := statuses.Value[0]
		if st.Err != nil {
			log.Printf("[%s] ✗ FAIL %s | %s | err: %v", tag, txSig.String()[:12], short(mintStr), st.Err)
			if onFail != nil {
				onFail()
			}
			return
		}
		if st.ConfirmationStatus == rpc.ConfirmationStatusConfirmed ||
			st.ConfirmationStatus == rpc.ConfirmationStatusFinalized {
			log.Printf("[%s] ✓ OK %s | %s", tag, txSig.String()[:12], short(mintStr))
			return
		}
		time.Sleep(2 * time.Second)
	}
	log.Printf("[%s] ⚠ Не подтверждена за 28с: %s | %s", tag, txSig.String()[:12], short(mintStr))
	if onFail != nil {
		onFail()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  MONITOR — TP / SL / TRAILING / TIME-KILL (каждую секунду)
// ═══════════════════════════════════════════════════════════════════════════════

func runMonitor(ctx context.Context) {
	t := time.NewTicker(time.Duration(cfg.MonitorMs) * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			checkAll(ctx)
		}
	}
}

func checkAll(ctx context.Context) {
	posMu.Lock()
	defer posMu.Unlock()

	for mint, p := range pos {
		state, _, err := readBC(ctx, p.Mint)
		if err != nil {
			continue
		}
		if state.Done {
			log.Printf("[MON] %s graduated — удаляем", short(mint))
			delete(pos, mint)
			continue
		}

		solOut := calcSolOut(state.VTK, state.VSR, p.Tokens)
		solNet := solOut - solOut/100
		pnl := float64(solNet)/float64(p.Spent) - 1.0
		age := time.Since(p.Entry)

		// Track peak PnL for trailing stop
		if pnl > p.HiPnl {
			p.HiPnl = pnl
		}

		var reason string
		switch {
		case pnl >= cfg.TP:
			reason = fmt.Sprintf("TP +%.0f%%", pnl*100)
		case pnl <= -cfg.SL:
			reason = fmt.Sprintf("SL %.0f%%", pnl*100)
		// Trailing: был выше +15%, упал на 10% от пика → фиксируем
		case p.HiPnl >= 0.15 && pnl < p.HiPnl-0.10:
			reason = fmt.Sprintf("TRAIL peak+%.0f%% now%+.0f%%", p.HiPnl*100, pnl*100)
		case age >= time.Duration(cfg.TimeKillSec)*time.Second && pnl < cfg.TimeKillMin:
			reason = fmt.Sprintf("TIMEKILL %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		}
		if reason == "" {
			continue
		}

		profitable := pnl > 0
		ok := doSell(ctx, p, reason)
		if ok {
			delete(pos, mint)
			trackTargetResult(p.Wallet, profitable)
		}
	}
}

func trackTargetResult(wallet string, win bool) {
	tgtLossMu.Lock()
	if win {
		tgtLosses[wallet] = 0
	} else {
		tgtLosses[wallet]++
		if tgtLosses[wallet] >= 3 {
			tgtMu.Lock()
			delete(targets, wallet)
			tgtMu.Unlock()
			log.Printf("[MON] Удалена цель %s (3 убытка подряд)", short(wallet))
			notifyWSReconn()
		}
	}
	tgtLossMu.Unlock()
}

func doSell(ctx context.Context, p *Position, reason string) bool {
	if len(cfg.Key) != 64 {
		return false
	}

	mintStr := p.Mint.String()
	state, bc, err := readBC(ctx, p.Mint)
	if err != nil || state.Done {
		log.Printf("[SELL] BC: %v | %s", err, short(mintStr))
		return false
	}

	solOut := calcSolOut(state.VTK, state.VSR, p.Tokens)
	solNet := solOut - solOut/100
	minSol := solNet * (10000 - cfg.Slip) / 10000
	pnl := float64(solNet)/float64(p.Spent)*100 - 100

	user := cfg.Key.PublicKey()
	tokProg := p.TokProg
	if tokProg.IsZero() {
		tokProg = solana.TokenProgramID
	}
	assocBC := findATA(bc, p.Mint, tokProg)
	assocUser := findATA(user, p.Mint, tokProg)

	creatorVault, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("creator-vault"), state.Creator.Bytes()}, PumpProgram)

	data := make([]byte, 24)
	copy(data[:8], sellDisc[:])
	binary.LittleEndian.PutUint64(data[8:], p.Tokens)
	binary.LittleEndian.PutUint64(data[16:], minSol)

	cuLimit := uint32(250_000)
	ixs := []solana.Instruction{
		cuLimitIx(cuLimit),
		cuPriceIx(cfg.PrioLamp * 1_000_000 / uint64(cuLimit)),
		&genericIx{pid: PumpProgram, dat: data, accs: []*solana.AccountMeta{
			{PublicKey: PumpGlobal},
			{PublicKey: PumpFee, IsWritable: true},
			{PublicKey: p.Mint},
			{PublicKey: bc, IsWritable: true},
			{PublicKey: assocBC, IsWritable: true},
			{PublicKey: assocUser, IsWritable: true},
			{PublicKey: user, IsSigner: true, IsWritable: true},
			{PublicKey: solana.SystemProgramID},
			{PublicKey: creatorVault, IsWritable: true},
			{PublicKey: tokProg},
			{PublicKey: PumpEventAuth},
			{PublicKey: PumpProgram},
			{PublicKey: FeeConfig},
			{PublicKey: FeeProgram},
		}},
	}

	if !cfg.Live {
		log.Printf("[SELL][PAPER] %s | %s | PnL %+.1f%% | %.6f SOL",
			reason, short(mintStr), pnl, float64(solNet)/1e9)
		statSells.Add(1)
		return true
	}

	bh := cachedBH(ctx)
	tx, err := solana.NewTransaction(ixs, bh, solana.TransactionPayer(user))
	if err != nil {
		log.Printf("[SELL] Build: %v | %s", err, short(mintStr))
		return false
	}
	tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key == user {
			k := cfg.Key
			return &k
		}
		return nil
	})

	rpcWait()
	txSig, err := rpcCl.SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{SkipPreflight: false})
	if err != nil {
		errMsg := fmt.Sprintf("%v", err)
		if len(errMsg) > 200 {
			errMsg = errMsg[:200] + "…"
		}
		log.Printf("[SELL] ✗ %s | %s", errMsg, short(mintStr))
		return false
	}
	log.Printf("[SELL] %s | TX %s | %s | PnL %+.1f%%", reason, txSig.String()[:12], short(mintStr), pnl)
	statSells.Add(1)

	savedPos := *p
	go confirmTx(ctx, txSig, "SELL", mintStr, func() {
		addPos(savedPos.Mint, savedPos.Tokens, savedPos.Spent, savedPos.Wallet, savedPos.TokProg)
		log.Printf("[SELL] Позиция восстановлена (TX fail): %s", short(mintStr))
	})
	return true
}

// ═══════════════════════════════════════════════════════════════════════════════
//  PUMP.FUN HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

func readBC(ctx context.Context, mint solana.PublicKey) (*BondingCurve, solana.PublicKey, error) {
	bc, _, _ := solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, PumpProgram)
	rpcWait()
	info, err := rpcCl.GetAccountInfoWithOpts(ctx, bc, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64})
	if err != nil {
		return nil, bc, err
	}
	if info == nil || info.Value == nil {
		return nil, bc, fmt.Errorf("nil")
	}
	d := info.Value.Data.GetBinary()
	if len(d) < 49 {
		return nil, bc, fmt.Errorf("short")
	}
	s := &BondingCurve{
		VTK:    binary.LittleEndian.Uint64(d[8:]),
		VSR:    binary.LittleEndian.Uint64(d[16:]),
		RTK:    binary.LittleEndian.Uint64(d[24:]),
		RSR:    binary.LittleEndian.Uint64(d[32:]),
		Supply: binary.LittleEndian.Uint64(d[40:]),
		Done:   d[48] != 0,
	}
	if len(d) >= 81 {
		copy(s.Creator[:], d[49:81])
	}
	return s, bc, nil
}

func parseTxEvent(ctx context.Context, sig solana.Signature) *TradeEvent {
	rpcWait()
	v := uint64(0)
	out, err := rpcCl.GetTransaction(ctx, sig, &rpc.GetTransactionOpts{MaxSupportedTransactionVersion: &v})
	if err != nil || out == nil || out.Meta == nil {
		return nil
	}
	for _, l := range out.Meta.LogMessages {
		if !strings.HasPrefix(l, "Program data: ") {
			continue
		}
		raw, _ := base64.StdEncoding.DecodeString(l[14:])
		if len(raw) >= 8 && bytes.Equal(raw[:8], tradeDisc[:]) {
			if ev := parseTE(raw); ev != nil {
				return ev
			}
		}
	}
	return nil
}

func parseTE(d []byte) *TradeEvent {
	need := 8 + 32 + 8 + 8 + 1 + 32 + 8
	if len(d) < need {
		return nil
	}
	o := 8
	var e TradeEvent
	copy(e.Mint[:], d[o:o+32]); o += 32
	e.Sol = binary.LittleEndian.Uint64(d[o:]); o += 8
	e.Tokens = binary.LittleEndian.Uint64(d[o:]); o += 8
	e.IsBuy = d[o] != 0; o++
	copy(e.User[:], d[o:o+32]); o += 32
	e.Timestamp = int64(binary.LittleEndian.Uint64(d[o:]))
	return &e
}

func calcTokOut(vtk, vsr, sol uint64) uint64 {
	if vsr == 0 || vtk == 0 {
		return 0
	}
	return new(big.Int).Div(new(big.Int).Mul(big.NewInt(0).SetUint64(vtk), big.NewInt(0).SetUint64(sol)),
		new(big.Int).Add(big.NewInt(0).SetUint64(vsr), big.NewInt(0).SetUint64(sol))).Uint64()
}

func calcSolOut(vtk, vsr, tok uint64) uint64 {
	if vtk == 0 || vsr == 0 {
		return 0
	}
	return new(big.Int).Div(new(big.Int).Mul(big.NewInt(0).SetUint64(vsr), big.NewInt(0).SetUint64(tok)),
		new(big.Int).Add(big.NewInt(0).SetUint64(vtk), big.NewInt(0).SetUint64(tok))).Uint64()
}

// ═══════════════════════════════════════════════════════════════════════════════
//  INSTRUCTION BUILDERS
// ═══════════════════════════════════════════════════════════════════════════════

func getMintTokenProgram(ctx context.Context, mint solana.PublicKey) solana.PublicKey {
	rpcWait()
	info, err := rpcCl.GetAccountInfoWithOpts(ctx, mint, &rpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: rpc.CommitmentConfirmed,
	})
	if err != nil || info == nil || info.Value == nil {
		return solana.TokenProgramID
	}
	owner := info.Value.Owner
	if owner == Token2022 {
		return Token2022
	}
	return solana.TokenProgramID
}

func findATA(wallet, mint, tokProg solana.PublicKey) solana.PublicKey {
	a, _, _ := solana.FindProgramAddress(
		[][]byte{wallet.Bytes(), tokProg.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	return a
}

func cuLimitIx(u uint32) solana.Instruction {
	d := make([]byte, 5)
	d[0] = 2
	binary.LittleEndian.PutUint32(d[1:], u)
	return &genericIx{pid: CUBudget, dat: d}
}

func cuPriceIx(micro uint64) solana.Instruction {
	d := make([]byte, 9)
	d[0] = 3
	binary.LittleEndian.PutUint64(d[1:], micro)
	return &genericIx{pid: CUBudget, dat: d}
}

func ataIx(payer, wallet, mint, tokProg solana.PublicKey) solana.Instruction {
	a := findATA(wallet, mint, tokProg)
	return &genericIx{
		pid: solana.SPLAssociatedTokenAccountProgramID,
		accs: []*solana.AccountMeta{
			{PublicKey: payer, IsSigner: true, IsWritable: true},
			{PublicKey: a, IsWritable: true},
			{PublicKey: wallet},
			{PublicKey: mint},
			{PublicKey: solana.SystemProgramID},
			{PublicKey: tokProg},
		},
		dat: []byte{1},
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  UTILS
// ═══════════════════════════════════════════════════════════════════════════════

func httpGet(u string) ([]byte, error) {
	c := &http.Client{Timeout: 10 * time.Second}
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0")
	r, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", r.StatusCode)
	}
	return b, nil
}

func short(s string) string {
	if len(s) <= 10 {
		return s
	}
	return s[:4] + ".." + s[len(s)-4:]
}
