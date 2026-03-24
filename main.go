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
	copy(buyDisc[:], []byte{56, 252, 116, 8, 158, 223, 205, 95})
	copy(sellDisc[:], []byte{51, 230, 133, 164, 1, 127, 131, 173})
	h := sha256.Sum256([]byte("event:TradeEvent"))
	copy(tradeDisc[:], h[:8])
	ea, _, _ := solana.FindProgramAddress([][]byte{[]byte("__event_authority")}, PumpProgram)
	PumpEventAuth = ea
	feeKey := []byte{
		1, 86, 224, 246, 147, 102, 90, 207, 68, 219, 21, 104, 191, 23, 91, 170,
		81, 137, 203, 151, 245, 210, 255, 59, 101, 93, 43, 182, 253, 109, 24, 176,
	}
	fc, _, _ := solana.FindProgramAddress([][]byte{[]byte("fee_config"), feeKey}, FeeProgram)
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
	Mint        solana.PublicKey
	Tokens      uint64
	Spent       uint64 // lamports
	Entry       time.Time
	Wallet      string
	HiPnl       float64 // peak PnL for trailing stop
	TokProg     solana.PublicKey
	LastSellTry time.Time
	SellFails   int
}

type BondingCurve struct {
	VTK, VSR, RTK, RSR, Supply uint64
	Done                       bool
	Creator                    solana.PublicKey
	Cashback                   bool
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

	// Cooldown: skip mints recently attempted (success or fail)
	cdMu       sync.Mutex
	cdMap      = map[string]time.Time{} // mint → last attempt
	cdDuration = 3 * time.Minute

	// Buy lifecycle: separate context + waitgroup for in-flight buys
	buyCtx    context.Context
	buyCancel context.CancelFunc
	buyWg     sync.WaitGroup

	// Per-target consecutive loss tracking
	tgtLossMu sync.Mutex
	tgtLosses = map[string]int{}

	// Per-target buy cooldown (prevent spam from single wallet)
	tgtCDMu  sync.Mutex
	tgtCDMap = map[string]time.Time{}

	bhMu    sync.RWMutex
	bhHash  solana.Hash
	bhStale int64 // unix seconds when fetched

	statBuys   atomic.Int64
	statSells  atomic.Int64
	statWins   atomic.Int64
	statLosses atomic.Int64
	statPnlMu  sync.Mutex
	statPnlSum float64
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
	buyCtx, buyCancel = context.WithCancel(ctx)

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
	launch(runStats)

	osSig := make(chan os.Signal, 1)
	signal.Notify(osSig, os.Interrupt)
	<-osSig
	log.Println("[EXIT] Останавливаем... отменяем покупки")

	close(signalCh)
	buyCancel()
	buyWg.Wait()
	log.Println("[EXIT] Все покупки завершены, закрываем позиции...")

	// Даём монитору 30 секунд чтобы закрыть открытые позиции
	posMu.RLock()
	open := len(pos)
	posMu.RUnlock()
	if open > 0 {
		log.Printf("[EXIT] Открыто %d позиций, ждём закрытия (до 30с)...", open)
		deadline := time.After(30 * time.Second)
		tick := time.NewTicker(2 * time.Second)
	waitLoop:
		for {
			select {
			case <-deadline:
				log.Println("[EXIT] Таймаут — завершаем принудительно")
				break waitLoop
			case <-tick.C:
				posMu.RLock()
				n := len(pos)
				posMu.RUnlock()
				if n == 0 {
					log.Println("[EXIT] Все позиции закрыты")
					break waitLoop
				}
				checkAll(ctx)
			}
		}
		tick.Stop()
	}

	printStats()
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
	if age < 20 {
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
		if cnt < 2 {
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
		case sig, ok := <-signalCh:
			if !ok {
				return
			}
			buyWg.Add(1)
			go func() {
				defer buyWg.Done()
				doBuy(buyCtx, sig)
			}()
		}
	}
}

func mintOnCooldown(mint string) bool {
	cdMu.Lock()
	defer cdMu.Unlock()
	if t, ok := cdMap[mint]; ok && time.Since(t) < cdDuration {
		return true
	}
	return false
}

func setMintCooldown(mint string) {
	cdMu.Lock()
	cdMap[mint] = time.Now()
	cdMu.Unlock()
}

func doBuy(ctx context.Context, sig Signal) {
	if ctx.Err() != nil {
		return
	}
	mint := sig.Mint.String()

	if mintOnCooldown(mint) {
		return
	}

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
		return
	}
	if len(cfg.Key) != 64 {
		return
	}

	tgtCDMu.Lock()
	if t, ok := tgtCDMap[sig.Wallet]; ok && time.Since(t) < 2*time.Minute {
		tgtCDMu.Unlock()
		return
	}
	tgtCDMap[sig.Wallet] = time.Now()
	tgtCDMu.Unlock()

	rpcWait()
	bal, bErr := rpcCl.GetBalance(ctx, cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
	if bErr == nil && bal.Value < cfg.BuyLamp+3_000_000 {
		log.Printf("[BUY] Баланс слишком мал: %.4f SOL — пропуск", float64(bal.Value)/1e9)
		return
	}

	log.Printf("[BUY] Сигнал: %s от %s", short(mint), short(sig.Wallet))

	tokProg := getMintTokenProgram(ctx, sig.Mint)
	if tokProg == Token2022 {
		log.Printf("[BUY] Token-2022: %s", short(mint))
	}

	state, bc, err := readBC(ctx, sig.Mint)
	if err != nil || state.Done {
		setMintCooldown(mint)
		return
	}

	if state.VSR < 5_000_000_000 {
		setMintCooldown(mint)
		return
	}

	// Не входить если осталось <20% токенов (bonding curve почти завершена — опасно)
	if state.RTK > 0 && state.Supply > 0 && float64(state.RTK)/float64(state.Supply) < 0.20 {
		setMintCooldown(mint)
		return
	}

	fee := cfg.BuyLamp / 100
	tokOut := calcTokOut(state.VTK, state.VSR, cfg.BuyLamp-fee)
	if tokOut == 0 {
		return
	}
	minTokOut := tokOut * (10000 - cfg.Slip) / 10000

	user := cfg.Key.PublicKey()
	assocBC := findATA(bc, sig.Mint, tokProg)
	assocUser := findATA(user, sig.Mint, tokProg)

	creatorVault, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("creator-vault"), state.Creator.Bytes()}, PumpProgram)
	globalVolAcc, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("global_volume_accumulator")}, PumpProgram)
	userVolAcc, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("user_volume_accumulator"), user.Bytes()}, PumpProgram)
	bcV2, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("bonding-curve-v2"), sig.Mint.Bytes()}, PumpProgram)

	// buy_exact_sol_in: (sol_amount u64, min_tokens_out u64)
	data := make([]byte, 24)
	copy(data[:8], buyDisc[:])
	binary.LittleEndian.PutUint64(data[8:], cfg.BuyLamp)
	binary.LittleEndian.PutUint64(data[16:], minTokOut)

	cuLimit := uint32(250_000)
	ixs := []solana.Instruction{
		cuLimitIx(cuLimit),
		cuPriceIx(cfg.PrioLamp * 1_000_000 / uint64(cuLimit)),
		ataIx(user, user, sig.Mint, tokProg),
		&genericIx{pid: PumpProgram, dat: data, accs: []*solana.AccountMeta{
			{PublicKey: PumpGlobal},                             // 0
			{PublicKey: PumpFee, IsWritable: true},              // 1
			{PublicKey: sig.Mint},                               // 2
			{PublicKey: bc, IsWritable: true},                   // 3
			{PublicKey: assocBC, IsWritable: true},              // 4
			{PublicKey: assocUser, IsWritable: true},            // 5
			{PublicKey: user, IsSigner: true, IsWritable: true}, // 6
			{PublicKey: solana.SystemProgramID},                 // 7
			{PublicKey: tokProg},                                // 8
			{PublicKey: creatorVault, IsWritable: true},         // 9
			{PublicKey: PumpEventAuth},                          // 10
			{PublicKey: PumpProgram},                            // 11
			{PublicKey: globalVolAcc},                           // 12
			{PublicKey: userVolAcc, IsWritable: true},           // 13
			{PublicKey: FeeConfig},                              // 14
			{PublicKey: FeeProgram},                             // 15
			{PublicKey: bcV2},                                   // 16 bonding_curve_v2
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

	setMintCooldown(mint)

	if ctx.Err() != nil {
		return
	}

	rpcWait()
	noRetry := uint(0)
	txSig, err := rpcCl.SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
		SkipPreflight: false,
		MaxRetries:    &noRetry,
	})
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

	go func() {
		confirmTx(ctx, txSig, "BUY", mint, func() {
			removePos(mint)
			log.Printf("[BUY] Удалена фантомная позиция: %s", short(mint))
			return
		})
		// После подтверждения — обновить p.Tokens реальным балансом ATA
		user := cfg.Key.PublicKey()
		ata := findATA(user, sig.Mint, tokProg)
		real := getTokenBalance(ctx, ata)
		if real > 0 {
			posMu.Lock()
			if p, ok := pos[mint]; ok {
				old := p.Tokens
				p.Tokens = real
				if old != real {
					log.Printf("[BUY] Баланс скорр.: %s | %d → %d tok (%.0f%%)",
						short(mint), old, real, float64(real)/float64(old)*100-100)
				}
			}
			posMu.Unlock()
		}
	}()
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
	normalInterval := time.Duration(cfg.MonitorMs) * time.Millisecond
	fastInterval := 500 * time.Millisecond
	t := time.NewTicker(normalInterval)
	defer t.Stop()
	fast := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			hasProfit := checkAll(ctx)
			if hasProfit && !fast {
				t.Reset(fastInterval)
				fast = true
			} else if !hasProfit && fast {
				t.Reset(normalInterval)
				fast = false
			}
		}
	}
}

func checkAll(ctx context.Context) bool {
	hasProfit := false
	posMu.Lock()
	defer posMu.Unlock()

	for mint, p := range pos {
		// Cooldown между попытками продажи: 5с + 5с за каждый фейл (макс 30с)
		if !p.LastSellTry.IsZero() {
			wait := time.Duration(5+p.SellFails*5) * time.Second
			if wait > 30*time.Second {
				wait = 30 * time.Second
			}
			if time.Since(p.LastSellTry) < wait {
				continue
			}
		}

		if p.SellFails >= 7 {
			log.Printf("[MON] %s удалена (7 неудачных sell)", short(mint))
			delete(pos, mint)
			continue
		}

		state, bc, err := readBC(ctx, p.Mint)
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

		if pnl > p.HiPnl {
			p.HiPnl = pnl
		}

		if pnl > 0.05 {
			hasProfit = true
		}

		var reason string
		switch {
		case pnl >= cfg.TP && pnl > 0.03:
			reason = fmt.Sprintf("TP +%.0f%%", pnl*100)
		case pnl <= -cfg.SL:
			reason = fmt.Sprintf("SL %.0f%%", pnl*100)
		case p.HiPnl >= 0.05 && pnl < p.HiPnl-0.03 && pnl > 0.03:
			reason = fmt.Sprintf("TRAIL peak+%.0f%% now+%.0f%%", p.HiPnl*100, pnl*100)
		case p.HiPnl >= 0.05 && pnl < p.HiPnl-0.03 && pnl <= -cfg.SL:
			reason = fmt.Sprintf("SL(trail) %.0f%%", pnl*100)
		case age >= time.Duration(cfg.TimeKillSec)*time.Second && pnl < cfg.TimeKillMin && pnl > -cfg.SL:
			reason = fmt.Sprintf("TIMEKILL %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		case age >= 120*time.Second && pnl < 0.05:
			reason = fmt.Sprintf("HARDKILL %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		}
		if reason == "" {
			continue
		}

		p.LastSellTry = time.Now()
		profitable := pnl > 0
		ok := doSell(ctx, p, reason, state, bc)
		if ok {
			delete(pos, mint)
			trackTargetResult(p.Wallet, profitable)
		} else {
			p.SellFails++
		}
	}
	return hasProfit
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

func doSell(ctx context.Context, p *Position, reason string, cachedState *BondingCurve, cachedBC solana.PublicKey) bool {
	if len(cfg.Key) != 64 {
		return false
	}

	mintStr := p.Mint.String()
	var state *BondingCurve
	var bc solana.PublicKey
	if cachedState != nil {
		state = cachedState
		bc = cachedBC
	} else {
		var err error
		state, bc, err = readBC(ctx, p.Mint)
		if err != nil || state.Done {
			return false
		}
	}

	user := cfg.Key.PublicKey()
	tokProg := p.TokProg
	if tokProg.IsZero() {
		tokProg = solana.TokenProgramID
	}
	assocBC := findATA(bc, p.Mint, tokProg)
	assocUser := findATA(user, p.Mint, tokProg)

	realBal := getTokenBalance(ctx, assocUser)
	if realBal == 0 {
		return false
	}
	sellAmt := realBal

	solOut := calcSolOut(state.VTK, state.VSR, sellAmt)
	solNet := solOut - solOut/100
	sellSlip := uint64(3000)
	minSol := solNet * (10000 - sellSlip) / 10000
	pnl := float64(solNet)/float64(p.Spent)*100 - 100

	creatorVault, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("creator-vault"), state.Creator.Bytes()}, PumpProgram)
	bcV2, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("bonding-curve-v2"), p.Mint.Bytes()}, PumpProgram)

	data := make([]byte, 24)
	copy(data[:8], sellDisc[:])
	binary.LittleEndian.PutUint64(data[8:], sellAmt)
	binary.LittleEndian.PutUint64(data[16:], minSol)

	sellAccs := []*solana.AccountMeta{
		{PublicKey: PumpGlobal},                             // 0
		{PublicKey: PumpFee, IsWritable: true},              // 1
		{PublicKey: p.Mint},                                 // 2
		{PublicKey: bc, IsWritable: true},                   // 3
		{PublicKey: assocBC, IsWritable: true},              // 4
		{PublicKey: assocUser, IsWritable: true},            // 5
		{PublicKey: user, IsSigner: true, IsWritable: true}, // 6
		{PublicKey: solana.SystemProgramID},                 // 7
		{PublicKey: creatorVault, IsWritable: true},         // 8
		{PublicKey: tokProg},                                // 9
		{PublicKey: PumpEventAuth},                          // 10
		{PublicKey: PumpProgram},                            // 11
		{PublicKey: FeeConfig},                              // 12
		{PublicKey: FeeProgram},                             // 13
	}
	if state.Cashback {
		userVolAcc, _, _ := solana.FindProgramAddress(
			[][]byte{[]byte("user_volume_accumulator"), user.Bytes()}, PumpProgram)
		sellAccs = append(sellAccs, &solana.AccountMeta{PublicKey: userVolAcc, IsWritable: true}) // 14
	}
	sellAccs = append(sellAccs, &solana.AccountMeta{PublicKey: bcV2}) // last: bonding_curve_v2

	// CloseAccount — возвращает rent (~0.002 SOL) после продажи
	closeIx := &genericIx{
		pid: tokProg,
		dat: []byte{9}, // CloseAccount instruction index
		accs: []*solana.AccountMeta{
			{PublicKey: assocUser, IsWritable: true},
			{PublicKey: user, IsWritable: true},
			{PublicKey: user, IsSigner: true},
		},
	}

	cuLimit := uint32(300_000)
	ixs := []solana.Instruction{
		cuLimitIx(cuLimit),
		cuPriceIx(cfg.PrioLamp * 1_000_000 / uint64(cuLimit)),
		&genericIx{pid: PumpProgram, dat: data, accs: sellAccs},
		closeIx,
	}

	if !cfg.Live {
		log.Printf("[SELL][PAPER] %s | %s | PnL %+.1f%% | %.6f SOL",
			reason, short(mintStr), pnl, float64(solNet)/1e9)
		statSells.Add(1)
		recordPnl(pnl)
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
	noRetry := uint(0)
	txSig, err := rpcCl.SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
		SkipPreflight: false,
		MaxRetries:    &noRetry,
	})
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
	recordPnl(pnl)

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
	if len(d) > 82 {
		s.Cashback = d[82] != 0
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
	copy(e.Mint[:], d[o:o+32])
	o += 32
	e.Sol = binary.LittleEndian.Uint64(d[o:])
	o += 8
	e.Tokens = binary.LittleEndian.Uint64(d[o:])
	o += 8
	e.IsBuy = d[o] != 0
	o++
	copy(e.User[:], d[o:o+32])
	o += 32
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

func getTokenBalance(ctx context.Context, ata solana.PublicKey) uint64 {
	rpcWait()
	info, err := rpcCl.GetAccountInfoWithOpts(ctx, ata, &rpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: rpc.CommitmentConfirmed,
	})
	if err != nil || info == nil || info.Value == nil {
		return 0
	}
	d := info.Value.Data.GetBinary()
	if len(d) < 72 {
		return 0
	}
	return binary.LittleEndian.Uint64(d[64:72])
}

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

func recordPnl(pnlPct float64) {
	if pnlPct > 0 {
		statWins.Add(1)
	} else {
		statLosses.Add(1)
	}
	statPnlMu.Lock()
	statPnlSum += pnlPct
	statPnlMu.Unlock()
}

func printStats() {
	w := statWins.Load()
	l := statLosses.Load()
	total := w + l
	statPnlMu.Lock()
	pnl := statPnlSum
	statPnlMu.Unlock()
	wr := float64(0)
	if total > 0 {
		wr = float64(w) / float64(total) * 100
	}
	posMu.RLock()
	open := len(pos)
	posMu.RUnlock()
	log.Printf("[STAT] Wins=%d Losses=%d WR=%.0f%% | PnL=%+.1f%% | Open=%d | Buys=%d Sells=%d",
		w, l, wr, pnl, open, statBuys.Load(), statSells.Load())
}

func runStats(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			printStats()
		}
	}
}
