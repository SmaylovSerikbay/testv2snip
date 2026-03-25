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
	Key            solana.PrivateKey
	RPC            string
	WSS            string
	BuyLamp        uint64
	Slip           uint64  // buy slippage bps
	SellSlip       uint64  // sell slippage bps
	PrioLamp       uint64  // priority fee for BUY (lamports)
	PrioLampSell   uint64  // priority fee for SELL (lamports)
	TP             float64
	SL             float64
	TimeKillSec    int
	TimeKillMin    float64
	MaxTargets     int
	MaxPositions   int
	ScrapeIvl      time.Duration
	MonitorMs      int
	Live           bool
	MinReserve     uint64  // min SOL balance to keep (lamports)
	MaxSessionLoss float64 // stop trading if session PnL <= -X%
	WalletCD       time.Duration
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
	BadEntry    bool
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

type walletStats struct {
	mintBuys     map[string]int
	totalBuySOL  uint64
	totalSellSOL uint64
	trades       int
	win24h2x     bool
}

func getWS(freq map[string]*walletStats, wallet string) *walletStats {
	ws, ok := freq[wallet]
	if !ok {
		ws = &walletStats{mintBuys: map[string]int{}}
		freq[wallet] = ws
	}
	return ws
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
	rpcMu sync.RWMutex

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

	circuitOpen atomic.Bool // session PnL circuit breaker

	rpcCalls atomic.Int64
	rpcErrs  atomic.Int64

	wsRestartCh = make(chan struct{}, 1)
)

func rpcWait() { <-rpcBucket }

func rpcClient() *rpc.Client {
	rpcMu.RLock()
	c := rpcCl
	rpcMu.RUnlock()
	return c
}

func rpcNote(err error) {
	rpcCalls.Add(1)
	if err != nil {
		rpcErrs.Add(1)
	}
}

func requestWSRestart() {
	select {
	case wsRestartCh <- struct{}{}:
	default:
	}
}

func isSlippageErr(err error) bool {
	if err == nil {
		return false
	}
	// common: "custom program error: 0xbc4"
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "0xbc4") || strings.Contains(s, "slippage")
}

// ═══════════════════════════════════════════════════════════════════════════════
//  MAIN
// ═══════════════════════════════════════════════════════════════════════════════

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	_ = godotenv.Load()
	cfg = loadCfg()
	rpcCl = rpc.New(cfg.RPC)

	// Token bucket: 10 burst, refill at ~8/s
	// увеличено: меньше очередей => меньше lag в hot-path (BUY/SELL)
	rpcBucket = make(chan struct{}, 30)
	for i := 0; i < 30; i++ {
		rpcBucket <- struct{}{}
	}
	go func() {
		t := time.NewTicker(50 * time.Millisecond) // ~20 req/s sustained
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
		gInfo, gErr := rpcClient().GetAccountInfoWithOpts(context.Background(), PumpGlobal,
			&rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64})
		rpcNote(gErr)
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
	log.Printf("[INIT] BuySlip %dbps SellSlip %dbps | PrioBuy %.4f PrioSell %.4f SOL",
		cfg.Slip, cfg.SellSlip, float64(cfg.PrioLamp)/1e9, float64(cfg.PrioLampSell)/1e9)
	log.Printf("[INIT] MaxTargets %d | MaxPos %d | Scrape %v | Monitor %dms",
		cfg.MaxTargets, cfg.MaxPositions, cfg.ScrapeIvl, cfg.MonitorMs)
	log.Printf("[INIT] MinReserve %.4f SOL | MaxSessionLoss -%.0f%%",
		float64(cfg.MinReserve)/1e9, cfg.MaxSessionLoss)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buyCtx, buyCancel = context.WithCancel(ctx)

	if hasKey {
		rpcWait()
		b, err := rpcClient().GetBalance(ctx, cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
		rpcNote(err)
		if err == nil {
			log.Printf("[INIT] Баланс: %.6f SOL", float64(b.Value)/1e9)
		}
	}

	var wg sync.WaitGroup
	launch := func(f func(context.Context)) { wg.Add(1); go func() { defer wg.Done(); f(ctx) }() }

	launch(runBlockhashLoop)
	launch(runRPCHealth)
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
		BuyLamp:        7_000_000,   // 0.007 SOL
		Slip:           2000,        // 20% buy slippage
		SellSlip:       4000,        // 40% sell slippage
		PrioLamp:       2_000_000,   // 0.002 SOL buy priority
		PrioLampSell:   1_500_000,   // 0.0015 SOL sell priority
		TP:             0.40,
		SL:             0.20,
		TimeKillSec:    60,
		TimeKillMin:    0.05,
		MaxTargets:     50,
		MaxPositions:   2,
		ScrapeIvl:      3 * time.Minute,
		MonitorMs:      1000,
		MinReserve:     15_000_000,  // 0.015 SOL
		MaxSessionLoss: 30.0,        // -30%
		WalletCD:       5 * time.Second,
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
	c.PrioLampSell = evU("PRIORITY_FEE_SELL_LAMPORTS", c.PrioLampSell)
	c.SellSlip = evU("SELL_SLIPPAGE_BPS", c.SellSlip)
	c.MinReserve = evU("MIN_BALANCE_RESERVE", c.MinReserve)
	c.MaxSessionLoss = evF("MAX_SESSION_LOSS_PCT", c.MaxSessionLoss)
	c.TP = evF("TAKE_PROFIT_PCT", c.TP*100) / 100
	c.SL = evF("STOP_LOSS_PCT", c.SL*100) / 100
	c.TimeKillSec = int(evU("TIMEKILL_SEC", uint64(c.TimeKillSec)))
	c.TimeKillMin = evF("TIMEKILL_MIN_PCT", c.TimeKillMin*100) / 100
	c.MaxTargets = int(evU("MAX_TARGETS", uint64(c.MaxTargets)))
	c.MaxPositions = int(evU("MAX_POSITIONS", uint64(c.MaxPositions)))
	if s := evU("WALLET_COOLDOWN_SEC", 0); s > 0 {
		c.WalletCD = time.Duration(s) * time.Second
	}
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
		r, err := rpcClient().GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
		rpcNote(err)
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

func runRPCHealth(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			calls := rpcCalls.Swap(0)
			errs := rpcErrs.Swap(0)
			if calls < 20 {
				continue
			}
			rate := float64(errs) / float64(calls)
			if rate <= 0.10 {
				continue
			}
			backup := os.Getenv("BACKUP_RPC_URL")
			backupWSS := os.Getenv("BACKUP_WSS_URL")
			if backup == "" {
				continue
			}
			rpcMu.Lock()
			if cfg.RPC != backup {
				log.Printf("[RPC] High error rate %.0f%% (%d/%d) — switch RPC → backup", rate*100, errs, calls)
				cfg.RPC = backup
				if backupWSS != "" {
					cfg.WSS = backupWSS
				}
				rpcCl = rpc.New(cfg.RPC)
				requestWSRestart()
			}
			rpcMu.Unlock()
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
	r, err := rpcClient().GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	rpcNote(err)
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
	freq := map[string]*walletStats{}

	scrapeRPCLive(ctx, freq, myKey)
	scrapeDexEndpoint(ctx, freq, myKey,
		"https://api.dexscreener.com/token-profiles/latest/v1", 15, "profiles")
	scrapeDexEndpoint(ctx, freq, myKey,
		"https://api.dexscreener.com/token-boosts/latest/v1", 10, "boosts")

	now := time.Now()
	tgtMu.Lock()
	added := 0
	for w, ws := range freq {
		maxMintBuys := 0
		for _, cnt := range ws.mintBuys {
			if cnt > maxMintBuys {
				maxMintBuys = cnt
			}
		}
		profitOK := false
		if ws.totalBuySOL > 0 && ws.trades >= 3 {
			profit := float64(ws.totalSellSOL)/float64(ws.totalBuySOL) - 1.0
			if profit > 0.30 { // ослабляем порог, чтобы набрать больше целей
				profitOK = true
			}
		}
		if maxMintBuys < 3 && !profitOK && !ws.win24h2x {
			continue
		}
		if _, ok := targets[w]; !ok && len(targets) < cfg.MaxTargets {
			targets[w] = now
			added++
			reason := fmt.Sprintf("%d same-mint buys", maxMintBuys)
			if ws.win24h2x {
				reason = "2x/24h"
			} else if profitOK {
				reason = fmt.Sprintf("profit>30%% (%d trades)", ws.trades)
			}
			log.Printf("[SCRAPE] +Цель: %s (%s)", short(w), reason)
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

func scrapeRPCLive(ctx context.Context, freq map[string]*walletStats, myKey string) {
	cutoff := time.Now().Add(-24 * time.Hour).Unix()
	for _, addr := range []solana.PublicKey{PumpEventAuth, PumpFee} {
		rpcWait()
		lim := 50
		sigs, err := rpcClient().GetSignaturesForAddressWithOpts(ctx, addr,
			&rpc.GetSignaturesForAddressOpts{Limit: &lim})
		rpcNote(err)
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
			te := parseTxEvent(ctx, s.Signature)
			if te == nil {
				continue
			}
			w := te.User.String()
			if w == myKey {
				continue
			}
			ws := getWS(freq, w)
			ws.trades++
			if te.Timestamp >= cutoff && te.IsBuy && te.Sol > 0 {
				// эвристика "2x за 24ч": встречали buy >= 0.5 SOL (обычно это уже серьёзный памп)
				// точного PnL по кошельку здесь не считаем, но цель — расширить набор активных адресов.
				if te.Sol >= 500_000_000 {
					ws.win24h2x = true
				}
			}
			if te.IsBuy {
				buys++
				ws.mintBuys[te.Mint.String()]++
				ws.totalBuySOL += te.Sol
			} else {
				ws.totalSellSOL += te.Sol
			}
		}
		log.Printf("[SCRAPE] RPC %s: total=%d ok=%d err=%d buys=%d",
			short(addr.String()), len(sigs), ok, errs, buys)
		if buys > 0 {
			return
		}
	}
}

func scrapeDexEndpoint(ctx context.Context, freq map[string]*walletStats, myKey, url string, maxCheck int, tag string) {
	cutoff := time.Now().Add(-24 * time.Hour).Unix()
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
		sigs, err := rpcClient().GetSignaturesForAddressWithOpts(ctx, bc, &rpc.GetSignaturesForAddressOpts{Limit: &lim})
		rpcNote(err)
		if err != nil || len(sigs) == 0 {
			continue
		}
		checked++

		parsed := 0
		for _, s := range sigs {
			if s.Err != nil || parsed >= 5 {
				continue
			}
			te := parseTxEvent(ctx, s.Signature)
			if te == nil {
				continue
			}
			parsed++
			w := te.User.String()
			if w == myKey {
				continue
			}
			ws := getWS(freq, w)
			ws.trades++
			if te.Timestamp >= cutoff && te.IsBuy && te.Sol > 0 {
				if te.Sol >= 500_000_000 {
					ws.win24h2x = true
				}
			}
			if te.IsBuy {
				totalBuyers++
				ws.mintBuys[te.Mint.String()]++
				ws.totalBuySOL += te.Sol
			} else {
				ws.totalSellSOL += te.Sol
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
			if !strings.Contains(err.Error(), "use of closed") {
				log.Printf("[WS] Ошибка: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func wsLoop(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.WSS, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		return nil
	})

	var mu sync.Mutex
	subToWallet := map[int]string{}
	reqToWallet := map[int]string{}
	subbed := map[string]bool{}
	nextID := 0
	var writeMu sync.Mutex

	addNewSubs := func() int {
		tgtMu.RLock()
		defer tgtMu.RUnlock()
		added := 0
		for addr := range targets {
			if subbed[addr] {
				continue
			}
			nextID++
			reqToWallet[nextID] = addr
			subbed[addr] = true
			added++
			writeMu.Lock()
			_ = conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0", "id": nextID,
				"method": "logsSubscribe",
				"params": []any{
					map[string]any{"mentions": []string{addr}},
					map[string]any{"commitment": "confirmed"},
				},
			})
			writeMu.Unlock()
		}
		return added
	}

	mu.Lock()
	addNewSubs()
	count := len(subbed)
	mu.Unlock()
	log.Printf("[WS] Подключён, подписки: %d", count)

	go func() {
		ping := time.NewTicker(15 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				conn.Close()
				return
			case <-wsRestartCh:
				conn.Close()
				return
			case <-ping.C:
				writeMu.Lock()
				_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
				writeMu.Unlock()
			case <-wsReconnCh:
				mu.Lock()
				added := addNewSubs()
				mu.Unlock()
				if added > 0 {
					log.Printf("[WS] +%d подписок (без реконнекта), всего %d", added, len(subbed))
				}
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

		if m.ID != nil && m.Result != nil {
			var subID int
			if json.Unmarshal(m.Result, &subID) == nil && subID > 0 {
				mu.Lock()
				if w, ok := reqToWallet[*m.ID]; ok {
					subToWallet[subID] = w
				}
				mu.Unlock()
			}
			continue
		}

		if m.Method != "logsNotification" || m.Params == nil {
			continue
		}

		mu.Lock()
		wallet := subToWallet[m.Params.Sub]
		mu.Unlock()
		if wallet == "" {
			continue
		}

		tgtMu.RLock()
		_, active := targets[wallet]
		tgtMu.RUnlock()
		if !active {
			continue
		}

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
	if ev.Sol < 30_000_000 {
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
	signalT := time.Now()
	if ctx.Err() != nil {
		return
	}
	mint := sig.Mint.String()

	if circuitOpen.Load() {
		log.Printf("[BUY] Skip: circuit breaker active | %s", short(mint))
		return
	}

	if mintOnCooldown(mint) {
		log.Printf("[BUY] Skip: cooldown | %s", short(mint))
		return
	}

	buyingMu.Lock()
	if buying[mint] {
		buyingMu.Unlock()
		log.Printf("[BUY] Skip: already buying | %s", short(mint))
		return
	}
	buying[mint] = true
	buyingMu.Unlock()
	defer func() { buyingMu.Lock(); delete(buying, mint); buyingMu.Unlock() }()

	posMu.RLock()
	_, dup := pos[mint]
	cnt := len(pos)
	posMu.RUnlock()
	if dup {
		log.Printf("[BUY] Skip: already in position | %s", short(mint))
		return
	}
	if cnt >= cfg.MaxPositions {
		log.Printf("[BUY] Skip: max positions (%d/%d) | %s", cnt, cfg.MaxPositions, short(mint))
		return
	}
	if len(cfg.Key) != 64 {
		log.Printf("[BUY] Skip: no private key | %s", short(mint))
		return
	}

	tgtCDMu.Lock()
	if t, ok := tgtCDMap[sig.Wallet]; ok && time.Since(t) < cfg.WalletCD {
		tgtCDMu.Unlock()
		log.Printf("[BUY] Skip: wallet cooldown | %s from %s", short(mint), short(sig.Wallet))
		return
	}
	tgtCDMap[sig.Wallet] = time.Now()
	tgtCDMu.Unlock()

	statPnlMu.Lock()
	sessionPnl := statPnlSum
	statPnlMu.Unlock()
	if sessionPnl <= -cfg.MaxSessionLoss {
		circuitOpen.Store(true)
		log.Printf("[BUY] Skip: session PnL breaker (%.1f%% <= -%.0f%%) — торговля остановлена", sessionPnl, cfg.MaxSessionLoss)
		return
	}

	log.Printf("[BUY] Сигнал: %s от %s", short(mint), short(sig.Wallet))

	var (
		preBal    uint64
		preBalOK  bool
		preTokPrg solana.PublicKey
		preState  *BondingCurve
		preBC     solana.PublicKey
		preBCErr  error
	)
	var pwg sync.WaitGroup
	pwg.Add(3)
	go func() {
		defer pwg.Done()
		rpcWait()
		b, err := rpcClient().GetBalance(ctx, cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
		rpcNote(err)
		if err == nil {
			preBal = b.Value
			preBalOK = true
		}
	}()
	go func() {
		defer pwg.Done()
		preTokPrg = getMintTokenProgram(ctx, sig.Mint)
	}()
	go func() {
		defer pwg.Done()
		preState, preBC, preBCErr = readBC(ctx, sig.Mint)
	}()
	pwg.Wait()

	if preBalOK && preBal < cfg.BuyLamp+cfg.MinReserve {
		log.Printf("[BUY] Skip: balance reserve (%.4f SOL < %.4f needed) | %s",
			float64(preBal)/1e9, float64(cfg.BuyLamp+cfg.MinReserve)/1e9, short(mint))
		return
	}

	tokProg := preTokPrg
	if tokProg == Token2022 {
		log.Printf("[BUY] Token-2022: %s", short(mint))
	}

	state, bc := preState, preBC
	if preBCErr != nil || state == nil || state.Done {
		if preBCErr != nil {
			log.Printf("[BUY] Skip: bonding curve error (%v) | %s", preBCErr, short(mint))
		} else if state != nil && state.Done {
			log.Printf("[BUY] Skip: bonding curve done (graduated) | %s", short(mint))
		} else {
			log.Printf("[BUY] Skip: bonding curve nil | %s", short(mint))
		}
		setMintCooldown(mint)
		return
	}

	if state.VSR < 10_000_000_000 {
		log.Printf("[BUY] Skip: low liquidity (%.2f SOL) | %s", float64(state.VSR)/1e9, short(mint))
		setMintCooldown(mint)
		return
	}

	if state.RTK > 0 && state.Supply > 0 && float64(state.RTK)/float64(state.Supply) < 0.20 {
		log.Printf("[BUY] Skip: low remaining tokens (%.0f%%) | %s",
			float64(state.RTK)/float64(state.Supply)*100, short(mint))
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
		lagMs := time.Since(signalT).Milliseconds()
		log.Printf("[BUY][PAPER] %s | %.4f SOL → %d tok | lag=%dms", short(mint), float64(cfg.BuyLamp)/1e9, tokOut, lagMs)
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
	txSig, err := rpcClient().SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
		SkipPreflight: true,
		MaxRetries:    &noRetry,
	})
	rpcNote(err)
	if err != nil {
		errMsg := fmt.Sprintf("%v", err)
		if len(errMsg) > 200 {
			errMsg = errMsg[:200] + "…"
		}
		log.Printf("[BUY] ✗ %s | %s", errMsg, short(mint))
		return
	}
	lagMs := time.Since(signalT).Milliseconds()
	log.Printf("[BUY] TX: %s | %s | %.4f SOL | lag=%dms", txSig.String()[:12], short(mint), float64(cfg.BuyLamp)/1e9, lagMs)
	addPos(sig.Mint, tokOut, cfg.BuyLamp, sig.Wallet, tokProg)
	statBuys.Add(1)

	go func() {
		confirmTx(ctx, txSig, "BUY", mint, func() {
			removePos(mint)
			log.Printf("[BUY] Удалена фантомная позиция: %s", short(mint))
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
					diff := float64(real)/float64(old)*100 - 100
					log.Printf("[BUY] Баланс скорр.: %s | %d → %d tok (%.0f%%)",
						short(mint), old, real, diff)
					if diff < -8 {
						p.BadEntry = true
						log.Printf("[BUY] ⚠ Плохой вход (%.0f%%), быстрый выход через 10с", diff)
					}
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
	time.Sleep(3 * time.Second)

	for attempt := 0; attempt < 20; attempt++ {
		rpcWait()
		statuses, err := rpcClient().GetSignatureStatuses(ctx, false, txSig)
		rpcNote(err)
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
	log.Printf("[%s] ⚠ Не подтверждена за 43с: %s | %s", tag, txSig.String()[:12], short(mintStr))
	if onFail != nil {
		onFail()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  MONITOR — TP / SL / TRAILING / TIME-KILL (каждую секунду)
// ═══════════════════════════════════════════════════════════════════════════════

func runMonitor(ctx context.Context) {
	normalInterval := time.Duration(cfg.MonitorMs) * time.Millisecond
	fastInterval := 250 * time.Millisecond
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

type sellJob struct {
	mint       string
	p          *Position
	reason     string
	state      *BondingCurve
	bc         solana.PublicKey
	pnl        float64
	profitable bool
}

func checkAll(ctx context.Context) bool {
	hasProfit := false

	type posSnap struct {
		mint   string
		p      *Position
		tokens uint64
		spent  uint64
	}

	posMu.Lock()
	var snaps []posSnap
	for mint, p := range pos {
		if !p.LastSellTry.IsZero() && p.SellFails > 1 {
			wait := time.Duration(p.SellFails) * time.Second
			if wait > 10*time.Second {
				wait = 10 * time.Second
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
		snaps = append(snaps, posSnap{mint, p, p.Tokens, p.Spent})
	}
	posMu.Unlock()

	var jobs []sellJob
	for _, s := range snaps {
		state, bc, err := readBC(ctx, s.p.Mint)
		if err != nil {
			continue
		}
		if state.Done {
			log.Printf("[MON] %s graduated — удаляем", short(s.mint))
			removePos(s.mint)
			continue
		}

		solOut := calcSolOut(state.VTK, state.VSR, s.tokens)
		solNet := solOut - solOut/100
		pnl := float64(solNet)/float64(s.spent) - 1.0
		age := time.Since(s.p.Entry)

		if pnl > s.p.HiPnl {
			s.p.HiPnl = pnl
		}
		if pnl > 0.02 {
			hasProfit = true
		}

		var reason string
		switch {
		case pnl >= cfg.TP:
			reason = fmt.Sprintf("TP +%.0f%%", pnl*100)
		case age <= 15*time.Second && pnl >= 0.05:
			reason = fmt.Sprintf("QUICKTP %ds +%.0f%%", int(age.Seconds()), pnl*100)
		case s.p.BadEntry && age >= 10*time.Second && pnl < 0:
			reason = fmt.Sprintf("BADEXIT %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		case pnl <= -cfg.SL:
			reason = fmt.Sprintf("SL %.0f%%", pnl*100)
		case s.p.HiPnl >= 0.05 && pnl <= 0:
			reason = fmt.Sprintf("BREAKEVEN peak+%.0f%%→%.0f%%", s.p.HiPnl*100, pnl*100)
		case s.p.HiPnl >= 0.04 && pnl < s.p.HiPnl*0.65 && pnl > 0:
			reason = fmt.Sprintf("TRAIL peak+%.0f%% now+%.0f%%", s.p.HiPnl*100, pnl*100)
		case s.p.HiPnl >= 0.04 && pnl < s.p.HiPnl*0.65 && pnl <= 0:
			reason = fmt.Sprintf("SL(trail) %.0f%%", pnl*100)
		case age >= time.Duration(cfg.TimeKillSec)*time.Second && pnl < cfg.TimeKillMin && pnl > -cfg.SL:
			reason = fmt.Sprintf("TIMEKILL %ds pnl=%+.1f%%(<%+.1f%%)", int(age.Seconds()), pnl*100, cfg.TimeKillMin*100)
		case age >= 60*time.Second && pnl < 0.02 && pnl > -cfg.SL:
			reason = fmt.Sprintf("HARDKILL %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		}
		if reason == "" {
			continue
		}

		s.p.LastSellTry = time.Now()
		jobs = append(jobs, sellJob{s.mint, s.p, reason, state, bc, pnl, pnl > 0})
	}

	for _, j := range jobs {
		ok := doSell(ctx, j.p, j.reason, j.state, j.bc)
		if ok {
			removePos(j.mint)
			trackTargetResult(j.p.Wallet, j.profitable)
		} else {
			posMu.Lock()
			j.p.SellFails++
			posMu.Unlock()
			if j.pnl <= -cfg.SL {
				state2, bc2, err2 := readBC(ctx, j.p.Mint)
				if err2 == nil {
					log.Printf("[MON] Emergency retry: %s", short(j.mint))
					if doSell(ctx, j.p, "EMERGENCY", state2, bc2) {
						removePos(j.mint)
						trackTargetResult(j.p.Wallet, false)
					}
				}
			}
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
		if tgtLosses[wallet] >= 2 {
			tgtMu.Lock()
			delete(targets, wallet)
			tgtMu.Unlock()
			log.Printf("[MON] Удалена цель %s (2 убытка подряд)", short(wallet))
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

	assocUserForBal := findATA(user, p.Mint, tokProg)
	realBal := getTokenBalance(ctx, assocUserForBal)
	sellAmt := p.Tokens
	if realBal > 0 {
		sellAmt = realBal
	} else if realBal == 0 && time.Since(p.Entry) < 8*time.Second {
		return false
	}
	if sellAmt == 0 {
		return false
	}

	solOut := calcSolOut(state.VTK, state.VSR, sellAmt)
	solNet := solOut - solOut/100
	pnl := float64(solNet)/float64(p.Spent)*100 - 100

	creatorVault, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("creator-vault"), state.Creator.Bytes()}, PumpProgram)
	bcV2, _, _ := solana.FindProgramAddress(
		[][]byte{[]byte("bonding-curve-v2"), p.Mint.Bytes()}, PumpProgram)

	data := make([]byte, 24)
	copy(data[:8], sellDisc[:])
	binary.LittleEndian.PutUint64(data[8:], sellAmt)

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

	cuLimit := uint32(400_000)

	if !cfg.Live {
		minSol := solNet * (10000 - cfg.SellSlip) / 10000
		binary.LittleEndian.PutUint64(data[16:], minSol)
		log.Printf("[SELL][PAPER] %s | %s | PnL %+.1f%% | %.6f SOL",
			reason, short(mintStr), pnl, float64(solNet)/1e9)
		statSells.Add(1)
		recordPnl(pnl)
		return true
	}

	// анти-0xbc4: быстрые ретраи с увеличением slippage (если цена резко двигается)
	// базовый cfg.SellSlip (например 4000), затем 6000, 8000, 9500.
	trySlips := []uint64{cfg.SellSlip, 6000, 8000, 9500}
	for attempt, slip := range trySlips {
		if slip > 9900 {
			slip = 9900
		}
		minSol := solNet * (10000 - slip) / 10000
		binary.LittleEndian.PutUint64(data[16:], minSol)

		ixs := []solana.Instruction{
			cuLimitIx(cuLimit),
			cuPriceIx(cfg.PrioLampSell * 1_000_000 / uint64(cuLimit)),
			&genericIx{pid: PumpProgram, dat: data, accs: sellAccs},
			closeIx,
		}

		bh := cachedBH(ctx) // быстрее, чем GetLatestBlockhash на каждом sell
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
		sellRetry := uint(0)
		txSig, err := rpcClient().SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
			SkipPreflight: true,
			MaxRetries:    &sellRetry,
		})
		rpcNote(err)

		if err != nil {
			if isSlippageErr(err) && attempt < len(trySlips)-1 {
				log.Printf("[SELL] 0xbc4/slip retry %d/%d | %s | slip=%dbps", attempt+1, len(trySlips), short(mintStr), slip)
				continue
			}
			errMsg := fmt.Sprintf("%v", err)
			if len(errMsg) > 200 {
				errMsg = errMsg[:200] + "…"
			}
			log.Printf("[SELL] ✗ %s | %s", errMsg, short(mintStr))
			return false
		}

		log.Printf("[SELL] %s | TX %s | %s | PnL %+.1f%% | slip=%dbps", reason, txSig.String()[:12], short(mintStr), pnl, slip)
		statSells.Add(1)

	savedPos := *p
	savedPnl := pnl
	savedMint := mintStr
	savedTokProg := tokProg
	go func() {
		confirmTx(ctx, txSig, "SELL", savedMint, func() {
			ata := findATA(cfg.Key.PublicKey(), savedPos.Mint, savedTokProg)
			rpcWait()
			bal := getTokenBalance(ctx, ata)
			if bal == 0 {
				log.Printf("[SELL] TX не подтверждена, но токены проданы: %s", short(savedMint))
				recordPnl(savedPnl)
				return
			}
			addPos(savedPos.Mint, bal, savedPos.Spent, savedPos.Wallet, savedPos.TokProg)
			posMu.Lock()
			if p, ok := pos[savedMint]; ok {
				p.Entry = savedPos.Entry
				p.HiPnl = savedPos.HiPnl
				p.SellFails = savedPos.SellFails + 1
				p.LastSellTry = time.Now()
			}
			posMu.Unlock()
			log.Printf("[SELL] Позиция восстановлена: %s (fails=%d, bal=%d)", short(savedMint), savedPos.SellFails+1, bal)
		})
		rpcWait()
		st, err := rpcClient().GetSignatureStatuses(ctx, false, txSig)
		rpcNote(err)
		if err == nil && len(st.Value) > 0 && st.Value[0] != nil && st.Value[0].Err == nil {
			recordPnl(savedPnl)
		}
	}()
		return true
	}
	return false
}

// ═══════════════════════════════════════════════════════════════════════════════
//  PUMP.FUN HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

func readBC(ctx context.Context, mint solana.PublicKey) (*BondingCurve, solana.PublicKey, error) {
	bc, _, _ := solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, PumpProgram)
	rpcWait()
	info, err := rpcClient().GetAccountInfoWithOpts(ctx, bc, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64})
	rpcNote(err)
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
	out, err := rpcClient().GetTransaction(ctx, sig, &rpc.GetTransactionOpts{MaxSupportedTransactionVersion: &v})
	rpcNote(err)
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
	info, err := rpcClient().GetAccountInfoWithOpts(ctx, ata, &rpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: rpc.CommitmentConfirmed,
	})
	rpcNote(err)
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
	info, err := rpcClient().GetAccountInfoWithOpts(ctx, mint, &rpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: rpc.CommitmentConfirmed,
	})
	rpcNote(err)
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
	pnl := statPnlSum
	statPnlMu.Unlock()
	if pnl <= -cfg.MaxSessionLoss && !circuitOpen.Load() {
		circuitOpen.Store(true)
		log.Printf("[CIRCUIT] Session PnL %.1f%% <= -%.0f%% — торговля остановлена!", pnl, cfg.MaxSessionLoss)
	}
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
	balStr := ""
	if len(cfg.Key) == 64 {
		rpcWait()
		b, err := rpcClient().GetBalance(context.Background(), cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
		rpcNote(err)
		if err == nil {
			balStr = fmt.Sprintf(" | Bal=%.4f SOL", float64(b.Value)/1e9)
		}
	}
	log.Printf("[STAT] Wins=%d Losses=%d WR=%.0f%% | PnL=%+.1f%% | Open=%d | Buys=%d Sells=%d%s",
		w, l, wr, pnl, open, statBuys.Load(), statSells.Load(), balStr)
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
