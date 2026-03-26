package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sort"
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
	Key             solana.PrivateKey
	RPC             string
	WSS             string
	// Multi-WSS race (comma-separated URLs via env)
	WSSURLs []string
	BuyLamp         uint64
	Slip            uint64 // buy slippage bps
	SellSlip        uint64 // sell slippage bps
	PrioLamp        uint64 // priority fee for BUY (lamports)
	PrioLampSell    uint64 // priority fee for SELL (lamports)
	JitoTipLamp     uint64 // optional: lamports tip via SystemProgram transfer
	JitoTipAcc      solana.PublicKey
	JitoTipAccs     []solana.PublicKey // optional: pool of official Jito tip accounts
	JitoHTTP        bool
	JitoHTTPURL     string
	JitoHTTPTimeout time.Duration
	// Jito Bundles (sendBundle)
	JitoBundleBuy bool
	JitoBundleURL string
	JitoBundleURLs []string
	TP              float64
	QuickTPPct      float64       // ранний выход QUICKTP: в первые 15с при pnl >= этого порога (доля, напр. 0.22 = +22%)
	EarlySLWindow   time.Duration // 0 = выкл.; иначе в первые N сек выход если pnl <= -EarlySLPct (анти-просадка)
	EarlySLPct      float64       // доля, напр. 0.15 = −15%
	SL              float64
	TimeKillSec     int
	TimeKillMin     float64
	// TIMEKILL: не закрывать, если pnl уже хуже этого минуса (доля; 0.02 = −2%).
	// Защита от "TIMEKILL -12%": такие минуса пусть режет FLASHSL/SL, а не timekill.
	TimeKillMaxLoss float64
	// SOFT_HARDKILL: после N сек закрыть позицию, если pnl ниже "чистого" порога (minNetExit),
	// но не хуже SoftHardKillMaxLoss. Это защищает от долгой передержки до SL.
	SoftHardKillSec int
	// Отдельный лимит минуса для SOFTHARD/HARDKILL (доля; 0.10 = −10%).
	// Нужен, чтобы позиция не зависала между TimeKillMaxLoss и SL.
	SoftHardKillMaxLoss float64
	// Минимальный pnl для "профитных" выходов (TRAIL/PEAKPB/TIMEKILL в плюсе) с учётом fixed-cost.
	// 0 = авторасчёт из priority+jito + буфер NET_EXIT_BUFFER_PCT.
	MinNetExitPct float64 // доля (0.06 = +6%)
	NetExitBufPct float64 // доля (0.005 = +0.5%)
	MaxTargets    int
	MaxPositions  int
	ScrapeIvl     time.Duration
	MonitorMs     int
	// Пока позиция «молодая» (сек после Entry) — держать быстрый тик монитора (иначе при обвале ждём до MONITOR_MS).
	MonitorFastYoungSec int
	// FLASHSL: в первые N сек при pnl <= -X% — резать хвост (0 = выкл.)
	FlashSLWindow time.Duration
	FlashSLPct    float64
	// Не применять FLASHSL раньше чем через столько после Entry (кривая/RPC после BUY часто «ломают» pnl)
	FlashSLMinAge  time.Duration
	Live           bool
	MinReserve     uint64  // min SOL balance to keep (lamports)
	MaxSessionLoss float64 // stop trading if session PnL <= -X%
	WalletCD       time.Duration
	MaxTokInfoLag  time.Duration // if token info fetch exceeds this, skip buy
	MaxSignalLag   time.Duration // if total lag from signal to send exceeds this, skip buy
	// Pre-simulate BUY tx before sending to avoid paid fails (Custom:6000 etc.).
	PreSimBuy       bool
	PreSimTimeout   time.Duration
	PreSimLagBudget time.Duration // don't simulate if remaining lag budget is less than this
	// Confirmation timeouts
	BuyConfirmTimeout time.Duration
	MinWhaleBuyLam uint64        // min whale buy size to follow (lamports)
	MinVSRLam      uint64        // min virtual SOL reserve (lamports)
	// Минимальный «возраст» mint по нашему WS-стриму (без RPC): если mint впервые увидели меньше N сек назад — skip BUY.
	// 0 = выкл.
	MintMinAge time.Duration
	MintCD     time.Duration // skip re-buy same mint for this long after attempt/skip
	// После профитного закрытия — не входить в этот mint N сек (анти "второго захода" в уже отыгранный памп). 0 = выкл.
	MintWinCD time.Duration
	// После убыточного закрытия — не входить в этот mint N сек (0 = выкл.)
	MintLossCD time.Duration
	// Анти-скэм: проверка концентрации холдеров по RPC getTokenLargestAccounts (0 = выкл.)
	HolderMaxPct float64 // порог в долях, напр. 0.10 = 10%
	HolderTopN   int     // сколько крупнейших аккаунтов смотреть (по умолчанию 20)
	// Быстрый режим holder-check: если ликвидность >= этого порога, пропускаем holder RPC проверку.
	// 0 = всегда проверять (как раньше).
	HolderCheckMinVSR uint64
	// TTL кеша баланса в hot-path BUY (ms): снижает preSend, но баланс может быть слегка устаревшим.
	WalletBalCacheTTL time.Duration
	// Трейлинг сессии: если реализованный PnL сессии когда-то был ≥ Min (%% от старта), при откате от пика на Giveback п.п. — стоп новых BUY (как CIRCUIT).
	SessionTrailMinPeak  float64 // %% от баланса при старте, 0 = выкл.
	SessionTrailGiveback float64 // п.п. ниже пика сессии
	// Если >0: circuit по trail только при PnL сессии ≤ −X%% (не стопорить на −0.3%% после пары сделок)
	SessionTrailNetRedPct float64
	// BC retries when bonding curve account not yet readable (fresh mints)
	BCRetryMax   int
	BCRetrySleep time.Duration
	BCSigLagBuf  time.Duration // stop retries when signal lag exceeds MaxSignalLag minus this
	// After BUY confirm: if token balance vs expected is worse than -this %, mark BadEntry
	BadEntryPct float64
	// Если >0: при подтверждении BUY и плохом fill (скорр. токенов) ≤ -X% — немедленно закрыть позицию (анти мгновенных -20..-30%).
	BadEntryImmediateSellPct float64
	// Не продавать «мгновенно» раньше чем через N секунд после Entry (страховка от гонок confirm/ATA).
	BadEntryImmediateMinAge time.Duration
	// При BadEntry — ужесточить порог полного SL (доля; 0.14 = −14%%). 0 = не ужесточать (как STOP_LOSS)
	BadEntrySLPct float64
	// При BadEntry — раньше выходить в минус (сек; 0 = только классический BADEXIT через 10с)
	BadExitFastSec int
	// При BadEntry — FLASHSL по более узкому порогу (доля; 0.10 = −10%%). 0 = как FLASH_SL_MIN_PCT
	BadEntryFlashPct float64
	// Мин. число Buy по этому mint с глобального pump WS за окно (0 = выкл.) — меньше «одиноких» входов
	MintMinPumpBuys       int
	MintMinPumpBuysWindow time.Duration
	// BREAKEVEN: выход в ноль/минус только если пик HiPnl >= этого порога (доля, 0.30 = +30%)
	BreakevenMinPeak float64
	// PROFIT_LOCK: если позиция уже дала пик ≥ MinPeak, то не отдавать прибыль ниже Floor (доля; 0.005 = +0.5%).
	// MinPeak=0 = выкл.
	ProfitLockMinPeak float64
	ProfitLockFloor   float64
	// Мин. пик HiPnl (доля), с которого включаются TRAIL и PEAK_PULLBACK (раньше было жёстко +4%%)
	TrailMinPeak float64
	// Откат от пика позиции в долях (0.05 = 5 п.п.) — фиксация при медленном скатывании с графика; 0 = выкл.
	PeakPullbackPct float64
	// TRAIL: взять прибыль если pnl < пик×множитель (0.65 = оставить ≥65%% от пика по уровню pnl)
	TrailRelMult float64
	// COPY_WALLETS — опционально: доп. адреса; пусто = только авто-скрейп (ничего искать вручную не нужно)
	CopyWallets []string
	// SCRAPE_AUTO_TARGETS=0 — только COPY_WALLETS; 1 (дефолт) — бот сам набирает цели по WS
	ScrapeAutoTargets bool
	// Observe/Approve: сначала собираем статистику по кошелькам, и только потом добавляем их в цели.
	// OBSERVE_ONLY=1 — торгуем только по PINNED/COPY_WALLETS/уже утверждённым целям.
	ObserveOnly bool
	// Shadowing: если цель продала тот же mint — продаём нашу позицию сразу (copy-sell якорь).
	CopySellExit      bool
	CopySellMinAgeSec int
	// VSR Speed exit: выход по затуханию притока ликвидности на кривую.
	VSRSExit           bool
	VSRSWindowSec      int     // окно для оценки скорости VSR (сек)
	VSRSMinUpSOLPerSec float64 // если скорость ниже этого порога — считаем "затухло"
	VSRSMinPeakPnlPct  float64 // активировать только если пик pnl был ≥ этого (%%)
	VSRSMinAgeSec      int     // не применять раньше этого возраста позиции (сек)
	// Порог "утверждения" кандидата.
	CandMinAge         time.Duration
	CandMinTrades      int
	CandMinProfitPct   float64 // доля, 0.30 = +30%
	CandMinTotalBuyLam uint64  // суммарный объём покупок за окно (lamports)
	CandRequireWin2x   bool    // требовать флаг 2x/24h
	// Мин. покупок в один и тот же mint для эвристики «same-mint»; 0 = не использовать (только profitOK / крупный бай)
	ScrapeSameMintMin int
	// PIN: сохранять профитные кошельки в файл и всегда наблюдать за ними (переживает рестарт)
	PinWalletMinPnl float64 // доля, напр. 0.05 = +5%
	PinWalletsFile  string  // путь к файлу со списком (по одному адресу в строке)
	PinUnpinLosses  int     // после N убыточных закрытий подряд — unpin и удалить из файла (0 = выкл.)
}

type Signal struct {
	Mint   solana.PublicKey
	Wallet string
	Sig    string // tx signature (dedup across multi-WSS)
	At     time.Time // when WS signal was received
}

type Position struct {
	Mint            solana.PublicKey
	Tokens          uint64
	Spent           uint64 // lamports
	Entry           time.Time
	Wallet          string
	HiPnl           float64 // peak PnL for trailing stop
	TokProg         solana.PublicKey
	LastSellTry     time.Time
	SellFails       int
	BadEntry        bool
	BadEntryDiffPct float64 // отрицательное число, напр. -15 означает -15% токенов от ожидаемого
	Confirmed       bool

	// VSR speed tracking (for VSR_SPEED_EXIT)
	VSR0   uint64
	VSR0At time.Time
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

// walletPerf — статистика результата копирования по цели (по нашим сделкам).
// Используется для авто-ранжирования/отсева целей.
type walletPerf struct {
	Trades int
	Wins   int
	Losses int

	NetLamports int64 // Σ(netOut - spent - fixed round-trip) по закрытым сделкам (как recordPnl)

	BuyFailCount int // за окно
	MutedUntil   time.Time

	LastTrade time.Time
	LastFail  time.Time
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

	signalCh      = make(chan Signal, 64)
	wsReconnCh    = make(chan struct{}, 1)
	monitorWakeCh = make(chan struct{}, 1)

	// Signal dedup (for multi-WSS race)
	sigDedupMu sync.Mutex
	sigDedup   = map[string]time.Time{} // signature -> seen time

	tgtMu   sync.RWMutex
	targets = map[string]time.Time{} // wallet → lastSeen

	pinMu        sync.Mutex
	pinnedWallet = map[string]bool{} // wallet → pinned (persisted)

	posMu sync.RWMutex
	pos   = map[string]*Position{} // mint_str → position
	// Покупки в полёте, ещё не в pos (анти гонка MAX_POSITIONS при двух сигналах сразу)
	pendingPosBuys int

	// Dedup: prevents concurrent buys of the same mint
	buyingMu sync.Mutex
	buying   = map[string]bool{}

	// Cooldown: skip mints recently attempted (success or fail)
	cdMu      sync.Mutex
	cdMap     = map[string]time.Time{} // mint → last attempt
	lossCdMap = map[string]time.Time{} // mint → last убыточное закрытие (для MINT_LOSS_COOLDOWN)
	winCdMap  = map[string]time.Time{} // mint → last профитное закрытие (для MINT_REBUY_COOLDOWN_AFTER_WIN)

	// Макс. sessionPnlPct() с момента старта — для SESSION_TRAIL_*
	sessionPeakPctMu  sync.Mutex
	sessionPeakPct    float64
	sessionTrailDebMu sync.Mutex
	sessionTrailDeb   *time.Timer

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

	// Per-target performance (ranking / auto-filter)
	perfMu   sync.Mutex
	walletPF = map[string]*walletPerf{} // wallet -> perf

	// Candidate pool: наблюдаем и утверждаем перед добавлением в targets
	candMu     sync.Mutex
	candidates = map[string]struct {
		firstSeen time.Time
		lastSeen  time.Time
		stats     walletStats
	}{} // wallet -> rolling stats snapshot

	mintProgMu    sync.RWMutex
	mintProgCache = map[string]solana.PublicKey{}

	ataKnownMu   sync.RWMutex
	ataKnownUser = map[string]bool{} // key: user|mint|tokProg -> true (ATA exists)

	walletBalCache   atomic.Uint64
	walletBalCacheAt atomic.Int64 // unix ms

	bhMu    sync.RWMutex
	bhHash  solana.Hash
	bhStale int64 // unix seconds when fetched

	statBuys   atomic.Int64
	statSells  atomic.Int64
	statWins   atomic.Int64
	statLosses atomic.Int64
	statPnlMu  sync.Mutex
	// Реализованный PnL сессии в lamports (сумма (solNet−spent) по закрытым сделкам).
	statPnlLamports int64
	// Баланс кошелька при старте бота — база для MAX_SESSION_LOSS_PCT (0 = ещё не задан).
	sessionRefLamports uint64

	circuitOpen atomic.Bool // session PnL circuit breaker

	// Fee protection: если серия BUY FAIL (Custom:6000 / slippage), делаем короткую паузу,
	// чтобы не продолжать платить комиссии за заведомо неподходящие constraints.
	buyPauseUntil atomic.Int64 // unix ms
	buyFailMu     sync.Mutex
	buyFailTimes  []time.Time

	rpcCalls atomic.Int64
	rpcErrs  atomic.Int64

	wsRestartCh = make(chan struct{}, 1)

	// Scraper WS-only pipeline
	scrapeEvCh = make(chan scrapeEv, 4096)

	// Счётчик Buy по mint из logsSubscribe на pump (анти «тихий» токен)
	mintPumpBuyMu sync.Mutex
	mintPumpBuys  = map[string][]time.Time{} // mint → время каждого Buy с глобального стрима
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

func accountExists(ctx context.Context, pk solana.PublicKey) bool {
	rpcWait()
	info, err := rpcClient().GetAccountInfoWithOpts(ctx, pk, &rpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: rpc.CommitmentConfirmed,
	})
	rpcNote(err)
	return err == nil && info != nil && info.Value != nil
}

func setWalletBalCache(v uint64) {
	walletBalCache.Store(v)
	walletBalCacheAt.Store(time.Now().UnixMilli())
}

func getWalletBalCache(maxAge time.Duration) (uint64, bool) {
	ts := walletBalCacheAt.Load()
	if ts == 0 {
		return 0, false
	}
	if maxAge > 0 && time.Since(time.UnixMilli(ts)) > maxAge {
		return 0, false
	}
	return walletBalCache.Load(), true
}

func mintProgCached(mint solana.PublicKey) (solana.PublicKey, bool) {
	mintProgMu.RLock()
	v, ok := mintProgCache[mint.String()]
	mintProgMu.RUnlock()
	return v, ok
}

func setMintProgCache(mint solana.PublicKey, prog solana.PublicKey) {
	mintProgMu.Lock()
	mintProgCache[mint.String()] = prog
	mintProgMu.Unlock()
}

func getMintTokenProgramCached(ctx context.Context, mint solana.PublicKey) solana.PublicKey {
	if v, ok := mintProgCached(mint); ok && !v.IsZero() {
		return v
	}
	v := getMintTokenProgram(ctx, mint)
	setMintProgCache(mint, v)
	return v
}

func getMintTokenProgramCachedWithHit(ctx context.Context, mint solana.PublicKey) (solana.PublicKey, bool) {
	if v, ok := mintProgCached(mint); ok && !v.IsZero() {
		return v, true
	}
	v := getMintTokenProgram(ctx, mint)
	setMintProgCache(mint, v)
	return v, false
}

func ataKnownKey(user, mint, tokProg solana.PublicKey) string {
	return user.String() + "|" + mint.String() + "|" + tokProg.String()
}

func markUserATAKnown(user, mint, tokProg solana.PublicKey) {
	ataKnownMu.Lock()
	ataKnownUser[ataKnownKey(user, mint, tokProg)] = true
	ataKnownMu.Unlock()
}

func userATAExistsCached(ctx context.Context, user, mint, tokProg solana.PublicKey) bool {
	key := ataKnownKey(user, mint, tokProg)
	ataKnownMu.RLock()
	if ataKnownUser[key] {
		ataKnownMu.RUnlock()
		return true
	}
	ataKnownMu.RUnlock()
	ata := findATA(user, mint, tokProg)
	ok := accountExists(ctx, ata)
	if ok {
		markUserATAKnown(user, mint, tokProg)
	}
	return ok
}

func userATAExistsCachedWithHit(ctx context.Context, user, mint, tokProg solana.PublicKey) (bool, bool) {
	key := ataKnownKey(user, mint, tokProg)
	ataKnownMu.RLock()
	if ataKnownUser[key] {
		ataKnownMu.RUnlock()
		return true, true
	}
	ataKnownMu.RUnlock()
	ata := findATA(user, mint, tokProg)
	ok := accountExists(ctx, ata)
	if ok {
		markUserATAKnown(user, mint, tokProg)
	}
	return ok, false
}

func requestWSRestart() {
	select {
	case wsRestartCh <- struct{}{}:
	default:
	}
}

// rpcFallback: если true и Jito не ответил — отправить через Helius RPC (важно для SELL, иначе позиция «зависает»).
func sendTx(ctx context.Context, tx *solana.Transaction, rpcFallback bool) (solana.Signature, error) {
	// BUY: optionally send as Jito bundle (sendBundle) for validator-side atomic inclusion.
	// This is not the same as RPC pre-sim; BE decides inclusion close to block production.
	if cfg.JitoBundleBuy && !rpcFallback && len(cfg.JitoBundleURLs) > 0 {
		sig, err := sendBuyAsJitoBundleRace(ctx, cfg.JitoBundleURLs, tx)
		if err == nil {
			return sig, nil
		}
		// If bundle fails due to global rate limit / congestion, do NOT fallback to paid sendTransaction.
		// Better to skip the entry than to pay fees and likely fail/late-fill in the same congested conditions.
		if strings.Contains(err.Error(), "429") || strings.Contains(strings.ToLower(err.Error()), "rate limit") || strings.Contains(strings.ToLower(err.Error()), "congest") {
			return solana.Signature{}, err
		}
		// Otherwise fall through to normal Jito HTTP.
		log.Printf("[TX] Jito bundle BUY failed → fallback | %v", err)
	}
	if cfg.JitoHTTP && cfg.JitoHTTPURL != "" {
		sig, err := sendTxViaJitoHTTP(ctx, cfg.JitoHTTPURL, tx)
		if err == nil {
			return sig, nil
		}
		if rpcFallback {
			log.Printf("[TX] Jito failed → RPC fallback | %v", err)
			rpcWait()
			noRetry := uint(0)
			sig2, err2 := rpcClient().SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
				SkipPreflight: true,
				MaxRetries:    &noRetry,
			})
			rpcNote(err2)
			return sig2, err2
		}
		// BUY: не уходим в RPC — поздний вход хуже отмены
		return solana.Signature{}, err
	}
	rpcWait()
	noRetry := uint(0)
	sig, err := rpcClient().SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
		SkipPreflight: true,
		MaxRetries:    &noRetry,
	})
	rpcNote(err)
	return sig, err
}

func preSimBuyTx(ctx context.Context, tx *solana.Transaction) error {
	to := cfg.PreSimTimeout
	if to <= 0 {
		to = 140 * time.Millisecond
	}
	sctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()

	// Simulate exactly what we are about to send.
	rpcWait()
	out, err := rpcClient().SimulateTransactionWithOpts(sctx, tx, &rpc.SimulateTransactionOpts{
		SigVerify:              false,
		ReplaceRecentBlockhash: false,
		Commitment:             rpc.CommitmentProcessed,
	})
	rpcNote(err)
	if err != nil {
		return err
	}
	if out == nil || out.Value == nil {
		return errors.New("nil simulation result")
	}
	if out.Value.Err != nil {
		return fmt.Errorf("sim err: %v", out.Value.Err)
	}
	return nil
}

func sendTxViaJitoHTTP(ctx context.Context, url string, tx *solana.Transaction) (solana.Signature, error) {
	bin, err := tx.MarshalBinary()
	if err != nil {
		return solana.Signature{}, err
	}
	b64 := base64.StdEncoding.EncodeToString(bin)
	reqBody, _ := json.Marshal(map[string]any{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "sendTransaction",
		"params": []any{
			b64,
			map[string]any{"encoding": "base64"},
		},
	})
	hreq, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "application/json")
	to := cfg.JitoHTTPTimeout
	if to <= 0 {
		to = 400 * time.Millisecond
	}
	c := &http.Client{Timeout: to}
	r, err := c.Do(hreq)
	if err != nil {
		return solana.Signature{}, err
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		return solana.Signature{}, fmt.Errorf("HTTP %d", r.StatusCode)
	}
	var out struct {
		Result string `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &out) != nil {
		return solana.Signature{}, fmt.Errorf("bad json")
	}
	if out.Error != nil {
		return solana.Signature{}, fmt.Errorf("jito err %d: %s", out.Error.Code, out.Error.Message)
	}
	sig, err := solana.SignatureFromBase58(out.Result)
	if err != nil {
		return solana.Signature{}, err
	}
	return sig, nil
}

func sendBuyAsJitoBundle(ctx context.Context, url string, tx *solana.Transaction) (solana.Signature, error) {
	bin, err := tx.MarshalBinary()
	if err != nil {
		return solana.Signature{}, err
	}
	b64 := base64.StdEncoding.EncodeToString(bin)

	// sendBundle: params: [[tx1..txN], {encoding:"base64"}]
	reqBody, _ := json.Marshal(map[string]any{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "sendBundle",
		"params": []any{
			[]string{b64},
			map[string]any{"encoding": "base64"},
		},
	})
	hreq, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "application/json")
	to := cfg.JitoHTTPTimeout
	if to <= 0 {
		to = 400 * time.Millisecond
	}
	c := &http.Client{Timeout: to}
	r, err := c.Do(hreq)
	if err != nil {
		return solana.Signature{}, err
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		msg := strings.TrimSpace(string(body))
		if len(msg) > 400 {
			msg = msg[:400] + "…"
		}
		return solana.Signature{}, fmt.Errorf("HTTP %d: %s", r.StatusCode, msg)
	}

	// We return tx signature (what the rest of the code expects).
	// sendBundle returns bundle id; for our logging/tracking we can still use txSig.
	var out struct {
		Result any `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &out)
	if out.Error != nil {
		return solana.Signature{}, fmt.Errorf("jito bundle err %d: %s", out.Error.Code, out.Error.Message)
	}
	sig := tx.Signatures[0]
	if sig == (solana.Signature{}) {
		return solana.Signature{}, errors.New("nil tx signature")
	}
	return sig, nil
}

func sendBuyAsJitoBundleRace(ctx context.Context, urls []string, tx *solana.Transaction) (solana.Signature, error) {
	type res struct {
		url string
		sig solana.Signature
		err error
	}
	ch := make(chan res, len(urls))
	for _, u := range urls {
		u := strings.TrimSpace(u)
		if u == "" {
			continue
		}
		go func(url string) {
			sig, err := sendBuyAsJitoBundle(ctx, url, tx)
			ch <- res{url: url, sig: sig, err: err}
		}(u)
	}
	var firstErr error
	for i := 0; i < len(urls); i++ {
		r := <-ch
		if r.err == nil {
			return r.sig, nil
		}
		if firstErr == nil {
			firstErr = r.err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("no bundle urls")
	}
	return solana.Signature{}, firstErr
}

func isSlippageErr(err error) bool {
	if err == nil {
		return false
	}
	// common: "custom program error: 0xbc4"
	s := strings.ToLower(err.Error())
	// 0xbc4: classic slippage error
	// custom:6003: часто означает, что min_sol_out/min constraints не прошли (по сути тоже slippage на выходе)
	// custom:6000: часто встречается на pump-style curves как "min_tokens_out constraint failed"
	// (т.е. опять-таки слиппедж/минимальные выходные ограничения).
	return strings.Contains(s, "0xbc4") ||
		strings.Contains(s, "slippage") ||
		strings.Contains(s, "custom:6003") ||
		strings.Contains(s, "custom:6000")
}

func isBuyCustomErr6042(err error) bool {
	if err == nil {
		return false
	}
	// seen as: InstructionError ... Custom:6042
	return strings.Contains(err.Error(), "Custom:6042") || strings.Contains(err.Error(), "custom:6042")
}

func registerBuyFailure(now time.Time, err error) {
	if !(isBuyCustomErr6042(err) || isSlippageErr(err)) {
		return
	}

	// Пауза только от последовательных неудачных входов:
	// ограничивает "платить комиссии за очевидно неподходящие constraints".
	const (
		window    = 30 * time.Second
		threshold = 3
		pause     = 10 * time.Second
	)

	buyFailMu.Lock()
	defer buyFailMu.Unlock()

	buyFailTimes = append(buyFailTimes, now)
	cutoff := now.Add(-window)
	i := 0
	for ; i < len(buyFailTimes); i++ {
		if buyFailTimes[i].After(cutoff) {
			break
		}
	}
	if i > 0 {
		buyFailTimes = buyFailTimes[i:]
	}

	if len(buyFailTimes) >= threshold {
		cnt := len(buyFailTimes)
		// Сбрасываем историю, чтобы не уходить в бесконечные паузы подряд.
		buyFailTimes = nil
		buyPauseUntil.Store(now.Add(pause).UnixMilli())
		log.Printf("[BUY] Fee-protection: pause %.0fs after %d BUY FAILs | lastErr=%v", pause.Seconds(), cnt, err)
	}
}

type scrapeEv struct {
	ev       *TradeEvent
	recvTime time.Time
}

// ═══════════════════════════════════════════════════════════════════════════════
//  MAIN
// ═══════════════════════════════════════════════════════════════════════════════

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	_ = godotenv.Load()
	cfg = loadCfg()
	loadPinnedWallets()
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
	if cfg.EarlySLWindow > 0 {
		log.Printf("[INIT] Ставка %.4f SOL | TP +%.0f%% QUICKTP≤15s ≥+%.0f%% | EARLYSL первые %ds ≤-%.0f%% | SL -%.0f%% | TimeKill %ds<%+.0f%%",
			float64(cfg.BuyLamp)/1e9, cfg.TP*100, cfg.QuickTPPct*100, int(cfg.EarlySLWindow.Seconds()), cfg.EarlySLPct*100, cfg.SL*100, cfg.TimeKillSec, cfg.TimeKillMin*100)
	} else {
		log.Printf("[INIT] Ставка %.4f SOL | TP +%.0f%% QUICKTP≤15s ≥+%.0f%% | EARLYSL выкл. | SL -%.0f%% | TimeKill %ds<%+.0f%%",
			float64(cfg.BuyLamp)/1e9, cfg.TP*100, cfg.QuickTPPct*100, cfg.SL*100, cfg.TimeKillSec, cfg.TimeKillMin*100)
	}
	if cfg.PeakPullbackPct > 0 {
		log.Printf("[INIT] PEAK_PULLBACK: при пике ≥+%.1f%% — SELL если откат от пика ≥%.1f п.п. (медленное скатывание); TRAIL множ.=%.0f%% от пика",
			cfg.TrailMinPeak*100, cfg.PeakPullbackPct*100, cfg.TrailRelMult*100)
	} else {
		log.Printf("[INIT] PEAK_PULLBACK выкл.; TRAIL только относительный (пик≥+%.1f%%, текущий pnl < %.0f%% от пика)",
			cfg.TrailMinPeak*100, cfg.TrailRelMult*100)
	}
	log.Printf("[INIT] BuySlip %dbps SellSlip %dbps | PrioBuy %.4f PrioSell %.4f SOL",
		cfg.Slip, cfg.SellSlip, float64(cfg.PrioLamp)/1e9, float64(cfg.PrioLampSell)/1e9)
	if cfg.MonitorFastYoungSec > 0 {
		log.Printf("[INIT] MaxTargets %d | MaxPos %d | Scrape %v | Monitor %dms + fast %ds после входа",
			cfg.MaxTargets, cfg.MaxPositions, cfg.ScrapeIvl, cfg.MonitorMs, cfg.MonitorFastYoungSec)
	} else {
		log.Printf("[INIT] MaxTargets %d | MaxPos %d | Scrape %v | Monitor %dms (fast только при pnl>+2%%)",
			cfg.MaxTargets, cfg.MaxPositions, cfg.ScrapeIvl, cfg.MonitorMs)
	}
	if cfg.FlashSLWindow > 0 {
		if cfg.FlashSLMinAge > 0 {
			log.Printf("[INIT] FLASHSL с %ds до %ds при ≤-%.0f%% (без мгновенного срабатывания на шуме кривой)",
				int(cfg.FlashSLMinAge.Seconds()), int(cfg.FlashSLWindow.Seconds()), cfg.FlashSLPct*100)
		} else {
			log.Printf("[INIT] FLASHSL первые %ds при ≤-%.0f%%", int(cfg.FlashSLWindow.Seconds()), cfg.FlashSLPct*100)
		}
	} else {
		log.Printf("[INIT] FLASHSL выкл.")
	}
	log.Printf("[INIT] MinReserve %.4f SOL | MaxSessionLoss -%.0f%% (от стартового баланса, не сумма %% по сделкам)",
		float64(cfg.MinReserve)/1e9, cfg.MaxSessionLoss)
	log.Printf("[INIT] LagShields: tokeninfo<=%dms signal<=%dms",
		cfg.MaxTokInfoLag.Milliseconds(), cfg.MaxSignalLag.Milliseconds())
	log.Printf("[INIT] CopyFilters: whale>=%.2f SOL | minVSR>=%.1f SOL | mintCooldown=%v",
		float64(cfg.MinWhaleBuyLam)/1e9, float64(cfg.MinVSRLam)/1e9, cfg.MintCD)
	if cfg.MintMinPumpBuys > 0 {
		log.Printf("[INIT] Mint depth: ≥%d pump-Buy по mint за %v (глобальный WS; 0 в .env = выкл.)",
			cfg.MintMinPumpBuys, cfg.MintMinPumpBuysWindow.Round(time.Second))
	}
	if cfg.BadEntrySLPct > 0 {
		log.Printf("[INIT] BAD_ENTRY_SL: при плохом входе полный стоп −%.0f%% вместо −%.0f%%",
			cfg.BadEntrySLPct*100, cfg.SL*100)
	}
	if cfg.BadExitFastSec > 0 {
		log.Printf("[INIT] BADEXITfast: при BadEntry выход в минус через %ds (раньше чем 10с BADEXIT)", cfg.BadExitFastSec)
	}
	if cfg.BadEntryFlashPct > 0 {
		log.Printf("[INIT] BAD_ENTRY_FLASH: при BadEntry FLASHSL по −%.0f%% (а не −%.0f%%)",
			cfg.BadEntryFlashPct*100, cfg.FlashSLPct*100)
	}
	if cfg.MintLossCD > 0 {
		log.Printf("[INIT] После убытка по mint — пауза повторного входа %v (MINT_LOSS_COOLDOWN_SEC; 0=выкл.)",
			cfg.MintLossCD.Round(time.Second))
	} else {
		log.Printf("[INIT] MINT_LOSS_COOLDOWN выкл. — можно сразу снова покупать тот же mint после минуса")
	}
	if cfg.MintWinCD > 0 {
		log.Printf("[INIT] После профита по mint — пауза повторного входа %v (MINT_REBUY_COOLDOWN_AFTER_WIN_SEC; 0=выкл.)",
			cfg.MintWinCD.Round(time.Second))
	}
	if cfg.SessionTrailGiveback > 0 && cfg.SessionTrailMinPeak > 0 {
		if cfg.SessionTrailNetRedPct > 0 {
			log.Printf("[INIT] Session trail: пик≥+%.2f%%, откат≥%.2f п.п. — стоп BUY только если сессия ≤−%.2f%%",
				cfg.SessionTrailMinPeak, cfg.SessionTrailGiveback, cfg.SessionTrailNetRedPct)
		} else {
			log.Printf("[INIT] Session trail: при пике сессии ≥+%.2f%% — стоп BUY если откат от пика ≥%.2f п.п. (проверка с задержкой 2с)",
				cfg.SessionTrailMinPeak, cfg.SessionTrailGiveback)
		}
	} else {
		log.Printf("[INIT] Session trail выкл. (SESSION_TRAIL_MIN_PEAK_PCT / GIVEBACK_PCT)")
	}
	if cfg.ScrapeSameMintMin > 0 {
		log.Printf("[INIT] Scrape: same-mint min=%d (эвристика N баёв в один mint; 0 в .env = только profit/крупный бай)", cfg.ScrapeSameMintMin)
	} else {
		log.Printf("[INIT] Scrape: same-mint выкл. — цели только profit>30%% или крупный бай (план A)")
	}
	log.Printf("[INIT] BC retries: max=%d sleep=%v lagBuf=%v | badEntry if скорр. < -%.0f%%",
		cfg.BCRetryMax, cfg.BCRetrySleep, cfg.BCSigLagBuf, cfg.BadEntryPct)
	if cfg.BreakevenMinPeak > 0 {
		log.Printf("[INIT] BREAKEVEN только после пика ≥+%.0f%% (иначе ветка отключена)", cfg.BreakevenMinPeak*100)
	} else {
		log.Printf("[INIT] BREAKEVEN выкл. (BREAKEVEN_MIN_PEAK_PCT=0)")
	}
	log.Printf("[INIT] NET_EXIT floor: +%.2f%% (auto fixed-cost+buffer; MIN_NET_EXIT_PCT override)",
		minNetExitThreshold()*100)
	if cfg.ProfitLockMinPeak > 0 {
		log.Printf("[INIT] PROFIT_LOCK: при пике ≥+%.0f%% держим минимум %+.2f%%",
			cfg.ProfitLockMinPeak*100, cfg.ProfitLockFloor*100)
	} else {
		log.Printf("[INIT] PROFIT_LOCK выкл. (PROFIT_LOCK_MIN_PEAK_PCT=0)")
	}
	if cfg.ScrapeAutoTargets {
		if len(cfg.CopyWallets) > 0 {
			log.Printf("[INIT] Цели: авто WS-скрейп + %d доп. из COPY_WALLETS", len(cfg.CopyWallets))
		} else {
			log.Printf("[INIT] Цели: только авто-поиск по WS (скрейп), ручной список не нужен — COPY_WALLETS пуст")
		}
	} else {
		log.Printf("[INIT] Цели: только COPY_WALLETS (%d шт.), авто-скрейп выключен", len(cfg.CopyWallets))
		if len(cfg.CopyWallets) == 0 {
			log.Printf("[INIT] ⚠ SCRAPE_AUTO_TARGETS=0 и пустой COPY_WALLETS — добавь адреса или вручную включи SCRAPE_AUTO_TARGETS=1")
		}
	}
	if cfg.ObserveOnly {
		log.Printf("[INIT] OBSERVE_ONLY=1: новые кошельки сначала кандидаты; в targets попадут только после утверждения (CAND_*) или если PINNED/COPY_WALLETS")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buyCtx, buyCancel = context.WithCancel(ctx)

	if hasKey {
		rpcWait()
		b, err := rpcClient().GetBalance(ctx, cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
		rpcNote(err)
		if err == nil {
			setWalletBalCache(b.Value)
			log.Printf("[INIT] Баланс: %.6f SOL", float64(b.Value)/1e9)
			initSessionRefBalance(b.Value)
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
				checkAll(ctx) // EXIT: не важен fast-тик
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
		BuyLamp:                  7_000_000, // 0.007 SOL
		Slip:                     2000,      // 20% buy slippage
		SellSlip:                 4000,      // 40% sell slippage
		PrioLamp:                 2_000_000, // 0.002 SOL buy priority
		PrioLampSell:             1_500_000, // 0.0015 SOL sell priority
		TP:                       0.40,
		QuickTPPct:               0.22, // +22% за первые 15с — иначе ждём основной TP
		EarlySLWindow:            5 * time.Second,
		EarlySLPct:               0.08, // −8% в первые 5с — на pump часто шум; см. EARLYSL_* в .env
		SL:                       0.25,
		BreakevenMinPeak:         0.30,
		ProfitLockMinPeak:        0.06,  // +6% peak → начинаем «не отдавать»
		ProfitLockFloor:          0.005, // держим минимум +0.5% (учёт комиссий/проскальзывания)
		TrailMinPeak:             0.03,  // +3%% пика — уже следим за откатом (не ждать только +4%%)
		PeakPullbackPct:          0.05,  // −5 п.п. от пика → SELL (анти «висел в плюсе и уехал в минус»)
		TrailRelMult:             0.65,
		ScrapeAutoTargets:        true,
		TimeKillSec:              60,
		TimeKillMin:              0.05,
		TimeKillMaxLoss:          0.02,
		SoftHardKillSec:          90,
		SoftHardKillMaxLoss:      0.10,
		MinNetExitPct:            0,
		NetExitBufPct:            0.005, // +0.5% поверх fixed-cost
		MaxTargets:               50,
		MaxPositions:             2,
		ScrapeIvl:                3 * time.Minute,
		MonitorMs:                1000,
		MonitorFastYoungSec:      25,
		FlashSLWindow:            4 * time.Second,
		FlashSLPct:               0.12,       // −12% в первые 4с — до EARLYSL/SL
		MinReserve:               15_000_000, // 0.015 SOL
		MaxSessionLoss:           30.0,       // -30%
		WalletCD:                 5 * time.Second,
		MaxTokInfoLag:            500 * time.Millisecond,
		MaxSignalLag:             250 * time.Millisecond,
		PreSimBuy:                false,
		PreSimTimeout:            140 * time.Millisecond,
		PreSimLagBudget:          80 * time.Millisecond,
		BuyConfirmTimeout:        12 * time.Second,
		JitoHTTPURL:              "https://mainnet.block-engine.jito.wtf/api/v1/transactions",
		JitoHTTPTimeout:          400 * time.Millisecond,
		JitoBundleBuy:            false,
		JitoBundleURL:            "https://mainnet.block-engine.jito.wtf/api/v1/bundles",
		// Official Jito tip accounts (fallback pool). Pick one to write-lock for bundle eligibility.
		JitoTipAccs: []solana.PublicKey{
			solana.MustPublicKeyFromBase58("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49"),
			solana.MustPublicKeyFromBase58("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY"),
			solana.MustPublicKeyFromBase58("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe"),
			solana.MustPublicKeyFromBase58("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh"),
			solana.MustPublicKeyFromBase58("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"),
			solana.MustPublicKeyFromBase58("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt"),
			solana.MustPublicKeyFromBase58("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"),
			solana.MustPublicKeyFromBase58("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL"),
		},
		MinWhaleBuyLam:           200_000_000,    // 0.2 SOL
		MinVSRLam:                20_000_000_000, // 20 SOL liquidity floor
		MintMinAge:               0,
		MintCD:                   90 * time.Second,
		MintLossCD:               600 * time.Second, // 10 мин после минуса по mint — не повторять вход как с 6T7m
		SessionTrailMinPeak:      0,                 // включи в .env: SESSION_TRAIL_*
		SessionTrailGiveback:     0,
		SessionTrailNetRedPct:    0,
		BCRetryMax:               12,
		BCRetrySleep:             100 * time.Millisecond,
		BCSigLagBuf:              100 * time.Millisecond,
		BadEntryPct:              20,
		BadEntryImmediateSellPct: 8,               // если fill хуже -8% — закрываем сразу
		BadEntryImmediateMinAge:  2 * time.Second, // дать подтвердиться/обновиться ATA
		BadEntrySLPct:            0.14,
		BadExitFastSec:           4,
		BadEntryFlashPct:         0.10,
		ObserveOnly:              false,
		CandMinAge:               10 * time.Minute,
		CandMinTrades:            5,
		CandMinProfitPct:         0.30,
		CandMinTotalBuyLam:       5_000_000_000, // 5 SOL
		CandRequireWin2x:         false,
		CopySellExit:             false,
		CopySellMinAgeSec:        3,
		VSRSExit:                 false,
		VSRSWindowSec:            6,
		VSRSMinUpSOLPerSec:       0.8,
		VSRSMinPeakPnlPct:        6,
		VSRSMinAgeSec:            8,
		MintMinPumpBuys:          0,
		MintMinPumpBuysWindow:    12 * time.Second,
		ScrapeSameMintMin:        5,    // вариант B: было жёстко 3
		HolderMaxPct:             0.10, // 10%: анти-концентрация холдеров
		HolderTopN:               20,
		HolderCheckMinVSR:        80_000_000_000, // >=80 SOL: holder-check skip ради скорости
		WalletBalCacheTTL:        5 * time.Second,
		PinWalletMinPnl:          0.08, // +8% и выше — закрепляем кошелёк
		PinWalletsFile:           "pinned_wallets.txt",
		PinUnpinLosses:           2,
	}
	// RPC/WSS override (allows Helius Gatekeeper / other providers)
	// If not set, fall back to Helius URLs from HELIUS_API_KEY.
	if u := strings.TrimSpace(os.Getenv("RPC_URL")); u != "" {
		c.RPC = u
	}
	if u := strings.TrimSpace(os.Getenv("WSS_URL")); u != "" {
		c.WSS = u
	}
	if v := strings.TrimSpace(os.Getenv("HELIUS_API_KEY")); v != "" {
		// Optional: use Gatekeeper RPC (beta) without manually pasting URL.
		if c.RPC == "" && ev("HELIUS_GATEKEEPER_RPC", "0") == "1" {
			c.RPC = "https://beta.helius-rpc.com/?api-key=" + v
		}
		if c.RPC == "" {
			c.RPC = "https://mainnet.helius-rpc.com/?api-key=" + v
		}
		if c.WSS == "" {
			c.WSS = "wss://mainnet.helius-rpc.com/?api-key=" + v
		}
	} else if c.RPC == "" || c.WSS == "" {
		log.Fatalf("[CFG] HELIUS_API_KEY обязателен (если не задан RPC_URL/WSS_URL)")
	}
	// Optional multi-WSS race: additional WS endpoints (comma separated).
	if v := strings.TrimSpace(os.Getenv("WSS_URLS")); v != "" {
		parts := strings.Split(v, ",")
		for _, p := range parts {
			u := strings.TrimSpace(p)
			if u == "" {
				continue
			}
			c.WSSURLs = append(c.WSSURLs, u)
		}
	}
	// Always include primary WSS as first option.
	if c.WSS != "" {
		c.WSSURLs = append([]string{c.WSS}, c.WSSURLs...)
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
	c.JitoTipLamp = evU("JITO_TIP_LAMPORTS", c.JitoTipLamp)
	c.JitoHTTP = ev("JITO_HTTP_SEND", "0") == "1"
	if u := os.Getenv("JITO_HTTP_URL"); u != "" {
		c.JitoHTTPURL = u
	}
	c.JitoBundleBuy = ev("JITO_BUNDLE_BUY", "0") == "1"
	if u := os.Getenv("JITO_BUNDLE_URL"); u != "" {
		c.JitoBundleURL = u
	} else if strings.Contains(c.JitoHTTPURL, "/api/v1/transactions") {
		// удобный дефолт: если задан /transactions, то bundle рядом
		c.JitoBundleURL = strings.ReplaceAll(c.JitoHTTPURL, "/api/v1/transactions", "/api/v1/bundles")
	}
	if v := strings.TrimSpace(os.Getenv("JITO_BUNDLE_URLS")); v != "" {
		for _, p := range strings.Split(v, ",") {
			u := strings.TrimSpace(p)
			if u != "" {
				c.JitoBundleURLs = append(c.JitoBundleURLs, u)
			}
		}
	}
	if c.JitoBundleURL != "" {
		c.JitoBundleURLs = append([]string{c.JitoBundleURL}, c.JitoBundleURLs...)
	}
	if ms := evU("JITO_HTTP_TIMEOUT_MS", 0); ms > 0 {
		c.JitoHTTPTimeout = time.Duration(ms) * time.Millisecond
	}
	if a := os.Getenv("JITO_TIP_ACCOUNT"); a != "" {
		if pk, err := solana.PublicKeyFromBase58(a); err == nil {
			c.JitoTipAcc = pk
		}
	}
	c.TP = evF("TAKE_PROFIT_PCT", c.TP*100) / 100
	c.QuickTPPct = evF("QUICKTP_MIN_PCT", c.QuickTPPct*100) / 100
	c.SL = evF("STOP_LOSS_PCT", c.SL*100) / 100
	if v := os.Getenv("EARLYSL_SEC"); v != "" {
		if v == "0" {
			c.EarlySLWindow = 0
		} else if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.EarlySLWindow = time.Duration(n) * time.Second
		}
	}
	c.EarlySLPct = evF("EARLYSL_MIN_PCT", c.EarlySLPct*100) / 100
	if v := strings.TrimSpace(os.Getenv("BAD_ENTRY_IMMEDIATE_SELL_PCT")); v == "0" {
		c.BadEntryImmediateSellPct = 0
	} else {
		c.BadEntryImmediateSellPct = evF("BAD_ENTRY_IMMEDIATE_SELL_PCT", c.BadEntryImmediateSellPct)
	}
	if s := evU("BAD_ENTRY_IMMEDIATE_MIN_AGE_SEC", 0); s > 0 {
		c.BadEntryImmediateMinAge = time.Duration(s) * time.Second
	}
	c.BreakevenMinPeak = evF("BREAKEVEN_MIN_PEAK_PCT", c.BreakevenMinPeak*100) / 100
	if v := strings.TrimSpace(os.Getenv("PROFIT_LOCK_MIN_PEAK_PCT")); v == "0" {
		c.ProfitLockMinPeak = 0
	} else {
		c.ProfitLockMinPeak = evF("PROFIT_LOCK_MIN_PEAK_PCT", c.ProfitLockMinPeak*100) / 100
	}
	c.ProfitLockFloor = evF("PROFIT_LOCK_FLOOR_PCT", c.ProfitLockFloor*100) / 100
	c.TrailMinPeak = evF("TRAIL_MIN_PEAK_PCT", c.TrailMinPeak*100) / 100
	if v := strings.TrimSpace(os.Getenv("PEAK_PULLBACK_PCT")); v == "0" {
		c.PeakPullbackPct = 0
	} else {
		c.PeakPullbackPct = evF("PEAK_PULLBACK_PCT", c.PeakPullbackPct*100) / 100
	}
	c.TrailRelMult = evF("TRAIL_REL_MULT_PCT", c.TrailRelMult*100) / 100
	if c.TrailRelMult <= 0 || c.TrailRelMult >= 0.999 {
		c.TrailRelMult = 0.65
	}
	c.ScrapeAutoTargets = ev("SCRAPE_AUTO_TARGETS", "1") != "0"
	c.ObserveOnly = ev("OBSERVE_ONLY", "0") == "1"
	c.CopySellExit = ev("COPY_SELL_EXIT", "0") == "1"
	c.CopySellMinAgeSec = int(evU("COPY_SELL_MIN_AGE_SEC", uint64(c.CopySellMinAgeSec)))
	c.VSRSExit = ev("VSR_SPEED_EXIT", "0") == "1"
	c.VSRSWindowSec = int(evU("VSR_SPEED_WINDOW_SEC", uint64(c.VSRSWindowSec)))
	c.VSRSMinUpSOLPerSec = evF("VSR_SPEED_MIN_UP_SOL_PER_SEC", c.VSRSMinUpSOLPerSec)
	c.VSRSMinPeakPnlPct = evF("VSR_SPEED_MIN_PEAK_PNL_PCT", c.VSRSMinPeakPnlPct)
	c.VSRSMinAgeSec = int(evU("VSR_SPEED_MIN_AGE_SEC", uint64(c.VSRSMinAgeSec)))
	if v := strings.TrimSpace(os.Getenv("COPY_WALLETS")); v != "" {
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if _, err := solana.PublicKeyFromBase58(part); err != nil {
				log.Printf("[CFG] COPY_WALLETS: пропуск невалидного адреса %q", part)
				continue
			}
			c.CopyWallets = append(c.CopyWallets, part)
		}
	}
	c.TimeKillSec = int(evU("TIMEKILL_SEC", uint64(c.TimeKillSec)))
	c.TimeKillMin = evF("TIMEKILL_MIN_PCT", c.TimeKillMin*100) / 100
	c.TimeKillMaxLoss = evF("TIMEKILL_MAX_LOSS_PCT", c.TimeKillMaxLoss*100) / 100
	c.SoftHardKillSec = int(evU("SOFT_HARDKILL_SEC", uint64(c.SoftHardKillSec)))
	c.SoftHardKillMaxLoss = evF("SOFT_HARDKILL_MAX_LOSS_PCT", c.SoftHardKillMaxLoss*100) / 100
	if v := strings.TrimSpace(os.Getenv("MIN_NET_EXIT_PCT")); v == "0" {
		c.MinNetExitPct = 0
	} else if v != "" {
		c.MinNetExitPct = evF("MIN_NET_EXIT_PCT", 0) / 100
	}
	c.NetExitBufPct = evF("NET_EXIT_BUFFER_PCT", c.NetExitBufPct*100) / 100
	c.MaxTargets = int(evU("MAX_TARGETS", uint64(c.MaxTargets)))
	c.MaxPositions = int(evU("MAX_POSITIONS", uint64(c.MaxPositions)))
	if s := evU("WALLET_COOLDOWN_SEC", 0); s > 0 {
		c.WalletCD = time.Duration(s) * time.Second
	}
	if ms := evU("MAX_TOKENINFO_LAG_MS", 0); ms > 0 {
		c.MaxTokInfoLag = time.Duration(ms) * time.Millisecond
	}
	if ms := evU("MAX_SIGNAL_LAG_MS", 0); ms > 0 {
		c.MaxSignalLag = time.Duration(ms) * time.Millisecond
	}
	c.PreSimBuy = ev("PRE_SIMULATE_BUY", "0") == "1"
	if ms := evU("PRE_SIM_TIMEOUT_MS", 0); ms > 0 {
		c.PreSimTimeout = time.Duration(ms) * time.Millisecond
	}
	if ms := evU("PRE_SIM_LAG_BUDGET_MS", 0); ms > 0 {
		c.PreSimLagBudget = time.Duration(ms) * time.Millisecond
	}
	if s := evU("BUY_CONFIRM_TIMEOUT_SEC", 0); s > 0 {
		c.BuyConfirmTimeout = time.Duration(s) * time.Second
	}
	if v := os.Getenv("MIN_WHALE_BUY_SOL"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.MinWhaleBuyLam = uint64(f * 1e9)
		}
	}
	c.MinVSRLam = evU("MIN_VSR_LAMPORTS", c.MinVSRLam)
	if v := strings.TrimSpace(os.Getenv("HOLDER_MAX_PCT")); v == "0" {
		c.HolderMaxPct = 0
	} else if v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.HolderMaxPct = f / 100
		}
	}
	if n := evU("HOLDER_TOPN", 0); n > 0 {
		c.HolderTopN = int(n)
	}
	c.HolderCheckMinVSR = evU("HOLDER_CHECK_MIN_VSR_LAMPORTS", c.HolderCheckMinVSR)
	if ms := evU("WALLET_BAL_CACHE_TTL_MS", 0); ms > 0 {
		c.WalletBalCacheTTL = time.Duration(ms) * time.Millisecond
	}
	if s := evU("MINT_MIN_AGE_SEC", 0); s > 0 {
		c.MintMinAge = time.Duration(s) * time.Second
	}
	if s := evU("CAND_MIN_AGE_SEC", 0); s > 0 {
		c.CandMinAge = time.Duration(s) * time.Second
	}
	c.CandMinTrades = int(evU("CAND_MIN_TRADES", uint64(c.CandMinTrades)))
	c.CandMinProfitPct = evF("CAND_MIN_PROFIT_PCT", c.CandMinProfitPct*100) / 100
	if v := os.Getenv("CAND_MIN_TOTAL_BUY_SOL"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.CandMinTotalBuyLam = uint64(f * 1e9)
		}
	}
	c.CandRequireWin2x = ev("CAND_REQUIRE_2X24H", "0") == "1"
	if v := strings.TrimSpace(os.Getenv("MINT_REBUY_COOLDOWN_AFTER_WIN_SEC")); v == "0" {
		c.MintWinCD = 0
	} else if v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.MintWinCD = time.Duration(n) * time.Second
		}
	}
	if v := strings.TrimSpace(os.Getenv("SCRAPE_SAME_MINT_MIN")); v == "0" {
		c.ScrapeSameMintMin = 0
	} else {
		c.ScrapeSameMintMin = int(evU("SCRAPE_SAME_MINT_MIN", uint64(c.ScrapeSameMintMin)))
	}
	if v := strings.TrimSpace(os.Getenv("PIN_WALLET_MIN_PNL_PCT")); v == "0" {
		c.PinWalletMinPnl = 0
	} else {
		c.PinWalletMinPnl = evF("PIN_WALLET_MIN_PNL_PCT", c.PinWalletMinPnl*100) / 100
	}
	if v := strings.TrimSpace(os.Getenv("PIN_WALLETS_FILE")); v != "" {
		c.PinWalletsFile = v
	}
	if v := strings.TrimSpace(os.Getenv("PIN_UNPIN_AFTER_LOSSES")); v == "0" {
		c.PinUnpinLosses = 0
	} else if v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.PinUnpinLosses = n
		}
	}
	if s := evU("MINT_COOLDOWN_SEC", 0); s > 0 {
		c.MintCD = time.Duration(s) * time.Second
	}
	if v := strings.TrimSpace(os.Getenv("MINT_LOSS_COOLDOWN_SEC")); v == "0" {
		c.MintLossCD = 0
	} else if v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.MintLossCD = time.Duration(n) * time.Second
		}
	}
	c.SessionTrailMinPeak = evF("SESSION_TRAIL_MIN_PEAK_PCT", c.SessionTrailMinPeak)
	c.SessionTrailGiveback = evF("SESSION_TRAIL_GIVEBACK_PCT", c.SessionTrailGiveback)
	c.SessionTrailNetRedPct = evF("SESSION_TRAIL_NET_RED_PCT", c.SessionTrailNetRedPct)
	if m := evU("SCRAPE_INTERVAL_MIN", 0); m > 0 {
		c.ScrapeIvl = time.Duration(m) * time.Minute
	}
	c.MonitorMs = int(evU("MONITOR_MS", uint64(c.MonitorMs)))
	if v := strings.TrimSpace(os.Getenv("MONITOR_FAST_YOUNG_SEC")); v != "" {
		if v == "0" {
			c.MonitorFastYoungSec = 0
		} else if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MonitorFastYoungSec = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("FLASH_SL_SEC")); v != "" {
		if v == "0" {
			c.FlashSLWindow = 0
		} else if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.FlashSLWindow = time.Duration(n) * time.Second
		}
	}
	c.FlashSLPct = evF("FLASH_SL_MIN_PCT", c.FlashSLPct*100) / 100
	if v := strings.TrimSpace(os.Getenv("FLASH_SL_MIN_ENTRY_SEC")); v == "0" {
		c.FlashSLMinAge = 0
	} else if n := evU("FLASH_SL_MIN_ENTRY_SEC", 0); n > 0 {
		c.FlashSLMinAge = time.Duration(n) * time.Second
	} else if c.FlashSLWindow > 0 {
		c.FlashSLMinAge = 5 * time.Second
	}
	if c.FlashSLWindow > 0 && c.FlashSLMinAge > 0 && c.FlashSLWindow <= c.FlashSLMinAge {
		c.FlashSLWindow = c.FlashSLMinAge + 10*time.Second
		log.Printf("[CFG] FLASH_SL_SEC увеличен до %.0fs (должен быть > FLASH_SL_MIN_ENTRY_SEC=%.0fs)",
			c.FlashSLWindow.Seconds(), c.FlashSLMinAge.Seconds())
	}
	if n := evU("BC_RETRY_MAX", 0); n > 0 {
		c.BCRetryMax = int(n)
	}
	if ms := evU("BC_RETRY_SLEEP_MS", 0); ms > 0 {
		c.BCRetrySleep = time.Duration(ms) * time.Millisecond
	}
	if ms := evU("BC_SIGNAL_LAG_BUFFER_MS", 0); ms > 0 {
		c.BCSigLagBuf = time.Duration(ms) * time.Millisecond
	}
	if v := os.Getenv("BAD_ENTRY_PCT"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.BadEntryPct = f
		}
	}
	if v := strings.TrimSpace(os.Getenv("BAD_ENTRY_SL_PCT")); v == "0" {
		c.BadEntrySLPct = 0
	} else {
		c.BadEntrySLPct = evF("BAD_ENTRY_SL_PCT", c.BadEntrySLPct*100) / 100
	}
	if v := strings.TrimSpace(os.Getenv("BAD_ENTRY_FLASH_MIN_PCT")); v == "0" {
		c.BadEntryFlashPct = 0
	} else {
		c.BadEntryFlashPct = evF("BAD_ENTRY_FLASH_MIN_PCT", c.BadEntryFlashPct*100) / 100
	}
	c.BadExitFastSec = int(evU("BAD_EXIT_FAST_SEC", uint64(c.BadExitFastSec)))
	c.MintMinPumpBuys = int(evU("MIN_MINT_PUMP_BUYS", uint64(c.MintMinPumpBuys)))
	if w := evU("MIN_MINT_PUMP_BUYS_WINDOW_SEC", 0); w > 0 {
		c.MintMinPumpBuysWindow = time.Duration(w) * time.Second
	} else if c.MintMinPumpBuys > 0 && c.MintMinPumpBuysWindow == 0 {
		c.MintMinPumpBuysWindow = 12 * time.Second
	}
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
			// если используем Helius — не сваливаемся на бэкап (часто бэкап хуже и даёт те самые 800-900ms)
			rpcMu.RLock()
			onHelius := strings.Contains(cfg.RPC, "helius-rpc.com")
			rpcMu.RUnlock()
			if onHelius {
				continue
			}
			// Exclusive Helius mode: не переключаем RPC вообще, только логируем.
			log.Printf("[RPC] High error rate %.0f%% (%d/%d) — остаёмся на Helius (exclusive)", rate*100, errs, calls)
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

	// ZERO-RPC: цели набираем из WS логов. Для targets не ограничиваем по maxAge,
	// иначе при scrape-цикле в минуты все события будут "старее 250ms".
	scrapeFromWSEvents(freq, myKey, 0, 4000)
	scrapeDexEndpoint(ctx, freq, myKey,
		"https://api.dexscreener.com/token-profiles/latest/v1", 10, "profiles")
	scrapeDexEndpoint(ctx, freq, myKey,
		"https://api.dexscreener.com/token-boosts/latest/v1", 8, "boosts")

	now := time.Now()
	tgtMu.Lock()
	added := 0
	// Всегда добавляем pinned кошельки (переживают рестарт)
	for w := range snapshotPinnedWallets() {
		if _, ok := targets[w]; !ok && len(targets) < cfg.MaxTargets {
			targets[w] = now
			added++
			log.Printf("[SCRAPE] +Цель: %s (PINNED)", short(w))
		} else if ok {
			targets[w] = now
		}
	}
	for _, w := range cfg.CopyWallets {
		w = strings.TrimSpace(w)
		if w == "" {
			continue
		}
		if _, ok := targets[w]; !ok && len(targets) < cfg.MaxTargets {
			targets[w] = now
			added++
			log.Printf("[SCRAPE] +Цель: %s (COPY_WALLETS)", short(w))
		} else if ok {
			targets[w] = now
		}
	}
	if cfg.ScrapeAutoTargets {
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
			sameMintOK := cfg.ScrapeSameMintMin > 0 && maxMintBuys >= cfg.ScrapeSameMintMin
			if !sameMintOK && !profitOK && !ws.win24h2x {
				continue
			}

			// 1) всегда записываем как кандидата (наблюдаем без торговли)
			candMu.Lock()
			cit, exists := candidates[w]
			if !exists || cit.firstSeen.IsZero() {
				cit.firstSeen = now
			}
			cit.lastSeen = now
			cit.stats = *ws
			candidates[w] = cit
			candMu.Unlock()

			// 2) утверждаем и добавляем в цели только если кандидат прошёл пороги
			age := now.Sub(cit.firstSeen)
			profit := 0.0
			if ws.totalBuySOL > 0 {
				profit = float64(ws.totalSellSOL)/float64(ws.totalBuySOL) - 1.0
			}
			approved := age >= cfg.CandMinAge &&
				ws.trades >= cfg.CandMinTrades &&
				ws.totalBuySOL >= cfg.CandMinTotalBuyLam &&
				profit >= cfg.CandMinProfitPct &&
				(!cfg.CandRequireWin2x || ws.win24h2x)

			if !approved {
				continue
			}
			if _, ok := targets[w]; !ok && len(targets) < cfg.MaxTargets {
				targets[w] = now
				added++
				reason := fmt.Sprintf("approved: age=%s trades=%d profit=%.0f%% buy=%.1f SOL",
					age.Round(time.Second), ws.trades, profit*100, float64(ws.totalBuySOL)/1e9)
				if ws.win24h2x {
					reason += " +2x/24h"
				}
				log.Printf("[APPROVE] +Цель: %s (%s)", short(w), reason)
			} else if ok {
				targets[w] = now
			}
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

// scrapeRPCLive removed: sequential GetTransaction polling disabled (WS-only).

func scrapeFromWSEvents(freq map[string]*walletStats, myKey string, maxAge time.Duration, maxDrain int) {
	now := time.Now()
	drained := 0
	skippedLag := 0
	for drained < maxDrain {
		select {
		case it := <-scrapeEvCh:
			drained++
			if it.ev == nil || it.ev.User.IsZero() || it.ev.Mint.IsZero() {
				continue
			}
			if maxAge > 0 && now.Sub(it.recvTime) > maxAge {
				skippedLag++
				continue
			}
			w := it.ev.User.String()
			if w == myKey {
				continue
			}
			ws := getWS(freq, w)
			ws.trades++
			if it.ev.IsBuy && it.ev.Sol >= 500_000_000 {
				ws.win24h2x = true
			}
			if it.ev.IsBuy {
				ws.mintBuys[it.ev.Mint.String()]++
				ws.totalBuySOL += it.ev.Sol
			} else {
				ws.totalSellSOL += it.ev.Sol
			}
		default:
			drained = maxDrain
		}
	}
	if skippedLag > 0 {
		log.Printf("[SCRAPE] Skip: latency shield (%d ws-events >%dms old)", skippedLag, maxAge.Milliseconds())
	}
}

func parseTradeFromLogs(logs []string) *TradeEvent {
	hasPump := false
	var evData []byte
	for _, l := range logs {
		if strings.Contains(l, PumpProgram.String()) && strings.Contains(l, "invoke") {
			hasPump = true
		}
		if strings.HasPrefix(l, "Program data: ") {
			raw, _ := base64.StdEncoding.DecodeString(l[14:])
			if len(raw) >= 8 && bytes.Equal(raw[:8], tradeDisc[:]) {
				evData = raw
			}
		}
	}
	if !hasPump || evData == nil {
		return nil
	}
	return parseTE(evData)
}

// batch getTransaction removed (zero-RPC scraper).

func scrapeDexEndpoint(ctx context.Context, freq map[string]*walletStats, myKey, url string, maxCheck int, tag string) {
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
		_ = mint // kept for future optional per-token validation without RPC
		checked++
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

func notifyMonitorWake() {
	select {
	case monitorWakeCh <- struct{}{}:
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

// bonding-curve cache updated via WS accountSubscribe
var (
	bcMu    sync.RWMutex
	bcCache = map[string]struct {
		state *BondingCurve
		bc    solana.PublicKey
		ts    time.Time
	}{}
)

type wsSubKind int

const (
	wsSubWalletLogs wsSubKind = iota
	wsSubPumpLogs
	wsSubBCAccount
)

func parseBCData(d []byte) *BondingCurve {
	if len(d) < 49 {
		return nil
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
	return s
}

func cachedBCState(mint solana.PublicKey) (*BondingCurve, solana.PublicKey, bool) {
	key := mint.String()
	bcMu.RLock()
	it, ok := bcCache[key]
	bcMu.RUnlock()
	if !ok || it.state == nil {
		return nil, solana.PublicKey{}, false
	}
	// считаем свежим, если было обновление < 2s назад
	if time.Since(it.ts) > 2*time.Second {
		return nil, solana.PublicKey{}, false
	}
	return it.state, it.bc, true
}

func runWSListener(ctx context.Context) {
	// Multi-WSS: run one loop per WSS URL (race on signals; dedup by signature).
	urls := cfg.WSSURLs
	if len(urls) == 0 {
		urls = []string{cfg.WSS}
	}
	var wg sync.WaitGroup
	for _, u := range urls {
		u := strings.TrimSpace(u)
		if u == "" {
			continue
		}
		wg.Add(1)
		go func(wss string) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if err := wsLoop(ctx, wss); err != nil {
					if !strings.Contains(err.Error(), "use of closed") {
						log.Printf("[WS] Ошибка (%s): %v", short(wss), err)
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
		}(u)
	}
	wg.Wait()
}

func wsLoop(ctx context.Context, wssURL string) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wssURL, nil)
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
	subKind := map[int]wsSubKind{} // subscription id -> kind
	subToMint := map[int]string{}  // accountSubscribe subID -> mint
	reqToMint := map[int]string{}  // request id -> mint
	subbedBC := map[string]bool{}  // mint -> subscribed
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

	// One global logsSubscribe to pump program.
	addPumpSubOnce := func() {
		for _, k := range subKind {
			if k == wsSubPumpLogs {
				return
			}
		}
		nextID++
		id := nextID
		writeMu.Lock()
		_ = conn.WriteJSON(map[string]any{
			"jsonrpc": "2.0", "id": id,
			"method": "logsSubscribe",
			"params": []any{
				map[string]any{"mentions": []string{PumpProgram.String()}},
				map[string]any{"commitment": "confirmed"},
			},
		})
		writeMu.Unlock()
		// mark request id as "pump logs"
		reqToMint[id] = "__PUMP__"
	}

	addBCSubs := func() int {
		posMu.RLock()
		defer posMu.RUnlock()
		added := 0
		for mintStr, p := range pos {
			if p == nil {
				continue
			}
			if subbedBC[mintStr] {
				continue
			}
			mint, err := solana.PublicKeyFromBase58(mintStr)
			if err != nil {
				continue
			}
			bc, _, _ := solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, PumpProgram)
			nextID++
			reqToMint[nextID] = mintStr
			subbedBC[mintStr] = true
			added++
			writeMu.Lock()
			_ = conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0", "id": nextID,
				"method": "accountSubscribe",
				"params": []any{
					bc.String(),
					map[string]any{"encoding": "base64", "commitment": "confirmed"},
				},
			})
			writeMu.Unlock()
		}
		return added
	}

	mu.Lock()
	addNewSubs()
	addPumpSubOnce()
	addBCSubs()
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
				addPumpSubOnce()
				addedBC := addBCSubs()
				mu.Unlock()
				if added > 0 || addedBC > 0 {
					log.Printf("[WS] +%d logs, +%d account (без реконнекта)", added, addedBC)
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
					subKind[subID] = wsSubWalletLogs
				} else if mintStr, ok := reqToMint[*m.ID]; ok {
					if mintStr == "__PUMP__" {
						subKind[subID] = wsSubPumpLogs
					} else {
						subToMint[subID] = mintStr
						subKind[subID] = wsSubBCAccount
					}
				}
				mu.Unlock()
			}
			continue
		}

		if m.Method == "accountNotification" && m.Params != nil {
			mu.Lock()
			mintStr := subToMint[m.Params.Sub]
			mu.Unlock()
			if mintStr == "" {
				continue
			}
			var ar struct {
				Value struct {
					Data []any `json:"data"` // ["base64","..."]
				} `json:"value"`
			}
			if json.Unmarshal(m.Params.Result, &ar) != nil || len(ar.Value.Data) == 0 {
				continue
			}
			// first element: base64 string
			raw, ok := ar.Value.Data[0].(string)
			if !ok || raw == "" {
				continue
			}
			bin, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				continue
			}
			st := parseBCData(bin)
			if st == nil {
				continue
			}
			mint, err := solana.PublicKeyFromBase58(mintStr)
			if err != nil {
				continue
			}
			bc, _, _ := solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, PumpProgram)
			bcMu.Lock()
			bcCache[mintStr] = struct {
				state *BondingCurve
				bc    solana.PublicKey
				ts    time.Time
			}{state: st, bc: bc, ts: time.Now()}
			bcMu.Unlock()
			continue
		}

		if m.Method != "logsNotification" || m.Params == nil {
			continue
		}

		mu.Lock()
		kind := subKind[m.Params.Sub]
		wallet := subToWallet[m.Params.Sub]
		mu.Unlock()

		tgtMu.RLock()
		_, active := targets[wallet]
		tgtMu.RUnlock()
		if kind == wsSubWalletLogs && !active {
			continue
		}

		if kind == wsSubWalletLogs {
			tgtMu.Lock()
			targets[wallet] = time.Now()
			tgtMu.Unlock()
		}

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

		if kind == wsSubPumpLogs {
			// ZERO-RPC: фильтруем BUY и парсим TradeEvent прямо из logs.
			now := time.Now()
			isBuy := false
			for _, l := range lr.Value.Logs {
				if l == "Program log: Instruction: Buy" {
					isBuy = true
					break
				}
			}
			if !isBuy {
				continue
			}
			if ev := parseTradeFromLogs(lr.Value.Logs); ev != nil && ev.IsBuy {
				noteMintPumpBuy(ev.Mint.String())
				select {
				case scrapeEvCh <- scrapeEv{ev: ev, recvTime: now}:
				default:
				}
			}
			continue
		}
		if kind == wsSubWalletLogs {
			// BUY signal -> trade executor; SELL signal -> shadow exit (copy-sell)
			go parseBuySignal(wallet, lr.Value.Signature, lr.Value.Logs)
			go parseSellSignal(wallet, lr.Value.Logs)
		}
	}
}

func parseBuySignal(wallet string, sig string, logs []string) {
	isBuy := false
	for _, l := range logs {
		if l == "Program log: Instruction: Buy" {
			isBuy = true
			break
		}
	}
	if !isBuy {
		return
	}
	ev := parseTradeFromLogs(logs)
	if ev == nil || ev.User.String() != wallet {
		return
	}
	if ev.Sol < cfg.MinWhaleBuyLam {
		return
	}
	noteMintPumpBuy(ev.Mint.String())

	// Dedup by signature (multi-WSS race may deliver same tx from multiple providers).
	if sig != "" {
		now := time.Now()
		sigDedupMu.Lock()
		if t, ok := sigDedup[sig]; ok && now.Sub(t) < 2*time.Minute {
			sigDedupMu.Unlock()
			return
		}
		sigDedup[sig] = now
		// simple GC
		if len(sigDedup) > 5000 {
			cut := now.Add(-5 * time.Minute)
			for k, v := range sigDedup {
				if v.Before(cut) {
					delete(sigDedup, k)
				}
			}
		}
		sigDedupMu.Unlock()
	}

	select {
	case signalCh <- Signal{Mint: ev.Mint, Wallet: wallet, Sig: sig, At: time.Now()}:
	default:
		log.Println("[WS] signalCh full — пропуск")
	}
}

func parseSellSignal(wallet string, logs []string) {
	isSell := false
	for _, l := range logs {
		if l == "Program log: Instruction: Sell" {
			isSell = true
			break
		}
	}
	if !isSell {
		return
	}
	ev := parseTradeFromLogs(logs)
	if ev == nil || ev.User.String() != wallet || ev.IsBuy {
		return
	}
	if !cfg.CopySellExit {
		return
	}
	tryCopySellExit(wallet, ev.Mint)
}

func tryCopySellExit(wallet string, mint solana.PublicKey) {
	mintStr := mint.String()
	posMu.RLock()
	p := pos[mintStr]
	posMu.RUnlock()
	if p == nil {
		return
	}
	// выходим только если позиция была открыта именно по этому кошельку-цели
	if strings.TrimSpace(p.Wallet) != strings.TrimSpace(wallet) {
		return
	}
	// не продаём до подтверждения BUY и минимального возраста, чтобы избежать гонок confirm/ATA
	if cfg.Live && !p.Confirmed {
		return
	}
	if cfg.CopySellMinAgeSec > 0 && time.Since(p.Entry) < time.Duration(cfg.CopySellMinAgeSec)*time.Second {
		return
	}
	go func(saved *Position) {
		// Копи-селл: игнорируем текущий pnl — выходим по действию цели.
		if doSell(context.Background(), saved, "COPYSELL", nil, solana.PublicKey{}) {
			removePos(mintStr)
		}
	}(p)
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

// mintBuyCooldownWhy — пусто если BUY разрешён; иначе текст для лога.
func mintBuyCooldownWhy(mint string) string {
	cdMu.Lock()
	defer cdMu.Unlock()
	if t, ok := cdMap[mint]; ok && time.Since(t) < cfg.MintCD {
		return "mint cooldown"
	}
	if cfg.MintWinCD > 0 {
		if t, ok := winCdMap[mint]; ok && time.Since(t) < cfg.MintWinCD {
			left := cfg.MintWinCD - time.Since(t)
			return fmt.Sprintf("mint cooldown after win (%s left)", left.Round(time.Second))
		}
	}
	if cfg.MintLossCD > 0 {
		if t, ok := lossCdMap[mint]; ok && time.Since(t) < cfg.MintLossCD {
			left := cfg.MintLossCD - time.Since(t)
			return fmt.Sprintf("mint cooldown after loss (%s left)", left.Round(time.Second))
		}
	}
	return ""
}

func setMintCooldown(mint string) {
	cdMu.Lock()
	cdMap[mint] = time.Now()
	cdMu.Unlock()
}

func noteMintPumpBuy(mintStr string) {
	if mintStr == "" {
		return
	}
	keep := 60 * time.Second
	if cfg.MintMinPumpBuysWindow > keep {
		keep = cfg.MintMinPumpBuysWindow
	}
	now := time.Now()
	cutoff := now.Add(-keep)
	mintPumpBuyMu.Lock()
	defer mintPumpBuyMu.Unlock()
	lst := mintPumpBuys[mintStr]
	i := 0
	for _, t := range lst {
		if t.After(cutoff) {
			lst[i] = t
			i++
		}
	}
	lst = lst[:i]
	lst = append(lst, now)
	if len(lst) > 96 {
		lst = lst[len(lst)-96:]
	}
	mintPumpBuys[mintStr] = lst
}

func mintPumpBuysInWindow(mintStr string, window time.Duration) int {
	if window <= 0 {
		return 0
	}
	now := time.Now()
	cutoff := now.Add(-window)
	mintPumpBuyMu.Lock()
	defer mintPumpBuyMu.Unlock()
	n := 0
	for _, t := range mintPumpBuys[mintStr] {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}

func mintFirstSeenAge(mintStr string) (age time.Duration, ok bool) {
	mintPumpBuyMu.Lock()
	defer mintPumpBuyMu.Unlock()
	lst := mintPumpBuys[mintStr]
	if len(lst) == 0 {
		return 0, false
	}
	minT := lst[0]
	for _, t := range lst[1:] {
		if t.Before(minT) {
			minT = t
		}
	}
	return time.Since(minT), true
}

func holderConcentrationPct(ctx context.Context, mint solana.PublicKey, tokProg solana.PublicKey, excludeATA solana.PublicKey, topN int) (maxPct float64, maxAcc solana.PublicKey, err error) {
	if topN <= 0 {
		topN = 20
	}
	// Supply
	rpcWait()
	sup, e1 := rpcClient().GetTokenSupply(ctx, mint, rpc.CommitmentConfirmed)
	rpcNote(e1)
	if e1 != nil || sup == nil || sup.Value == nil {
		if e1 == nil {
			e1 = errors.New("nil token supply")
		}
		return 0, solana.PublicKey{}, e1
	}
	supply, e2 := strconv.ParseUint(sup.Value.Amount, 10, 64)
	if e2 != nil || supply == 0 {
		if e2 == nil {
			e2 = errors.New("zero supply")
		}
		return 0, solana.PublicKey{}, e2
	}

	// Largest accounts (token accounts)
	rpcWait()
	la, e3 := rpcClient().GetTokenLargestAccounts(ctx, mint, rpc.CommitmentConfirmed)
	rpcNote(e3)
	if e3 != nil || la == nil || la.Value == nil {
		if e3 == nil {
			e3 = errors.New("nil largest accounts")
		}
		return 0, solana.PublicKey{}, e3
	}
	lim := topN
	if lim > len(la.Value) {
		lim = len(la.Value)
	}
	for i := 0; i < lim; i++ {
		it := la.Value[i]
		// it.Address is token account pubkey
		if excludeATA != (solana.PublicKey{}) && it.Address == excludeATA {
			continue
		}
		amt, pe := strconv.ParseUint(it.Amount, 10, 64)
		if pe != nil {
			continue
		}
		pct := float64(amt) / float64(supply)
		if pct > maxPct {
			maxPct = pct
			maxAcc = it.Address
		}
	}

	_ = tokProg // reserved: for future exclusions if needed
	return maxPct, maxAcc, nil
}

func positionSLlimit(p *Position) float64 {
	if p == nil {
		return cfg.SL
	}
	if p.BadEntry && cfg.BadEntrySLPct > 0 && cfg.BadEntrySLPct < cfg.SL {
		return cfg.BadEntrySLPct
	}
	return cfg.SL
}

func flashSLThreshold(p *Position) float64 {
	th := cfg.FlashSLPct
	if p != nil && p.BadEntry && cfg.BadEntryFlashPct > 0 && cfg.BadEntryFlashPct < th {
		th = cfg.BadEntryFlashPct
	}
	// Если вход уже подтвердился с заметным плохим fill (>=3%),
	// режем раньше, чтобы не ждать стандартный FLASHSL.
	// Важно: НЕ делаем порог агрессивнее, чем это явно настроено cfg.BadEntryFlashPct.
	// Раньше было принудительное ужесточение до -6%, из-за чего FLASHSL мог срабатывать
	// слишком рано даже при включённом BAD_ENTRY_FLASH_MIN_PCT.
	if p != nil && cfg.BadEntryFlashPct <= 0 && p.BadEntryDiffPct <= -3 && th > 0.06 {
		th = 0.06
	}
	return th
}

func minNetExitThreshold() float64 {
	// Fixed-cost часть в долях от ставки: priority (buy+sell) + jito tip (buy+sell)
	auto := 0.0
	if cfg.BuyLamp > 0 {
		fixedLam := cfg.PrioLamp + cfg.PrioLampSell + 2*cfg.JitoTipLamp
		auto = float64(fixedLam) / float64(cfg.BuyLamp)
	}
	auto += cfg.NetExitBufPct
	if cfg.MinNetExitPct > auto {
		return cfg.MinNetExitPct
	}
	return auto
}

func fixedTradeCostsLamports() uint64 {
	// Round-trip fixed costs per completed trade (buy+sell): priority + jito tips.
	return cfg.PrioLamp + cfg.PrioLampSell + 2*cfg.JitoTipLamp
}

func walletBalanceLamports(ctx context.Context) (uint64, bool) {
	rpcWait()
	b, err := rpcClient().GetBalance(ctx, cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
	rpcNote(err)
	if err != nil {
		return 0, false
	}
	setWalletBalCache(b.Value)
	return b.Value, true
}

func txPayerDeltaLamports(ctx context.Context, sig solana.Signature) (int64, bool) {
	maxVer := uint64(0)
	rpcWait()
	tx, err := rpcClient().GetTransaction(ctx, sig, &rpc.GetTransactionOpts{
		Commitment:                     rpc.CommitmentConfirmed,
		MaxSupportedTransactionVersion: &maxVer,
	})
	rpcNote(err)
	if err != nil || tx == nil || tx.Meta == nil {
		return 0, false
	}
	if len(tx.Meta.PreBalances) == 0 || len(tx.Meta.PostBalances) == 0 {
		return 0, false
	}
	// Для наших TX payer всегда cfg.Key и он в индексе 0.
	pre := tx.Meta.PreBalances[0]
	post := tx.Meta.PostBalances[0]
	return int64(post) - int64(pre), true
}

func finalizeSellOutcome(wallet string, spent uint64, solNet uint64, mint string) {
	if spent == 0 {
		return
	}
	netOut := int64(solNet) - int64(fixedTradeCostsLamports())
	netFrac := float64(netOut)/float64(spent) - 1.0
	// deltaLamports в той же модели, что recordPnl: (solNet-spent)-fixed
	deltaLamports := (int64(solNet) - int64(spent)) - int64(fixedTradeCostsLamports())
	recordPnl(spent, solNet, mint)
	trackTargetResult(wallet, netFrac > 0)
	noteWalletTradeOutcome(wallet, deltaLamports, netFrac > 0)
	notePinnedWalletOnWin(wallet, netFrac)
	pruneTargetsToMax()
	log.Printf("[SELL][FACT] %s | accounted_in=%0.6f SOL | spent=%0.6f SOL | net=%+.2f%%",
		short(mint), float64(solNet)/1e9, float64(spent)/1e9, netFrac*100)
}

func doBuy(ctx context.Context, sig Signal) {
	signalT := sig.At
	if signalT.IsZero() {
		signalT = time.Now()
	}
	if ctx.Err() != nil {
		return
	}
	mint := sig.Mint.String()

	// hard skip if the WS signal is already stale before we even start work
	if cfg.MaxSignalLag > 0 && time.Since(signalT) > cfg.MaxSignalLag {
		log.Printf("[BUY] Skip: signal stale %dms > %dms | %s", time.Since(signalT).Milliseconds(), cfg.MaxSignalLag.Milliseconds(), short(mint))
		return
	}

	if circuitOpen.Load() {
		log.Printf("[BUY] Skip: circuit breaker active | %s", short(mint))
		return
	}

	// Fee protection: после серии BUY FAIL делаем короткую паузу.
	if untilMs := buyPauseUntil.Load(); untilMs > 0 {
		nowMs := time.Now().UnixMilli()
		if nowMs < untilMs {
			left := time.Duration(untilMs-nowMs) * time.Millisecond
			log.Printf("[BUY] Skip: fee-protection pause %.0fs left | %s", left.Seconds(), short(mint))
			return
		}
	}

	if why := mintBuyCooldownWhy(mint); why != "" {
		log.Printf("[BUY] Skip: %s | %s", why, short(mint))
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

	posMu.Lock()
	if _, dup := pos[mint]; dup {
		posMu.Unlock()
		log.Printf("[BUY] Skip: already in position | %s", short(mint))
		return
	}
	if len(pos)+pendingPosBuys >= cfg.MaxPositions {
		posMu.Unlock()
		log.Printf("[BUY] Skip: max positions (%d/%d) | %s", len(pos)+pendingPosBuys, cfg.MaxPositions, short(mint))
		return
	}
	pendingPosBuys++
	posMu.Unlock()
	defer func() {
		posMu.Lock()
		pendingPosBuys--
		posMu.Unlock()
	}()

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

	if sp := sessionPnlPct(); sp <= -cfg.MaxSessionLoss {
		circuitOpen.Store(true)
		log.Printf("[BUY] Skip: session PnL breaker (%.2f%% от старта <= -%.0f%%) — торговля остановлена", sp, cfg.MaxSessionLoss)
		return
	}

	if cfg.MintMinPumpBuys > 0 {
		win := cfg.MintMinPumpBuysWindow
		if win <= 0 {
			win = 12 * time.Second
		}
		if n := mintPumpBuysInWindow(mint, win); n < cfg.MintMinPumpBuys {
			log.Printf("[BUY] Skip: mint pump depth %d<%d за %v (мало Buy на mint с pump WS) | %s",
				n, cfg.MintMinPumpBuys, win.Round(time.Second), short(mint))
			return
		}
	}
	if cfg.MintMinAge > 0 {
		if age, ok := mintFirstSeenAge(mint); ok && age < cfg.MintMinAge {
			log.Printf("[BUY] Skip: mint too fresh (firstSeen %ds < %ds) | %s",
				int(age.Seconds()), int(cfg.MintMinAge.Seconds()), short(mint))
			return
		}
	}

	// Observe-only: торгуем только по утверждённым целям (или pinned/copy_wallets).
	if cfg.ObserveOnly {
		if !isPinnedWallet(sig.Wallet) {
			isCopy := false
			for _, w := range cfg.CopyWallets {
				if strings.TrimSpace(w) == strings.TrimSpace(sig.Wallet) {
					isCopy = true
					break
				}
			}
			if !isCopy {
				tgtMu.RLock()
				_, ok := targets[sig.Wallet]
				tgtMu.RUnlock()
				if !ok {
					log.Printf("[BUY] Skip: target not approved (OBSERVE_ONLY) | %s from %s", short(mint), short(sig.Wallet))
					return
				}
			}
		}
	}

	log.Printf("[BUY] Сигнал: %s от %s", short(mint), short(sig.Wallet))

	var (
		preBal    uint64
		preBalOK  bool
		preTokPrg solana.PublicKey
		tokInfoMs int64
		preATAOK  bool
		preState  *BondingCurve
		preBC     solana.PublicKey
		preBCErr  error
		preBalMs  int64
		preBCMs   int64
		preATAMs  int64
		holderMs  int64
		balHit    bool
		tokHit    bool
		ataHit    bool
	)
	var pwg sync.WaitGroup
	pwg.Add(3)
	go func() {
		defer pwg.Done()
		t0 := time.Now()
		if v, ok := getWalletBalCache(cfg.WalletBalCacheTTL); ok {
			preBal = v
			preBalOK = true
			balHit = true
			preBalMs = time.Since(t0).Milliseconds()
			return
		}
		rpcWait()
		b, err := rpcClient().GetBalance(ctx, cfg.Key.PublicKey(), rpc.CommitmentConfirmed)
		rpcNote(err)
		if err == nil {
			preBal = b.Value
			preBalOK = true
			setWalletBalCache(b.Value)
		}
		preBalMs = time.Since(t0).Milliseconds()
	}()
	go func() {
		defer pwg.Done()
		t0 := time.Now()
		preTokPrg, tokHit = getMintTokenProgramCachedWithHit(ctx, sig.Mint)
		tokInfoMs = time.Since(t0).Milliseconds()
	}()
	go func() {
		defer pwg.Done()
		t0 := time.Now()
		preState, preBC, preBCErr = readBC(ctx, sig.Mint)
		preBCMs = time.Since(t0).Milliseconds()
	}()
	pwg.Wait()
	if cfg.MaxTokInfoLag > 0 && time.Duration(tokInfoMs)*time.Millisecond > cfg.MaxTokInfoLag {
		log.Printf("[BUY] Skip: tokeninfo lag %dms > %dms | %s", tokInfoMs, cfg.MaxTokInfoLag.Milliseconds(), short(mint))
		return
	}
	if cfg.MaxSignalLag > 0 && time.Since(signalT) > cfg.MaxSignalLag {
		log.Printf("[BUY] Skip: signal lag %dms > %dms | %s", time.Since(signalT).Milliseconds(), cfg.MaxSignalLag.Milliseconds(), short(mint))
		return
	}

	if preBalOK {
		rentNeed := uint64(0)
		if !preATAOK {
			rentNeed = 2_100_000 // ~0.0021 SOL token account rent
		}
		feeBuf := uint64(600_000) // buffer for network/compute
		need := cfg.BuyLamp + cfg.MinReserve + rentNeed + feeBuf
		if preBal < need {
			log.Printf("[BUY] Skip: low balance (bal=%.4f need=%.4f incl rent=%.4f) | %s",
				float64(preBal)/1e9, float64(need)/1e9, float64(rentNeed)/1e9, short(mint))
			return
		}
	}

	tokProg := preTokPrg
	if tokProg == Token2022 {
		log.Printf("[BUY] Token-2022: %s", short(mint))
	}
	ataT0 := time.Now()
	preATAOK, ataHit = userATAExistsCachedWithHit(ctx, cfg.Key.PublicKey(), sig.Mint, tokProg)
	preATAMs = time.Since(ataT0).Milliseconds()

	state, bc := preState, preBC
	// retry for freshest mints: sometimes bonding curve account isn't readable immediately
	if preBCErr != nil || state == nil {
		for i := 0; i < cfg.BCRetryMax; i++ {
			if cfg.MaxSignalLag > 0 && time.Since(signalT) > cfg.MaxSignalLag-cfg.BCSigLagBuf {
				break
			}
			time.Sleep(cfg.BCRetrySleep)
			s2, bc2, e2 := readBC(ctx, sig.Mint)
			if e2 == nil && s2 != nil {
				state, bc, preBCErr = s2, bc2, nil
				break
			}
		}
	}
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

	if state.VSR < cfg.MinVSRLam {
		log.Printf("[BUY] Skip: low liquidity (%.2f SOL) | %s", float64(state.VSR)/1e9, short(mint))
		setMintCooldown(mint)
		return
	}

	if cfg.HolderMaxPct > 0 && (cfg.HolderCheckMinVSR == 0 || state.VSR < cfg.HolderCheckMinVSR) {
		// Exclude bonding-curve ATA from holder concentration check
		excl := findATA(bc, sig.Mint, tokProg)
		hctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		h0 := time.Now()
		maxPct, maxAcc, herr := holderConcentrationPct(hctx, sig.Mint, tokProg, excl, cfg.HolderTopN)
		holderMs = time.Since(h0).Milliseconds()
		cancel()
		if herr == nil && maxPct >= cfg.HolderMaxPct {
			log.Printf("[BUY] Skip: whale concentration %.1f%% >= %.1f%% (acc %s) | %s",
				maxPct*100, cfg.HolderMaxPct*100, short(maxAcc.String()), short(mint))
			setMintCooldown(mint)
			return
		}
	} else if cfg.HolderMaxPct > 0 {
		holderMs = -1 // skipped by fast-path (high VSR)
	}

	if state.RTK > 0 && state.Supply > 0 && float64(state.RTK)/float64(state.Supply) < 0.05 {
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

	buildBuyData := func(minTok uint64) []byte {
		// buy_exact_sol_in: (sol_amount u64, min_tokens_out u64)
		data := make([]byte, 24)
		copy(data[:8], buyDisc[:])
		binary.LittleEndian.PutUint64(data[8:], cfg.BuyLamp)
		binary.LittleEndian.PutUint64(data[16:], minTok)
		return data
	}

	cuLimit := uint32(250_000)
	ixsBase := []solana.Instruction{cuLimitIx(cuLimit)}
	if tip := jitoTipIx(user); tip != nil {
		ixsBase = append(ixsBase, tip)
	}
	buildIxs := func(data []byte) []solana.Instruction {
		ixs := append([]solana.Instruction{}, ixsBase...)
		ixs = append(ixs,
			cuPriceIx(cfg.PrioLamp*1_000_000/uint64(cuLimit)),
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
		)
		return ixs
	}

	signTx := func(ixs []solana.Instruction) (*solana.Transaction, error) {
		bh := cachedBH(ctx)
		tx, err := solana.NewTransaction(ixs, bh, solana.TransactionPayer(user))
		if err != nil {
			return nil, err
		}
		_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
			if key == user {
				k := cfg.Key
				return &k
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return tx, nil
	}

	if !cfg.Live {
		lagMs := time.Since(signalT).Milliseconds()
		log.Printf("[BUY][PAPER] %s | %.4f SOL → %d tok | lag=%dms", short(mint), float64(cfg.BuyLamp)/1e9, tokOut, lagMs)
		addPos(sig.Mint, tokOut, cfg.BuyLamp, sig.Wallet, tokProg)
		logBuyEntryInfo(sig.Mint, cfg.BuyLamp, tokOut)
		statBuys.Add(1)
		return
	}

	setMintCooldown(mint)

	if ctx.Err() != nil {
		return
	}

	// final lag shield right before sending (covers build/sign/send time too)
	if cfg.MaxSignalLag > 0 && time.Since(signalT) > cfg.MaxSignalLag {
		log.Printf("[BUY] Skip: signal lag %dms > %dms (pre-send) | %s", time.Since(signalT).Milliseconds(), cfg.MaxSignalLag.Milliseconds(), short(mint))
		return
	}

	// Buy retry on fast-moving curve: if program rejects (e.g. Custom:6042), retry with looser minTokOut.
	// Higher slips increase fill probability on fast curves, at the cost of worse fills.
	trySlips := []uint64{cfg.Slip, 3000, 4500, 6000}
	var (
		txSig            solana.Signature
		err              error
		lastPreSendLagMs int64
		lastSendRTTMs    int64
	)
	tokOutCur := tokOut
	for attempt, slip := range trySlips {
		if cfg.MaxSignalLag > 0 && time.Since(signalT) > cfg.MaxSignalLag {
			log.Printf("[BUY] Skip: signal lag %dms > %dms (pre-build) | %s", time.Since(signalT).Milliseconds(), cfg.MaxSignalLag.Milliseconds(), short(mint))
			return
		}
		if slip > 9900 {
			slip = 9900
		}
		minTok := tokOutCur * (10000 - slip) / 10000
		tx, buildErr := signTx(buildIxs(buildBuyData(minTok)))
		if buildErr != nil {
			log.Printf("[BUY] Build/Sign: %v", buildErr)
			return
		}
		// Pre-simulate BUY to avoid paid fails (Custom:6000 etc.).
		if cfg.PreSimBuy {
			// Don't waste time simulating if we are too close to lag limit.
			if cfg.MaxSignalLag > 0 && cfg.PreSimLagBudget > 0 {
				if time.Since(signalT) > cfg.MaxSignalLag-cfg.PreSimLagBudget {
					log.Printf("[BUY] Skip: pre-sim lag budget exceeded | %s", short(mint))
					return
				}
			}
			if simErr := preSimBuyTx(ctx, tx); simErr != nil {
				// Treat slippage/constraints as retryable without paying fees.
				if (isBuyCustomErr6042(simErr) || isSlippageErr(simErr)) && attempt < len(trySlips)-1 {
					log.Printf("[BUY] pre-sim retry %d/%d | %s | slip=%dbps | err=%v",
						attempt+1, len(trySlips), short(mint), slip, simErr)
					// refresh curve estimate for next attempt
					if freshState, _, e2 := readBC(ctx, sig.Mint); e2 == nil && freshState != nil && !freshState.Done {
						state = freshState
						tokOutCur = calcTokOut(state.VTK, state.VSR, cfg.BuyLamp-fee)
						tokOut = tokOutCur
					}
					continue
				}
				log.Printf("[BUY] Skip: pre-sim failed | %s | err=%v", short(mint), simErr)
				return
			}
		}
		if cfg.MaxSignalLag > 0 && time.Since(signalT) > cfg.MaxSignalLag {
			log.Printf("[BUY] Skip: signal lag %dms > %dms (pre-send) | %s", time.Since(signalT).Milliseconds(), cfg.MaxSignalLag.Milliseconds(), short(mint))
			return
		}
		sendStart := time.Now()
		preSendLagMs := sendStart.Sub(signalT).Milliseconds()
		txSig, err = sendTx(ctx, tx, false)
		lastPreSendLagMs = preSendLagMs
		lastSendRTTMs = time.Since(sendStart).Milliseconds()
		if err != nil && (isBuyCustomErr6042(err) || isSlippageErr(err)) && attempt < len(trySlips)-1 {
			log.Printf("[BUY] retry %d/%d | %s | slip=%dbps | preSendLag=%dms | err=%v",
				attempt+1, len(trySlips), short(mint), slip, preSendLagMs, err)
			// На pump.fun кривая меняется очень быстро.
			// Если мы не обновим BC/estimations, то повтор с другим minTok может быть всё равно обречён
			// и мы продолжим платить комиссии за заведомо неподходящие constraints.
			if freshState, _, e2 := readBC(ctx, sig.Mint); e2 == nil && freshState != nil && !freshState.Done {
				state = freshState
				tokOutCur = calcTokOut(state.VTK, state.VSR, cfg.BuyLamp-fee)
				tokOut = tokOutCur
			}
			continue
		}
		break
	}
	if err != nil {
		errMsg := fmt.Sprintf("%v", err)
		if len(errMsg) > 200 {
			errMsg = errMsg[:200] + "…"
		}
		log.Printf("[BUY] ✗ %s | %s", errMsg, short(mint))
		registerBuyFailure(time.Now(), err)
		noteWalletBuyFail(sig.Wallet, err)
		pruneTargetsToMax()
		return
	}
	totalLagMs := time.Since(signalT).Milliseconds()
	log.Printf("[BUY] TX: %s | %s | %.4f SOL | preSend=%dms sendRTT=%dms total=%dms",
		txSig.String()[:12], short(mint), float64(cfg.BuyLamp)/1e9,
		lastPreSendLagMs, lastSendRTTMs, totalLagMs)
	log.Printf("[BUY][TIMING] %s | bal=%dms(hit=%t) tok=%dms(hit=%t) bc=%dms ata=%dms(hit=%t) holders=%dms (-1=skipped)",
		short(mint), preBalMs, balHit, tokInfoMs, tokHit, preBCMs, preATAMs, ataHit, holderMs)
	addPos(sig.Mint, tokOut, cfg.BuyLamp, sig.Wallet, tokProg)
	logBuyEntryInfo(sig.Mint, cfg.BuyLamp, tokOut)
	statBuys.Add(1)

	go func() {
		confirmTx(ctx, txSig, "BUY", mint, func() {
			// RPC может не увидеть confirm вовремя. Проверяем ATA: если токены уже пришли,
			// позицию не удаляем и разрешаем монитору SELL.
			user := cfg.Key.PublicKey()
			ata := findATA(user, sig.Mint, tokProg)
			rpcWait()
			bal := getTokenBalance(ctx, ata)
			if bal == 0 {
				removePos(mint)
				log.Printf("[BUY] Удалена фантомная позиция: %s", short(mint))
				return
			}
			markUserATAKnown(user, sig.Mint, tokProg)
			posMu.Lock()
			if p, ok := pos[mint]; ok {
				p.Confirmed = true
				p.Tokens = bal
			}
			posMu.Unlock()
			log.Printf("[BUY] confirm-timeout, но токены есть: %s | bal=%d (позиция сохранена)", short(mint), bal)
		})
		// После подтверждения — обновить p.Tokens реальным балансом ATA
		user := cfg.Key.PublicKey()
		ata := findATA(user, sig.Mint, tokProg)
		real := getTokenBalance(ctx, ata)
		posMu.Lock()
		if p, ok := pos[mint]; ok {
			// Важно: p.Spent используем как principal (ставка BUY_AMOUNT_SOL),
			// а не как полный wallet-delta на BUY (который включает rent на ATA и комиссии).
			// Иначе почти каждый новый mint выглядит как "мгновенный -20%+" и SL/ветки
			// срабатывают ложно (rent ~0.0021 SOL на каждый новый ATA).
			// В live-режиме SELL-логика требует Confirmed=true.
			// Ставим флаг после confirm-check даже если ATA вернул 0 (временный RPC-глюк).
			p.Confirmed = true
			if real > 0 {
				old := p.Tokens
				p.Tokens = real
				if old != real {
					diff := float64(real)/float64(old)*100 - 100
					p.BadEntryDiffPct = diff
					log.Printf("[BUY] Баланс скорр.: %s | %d → %d tok (%.0f%%)",
						short(mint), old, real, diff)
					// Защита от слишком "мягкого" порога: для pump-рынка fill хуже ~4% уже риск.
					badEntryTrigger := cfg.BadEntryPct
					if badEntryTrigger <= 0 || badEntryTrigger > 4 {
						badEntryTrigger = 4
					}
					if diff < -badEntryTrigger {
						p.BadEntry = true
						log.Printf("[BUY] ⚠ Плохой вход (%.0f%%) порог -%.0f%%, быстрый выход через 10с", diff, badEntryTrigger)
					}
				}
			}
		}
		posMu.Unlock()
		if real > 0 {
			markUserATAKnown(user, sig.Mint, tokProg)
		}
	}()
}

func logBuyEntryInfo(mint solana.PublicKey, spentLamports, tokens uint64) {
	if spentLamports == 0 || tokens == 0 {
		return
	}
	solSpent := float64(spentLamports) / 1e9
	priceSolPerTok := solSpent / float64(tokens)
	tokPerSol := float64(tokens) / solSpent
	url := "https://dexscreener.com/solana/" + mint.String()
	log.Printf("[BUY] ENTRY %s | at=%s | price=%.12f SOL/tok (%.0f tok/SOL) | %s",
		short(mint.String()),
		time.Now().Format("15:04:05"),
		priceSolPerTok,
		tokPerSol,
		url,
	)
}

func addPos(mint solana.PublicKey, tok, spent uint64, wallet string, tokProg solana.PublicKey) {
	posMu.Lock()
	pos[mint.String()] = &Position{Mint: mint, Tokens: tok, Spent: spent, Entry: time.Now(), Wallet: wallet, TokProg: tokProg, Confirmed: !cfg.Live}
	posMu.Unlock()
	notifyWSReconn() // добавим accountSubscribe на bonding curve без реконнекта
	notifyMonitorWake()
}

func removePos(mint string) {
	posMu.Lock()
	delete(pos, mint)
	posMu.Unlock()
}

func confirmTx(ctx context.Context, txSig solana.Signature, tag, mintStr string, onFail func()) {
	// BUY: не держим слот позиции 43 секунды — это убивает throughput при MAX_POSITIONS=1.
	initialSleep := 3 * time.Second
	timeout := 43 * time.Second
	if tag == "BUY" && cfg.BuyConfirmTimeout > 0 {
		timeout = cfg.BuyConfirmTimeout
		// быстрее начинаем проверку, чтобы раньше освободить слот при фантоме
		initialSleep = 1 * time.Second
	}
	time.Sleep(initialSleep)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
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
	log.Printf("[%s] ⚠ Не подтверждена за %ds: %s | %s", tag, int(timeout.Seconds()), txSig.String()[:12], short(mintStr))
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
			hasProfit, needFastYoung := checkAll(ctx)
			wantFast := hasProfit || needFastYoung
			if wantFast && !fast {
				t.Reset(fastInterval)
				fast = true
			} else if !wantFast && fast {
				t.Reset(normalInterval)
				fast = false
			}
		case <-monitorWakeCh:
			// Сразу пробуждаем monitor при открытии новой позиции (уменьшаем шанс "проморгать" FlashSL/SL).
			// Без этого первые проверки могут ждать до cfg.MonitorMs после BUY.
			hasProfit, needFastYoung := checkAll(ctx)
			wantFast := hasProfit || needFastYoung
			if wantFast && !fast {
				t.Reset(fastInterval)
				fast = true
			} else if !wantFast && fast {
				t.Reset(normalInterval)
				fast = false
			}
		}
	}
}

type sellJob struct {
	mint   string
	p      *Position
	reason string
	state  *BondingCurve
	bc     solana.PublicKey
	pnl    float64
}

func checkAll(ctx context.Context) (hasProfit bool, needFastYoung bool) {

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
		if cfg.MonitorFastYoungSec > 0 && s.p != nil && (!cfg.Live || s.p.Confirmed) {
			age := time.Since(s.p.Entry)
			// Базовая логика: первые N секунд после входа.
			if age < time.Duration(cfg.MonitorFastYoungSec)*time.Second {
				needFastYoung = true
			} else {
				// Дополнительный хвост: если уже был net-плюс-пик,
				// то выход из прибыли часто происходит быстро (giveback),
				// поэтому не переключаемся на медленный тик сразу по достижению N сек.
				// Это снижает шанс “проскочить” через net-floor между итерациями.
				extra := 10 * time.Second
				minNetExit := minNetExitThreshold()
				if s.p.HiPnl >= minNetExit+0.003 && age < time.Duration(cfg.MonitorFastYoungSec)*time.Second+extra {
					needFastYoung = true
				}
			}
		}
		// Не пытаемся продавать до подтверждения BUY и получения реального баланса ATA.
		// Иначе возможна гонка: SELL до BUY-confirm => "минус в SOL без видимого токена".
		if cfg.Live && (s.p == nil || !s.p.Confirmed) {
			continue
		}
		state, bc, ok := cachedBCState(s.p.Mint)
		if !ok {
			var err error
			state, bc, err = readBC(ctx, s.p.Mint)
			if err != nil {
				continue
			}
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
		// Для kill-веток используем максимально свежую BC-оценку, чтобы не закрывать
		// по устаревшему pnl (особенно на резких движениях).
		if age >= time.Duration(cfg.TimeKillSec)*time.Second || age >= 60*time.Second ||
			(cfg.SoftHardKillSec > 0 && age >= time.Duration(cfg.SoftHardKillSec)*time.Second) {
			if freshState, freshBC, err := readBC(ctx, s.p.Mint); err == nil && freshState != nil {
				state = freshState
				bc = freshBC
				solOut = calcSolOut(state.VTK, state.VSR, s.tokens)
				solNet = solOut - solOut/100
				pnl = float64(solNet)/float64(s.spent) - 1.0
			}
		}

		if pnl > s.p.HiPnl {
			s.p.HiPnl = pnl
		}
		// If we already achieved a net-positive peak, keep fast monitoring on.
		// Otherwise NETLOCK can miss the net-floor and sell below it (turning into net-negative).
		minNetExit := minNetExitThreshold()
		if s.p.HiPnl >= minNetExit+0.003 {
			hasProfit = true
		}
		// VSR speed tracking: обновляем "точку отсчёта" раз в окно.
		if cfg.VSRSExit && state != nil && state.VSR > 0 && cfg.VSRSWindowSec > 0 {
			if s.p.VSR0At.IsZero() || time.Since(s.p.VSR0At) >= time.Duration(cfg.VSRSWindowSec)*time.Second {
				s.p.VSR0 = state.VSR
				s.p.VSR0At = time.Now()
			}
		}
		if pnl >= minNetExit {
			hasProfit = true
		}

		var reason string
		switch {
		case pnl >= cfg.TP:
			reason = fmt.Sprintf("TP +%.0f%%", pnl*100)
		case s.p.HiPnl >= minNetExit+0.003 && pnl <= minNetExit:
			// Защита "набрал net-плюс и отдал обратно": выходим сразу у net-порога,
			// не дожидаясь PROFITLOCK/HARDKILL.
			reason = fmt.Sprintf("NETLOCK peak+%.1f%% now%+.1f%% (net floor %+.2f%%)", s.p.HiPnl*100, pnl*100, minNetExit*100)
		case cfg.BadEntryImmediateSellPct > 0 &&
			s.p.BadEntryDiffPct <= -cfg.BadEntryImmediateSellPct &&
			age >= cfg.BadEntryImmediateMinAge &&
			// Раньше BADENTRYIMM резал только по "fill", не проверяя реальный текущий pnl.
			// Это могло приводить к продажам с небольшим pnl ниже 0%, когда round-trip fixed-cost
			// гарантирует net-минус. Теперь режем только если pnl ушёл достаточно глубоко.
			pnl <= -cfg.BadEntryImmediateSellPct/100:
			reason = fmt.Sprintf("BADENTRYIMM %.0f%% (fill %.0f%%, pnl %.1f%%) age=%ds",
				cfg.BadEntryImmediateSellPct, s.p.BadEntryDiffPct, pnl*100, int(age.Seconds()))
		case s.p.BadEntry && cfg.BadExitFastSec > 0 && age >= time.Duration(cfg.BadExitFastSec)*time.Second && pnl < 0:
			reason = fmt.Sprintf("BADEXITfast %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		case cfg.FlashSLWindow > 0 && age >= cfg.FlashSLMinAge && age <= cfg.FlashSLWindow && pnl <= -flashSLThreshold(s.p):
			reason = fmt.Sprintf("FLASHSL %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		case cfg.VSRSExit &&
			cfg.VSRSWindowSec > 0 &&
			age >= time.Duration(cfg.VSRSMinAgeSec)*time.Second &&
			s.p.VSR0At.After(time.Time{}) &&
			time.Since(s.p.VSR0At) >= time.Duration(cfg.VSRSWindowSec)*time.Second &&
			(s.p.HiPnl*100) >= cfg.VSRSMinPeakPnlPct:
			// Если приток ликвидности на кривую (VSR) затух — фиксируем прибыль/микро-плюс,
			// чтобы не ждать "дампа толпы" и не отдавать пик.
			dVSR := int64(state.VSR) - int64(s.p.VSR0)
			sec := time.Since(s.p.VSR0At).Seconds()
			if sec > 0 {
				speedSOL := (float64(dVSR) / 1e9) / sec
				if speedSOL <= cfg.VSRSMinUpSOLPerSec && pnl >= minNetExit {
					reason = fmt.Sprintf("VSRSPEED %.2f SOL/s (peak+%.1f%% now%+.1f%%)", speedSOL, s.p.HiPnl*100, pnl*100)
				}
			}
		case cfg.EarlySLWindow > 0 && age <= cfg.EarlySLWindow && pnl <= -cfg.EarlySLPct:
			reason = fmt.Sprintf("EARLYSL %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		case age <= 15*time.Second && pnl >= cfg.QuickTPPct:
			reason = fmt.Sprintf("QUICKTP %ds +%.0f%%", int(age.Seconds()), pnl*100)
		case s.p.BadEntry && age >= 10*time.Second && pnl < 0:
			reason = fmt.Sprintf("BADEXIT %ds pnl=%+.1f%%", int(age.Seconds()), pnl*100)
		case pnl <= -positionSLlimit(s.p):
			reason = fmt.Sprintf("SL %.0f%%", pnl*100)
		case cfg.ProfitLockMinPeak > 0 && s.p.HiPnl >= cfg.ProfitLockMinPeak:
			lockFloor := cfg.ProfitLockFloor
			if minNetExit > lockFloor {
				lockFloor = minNetExit
			}
			if pnl <= lockFloor {
				reason = fmt.Sprintf("PROFITLOCK peak+%.1f%% now%+.1f%% (floor %+.2f%%)", s.p.HiPnl*100, pnl*100, lockFloor*100)
			}
		case cfg.BreakevenMinPeak > 0 && s.p.HiPnl >= cfg.BreakevenMinPeak && pnl <= 0:
			reason = fmt.Sprintf("BREAKEVEN peak+%.0f%%→%.0f%%", s.p.HiPnl*100, pnl*100)
		case cfg.PeakPullbackPct > 0 && s.p.HiPnl >= cfg.TrailMinPeak && (s.p.HiPnl-pnl) >= cfg.PeakPullbackPct:
			if pnl >= minNetExit {
				reason = fmt.Sprintf("PEAKPB peak+%.1f%% now%+.1f%% (откат %.1fп.п.)", s.p.HiPnl*100, pnl*100, (s.p.HiPnl-pnl)*100)
			}
		case s.p.HiPnl >= cfg.TrailMinPeak && pnl < s.p.HiPnl*cfg.TrailRelMult && pnl >= minNetExit:
			reason = fmt.Sprintf("TRAIL peak+%.0f%% now+%.0f%%", s.p.HiPnl*100, pnl*100)
		case s.p.HiPnl >= cfg.TrailMinPeak && pnl < s.p.HiPnl*cfg.TrailRelMult && pnl <= 0:
			reason = fmt.Sprintf("SL(trail) %.0f%%", pnl*100)
		case age >= time.Duration(cfg.TimeKillSec)*time.Second && pnl < cfg.TimeKillMin && (pnl <= 0 || pnl >= minNetExit) && pnl >= -cfg.TimeKillMaxLoss && pnl > -positionSLlimit(s.p):
			reason = fmt.Sprintf("TIMEKILL %ds pnl=%+.1f%%(<%+.1f%%; min -%.1f%%)", int(age.Seconds()), pnl*100, cfg.TimeKillMin*100, cfg.TimeKillMaxLoss*100)
		case cfg.SoftHardKillSec > 0 && age >= time.Duration(cfg.SoftHardKillSec)*time.Second && pnl < minNetExit && pnl >= -cfg.SoftHardKillMaxLoss && pnl > -positionSLlimit(s.p):
			reason = fmt.Sprintf("SOFTHARD %ds pnl=%+.1f%%(<net %+.1f%%; min -%.1f%%)", int(age.Seconds()), pnl*100, minNetExit*100, cfg.SoftHardKillMaxLoss*100)
		case age >= 60*time.Second && pnl < 0.02 && (pnl <= 0 || pnl >= minNetExit) && pnl >= -cfg.SoftHardKillMaxLoss && pnl > -positionSLlimit(s.p):
			reason = fmt.Sprintf("HARDKILL %ds pnl=%+.1f%% (min -%.1f%%)", int(age.Seconds()), pnl*100, cfg.SoftHardKillMaxLoss*100)
		}
		if reason == "" {
			continue
		}

		s.p.LastSellTry = time.Now()
		jobs = append(jobs, sellJob{s.mint, s.p, reason, state, bc, pnl})
	}

	for _, j := range jobs {
		ok := doSell(ctx, j.p, j.reason, j.state, j.bc)
		if ok {
			removePos(j.mint)
		} else {
			posMu.Lock()
			j.p.SellFails++
			posMu.Unlock()
			if j.pnl <= -positionSLlimit(j.p) {
				state2, bc2, err2 := readBC(ctx, j.p.Mint)
				if err2 == nil {
					log.Printf("[MON] Emergency retry: %s", short(j.mint))
					if doSell(ctx, j.p, "EMERGENCY", state2, bc2) {
						removePos(j.mint)
					}
				}
			}
		}
	}
	return hasProfit, needFastYoung
}

func trackTargetResult(wallet string, win bool) {
	tgtLossMu.Lock()
	if win {
		tgtLosses[wallet] = 0
	} else {
		tgtLosses[wallet]++
		if tgtLosses[wallet] >= 2 {
			if isPinnedWallet(wallet) && cfg.PinUnpinLosses > 0 && tgtLosses[wallet] >= cfg.PinUnpinLosses {
				tgtLosses[wallet] = 0
				tgtLossMu.Unlock()
				unpinWallet(wallet, fmt.Sprintf("%d losses подряд", cfg.PinUnpinLosses))
				return
			}
			tgtMu.Lock()
			delete(targets, wallet)
			tgtMu.Unlock()
			log.Printf("[MON] Удалена цель %s (2 убытка подряд)", short(wallet))
			notifyWSReconn()
		}
	}
	tgtLossMu.Unlock()
}

func getWalletPerfLocked(wallet string) *walletPerf {
	if wallet == "" {
		return nil
	}
	p := walletPF[wallet]
	if p == nil {
		p = &walletPerf{}
		walletPF[wallet] = p
	}
	return p
}

func noteWalletTradeOutcome(wallet string, deltaLamports int64, win bool) {
	wallet = strings.TrimSpace(wallet)
	if wallet == "" {
		return
	}
	perfMu.Lock()
	p := getWalletPerfLocked(wallet)
	if p != nil {
		p.Trades++
		if win {
			p.Wins++
		} else {
			p.Losses++
		}
		p.NetLamports += deltaLamports
		p.LastTrade = time.Now()
	}
	perfMu.Unlock()
}

func noteWalletBuyFail(wallet string, err error) {
	wallet = strings.TrimSpace(wallet)
	if wallet == "" {
		return
	}
	// учитываем только slippage/constraint-ошибки — они чаще означают "слишком ранний вход"
	if !(isBuyCustomErr6042(err) || isSlippageErr(err)) {
		return
	}
	now := time.Now()
	perfMu.Lock()
	p := getWalletPerfLocked(wallet)
	if p != nil {
		p.BuyFailCount++
		p.LastFail = now
		// если цель систематически даёт фейлы — временно "мутим" её
		if p.BuyFailCount >= 3 {
			p.BuyFailCount = 0
			p.MutedUntil = now.Add(10 * time.Minute)
		}
	}
	perfMu.Unlock()
}

func walletScore(wallet string, lastSeen time.Time) float64 {
	perfMu.Lock()
	p := walletPF[wallet]
	perfMu.Unlock()
	score := 0.0
	if p != nil {
		// базовая метрика — net результат в SOL
		score += float64(p.NetLamports) / 1e9
		// штраф за малую выборку (не доверяем 1-2 сделкам)
		if p.Trades < 3 {
			score -= 0.05
		}
		// бонус за винрейт (лёгкий)
		if p.Trades > 0 {
			wr := float64(p.Wins) / float64(p.Trades)
			score += (wr - 0.5) * 0.02
		}
		// штраф за недавний mute (чтобы уходили вниз рейтинга)
		if !p.MutedUntil.IsZero() && time.Now().Before(p.MutedUntil) {
			score -= 1.0
		}
	}
	// лёгкий бонус за свежесть, чтобы новые цели могли попасть в топ
	if !lastSeen.IsZero() {
		age := time.Since(lastSeen)
		if age < 10*time.Minute {
			score += 0.005
		}
	}
	return score
}

func pruneTargetsToMax() {
	if cfg.MaxTargets <= 0 {
		return
	}
	pins := snapshotPinnedWallets()
	now := time.Now()

	// Удаляем замученные (mute) цели, если они не pinned
	perfMu.Lock()
	for w, p := range walletPF {
		if p == nil || p.MutedUntil.IsZero() || now.After(p.MutedUntil) {
			continue
		}
		if pins[w] {
			continue
		}
		tgtMu.Lock()
		if _, ok := targets[w]; ok {
			delete(targets, w)
			log.Printf("[TGT] -Removed: %s (muted %.0fs left)", short(w), p.MutedUntil.Sub(now).Seconds())
		}
		tgtMu.Unlock()
	}
	perfMu.Unlock()

	tgtMu.Lock()
	defer tgtMu.Unlock()
	if len(targets) <= cfg.MaxTargets {
		return
	}

	type it struct {
		w    string
		seen time.Time
		sc   float64
		pin  bool
	}
	lst := make([]it, 0, len(targets))
	for w, seen := range targets {
		lst = append(lst, it{w: w, seen: seen, sc: walletScore(w, seen), pin: pins[w]})
	}
	sort.Slice(lst, func(i, j int) bool {
		// pinned всегда выше
		if lst[i].pin != lst[j].pin {
			return lst[i].pin
		}
		if lst[i].sc == lst[j].sc {
			return lst[i].seen.After(lst[j].seen)
		}
		return lst[i].sc > lst[j].sc
	})

	keep := map[string]bool{}
	for i := 0; i < len(lst) && len(keep) < cfg.MaxTargets; i++ {
		keep[lst[i].w] = true
	}
	removed := 0
	for w := range targets {
		if keep[w] {
			continue
		}
		delete(targets, w)
		removed++
	}
	if removed > 0 {
		log.Printf("[TGT] Pruned targets: -%d (keep top %d by score; pinned first)", removed, cfg.MaxTargets)
		notifyWSReconn()
	}
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

	cuLimit := uint32(400_000)

	if !cfg.Live {
		minSol := solNet * (10000 - cfg.SellSlip) / 10000
		binary.LittleEndian.PutUint64(data[16:], minSol)
		log.Printf("[SELL][PAPER] %s | %s | PnL %+.1f%% | %.6f SOL",
			reason, short(mintStr), pnl, float64(solNet)/1e9)
		statSells.Add(1)
		recordPnl(p.Spent, solNet, mintStr)
		return true
	}

	// Для SELL строго уважаем лимит из .env (SELL_SLIPPAGE_BPS),
	// но для net-ориентированных выходов (NETLOCK) сначала пробуем более жёсткий slip,
	// чтобы не превращать "плюс по оценке" в большой net-минус из-за движения кривой.
	firstSlip := cfg.SellSlip
	if strings.Contains(reason, "NETLOCK") {
		// 1500 bps = 15% — заметно меньше 3000 bps (30%), но часто всё ещё хватает на fill.
		if firstSlip > 1500 {
			firstSlip = 1500
		}
	}
	trySlips := []uint64{firstSlip}
	if firstSlip != cfg.SellSlip && cfg.SellSlip > 0 {
		trySlips = append(trySlips, cfg.SellSlip)
	}
	for attempt, slip := range trySlips {
		if slip > 9900 {
			slip = 9900
		}
		minSol := solNet * (10000 - slip) / 10000
		binary.LittleEndian.PutUint64(data[16:], minSol)

		ixs := []solana.Instruction{
			cuLimitIx(cuLimit),
		}
		if tip := jitoTipIx(user); tip != nil {
			ixs = append(ixs, tip)
		}
		ixs = append(ixs,
			cuPriceIx(cfg.PrioLampSell*1_000_000/uint64(cuLimit)),
			&genericIx{pid: PumpProgram, dat: data, accs: sellAccs},
			// Не закрываем ATA в hot-path SELL: возврат rent искажает tx-delta PnL
			// и может давать "ложный плюс" в статистике.
		)

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

		txSig, err := sendTx(ctx, tx, true)

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

		netFloor := minNetExitThreshold() * 100
		log.Printf("[SELL] %s | TX %s | %s | est_pnl=%+.1f%% | net_floor=%+.2f%% | slip=%dbps",
			reason, txSig.String()[:12], short(mintStr), pnl, netFloor, slip)
		statSells.Add(1)

		savedPos := *p
		savedSpent := p.Spent
		savedSolNet := solNet
		savedMint := mintStr
		savedTokProg := tokProg
		go func() {
			accounted := false
			confirmTx(ctx, txSig, "SELL", savedMint, func() {
				ata := findATA(cfg.Key.PublicKey(), savedPos.Mint, savedTokProg)
				rpcWait()
				bal := getTokenBalance(ctx, ata)
				if bal == 0 {
					log.Printf("[SELL] TX не подтверждена, но токены проданы: %s", short(savedMint))
					if !accounted {
						finalizeSellOutcome(savedPos.Wallet, savedSpent, savedSolNet, savedMint)
						accounted = true
					}
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
			if !accounted && err == nil && len(st.Value) > 0 && st.Value[0] != nil && st.Value[0].Err == nil {
				usedSolNet := savedSolNet
				if delta, ok := txPayerDeltaLamports(ctx, txSig); ok && delta > 0 {
					log.Printf("[SELL][TXDELTA] %s | wallet_delta=%+.6f SOL", short(savedMint), float64(delta)/1e9)
					// delta уже "после" sell-fees/tip этой транзакции.
					// Чтобы корректно попасть в модель recordPnl (которая сама вычитает round-trip fixed),
					// нужно вернуть только sell-fixed часть, а не весь round-trip.
					sellFixed := cfg.PrioLampSell + cfg.JitoTipLamp
					usedSolNet = uint64(delta) + sellFixed
				}
				finalizeSellOutcome(savedPos.Wallet, savedSpent, usedSolNet, savedMint)
				accounted = true
			}
		}()
		return true
	}
	return false
}

func snapshotPinnedWallets() map[string]bool {
	pinMu.Lock()
	defer pinMu.Unlock()
	cp := make(map[string]bool, len(pinnedWallet))
	for w, v := range pinnedWallet {
		if v {
			cp[w] = true
		}
	}
	return cp
}

func isPinnedWallet(w string) bool {
	w = strings.TrimSpace(w)
	if w == "" {
		return false
	}
	pinMu.Lock()
	defer pinMu.Unlock()
	return pinnedWallet[w]
}

func unpinWallet(wallet string, reason string) {
	wallet = strings.TrimSpace(wallet)
	if wallet == "" {
		return
	}
	pinMu.Lock()
	if !pinnedWallet[wallet] {
		pinMu.Unlock()
		return
	}
	delete(pinnedWallet, wallet)
	savePinnedWalletsLocked()
	pinMu.Unlock()
	log.Printf("[PIN] -Unpinned: %s (%s)", short(wallet), reason)
	notifyWSReconn()
}

func loadPinnedWallets() {
	if cfg.PinWalletsFile == "" {
		return
	}
	f, err := os.Open(cfg.PinWalletsFile)
	if err != nil {
		// file might not exist on first run
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		log.Printf("[PIN] load pinned wallets: %v", err)
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		w := strings.TrimSpace(sc.Text())
		if w == "" || strings.HasPrefix(w, "#") {
			continue
		}
		if _, err := solana.PublicKeyFromBase58(w); err != nil {
			continue
		}
		pinMu.Lock()
		if !pinnedWallet[w] {
			pinnedWallet[w] = true
			n++
		}
		pinMu.Unlock()
	}
	if err := sc.Err(); err != nil {
		log.Printf("[PIN] read pinned wallets: %v", err)
	}
	if n > 0 {
		log.Printf("[PIN] Загружено pinned кошельков: %d", n)
	}
}

func savePinnedWalletsLocked() {
	if cfg.PinWalletsFile == "" {
		return
	}
	tmp := cfg.PinWalletsFile + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Printf("[PIN] save pinned wallets: %v", err)
		return
	}
	bw := bufio.NewWriter(f)
	for w, ok := range pinnedWallet {
		if !ok {
			continue
		}
		_, _ = bw.WriteString(w)
		_, _ = bw.WriteString("\n")
	}
	_ = bw.Flush()
	_ = f.Close()
	_ = os.Rename(tmp, cfg.PinWalletsFile)
}

func notePinnedWalletOnWin(wallet string, pnl float64) {
	if cfg.PinWalletMinPnl <= 0 {
		return
	}
	wallet = strings.TrimSpace(wallet)
	if wallet == "" {
		return
	}
	if pnl < cfg.PinWalletMinPnl {
		return
	}
	if _, err := solana.PublicKeyFromBase58(wallet); err != nil {
		return
	}
	pinMu.Lock()
	if pinnedWallet[wallet] {
		pinMu.Unlock()
		return
	}
	pinnedWallet[wallet] = true
	savePinnedWalletsLocked()
	pinMu.Unlock()
	log.Printf("[PIN] +Pinned: %s (win pnl=%+.1f%%)", short(wallet), pnl*100)
	notifyWSReconn()
}

// ═══════════════════════════════════════════════════════════════════════════════
//  PUMP.FUN HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

func readBC(ctx context.Context, mint solana.PublicKey) (*BondingCurve, solana.PublicKey, error) {
	// PDA bonding-curve уже считается так же, как в программе pump; «not found» = аккаунт ещё не на RPC.
	bc, _, _ := solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, PumpProgram)
	rpcWait()
	info, err := rpcClient().GetAccountInfoWithOpts(ctx, bc, &rpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: rpc.CommitmentConfirmed,
	})
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

// parseTxEvent removed: scraper no longer uses sequential GetTransaction polling.

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

func jitoTipIx(payer solana.PublicKey) solana.Instruction {
	if cfg.JitoTipLamp == 0 {
		return nil
	}
	acc := cfg.JitoTipAcc
	// For bundles we must write-lock an official tip account to be eligible for auction.
	if (cfg.JitoBundleBuy || cfg.JitoHTTP) && acc.IsZero() && len(cfg.JitoTipAccs) > 0 {
		acc = cfg.JitoTipAccs[int(time.Now().UnixNano())%len(cfg.JitoTipAccs)]
	}
	if acc.IsZero() {
		return nil
	}
	// SystemProgram::Transfer (index 2), lamports u64 LE
	d := make([]byte, 12)
	binary.LittleEndian.PutUint32(d[0:], 2)
	binary.LittleEndian.PutUint64(d[4:], cfg.JitoTipLamp)
	return &genericIx{
		pid: solana.SystemProgramID,
		accs: []*solana.AccountMeta{
			{PublicKey: payer, IsSigner: true, IsWritable: true},
			{PublicKey: acc, IsWritable: true},
		},
		dat: d,
	}
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

func initSessionRefBalance(lamports uint64) {
	statPnlMu.Lock()
	defer statPnlMu.Unlock()
	if sessionRefLamports != 0 || lamports == 0 {
		return
	}
	sessionRefLamports = lamports
	log.Printf("[INIT] База session PnL: %.6f SOL (circuit при накопленном убытке ≥%.0f%% от этой суммы)",
		float64(lamports)/1e9, cfg.MaxSessionLoss)
}

// sessionPnlPct — реализованный PnL сессии в %% от баланса при старте (не сумма %% по сделкам).
func sessionPnlPct() float64 {
	statPnlMu.Lock()
	defer statPnlMu.Unlock()
	if sessionRefLamports == 0 {
		return 0
	}
	return 100 * float64(statPnlLamports) / float64(sessionRefLamports)
}

func scheduleSessionTrailCheck() {
	if cfg.SessionTrailGiveback <= 0 || cfg.SessionTrailMinPeak <= 0 {
		return
	}
	sessionTrailDebMu.Lock()
	defer sessionTrailDebMu.Unlock()
	if sessionTrailDeb != nil {
		sessionTrailDeb.Stop()
	}
	sessionTrailDeb = time.AfterFunc(2*time.Second, func() {
		sessionTrailDebMu.Lock()
		sessionTrailDeb = nil
		sessionTrailDebMu.Unlock()
		checkSessionTrail()
	})
}

func checkSessionTrail() {
	if cfg.SessionTrailGiveback <= 0 || cfg.SessionTrailMinPeak <= 0 || circuitOpen.Load() {
		return
	}
	sp := sessionPnlPct()
	statPnlMu.Lock()
	ref := sessionRefLamports
	statPnlMu.Unlock()
	if ref == 0 {
		return
	}
	sessionPeakPctMu.Lock()
	peak := sessionPeakPct
	sessionPeakPctMu.Unlock()
	if peak < cfg.SessionTrailMinPeak {
		return
	}
	if sp > peak-cfg.SessionTrailGiveback {
		return
	}
	if cfg.SessionTrailNetRedPct > 0 && sp > -cfg.SessionTrailNetRedPct {
		return
	}
	circuitOpen.Store(true)
	log.Printf("[CIRCUIT] Session trail: пик +%.2f%%, сейчас %+.2f%% (откат ≥%.2f п.п.) — новые BUY остановлены",
		peak, sp, cfg.SessionTrailGiveback)
}

// recordPnl учитывает оценку выхода: solNet (lamports) минус spent на входе
// и вычитает фиксированные round-trip издержки (priority/jito), чтобы метрика была ближе к балансу кошелька.
// mintOpt — base58 mint; при убытке ставит loss-cooldown на этот mint (если включён).
func recordPnl(spent uint64, solNet uint64, mintOpt string) {
	if spent == 0 {
		return
	}
	grossDelta := int64(solNet) - int64(spent)
	fixed := int64(fixedTradeCostsLamports())
	delta := grossDelta - fixed
	netOut := int64(solNet) - fixed
	tradePct := float64(netOut)/float64(spent)*100 - 100
	if tradePct > 0 {
		statWins.Add(1)
	} else {
		statLosses.Add(1)
	}

	statPnlMu.Lock()
	statPnlLamports += delta
	lam := statPnlLamports
	ref := sessionRefLamports
	statPnlMu.Unlock()

	if mintOpt != "" && delta < 0 && cfg.MintLossCD > 0 {
		cdMu.Lock()
		lossCdMap[mintOpt] = time.Now()
		cdMu.Unlock()
		log.Printf("[PNL] %s — повторный BUY этого mint заблокирован на %v (закрытие в минус)",
			short(mintOpt), cfg.MintLossCD.Round(time.Second))
	}
	if mintOpt != "" && delta > 0 && cfg.MintWinCD > 0 {
		cdMu.Lock()
		winCdMap[mintOpt] = time.Now()
		cdMu.Unlock()
		log.Printf("[PNL] %s — повторный BUY этого mint заблокирован на %v (закрытие в плюс)",
			short(mintOpt), cfg.MintWinCD.Round(time.Second))
	}

	sp := sessionPnlPct()
	sessionPeakPctMu.Lock()
	if sp > sessionPeakPct {
		sessionPeakPct = sp
	}
	sessionPeakPctMu.Unlock()
	scheduleSessionTrailCheck()

	if ref == 0 || cfg.MaxSessionLoss <= 0 {
		return
	}
	pct := 100 * float64(lam) / float64(ref)
	if pct <= -cfg.MaxSessionLoss && !circuitOpen.Load() {
		circuitOpen.Store(true)
		log.Printf("[CIRCUIT] Session PnL %.2f%% от старта (Σ≈%+.6f SOL) ≤ -%.0f%% — торговля остановлена!",
			pct, float64(lam)/1e9, cfg.MaxSessionLoss)
	}
}

func printStats() {
	w := statWins.Load()
	l := statLosses.Load()
	total := w + l
	sessPct := sessionPnlPct()
	statPnlMu.Lock()
	lam := statPnlLamports
	ref := sessionRefLamports
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
			setWalletBalCache(b.Value)
			balStr = fmt.Sprintf(" | Bal=%.4f SOL", float64(b.Value)/1e9)
		}
	}
	sessExtra := ""
	if ref > 0 {
		sessExtra = fmt.Sprintf(" (sess %+.4f SOL)", float64(lam)/1e9)
	}
	log.Printf("[STAT] Wins=%d Losses=%d WR=%.0f%% | PnL=%+.2f%% от запуска%s | Open=%d | Buys=%d Sells=%d%s",
		w, l, wr, sessPct, sessExtra, open, statBuys.Load(), statSells.Load(), balStr)
	checkSessionTrail()
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
