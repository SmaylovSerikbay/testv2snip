// РџСЂСЏРјС‹Рµ СЃРґРµР»РєРё Pump.fun (buy_exact_sol_in / sell) Р±РµР· Jupiter вЂ” РєР°Рє РІ apexsnip/pump_live.go.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/mr-tron/base58"
)

// Jito tip account (РѕРґРёРЅ РёР· РІРѕСЃСЊРјРё вЂ” РјРѕР¶РЅРѕ РІС‹Р±СЂР°С‚СЊ Р»СЋР±РѕР№)
const jitoTipAccount = "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"

// РџСЂРѕРіСЂР°РјРјР° РєРѕРјРёСЃСЃРёР№ Pump (IDL).
var pumpFeeProgramPK = solana.MustPublicKeyFromBase58("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")

var pumpFeeConfigSeed32 = []byte{
	1, 86, 224, 246, 147, 102, 90, 207, 68, 219, 21, 104, 191, 23, 91, 170,
	81, 137, 203, 151, 245, 210, 255, 59, 101, 93, 43, 182, 253, 109, 24, 176,
}

var pumpBuyExactSolInDisc = []byte{56, 252, 116, 8, 158, 223, 205, 95}
var pumpSellDisc = []byte{51, 230, 133, 164, 1, 127, 131, 173}

var pumpProgramPK = solana.MustPublicKeyFromBase58(PUMP_PROGRAM)

type buyLatencyBreakdown struct {
	FiltersMs       int64
	BlockhashMs     int64
	SigningMs       int64
	SendingMs       int64
	SignedAt        time.Time
	BlockhashCached bool
}

const (
	minMicroLamportsPerCU uint64 = 100_000
	pumpComputeUnitLimit  uint32 = 600_000
)

var (
	pumpBuySlippageBps       uint64 = 2000    // buy 20%
	pumpSellSlippageBps      uint64 = 2000    // sell 20%
	pumpPriorityFeeLamports  uint64 = 200_000 // 0.0002 SOL
	pumpPriorityFeeHardCap   uint64 = 800_000 // 0.0008 SOL: hard cap for small bankroll
	jitoMinTipLamports       uint64 = 500_000 // 0.0005 SOL вЂ” "РІС…РѕРґРЅРѕР№ Р±РёР»РµС‚" РґР»СЏ РІРєР»СЋС‡РµРЅРёСЏ Р±Р°РЅРґР»Р° РІ Р±Р»РѕРє
	pumpPriorityMaxFeeBps    uint64 = 100     // РјР°РєСЃРёРјСѓРј РїСЂРёРѕСЂРёС‚РµС‚Р° РєР°Рє РґРѕР»СЏ РѕС‚ СЂР°Р·РјРµСЂР° СЃРґРµР»РєРё (1.0%)
	pumpSellRetryPriorityFee uint64 = 800_000
	pumpDirectRPC            *solanarpc.Client
	pumpDirectRPCOnce        sync.Once
	priorityFeeCache         struct {
		mu       sync.Mutex
		lamports uint64
		ts       time.Time
	}
	lastBuyLatency struct {
		mu sync.Mutex
		v  buyLatencyBreakdown
	}
	recentBlockhashCache struct {
		mu        sync.Mutex
		blockhash solana.Hash
		ts        time.Time
	}
	jitoLastSend struct {
		mu sync.Mutex
		ts time.Time
	}
	jitoRateLimitState struct {
		mu         sync.Mutex
		until      time.Time
		lastLogAt  time.Time
		lastReason string
	}
	jitoHTTPClient = &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	mintInfoFastPathState struct {
		mu           sync.Mutex
		disabledTill time.Time
	}
)

const jitoRateLimitCooldown = 12 * time.Second
const mintInfoFastPathCooldown = 10 * time.Minute

func initPumpDirectFromEnv() {
	if s := strings.TrimSpace(os.Getenv("PUMP_SLIPPAGE_BPS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v < 10000 {
			pumpBuySlippageBps = v
			pumpSellSlippageBps = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("PUMP_BUY_SLIPPAGE_BPS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v < 10000 {
			pumpBuySlippageBps = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("PUMP_SELL_SLIPPAGE_BPS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v < 10000 {
			pumpSellSlippageBps = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("PRIORITY_FEE_LAMPORTS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			pumpPriorityFeeLamports = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("PUMP_PRIORITY_FEE_HARD_CAP_LAMPORTS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v >= 100_000 {
			pumpPriorityFeeHardCap = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("PUMP_PRIORITY_MAX_FEE_BPS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v <= 2_000 {
			pumpPriorityMaxFeeBps = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("PUMP_SELL_RETRY_PRIORITY_LAMPORTS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v >= 100_000 {
			pumpSellRetryPriorityFee = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("JITO_MIN_TIP_LAMPORTS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			jitoMinTipLamports = v
		}
	}
}

func rpcPumpDirect() *solanarpc.Client {
	pumpDirectRPCOnce.Do(func() {
		pumpDirectRPC = solanarpc.New(heliusURL())
	})
	return pumpDirectRPC
}

func refreshRecentBlockhashCache() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	recent, err := rpcPumpDirect().GetLatestBlockhash(ctx, solanarpc.CommitmentProcessed)
	if err != nil || recent == nil {
		return
	}
	recentBlockhashCache.mu.Lock()
	recentBlockhashCache.blockhash = recent.Value.Blockhash
	recentBlockhashCache.ts = time.Now()
	recentBlockhashCache.mu.Unlock()
}

func getRecentBlockhashCached() (solana.Hash, bool) {
	recentBlockhashCache.mu.Lock()
	defer recentBlockhashCache.mu.Unlock()
	if recentBlockhashCache.ts.IsZero() {
		return solana.Hash{}, false
	}
	if time.Since(recentBlockhashCache.ts) > 8*time.Second {
		return solana.Hash{}, false
	}
	return recentBlockhashCache.blockhash, true
}

func getCachedBlockhash() (solana.Hash, bool) {
	return getRecentBlockhashCached()
}

func prewarmJitoConnection() {
	url := strings.TrimSpace(os.Getenv("JITO_BLOCK_ENGINE_URL"))
	if url == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return
	}
	_, _ = jitoHTTPClient.Do(req)
}

func startPumpHotCaches() {
	refreshRecentBlockhashCache()
	prewarmJitoConnection()
	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for range t.C {
			refreshRecentBlockhashCache()
		}
	}()
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C {
			prewarmJitoConnection()
		}
	}()
}

func setLastBuyLatency(v buyLatencyBreakdown) {
	lastBuyLatency.mu.Lock()
	lastBuyLatency.v = v
	lastBuyLatency.mu.Unlock()
}

func getLastBuyLatency() buyLatencyBreakdown {
	lastBuyLatency.mu.Lock()
	defer lastBuyLatency.mu.Unlock()
	return lastBuyLatency.v
}

// fastHotBuyMode: РІ РіРѕСЂСЏС‡РµРј РІС…РѕРґРµ РЅРµ РґРµР»Р°РµРј Р»РёС€РЅРёРµ pre-check RPC РїРµСЂРµРґ РѕС‚РїСЂР°РІРєРѕР№.
func fastHotBuyMode() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("HOT_PATH_FAST_BUY")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return true
}

func skipMintInfoInFastBuy() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("PUMP_SKIP_MINT_INFO")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	mintInfoFastPathState.mu.Lock()
	disabled := time.Now().Before(mintInfoFastPathState.disabledTill)
	mintInfoFastPathState.mu.Unlock()
	if disabled {
		return false
	}
	return true
}

func disableMintInfoFastPath(reason string) {
	now := time.Now()
	mintInfoFastPathState.mu.Lock()
	mintInfoFastPathState.disabledTill = now.Add(mintInfoFastPathCooldown)
	mintInfoFastPathState.mu.Unlock()
	fmt.Printf("вљ  fast buy path cooldown %s: %s\n", mintInfoFastPathCooldown.String(), reason)
}

func preferJitoBundleFirst() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("JITO_PREFER_BUNDLE")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return true
}

func useJitoEnabled() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("USE_JITO")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return strings.TrimSpace(os.Getenv("JITO_BLOCK_ENGINE_URL")) != ""
}

func pumpBuySignDelay() time.Duration {
	if s := strings.TrimSpace(os.Getenv("PUMP_BUY_SIGN_DELAY_MS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 && v <= 500 {
			return time.Duration(v) * time.Millisecond
		}
	}
	return 0
}

func applySlippagePump(amount uint64, slippageBps uint64) uint64 {
	if amount == 0 || slippageBps >= 10_000 {
		return 0
	}
	a := new(big.Int).SetUint64(amount)
	mul := new(big.Int).Mul(a, big.NewInt(int64(10_000-slippageBps)))
	mul.Div(mul, big.NewInt(10_000))
	if !mul.IsUint64() {
		return 0
	}
	return mul.Uint64()
}

func effectiveMicroLamportsPerCUPump(priorityLamports uint64) uint64 {
	calc := priorityLamports * 1_000_000 / uint64(pumpComputeUnitLimit)
	if calc < minMicroLamportsPerCU {
		return minMicroLamportsPerCU
	}
	return calc
}

func choosePriorityFeeLamports(baseTradeLamports uint64) uint64 {
	fee := pumpPriorityFeeLamports
	if dyn, ok := cachedDynamicPriorityFeeLamports(); ok && dyn > 0 {
		fee = dyn
	}
	if useJitoEnabled() && fee < jitoMinTipLamports {
		fee = jitoMinTipLamports
	}
	if baseTradeLamports > 0 && pumpPriorityMaxFeeBps > 0 {
		capFee := (baseTradeLamports * pumpPriorityMaxFeeBps) / 10_000
		if capFee > 0 && fee > capFee {
			fee = capFee
		}
	}
	if useJitoEnabled() && fee < jitoMinTipLamports {
		fee = jitoMinTipLamports
	}
	if pumpPriorityFeeHardCap > 0 && fee > pumpPriorityFeeHardCap {
		fee = pumpPriorityFeeHardCap
	}
	return fee
}

func cachedDynamicPriorityFeeLamports() (uint64, bool) {
	priorityFeeCache.mu.Lock()
	defer priorityFeeCache.mu.Unlock()
	if priorityFeeCache.lamports == 0 {
		return 0, false
	}
	if time.Since(priorityFeeCache.ts) > 90*time.Second {
		return 0, false
	}
	return priorityFeeCache.lamports, true
}

// refreshDynamicPriorityFeeFromRPC РѕР±РЅРѕРІР»СЏРµС‚ РєСЌС€ С„Рё РІ С„РѕРЅРµ.
// Р’ buy-РїСѓС‚Рё СЌС‚Р° С„СѓРЅРєС†РёСЏ РЅРµ РІС‹Р·С‹РІР°РµС‚СЃСЏ, С‡С‚РѕР±С‹ РЅРµ РґРѕР±Р°РІР»СЏС‚СЊ latency РѕС‚ retry/backoff.
func refreshDynamicPriorityFeeFromRPC() {
	raw, err := rpc("getRecentPrioritizationFees", []interface{}{[]string{}})
	if err != nil {
		return
	}
	var out struct {
		Result []struct {
			PrioritizationFee uint64 `json:"prioritizationFee"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &out) != nil || len(out.Result) == 0 {
		return
	}
	fees := make([]uint64, 0, len(out.Result))
	for _, r := range out.Result {
		if r.PrioritizationFee > 0 {
			fees = append(fees, r.PrioritizationFee)
		}
	}
	if len(fees) == 0 {
		return
	}
	sort.Slice(fees, func(i, j int) bool { return fees[i] < fees[j] })
	// Р‘РµСЂС‘Рј РІРµСЂС…РЅРёР№ РєРІР°СЂС‚РёР»СЊ microLamports/CU РєР°Рє СЂР°Р±РѕС‡РёР№ РєРѕРјРїСЂРѕРјРёСЃСЃ СЃРєРѕСЂРѕСЃС‚Рё/РєРѕРјРёСЃСЃРёРё.
	microPerCU := fees[(len(fees)*3)/4]
	lamports := microPerCU * uint64(pumpComputeUnitLimit) / 1_000_000
	if lamports == 0 {
		lamports = pumpPriorityFeeLamports
	}
	priorityFeeCache.mu.Lock()
	priorityFeeCache.lamports = lamports
	priorityFeeCache.ts = time.Now()
	priorityFeeCache.mu.Unlock()
}

const jitoMinInterval = 2 * time.Second // РЅРµ С‡Р°С‰Рµ 1 СЂР°Р· РІ 2 СЃРµРє вЂ” РёРЅР°С‡Рµ 429 Too Many Requests

func jitoRateLimitedNow() bool {
	jitoRateLimitState.mu.Lock()
	defer jitoRateLimitState.mu.Unlock()
	return time.Now().Before(jitoRateLimitState.until)
}

func markJitoRateLimited(reason string) {
	now := time.Now()
	jitoRateLimitState.mu.Lock()
	jitoRateLimitState.until = now.Add(jitoRateLimitCooldown)
	shouldLog := now.Sub(jitoRateLimitState.lastLogAt) > 5*time.Second || jitoRateLimitState.lastReason != reason
	jitoRateLimitState.lastLogAt = now
	jitoRateLimitState.lastReason = reason
	jitoRateLimitState.mu.Unlock()
	if shouldLog {
		fmt.Printf("вљ  Jito cooldown %ds: %s; fallback РЅР° RPC\n", int(jitoRateLimitCooldown.Seconds()), reason)
	}
}

func shouldUseJitoPath() bool {
	if !useJitoEnabled() {
		return false
	}
	if jitoRateLimitedNow() {
		return false
	}
	jitoLastSend.mu.Lock()
	elapsed := time.Since(jitoLastSend.ts)
	jitoLastSend.mu.Unlock()
	return elapsed >= jitoMinInterval
}

func sendPumpTransaction(ctx context.Context, rpcClient *solanarpc.Client, tx *solana.Transaction) (solana.Signature, time.Time, error) {
	txSig := solana.Signature{}
	if len(tx.Signatures) > 0 {
		txSig = tx.Signatures[0]
	}
	j := strings.TrimSpace(os.Getenv("JITO_BLOCK_ENGINE_URL"))
	useJitoInRace := shouldUseJitoPath()
	if useJitoInRace && preferJitoBundleFirst() {
		jitoCtx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
		_, _, jErr := sendJitoBundle(jitoCtx, j, tx)
		cancel()
		if jErr == nil {
			jitoLastSend.mu.Lock()
			jitoLastSend.ts = time.Now()
			jitoLastSend.mu.Unlock()
			return txSig, time.Now(), nil
		}
		rpcCtx, cancelRPC := context.WithTimeout(ctx, 5*time.Second)
		defer cancelRPC()
		sig, rErr := rpcClient.SendTransactionWithOpts(rpcCtx, tx, solanarpc.TransactionOpts{
			SkipPreflight: false, PreflightCommitment: solanarpc.CommitmentProcessed,
		})
		if rErr == nil {
			return sig, time.Now(), nil
		}
		rpcCtx2, cancelRPC2 := context.WithTimeout(ctx, 6*time.Second)
		defer cancelRPC2()
		if sig2, err2 := rpcClient.SendTransactionWithOpts(rpcCtx2, tx, solanarpc.TransactionOpts{
			SkipPreflight: true, PreflightCommitment: solanarpc.CommitmentProcessed,
		}); err2 == nil {
			return sig2, time.Now(), nil
		}
		return solana.Signature{}, time.Time{}, fmt.Errorf("jito: %v; rpc: %w", jErr, rErr)
	}

	if useJitoInRace {
		// Race: Jito Рё RPC РѕРґРЅРѕРІСЂРµРјРµРЅРЅРѕ вЂ” РїРµСЂРІС‹Р№ СѓСЃРїРµС… РїРѕР±РµР¶РґР°РµС‚ (~50ms РІРјРµСЃС‚Рѕ 300ms+)
		type result struct {
			ok  bool
			err error
		}
		jitoCh := make(chan result, 1)
		rpcCh := make(chan result, 1)

		go func() {
			jitoCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			_, _, err := sendJitoBundle(jitoCtx, j, tx)
			jitoCh <- result{ok: err == nil, err: err}
			if err == nil {
				jitoLastSend.mu.Lock()
				jitoLastSend.ts = time.Now()
				jitoLastSend.mu.Unlock()
			}
		}()
		go func() {
			rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := rpcClient.SendTransactionWithOpts(rpcCtx, tx, solanarpc.TransactionOpts{
				SkipPreflight: false, PreflightCommitment: solanarpc.CommitmentProcessed,
			})
			rpcCh <- result{ok: err == nil, err: err}
		}()

		var jitoRes, rpcRes result
		var jitoDone, rpcDone bool
		for {
			select {
			case jitoRes = <-jitoCh:
				jitoDone = true
				if jitoRes.ok {
					return txSig, time.Now(), nil
				}
			case rpcRes = <-rpcCh:
				rpcDone = true
				if rpcRes.ok {
					return txSig, time.Now(), nil
				}
			}
			if jitoDone && rpcDone {
				if jitoRes.err != nil && !strings.Contains(jitoRes.err.Error(), "429") {
					fmt.Printf("вљ  Jito: %v\n", jitoRes.err)
				}
				if rpcRes.err != nil && jitoRes.err != nil {
					// РџРѕСЃР»РµРґРЅСЏСЏ РїРѕРїС‹С‚РєР°: РѕС‚РїСЂР°РІРєР° Р±РµР· preflight, С‡С‚РѕР±С‹ РЅРµ С‚РµСЂСЏС‚СЊ РІС…РѕРґ РёР·-Р·Р° РїРµСЂРµРіСЂСѓР·Р°.
					rpcCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
					defer cancel()
					if sig, err := rpcClient.SendTransactionWithOpts(rpcCtx, tx, solanarpc.TransactionOpts{
						SkipPreflight: true, PreflightCommitment: solanarpc.CommitmentProcessed,
					}); err == nil {
						return sig, time.Now(), nil
					}
				}
				if rpcRes.err != nil {
					return solana.Signature{}, time.Time{}, fmt.Errorf("SendTransactionWithOpts: %w", rpcRes.err)
				}
				return solana.Signature{}, time.Time{}, fmt.Errorf("jito: %w", jitoRes.err)
			}
		}
	}

	// РўРѕР»СЊРєРѕ RPC (Jito РІС‹РєР» РёР»Рё rate limited)
	sig, err := rpcClient.SendTransactionWithOpts(ctx, tx, solanarpc.TransactionOpts{
		SkipPreflight: false, PreflightCommitment: solanarpc.CommitmentProcessed,
	})
	if err != nil {
		return solana.Signature{}, time.Time{}, fmt.Errorf("SendTransactionWithOpts: %w", err)
	}
	return sig, time.Now(), nil
}

func sendJitoBundle(ctx context.Context, blockEngineURL string, tx *solana.Transaction) (solana.Signature, time.Time, error) {
	rawTx, err := tx.MarshalBinary()
	if err != nil {
		return solana.Signature{}, time.Time{}, fmt.Errorf("marshal tx: %w", err)
	}
	// Jito block-engine РѕР¶РёРґР°РµС‚ base58; base64 РґР°С‘С‚ "transaction #0 could not be decoded"
	encodedTx := base58.Encode(rawTx)
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendBundle",
		"params":  []interface{}{[]string{encodedTx}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return solana.Signature{}, time.Time{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, blockEngineURL, bytes.NewReader(body))
	if err != nil {
		return solana.Signature{}, time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := jitoHTTPClient.Do(req)
	if err != nil {
		return solana.Signature{}, time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		bodyStr := strings.TrimSpace(string(bodyBytes))
		if resp.StatusCode == http.StatusTooManyRequests ||
			strings.Contains(bodyStr, "globally rate limited") ||
			strings.Contains(bodyStr, "Network congested") ||
			strings.Contains(bodyStr, "Too Many Requests") {
			markJitoRateLimited("429/rate limited")
		}
		if bodyStr != "" {
			fmt.Printf("вќЊ Jito API error | status=%s | body=%s\n", resp.Status, bodyStr)
		} else {
			fmt.Printf("вќЊ Jito API error | status=%s\n", resp.Status)
		}
		return solana.Signature{}, time.Time{}, fmt.Errorf("jito status: %s", resp.Status)
	}
	sig := solana.Signature{}
	if len(tx.Signatures) > 0 {
		sig = tx.Signatures[0]
	}
	return sig, time.Now(), nil
}

func signTransactionAsync(tx *solana.Transaction, owner solana.PublicKey, wallet solana.PrivateKey) error {
	done := make(chan error, 1)
	go func() {
		_, err := tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
			if key.Equals(owner) {
				return &wallet
			}
			return nil
		})
		done <- err
	}()
	return <-done
}

func nativeBalanceLamports(ctx context.Context, rpcClient *solanarpc.Client, owner solana.PublicKey) (uint64, error) {
	b, err := rpcClient.GetBalance(ctx, owner, solanarpc.CommitmentProcessed)
	if err != nil || b == nil {
		return 0, fmt.Errorf("getBalance: %w", err)
	}
	return b.Value, nil
}

// waitNativeBalanceDelta вЂ” РїС‹С‚Р°РµС‚СЃСЏ РїРѕР№РјР°С‚СЊ РёР·РјРµРЅРµРЅРёРµ SOL РїРѕСЃР»Рµ РѕС‚РїСЂР°РІРєРё tx.
func waitNativeBalanceDelta(ctx context.Context, rpcClient *solanarpc.Client, owner solana.PublicKey, before uint64, expectIncrease bool) (delta uint64, ok bool) {
	for i := 0; i < 8; i++ {
		select {
		case <-ctx.Done():
			return 0, false
		default:
		}
		time.Sleep(650 * time.Millisecond)
		after, err := nativeBalanceLamports(ctx, rpcClient, owner)
		if err != nil {
			continue
		}
		if expectIncrease {
			if after > before {
				return after - before, true
			}
		} else {
			if before > after {
				return before - after, true
			}
		}
	}
	return 0, false
}

func derivePumpGlobal() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("global")}, pumpProgramPK)
}

func derivePumpBondingCurve(mint solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, pumpProgramPK)
}

func derivePumpBondingCurveV2(mint solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("bonding-curve-v2"), mint.Bytes()}, pumpProgramPK)
}

func derivePumpEventAuthority() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("__event_authority")}, pumpProgramPK)
}

func derivePumpFeeConfig() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("fee_config"), pumpFeeConfigSeed32}, pumpFeeProgramPK)
}

func derivePumpGlobalVolumeAccumulator() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("global_volume_accumulator")}, pumpProgramPK)
}

func derivePumpUserVolumeAccumulator(user solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("user_volume_accumulator"), user.Bytes()}, pumpProgramPK)
}

func derivePumpCreatorVault(creator solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("creator-vault"), creator.Bytes()}, pumpProgramPK)
}

func parsePumpBondingCurveData(data []byte) (virtualToken, virtualSol, realToken, realSol, tokenTotal uint64, complete bool, creator solana.PublicKey, err error) {
	if len(data) < 8+8*5+1+32 {
		return 0, 0, 0, 0, 0, false, solana.PublicKey{}, fmt.Errorf("bonding curve data too short")
	}
	off := 8
	virtualToken = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	virtualSol = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	realToken = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	realSol = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	tokenTotal = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	complete = data[off] != 0
	off++
	copy(creator[:], data[off:off+32])
	return virtualToken, virtualSol, realToken, realSol, tokenTotal, complete, creator, nil
}

func parsePumpGlobalFees(data []byte) (feeRecipient solana.PublicKey, feeBps, creatorFeeBps uint64, err error) {
	if len(data) < 170 {
		return solana.PublicKey{}, 0, 0, fmt.Errorf("global data too short")
	}
	off := 8
	off++
	off += 32
	copy(feeRecipient[:], data[off:off+32])
	off += 32
	off += 8 * 4
	feeBps = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	off += 32
	off++
	off += 8
	creatorFeeBps = binary.LittleEndian.Uint64(data[off : off+8])
	return feeRecipient, feeBps, creatorFeeBps, nil
}

func pumpQuoteExpectedTokensBuyExactSolIn(spendableSolIn, vSol, vToken, protocolFeeBps, creatorFeeBps uint64) uint64 {
	totalFeeBps := protocolFeeBps + creatorFeeBps
	if totalFeeBps > 10000 {
		totalFeeBps = 10000
	}
	sp := new(big.Int).SetUint64(spendableSolIn)
	netSol := new(big.Int).Mul(sp, big.NewInt(10000))
	netSol.Div(netSol, big.NewInt(int64(10000+totalFeeBps)))

	fees1 := ceilDivBigPump(new(big.Int).Mul(netSol, big.NewInt(int64(protocolFeeBps))), big.NewInt(10000))
	fees2 := ceilDivBigPump(new(big.Int).Mul(netSol, big.NewInt(int64(creatorFeeBps))), big.NewInt(10000))
	fees := new(big.Int).Add(fees1, fees2)

	sum := new(big.Int).Add(netSol, fees)
	if sum.Cmp(sp) > 0 {
		adj := new(big.Int).Sub(sum, sp)
		netSol.Sub(netSol, adj)
	}
	if netSol.Sign() <= 0 || netSol.Cmp(big.NewInt(1)) <= 0 {
		return 0
	}
	ns := netSol.Uint64()
	if ns <= 1 {
		return 0
	}
	num := new(big.Int).SetUint64(ns - 1)
	num.Mul(num, new(big.Int).SetUint64(vToken))
	den := new(big.Int).SetUint64(vSol)
	den.Add(den, big.NewInt(int64(ns)))
	den.Sub(den, big.NewInt(1))
	if den.Sign() <= 0 {
		return 0
	}
	num.Div(num, den)
	if !num.IsUint64() {
		return 0
	}
	return num.Uint64()
}

const (
	pumpMinOutExtraHaircutBps uint64 = 2000
	pumpSpendableBufferBps    uint64 = 0
)

func pumpSpendableWithBuffer(baseLamports uint64, bufferBps uint64) uint64 {
	if baseLamports == 0 || bufferBps == 0 {
		return baseLamports
	}
	delta := new(big.Int).SetUint64(baseLamports)
	delta.Mul(delta, big.NewInt(int64(bufferBps)))
	delta.Div(delta, big.NewInt(10000))
	out := new(big.Int).SetUint64(baseLamports)
	out.Add(out, delta)
	if !out.IsUint64() {
		return ^uint64(0)
	}
	return out.Uint64()
}

func pumpExtraHaircutMinOut(minOut uint64, extraBps uint64) uint64 {
	if minOut == 0 || extraBps >= 10000 {
		return minOut
	}
	a := new(big.Int).SetUint64(minOut)
	a.Mul(a, big.NewInt(int64(10000-extraBps)))
	a.Div(a, big.NewInt(10000))
	if !a.IsUint64() {
		return 1
	}
	x := a.Uint64()
	if x == 0 {
		return 1
	}
	return x
}

func pumpEnvForceMinOutOne() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("PUMP_MIN_OUT_ONE")))
	if s == "false" || s == "0" || s == "no" {
		return false
	}
	return true
}

// pumpEnvForceSellMinSolOne вЂ” РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ true: min_sol_out=1 РїСЂРё РїСЂРѕРґР°Р¶Рµ, С‡С‚РѕР±С‹ РЅРµ Р»РѕРІРёС‚СЊ 6024 Overflow
// (РєР»РёРµРЅС‚СЃРєР°СЏ CPMM в‰  GetFees on-chain). РћС‚РєР»СЋС‡РёС‚СЊ: PUMP_SELL_MIN_SOL_ONE=false
func pumpEnvForceSellMinSolOne() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("PUMP_SELL_MIN_SOL_ONE")))
	if s == "false" || s == "0" || s == "no" {
		return false
	}
	return true
}

func pumpComputeMinTokensOut(spendable, vSol, vToken, realToken, feeBps, creatorBps, slipBps uint64) (expectedOut, minOut uint64) {
	expectedOut = pumpQuoteExpectedTokensBuyExactSolIn(spendable, vSol, vToken, feeBps, creatorBps)
	if expectedOut == 0 {
		return 0, 0
	}
	if realToken > 0 && expectedOut > realToken {
		expectedOut = realToken
	}
	const pfeeSlipCushionBps uint64 = 2000
	totalSlip := slipBps + pfeeSlipCushionBps
	if totalSlip >= 9900 {
		totalSlip = 9899
	}
	minOut = applySlippagePump(expectedOut, totalSlip)
	if minOut == 0 && expectedOut > 0 {
		minOut = 1
	}
	if realToken > 0 && minOut > realToken {
		minOut = realToken
	}
	if minOut > expectedOut {
		minOut = expectedOut
	}
	return expectedOut, minOut
}

func pumpValidateBuyQuote(expectedOut, minOut, realToken uint64, mintDecimals uint8) error {
	if expectedOut == 0 || minOut == 0 {
		return fmt.Errorf("quote invalid: expected=%d min_out=%d", expectedOut, minOut)
	}
	if minOut > expectedOut {
		return fmt.Errorf("min_out>expected: %d > %d", minOut, expectedOut)
	}
	if realToken == 0 {
		return fmt.Errorf("real_token_reserves=0 (empty curve)")
	}
	if minOut > realToken {
		return fmt.Errorf("min_out>real_token_reserves: %d > %d", minOut, realToken)
	}
	const maxAtoms = uint64(1 << 62)
	if expectedOut > maxAtoms || minOut > maxAtoms || realToken > maxAtoms {
		return fmt.Errorf("amounts too large (sanity cap)")
	}
	if mintDecimals > 9 {
		return fmt.Errorf("mint decimals=%d invalid", mintDecimals)
	}
	return nil
}

func mintMetaFromMintData(ctx context.Context, c *solanarpc.Client, mint solana.PublicKey) (tokenProgram solana.PublicKey, decimals uint8, err error) {
	acc, err := c.GetAccountInfoWithOpts(ctx, mint, &solanarpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: solanarpc.CommitmentProcessed})
	if err != nil || acc == nil || acc.Value == nil {
		return solana.PublicKey{}, 0, fmt.Errorf("get mint account")
	}
	data := acc.Value.Data.GetBinary()
	if len(data) < 45 {
		return solana.PublicKey{}, 0, fmt.Errorf("mint data too short")
	}
	return acc.Value.Owner, data[44], nil
}

func ensurePumpUserATA(preIxs *[]solana.Instruction, owner, mint, tokenProgram solana.PublicKey) (solana.PublicKey, error) {
	ata, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.PublicKey{}, err
	}
	// Idempotent ATA create: Р±РµР·РѕРїР°СЃРЅРѕ РґРѕР±Р°РІР»СЏС‚СЊ РІСЃРµРіРґР°, Р±РµР· RPC pre-check.
	ix := solana.NewInstruction(
		solana.SPLAssociatedTokenAccountProgramID,
		[]*solana.AccountMeta{
			{PublicKey: owner, IsSigner: true, IsWritable: true},
			{PublicKey: ata, IsSigner: false, IsWritable: true},
			{PublicKey: owner, IsSigner: false, IsWritable: false},
			{PublicKey: mint, IsSigner: false, IsWritable: false},
			{PublicKey: solana.SystemProgramID, IsSigner: false, IsWritable: false},
			{PublicKey: tokenProgram, IsSigner: false, IsWritable: false},
		},
		[]byte{1},
	)
	*preIxs = append(*preIxs, ix)
	return ata, nil
}

func mintTokenProgram(ctx context.Context, c *solanarpc.Client, mint solana.PublicKey) (solana.PublicKey, error) {
	tp, _, err := mintMetaFromMintData(ctx, c, mint)
	return tp, err
}

func ceilDivBigPump(a, b *big.Int) *big.Int {
	if b.Sign() == 0 {
		return big.NewInt(0)
	}
	num := new(big.Int).Add(a, new(big.Int).Sub(b, big.NewInt(1)))
	return num.Div(num, b)
}

func encodePumpBuyExactSolInData(spendableSolIn, minTokensOut uint64) []byte {
	buf := make([]byte, 8+8+8+1)
	copy(buf[0:8], pumpBuyExactSolInDisc)
	binary.LittleEndian.PutUint64(buf[8:16], spendableSolIn)
	binary.LittleEndian.PutUint64(buf[16:24], minTokensOut)
	buf[24] = 0
	return buf
}

func encodePumpSellData(tokenAmount, minSolOut uint64) []byte {
	buf := make([]byte, 8+8+8)
	copy(buf[0:8], pumpSellDisc)
	binary.LittleEndian.PutUint64(buf[8:16], tokenAmount)
	binary.LittleEndian.PutUint64(buf[16:24], minSolOut)
	return buf
}

func swapPumpFun(ctx context.Context, rpcClient *solanarpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, spendableLamports uint64, forceMintInfo bool) (solana.Signature, uint64, uint64, time.Time, error) {
	owner := wallet.PublicKey()
	startAt := time.Now()
	fastMode := fastHotBuyMode()
	skipMintInfo := fastMode && skipMintInfoInFastBuy() && !forceMintInfo
	var balBefore uint64
	if !fastMode {
		var err error
		balBefore, err = nativeBalanceLamports(ctx, rpcClient, owner)
		if err != nil {
			return solana.Signature{}, 0, 0, time.Time{}, err
		}
	}

	bondingCurve, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}

	globalPK, _, err := derivePumpGlobal()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}

	var (
		tokenProgram  solana.PublicKey
		mintDecimals  uint8
		feeRecipient  solana.PublicKey
		feeBps        uint64
		creatorFeeBps uint64
		vTok          uint64
		vSol          uint64
		realToken     uint64
		complete      bool
		creator       solana.PublicKey
		firstErr      error
	)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	setErr := func(e error) {
		if e == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		errMu.Unlock()
	}
	if skipMintInfo {
		// РњР°РєСЃРёРјР°Р»СЊРЅРѕ Р±С‹СЃС‚СЂС‹Р№ РїСѓС‚СЊ: РЅРµ РґС‘СЂРіР°РµРј mint account (getAccountInfo) РґРѕ РїРѕРєСѓРїРєРё.
		tokenProgram = solana.TokenProgramID
		mintDecimals = 6
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tp, md, e := mintMetaFromMintData(ctx, rpcClient, mint)
			if e != nil {
				setErr(e)
				return
			}
			tokenProgram = tp
			mintDecimals = md
		}()
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		gInfo, e := rpcClient.GetAccountInfoWithOpts(ctx, globalPK, &solanarpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: solanarpc.CommitmentProcessed})
		if e != nil || gInfo == nil || gInfo.Value == nil || gInfo.Value.Data == nil {
			setErr(fmt.Errorf("global account missing"))
			return
		}
		fr, fbps, cfbps, e := parsePumpGlobalFees(gInfo.Value.Data.GetBinary())
		if e != nil {
			setErr(e)
			return
		}
		feeRecipient, feeBps, creatorFeeBps = fr, fbps, cfbps
	}()
	go func() {
		defer wg.Done()
		bcInfo, e := rpcClient.GetAccountInfoWithOpts(ctx, bondingCurve, &solanarpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: solanarpc.CommitmentProcessed})
		if e != nil || bcInfo == nil || bcInfo.Value == nil || bcInfo.Value.Data == nil {
			setErr(fmt.Errorf("bonding curve account missing"))
			return
		}
		vt, vs, rt, _, _, c, cr, e := parsePumpBondingCurveData(bcInfo.Value.Data.GetBinary())
		if e != nil {
			setErr(e)
			return
		}
		vTok, vSol, realToken, complete, creator = vt, vs, rt, c, cr
	}()
	wg.Wait()
	if firstErr != nil {
		return solana.Signature{}, 0, 0, time.Time{}, firstErr
	}
	filtersMs := time.Since(startAt).Milliseconds()

	spendableBudget := pumpSpendableWithBuffer(spendableLamports, pumpSpendableBufferBps)

	if complete {
		return solana.Signature{}, 0, 0, time.Time{}, fmt.Errorf("bonding curve complete (migrated)")
	}

	expectedOut, minOut := pumpComputeMinTokensOut(
		spendableBudget, vSol, vTok, realToken, feeBps, creatorFeeBps, pumpBuySlippageBps,
	)
	minOut = pumpExtraHaircutMinOut(minOut, pumpMinOutExtraHaircutBps)
	if pumpEnvForceMinOutOne() {
		minOut = 1
	}
	if err := pumpValidateBuyQuote(expectedOut, minOut, realToken, mintDecimals); err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, fmt.Errorf("pump buy quote: %w", err)
	}

	assocBonding, _, err := solana.FindProgramAddress(
		[][]byte{bondingCurve.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}

	assocUser, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}

	creatorVault, _, err := derivePumpCreatorVault(creator)
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	eventAuth, _, err := derivePumpEventAuthority()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	gVol, _, err := derivePumpGlobalVolumeAccumulator()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	uVol, _, err := derivePumpUserVolumeAccumulator(owner)
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	feeCfg, _, err := derivePumpFeeConfig()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	bondingCurveV2, _, err := derivePumpBondingCurveV2(mint)
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}

	data := encodePumpBuyExactSolInData(spendableBudget, minOut)

	metas := []*solana.AccountMeta{
		{PublicKey: globalPK, IsSigner: false, IsWritable: false},
		{PublicKey: feeRecipient, IsSigner: false, IsWritable: true},
		{PublicKey: mint, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurve, IsSigner: false, IsWritable: true},
		{PublicKey: assocBonding, IsSigner: false, IsWritable: true},
		{PublicKey: assocUser, IsSigner: false, IsWritable: true},
		{PublicKey: owner, IsSigner: true, IsWritable: true},
		{PublicKey: solana.SystemProgramID, IsSigner: false, IsWritable: false},
		{PublicKey: tokenProgram, IsSigner: false, IsWritable: false},
		{PublicKey: creatorVault, IsSigner: false, IsWritable: true},
		{PublicKey: eventAuth, IsSigner: false, IsWritable: false},
		{PublicKey: pumpProgramPK, IsSigner: false, IsWritable: false},
		{PublicKey: gVol, IsSigner: false, IsWritable: false},
		{PublicKey: uVol, IsSigner: false, IsWritable: true},
		{PublicKey: feeCfg, IsSigner: false, IsWritable: false},
		{PublicKey: pumpFeeProgramPK, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurveV2, IsSigner: false, IsWritable: false},
	}

	buyIx := solana.NewInstruction(pumpProgramPK, metas, data)

	var preIxs []solana.Instruction
	if _, err := ensurePumpUserATA(&preIxs, owner, mint, tokenProgram); err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, fmt.Errorf("user ATA: %w", err)
	}

	cuLimitIx, err := computebudget.NewSetComputeUnitLimitInstruction(pumpComputeUnitLimit).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	priorityLamports := choosePriorityFeeLamports(spendableBudget)
	microPerCU := effectiveMicroLamportsPerCUPump(priorityLamports)
	cuPriceIx, err := computebudget.NewSetComputeUnitPriceInstruction(microPerCU).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}

	bhStart := time.Now()
	cachedBH, ok := getCachedBlockhash()
	blockhashMs := time.Since(bhStart).Milliseconds()
	if !ok {
		return solana.Signature{}, 0, 0, time.Time{}, fmt.Errorf("blockhash cache cold")
	}

	all := append([]solana.Instruction{}, cuLimitIx, cuPriceIx)
	all = append(all, preIxs...)
	all = append(all, buyIx)
	if shouldUseJitoPath() && jitoMinTipLamports > 0 {
		tipIx := system.NewTransferInstruction(jitoMinTipLamports, owner, solana.MustPublicKeyFromBase58(jitoTipAccount)).Build()
		all = append(all, tipIx)
	}

	tx, err := solana.NewTransaction(all, cachedBH, solana.TransactionPayer(owner))
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	if d := pumpBuySignDelay(); d > 0 {
		time.Sleep(d)
	}
	signStart := time.Now()
	err = signTransactionAsync(tx, owner, wallet)
	signedAt := time.Now()
	signingMs := time.Since(signStart).Milliseconds()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}

	sendStart := time.Now()
	sig, sentAt, err := sendPumpTransaction(ctx, rpcClient, tx)
	sendingMs := time.Since(sendStart).Milliseconds()
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
	}
	setLastBuyLatency(buyLatencyBreakdown{
		FiltersMs:       filtersMs,
		BlockhashMs:     blockhashMs,
		SigningMs:       signingMs,
		SendingMs:       sendingMs,
		SignedAt:        signedAt,
		BlockhashCached: true,
	})
	actualSpent := spendableBudget
	if !fastMode {
		// Р¤Р°РєС‚РёС‡РµСЃРєРёРµ Р»Р°РјРїРѕСЂС‚С‹, СЃРїРёСЃР°РЅРЅС‹Рµ СЃ РєРѕС€РµР»СЊРєР°, РІР°Р¶РЅРµРµ РѕС†РµРЅРѕРє (СѓС‡С‘С‚ РєРѕРјРёСЃСЃРёР№/priority/СЂРµР°Р»СЊРЅРѕРіРѕ РёСЃРїРѕР»РЅРµРЅРёСЏ).
		if measured, ok := waitNativeBalanceDelta(ctx, rpcClient, owner, balBefore, false); ok && measured > 0 {
			actualSpent = measured
		}
	}
	return sig, expectedOut, actualSpent, sentAt, nil
}

func pumpQuoteMinSolForSell(tokenAmount, vSol, vToken uint64, slipBps uint64) uint64 {
	if tokenAmount == 0 || vToken <= tokenAmount {
		return 0
	}
	num := new(big.Int).SetUint64(tokenAmount)
	num.Mul(num, new(big.Int).SetUint64(vSol))
	den := new(big.Int).SetUint64(vToken)
	den.Sub(den, new(big.Int).SetUint64(tokenAmount))
	if den.Sign() <= 0 {
		return 0
	}
	num.Div(num, den)
	if !num.IsUint64() {
		return 0
	}
	return applySlippagePump(num.Uint64(), slipBps)
}

func pumpGrossSolForSell(tokenAmount, vSol, vToken uint64) uint64 {
	if tokenAmount == 0 || vToken <= tokenAmount {
		return 0
	}
	num := new(big.Int).SetUint64(tokenAmount)
	num.Mul(num, new(big.Int).SetUint64(vSol))
	den := new(big.Int).SetUint64(vToken)
	den.Sub(den, new(big.Int).SetUint64(tokenAmount))
	if den.Sign() <= 0 {
		return 0
	}
	num.Div(num, den)
	if !num.IsUint64() {
		return 0
	}
	return num.Uint64()
}

func swapPumpFunSellAmount(ctx context.Context, rpcClient *solanarpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, tokenAmount uint64, slipBps uint64) (solana.Signature, uint64, error) {
	owner := wallet.PublicKey()
	balBefore, err := nativeBalanceLamports(ctx, rpcClient, owner)
	if err != nil {
		return solana.Signature{}, 0, err
	}

	bondingCurve, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return solana.Signature{}, 0, err
	}
	bcInfo, err := rpcClient.GetAccountInfoWithOpts(ctx, bondingCurve, &solanarpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: solanarpc.CommitmentProcessed})
	if err != nil || bcInfo == nil || bcInfo.Value == nil {
		return solana.Signature{}, 0, fmt.Errorf("bonding curve")
	}
	bcData := bcInfo.Value.Data.GetBinary()
	vTok, vSol, _, _, _, complete, creator, err := parsePumpBondingCurveData(bcData)
	if err != nil {
		return solana.Signature{}, 0, err
	}
	if complete {
		return solana.Signature{}, 0, fmt.Errorf("curve complete вЂ” РёСЃРїРѕР»СЊР·СѓР№ DEX, РЅРµ pump sell")
	}

	grossSol := pumpGrossSolForSell(tokenAmount, vSol, vTok)
	minSol := pumpQuoteMinSolForSell(tokenAmount, vSol, vTok, slipBps)
	if minSol == 0 {
		minSol = 1
	}
	if pumpEnvForceSellMinSolOne() {
		// РљР°Рє PUMP_MIN_OUT_ONE РЅР° РїРѕРєСѓРїРєРµ: min_sol_out=1 вЂ” РёРЅР°С‡Рµ С‡Р°СЃС‚Рѕ 6024 Overflow (РєР»РёРµРЅС‚ в‰  GetFees).
		minSol = 1
	} else {
		minSol = pumpExtraHaircutMinOut(minSol, pumpMinOutExtraHaircutBps)
		if minSol == 0 {
			minSol = 1
		}
	}

	globalPK, _, _ := derivePumpGlobal()
	gInfo, err := rpcClient.GetAccountInfoWithOpts(ctx, globalPK, &solanarpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: solanarpc.CommitmentProcessed})
	if err != nil || gInfo == nil || gInfo.Value == nil {
		return solana.Signature{}, 0, fmt.Errorf("global")
	}
	feeRecipient, _, _, err := parsePumpGlobalFees(gInfo.Value.Data.GetBinary())
	if err != nil {
		return solana.Signature{}, 0, err
	}

	tokenProgram, err := mintTokenProgram(ctx, rpcClient, mint)
	if err != nil {
		return solana.Signature{}, 0, err
	}

	assocBonding, _, err := solana.FindProgramAddress(
		[][]byte{bondingCurve.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, 0, err
	}

	assocUser, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, 0, err
	}

	creatorVault, _, err := derivePumpCreatorVault(creator)
	if err != nil {
		return solana.Signature{}, 0, err
	}
	eventAuth, _, err := derivePumpEventAuthority()
	if err != nil {
		return solana.Signature{}, 0, err
	}
	feeCfg, _, err := derivePumpFeeConfig()
	if err != nil {
		return solana.Signature{}, 0, err
	}
	bondingCurveV2, _, err := derivePumpBondingCurveV2(mint)
	if err != nil {
		return solana.Signature{}, 0, err
	}

	data := encodePumpSellData(tokenAmount, minSol)

	metas := []*solana.AccountMeta{
		{PublicKey: globalPK, IsSigner: false, IsWritable: false},
		{PublicKey: feeRecipient, IsSigner: false, IsWritable: true},
		{PublicKey: mint, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurve, IsSigner: false, IsWritable: true},
		{PublicKey: assocBonding, IsSigner: false, IsWritable: true},
		{PublicKey: assocUser, IsSigner: false, IsWritable: true},
		{PublicKey: owner, IsSigner: true, IsWritable: true},
		{PublicKey: solana.SystemProgramID, IsSigner: false, IsWritable: false},
		{PublicKey: creatorVault, IsSigner: false, IsWritable: true},
		{PublicKey: tokenProgram, IsSigner: false, IsWritable: false},
		{PublicKey: eventAuth, IsSigner: false, IsWritable: false},
		{PublicKey: pumpProgramPK, IsSigner: false, IsWritable: false},
		{PublicKey: feeCfg, IsSigner: false, IsWritable: false},
		{PublicKey: pumpFeeProgramPK, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurveV2, IsSigner: false, IsWritable: false},
	}

	sellIx := solana.NewInstruction(pumpProgramPK, metas, data)

	cuLimitIx, err := computebudget.NewSetComputeUnitLimitInstruction(pumpComputeUnitLimit).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, 0, err
	}
	priorityLamports := choosePriorityFeeLamports(grossSol)
	microPerCU := effectiveMicroLamportsPerCUPump(priorityLamports)
	cuPriceIx, err := computebudget.NewSetComputeUnitPriceInstruction(microPerCU).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, 0, err
	}

	cachedBH, ok := getCachedBlockhash()
	if !ok {
		return solana.Signature{}, 0, fmt.Errorf("blockhash cache cold")
	}

	all := []solana.Instruction{cuLimitIx, cuPriceIx, sellIx}
	if shouldUseJitoPath() && jitoMinTipLamports > 0 {
		tipIx := system.NewTransferInstruction(jitoMinTipLamports, owner, solana.MustPublicKeyFromBase58(jitoTipAccount)).Build()
		all = append(all, tipIx)
	}
	tx, err := solana.NewTransaction(all, cachedBH, solana.TransactionPayer(owner))
	if err != nil {
		return solana.Signature{}, 0, err
	}
	err = signTransactionAsync(tx, owner, wallet)
	if err != nil {
		return solana.Signature{}, 0, err
	}
	sig, _, err := sendPumpTransaction(ctx, rpcClient, tx)
	if err != nil {
		return solana.Signature{}, 0, err
	}
	// Р”Р»СЏ PnL Р±РµСЂС‘Рј С„Р°РєС‚ РїСЂРёС…РѕРґР° SOL, РЅРµ С‚РµРѕСЂРµС‚РёС‡РµСЃРєРёР№ gross.
	actualOut, ok := waitNativeBalanceDelta(ctx, rpcClient, owner, balBefore, true)
	if ok && actualOut > 0 {
		return sig, actualOut, nil
	}
	return sig, grossSol, nil
}

func swapPumpFunSellAll(ctx context.Context, rpcClient *solanarpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, slipBps uint64) (solana.Signature, uint64, error) {
	owner := wallet.PublicKey()
	tokenProgram, err := mintTokenProgram(ctx, rpcClient, mint)
	if err != nil {
		return solana.Signature{}, 0, err
	}
	ata, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, 0, err
	}
	bal, err := rpcClient.GetTokenAccountBalance(ctx, ata, solanarpc.CommitmentProcessed)
	if err != nil || bal == nil || bal.Value == nil {
		return solana.Signature{}, 0, fmt.Errorf("token balance: %w", err)
	}
	raw, err := strconv.ParseUint(bal.Value.Amount, 10, 64)
	if err != nil || raw == 0 {
		return solana.Signature{}, 0, fmt.Errorf("zero token balance")
	}
	return swapPumpFunSellWithFallback(ctx, rpcClient, wallet, mint, raw, slipBps)
}

func isPumpOverflow6024(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "custom program error: 0x1788") ||
		strings.Contains(s, "Error Number: 6024") ||
		strings.Contains(s, "Overflow")
}

func isIncorrectProgramIDErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "incorrectprogramid") ||
		strings.Contains(s, "incorrect program id for instruction")
}

// swapPumpFunSellWithFallback вЂ” РїСЂРё 6024 РїСЂРѕР±СѓРµС‚ РјРµРЅСЊС€РёР№ РѕР±СЉС‘Рј, С‡С‚РѕР±С‹ РЅРµ Р·Р°СЃС‚СЂРµРІР°С‚СЊ РІ РїРѕР·РёС†РёРё.
func swapPumpFunSellWithFallback(
	ctx context.Context,
	rpcClient *solanarpc.Client,
	wallet solana.PrivateKey,
	mint solana.PublicKey,
	rawAmount uint64,
	slipBps uint64,
) (solana.Signature, uint64, error) {
	if rawAmount == 0 {
		return solana.Signature{}, 0, fmt.Errorf("zero token amount")
	}
	amounts := []uint64{
		rawAmount,
		rawAmount * 99 / 100,
		rawAmount * 97 / 100,
		rawAmount * 95 / 100,
		rawAmount * 90 / 100,
		rawAmount * 80 / 100,
	}

	var lastErr error
	for _, amt := range amounts {
		if amt == 0 {
			continue
		}
		sig, gross, err := swapPumpFunSellAmount(ctx, rpcClient, wallet, mint, amt, slipBps)
		if err == nil {
			return sig, gross, nil
		}
		lastErr = err
		if !isPumpOverflow6024(err) {
			return solana.Signature{}, 0, err
		}
	}
	// Аварийный повтор: 50% slippage + умеренный priority fee (без убийства банка комиссиями).
	prevPriority := pumpPriorityFeeLamports
	prevCap := pumpPriorityMaxFeeBps
	if pumpPriorityFeeLamports < pumpSellRetryPriorityFee {
		pumpPriorityFeeLamports = pumpSellRetryPriorityFee
	}
	pumpPriorityMaxFeeBps = 300
	defer func() {
		pumpPriorityFeeLamports = prevPriority
		pumpPriorityMaxFeeBps = prevCap
	}()

	for _, amt := range amounts {
		if amt == 0 {
			continue
		}
		sig, gross, err := swapPumpFunSellAmount(ctx, rpcClient, wallet, mint, amt, 5000)
		if err == nil {
			return sig, gross, nil
		}
		lastErr = err
		if !isPumpOverflow6024(err) {
			return solana.Signature{}, 0, err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("sell fallback failed: no attempts")
	}
	return solana.Signature{}, 0, fmt.Errorf("sell failed after fallback attempts: %w", lastErr)
}

// liveUsePumpDirect вЂ” РІ live С‚РѕР»СЊРєРѕ С‡РёСЃС‚С‹Р№ Pump.fun (вЂ¦pump); LaunchLab РЅРµ С‡РµСЂРµР· СЌС‚РѕС‚ РїСѓС‚СЊ.
func liveUsePumpDirect(tok NewToken) bool {
	if strings.TrimSpace(tok.Source) == "launchlab" {
		return false
	}
	if strings.TrimSpace(tok.Source) == "pump" {
		return true
	}
	return strings.HasSuffix(strings.TrimSpace(tok.Mint), "pump")
}

func liveUsePumpDirectClose(pos *Position) bool {
	if strings.TrimSpace(pos.Source) == "launchlab" {
		return false
	}
	if strings.TrimSpace(pos.Source) == "pump" {
		return true
	}
	return strings.HasSuffix(strings.TrimSpace(pos.Mint), "pump")
}

// PumpDirectBuy вЂ” РїРѕРєСѓРїРєР° РЅР° bonding curve; tokenRaw вЂ” РѕР¶РёРґР°РµРјС‹Рµ Р°С‚РѕРјС‹ РїРѕ РєРѕС‚РёСЂРѕРІРєРµ; solIn вЂ” lamports РІ РёРЅСЃС‚СЂСѓРєС†РёРё.
func PumpDirectBuy(mintStr string, spendLamports uint64) (tokenRaw uint64, sig string, solIn uint64, sentAt time.Time, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mint, err := solana.PublicKeyFromBase58(mintStr)
	if err != nil {
		return 0, "", 0, time.Time{}, err
	}
	s, expectedOut, spendBudget, sentAt, err := swapPumpFun(ctx, rpcPumpDirect(), livePrivKey, mint, spendLamports, false)
	if err != nil && isIncorrectProgramIDErr(err) && skipMintInfoInFastBuy() {
		// Fast-path РјРѕРі РїСЂРѕРјР°С…РЅСѓС‚СЊСЃСЏ СЃ token program (Tokenkeg vs Token-2022).
		// РџРѕРІС‚РѕСЂСЏРµРј СЃ РѕР±СЏР·Р°С‚РµР»СЊРЅС‹Рј С‡С‚РµРЅРёРµРј mint account.
		disableMintInfoFastPath("incorrect program id on ATA/mint path")
		fmt.Printf("вљ  buy fallback %s: retry with mint info (token program)\n", mintStr[:8]+"..")
		s, expectedOut, spendBudget, sentAt, err = swapPumpFun(ctx, rpcPumpDirect(), livePrivKey, mint, spendLamports, true)
	}
	if err != nil {
		return 0, "", 0, time.Time{}, err
	}
	return expectedOut, s.String(), spendBudget, sentAt, nil
}

// PumpDirectSellAll вЂ” РїСЂРѕРґР°Р¶Р° РІСЃРµРіРѕ Р±Р°Р»Р°РЅСЃР° С‚РѕРєРµРЅР°; solOutLamports вЂ” РіСЂСѓР±Р°СЏ РѕС†РµРЅРєР° РІС‹С…РѕРґР° SOL (РґРѕ slippage РІ min_out).
func PumpDirectSellAll(mintStr string) (sig string, solOutLamports uint64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mint, err := solana.PublicKeyFromBase58(mintStr)
	if err != nil {
		return "", 0, err
	}
	s, gross, err := swapPumpFunSellAll(ctx, rpcPumpDirect(), livePrivKey, mint, pumpSellSlippageBps)
	if err != nil {
		return "", 0, err
	}
	return s.String(), gross, nil
}

func PumpDirectSellFraction(mintStr string, fraction float64) (sig string, soldRaw uint64, solOutLamports uint64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mint, err := solana.PublicKeyFromBase58(mintStr)
	if err != nil {
		return "", 0, 0, err
	}
	owner := livePrivKey.PublicKey()
	tokenProgram, err := mintTokenProgram(ctx, rpcPumpDirect(), mint)
	if err != nil {
		return "", 0, 0, err
	}
	ata, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return "", 0, 0, err
	}
	bal, err := rpcPumpDirect().GetTokenAccountBalance(ctx, ata, solanarpc.CommitmentProcessed)
	if err != nil || bal == nil || bal.Value == nil {
		return "", 0, 0, fmt.Errorf("token balance: %w", err)
	}
	raw, err := strconv.ParseUint(bal.Value.Amount, 10, 64)
	if err != nil || raw == 0 {
		return "", 0, 0, fmt.Errorf("zero token balance")
	}
	if fraction <= 0 || fraction > 1 {
		return "", 0, 0, fmt.Errorf("fraction out of range")
	}
	soldRaw = uint64(float64(raw) * fraction)
	if soldRaw == 0 {
		soldRaw = 1
	}
	s, out, err := swapPumpFunSellWithFallback(ctx, rpcPumpDirect(), livePrivKey, mint, soldRaw, pumpSellSlippageBps)
	if err != nil {
		return "", 0, 0, err
	}
	return s.String(), soldRaw, out, nil
}

// PumpDirectTokenRawBalance вЂ” С‚РµРєСѓС‰РёР№ raw-Р±Р°Р»Р°РЅСЃ С‚РѕРєРµРЅР° РІ ATA live-РєРѕС€РµР»СЊРєР°.
func PumpDirectTokenRawBalance(mintStr string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	mint, err := solana.PublicKeyFromBase58(mintStr)
	if err != nil {
		return 0, err
	}
	owner := livePrivKey.PublicKey()
	tokenProgram, err := mintTokenProgram(ctx, rpcPumpDirect(), mint)
	if err != nil {
		return 0, err
	}
	ata, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return 0, err
	}
	bal, err := rpcPumpDirect().GetTokenAccountBalance(ctx, ata, solanarpc.CommitmentProcessed)
	if err != nil || bal == nil || bal.Value == nil {
		// РќРµС‚ ATA/Р±Р°Р»Р°РЅСЃР° вЂ” СЃС‡РёС‚Р°РµРј С‡С‚Рѕ РїРѕР·РёС†РёРё РЅРµС‚.
		return 0, nil
	}
	raw, err := strconv.ParseUint(bal.Value.Amount, 10, 64)
	if err != nil {
		return 0, err
	}
	return raw, nil
}

// PumpDirectEstimateSellSlippage вЂ” РѕС†РµРЅРєР° РїСЂРѕСЃРєР°Р»СЊР·С‹РІР°РЅРёСЏ РїСЂРѕРґР°Р¶Рё РѕС‚РЅРѕСЃРёС‚РµР»СЊРЅРѕ spot С†РµРЅС‹.
// Р’РѕР·РІСЂР°С‰Р°РµС‚ РґРѕР»СЋ (0.15 = 15% С…СѓР¶Рµ spot).
func PumpDirectEstimateSellSlippage(mintStr string, tokenRaw uint64, spotUSD float64) (float64, error) {
	if tokenRaw == 0 || spotUSD <= 0 {
		return 0, fmt.Errorf("bad inputs")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	mint, err := solana.PublicKeyFromBase58(mintStr)
	if err != nil {
		return 0, err
	}
	bondingCurve, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return 0, err
	}
	bcInfo, err := rpcPumpDirect().GetAccountInfoWithOpts(ctx, bondingCurve, &solanarpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: solanarpc.CommitmentProcessed,
	})
	if err != nil || bcInfo == nil || bcInfo.Value == nil {
		return 0, fmt.Errorf("bonding curve")
	}
	vTok, vSol, _, _, _, _, _, err := parsePumpBondingCurveData(bcInfo.Value.Data.GetBinary())
	if err != nil {
		return 0, err
	}
	grossSol := pumpGrossSolForSell(tokenRaw, vSol, vTok)
	if grossSol == 0 {
		return 0, fmt.Errorf("gross=0")
	}
	expectedUSD := (float64(tokenRaw) / 1e6) * spotUSD
	if expectedUSD <= 0 {
		return 0, fmt.Errorf("expected=0")
	}
	grossUSD := (float64(grossSol) / 1e9) * getSolUSD()
	slip := 1.0 - (grossUSD / expectedUSD)
	if slip < 0 {
		slip = 0
	}
	return slip, nil
}
