// Прямые сделки Pump.fun (buy_exact_sol_in / sell) без Jupiter — как в apexsnip/pump_live.go.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
	solanarpc "github.com/gagliardetto/solana-go/rpc"
)

// Программа комиссий Pump (IDL).
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
	pumpComputeUnitLimit    uint32 = 600_000
)

var (
	pumpBuySlippageBps       uint64 = 3000 // buy 30%
	pumpSellSlippageBps      uint64 = 4000 // sell 40%
	pumpPriorityFeeLamports  uint64 = 900_000 // 0.0009 SOL (минимально-жизнеспособный дефолт)
	pumpPriorityMaxFeeBps    uint64 = 100 // максимум приоритета как доля от размера сделки (1.0%)
	pumpSellRetryPriorityFee uint64 = 8_000_000
	pumpDirectRPC     *solanarpc.Client
	pumpDirectRPCOnce sync.Once
	priorityFeeCache struct {
		mu       sync.Mutex
		lamports uint64
		ts       time.Time
	}
	lastBuyLatency struct {
		mu sync.Mutex
		v  buyLatencyBreakdown
	}
	recentBlockhashCache struct {
		mu       sync.Mutex
		blockhash solana.Hash
		ts       time.Time
	}
	jitoHTTPClient = &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
	}
)

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
	if s := strings.TrimSpace(os.Getenv("PUMP_PRIORITY_MAX_FEE_BPS")); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v <= 2_000 {
			pumpPriorityMaxFeeBps = v
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

// fastHotBuyMode: в горячем входе не делаем лишние pre-check RPC перед отправкой.
func fastHotBuyMode() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("HOT_PATH_FAST_BUY")))
	if s == "0" || s == "false" || s == "no" {
		return false
	}
	return true
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
	if strings.TrimSpace(os.Getenv("JITO_BLOCK_ENGINE_URL")) != "" && fee < 1_000_000 {
		fee = 1_000_000 // 0.001 SOL минимум для быстрого попадания в bundle
	}
	if baseTradeLamports > 0 && pumpPriorityMaxFeeBps > 0 {
		capFee := (baseTradeLamports * pumpPriorityMaxFeeBps) / 10_000
		if capFee > 0 && fee > capFee {
			fee = capFee
		}
	}
	if strings.TrimSpace(os.Getenv("JITO_BLOCK_ENGINE_URL")) != "" && fee < 1_000_000 {
		fee = 1_000_000
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

// refreshDynamicPriorityFeeFromRPC обновляет кэш фи в фоне.
// В buy-пути эта функция не вызывается, чтобы не добавлять latency от retry/backoff.
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
	// Берём верхний квартиль microLamports/CU как рабочий компромисс скорости/комиссии.
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

func sendPumpTransaction(ctx context.Context, rpcClient *solanarpc.Client, tx *solana.Transaction) (solana.Signature, time.Time, error) {
	if j := strings.TrimSpace(os.Getenv("JITO_BLOCK_ENGINE_URL")); j != "" {
		fireAndForget := strings.TrimSpace(strings.ToLower(os.Getenv("JITO_FIRE_AND_FORGET"))) != "0"
		if fireAndForget {
			txSig := solana.Signature{}
			if len(tx.Signatures) > 0 {
				txSig = tx.Signatures[0]
			}
			go func(url string, txCopy *solana.Transaction) {
				bgCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				defer cancel()
				_, _, _ = sendJitoBundle(bgCtx, url, txCopy)
			}(j, tx)
			return txSig, time.Now(), nil
		}
		jitoCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		sig, sentAt, err := sendJitoBundle(jitoCtx, j, tx)
		cancel()
		if err == nil {
			return sig, sentAt, nil
		}
	}
	sig, err := rpcClient.SendTransactionWithOpts(ctx, tx, solanarpc.TransactionOpts{
		SkipPreflight:       false,
		PreflightCommitment: solanarpc.CommitmentProcessed,
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
	b64Tx := base64.StdEncoding.EncodeToString(rawTx)
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendBundle",
		"params":  []interface{}{[]string{b64Tx}},
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

// waitNativeBalanceDelta — пытается поймать изменение SOL после отправки tx.
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

// pumpEnvForceSellMinSolOne — по умолчанию true: min_sol_out=1 при продаже, чтобы не ловить 6024 Overflow
// (клиентская CPMM ≠ GetFees on-chain). Отключить: PUMP_SELL_MIN_SOL_ONE=false
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
	// Idempotent ATA create: безопасно добавлять всегда, без RPC pre-check.
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

func swapPumpFun(ctx context.Context, rpcClient *solanarpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, spendableLamports uint64) (solana.Signature, uint64, uint64, time.Time, error) {
	owner := wallet.PublicKey()
	startAt := time.Now()
	fastMode := fastHotBuyMode()
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
		tokenProgram solana.PublicKey
		mintDecimals uint8
		feeRecipient solana.PublicKey
		feeBps       uint64
		creatorFeeBps uint64
		vTok         uint64
		vSol         uint64
		realToken    uint64
		complete     bool
		creator      solana.PublicKey
		firstErr     error
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
	wg.Add(3)
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
	cachedBH, ok := getRecentBlockhashCached()
	blockhashMs := time.Since(bhStart).Milliseconds()
	if !ok {
		return solana.Signature{}, 0, 0, time.Time{}, fmt.Errorf("blockhash cache cold")
	}

	all := append([]solana.Instruction{}, cuLimitIx, cuPriceIx)
	all = append(all, preIxs...)
	all = append(all, buyIx)

	tx, err := solana.NewTransaction(all, cachedBH, solana.TransactionPayer(owner))
	if err != nil {
		return solana.Signature{}, 0, 0, time.Time{}, err
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
		// Фактические лампорты, списанные с кошелька, важнее оценок (учёт комиссий/priority/реального исполнения).
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
		return solana.Signature{}, 0, fmt.Errorf("curve complete — используй DEX, не pump sell")
	}

	grossSol := pumpGrossSolForSell(tokenAmount, vSol, vTok)
	minSol := pumpQuoteMinSolForSell(tokenAmount, vSol, vTok, slipBps)
	if minSol == 0 {
		minSol = 1
	}
	if pumpEnvForceSellMinSolOne() {
		// Как PUMP_MIN_OUT_ONE на покупке: min_sol_out=1 — иначе часто 6024 Overflow (клиент ≠ GetFees).
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

	cachedBH, ok := getRecentBlockhashCached()
	if !ok {
		return solana.Signature{}, 0, fmt.Errorf("blockhash cache cold")
	}

	all := []solana.Instruction{cuLimitIx, cuPriceIx, sellIx}
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
	// Для PnL берём факт прихода SOL, не теоретический gross.
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

// swapPumpFunSellWithFallback — при 6024 пробует меньший объём, чтобы не застревать в позиции.
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
	// Аварийный повтор: 50% slippage + повышенный priority fee (как requested anti-6024 path).
	prevPriority := pumpPriorityFeeLamports
	prevCap := pumpPriorityMaxFeeBps
	if pumpPriorityFeeLamports < pumpSellRetryPriorityFee {
		pumpPriorityFeeLamports = pumpSellRetryPriorityFee
	}
	pumpPriorityMaxFeeBps = 0 // на аварийном проходе не режем fee cap
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

// liveUsePumpDirect — в live только чистый Pump.fun (…pump); LaunchLab не через этот путь.
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

// PumpDirectBuy — покупка на bonding curve; tokenRaw — ожидаемые атомы по котировке; solIn — lamports в инструкции.
func PumpDirectBuy(mintStr string, spendLamports uint64) (tokenRaw uint64, sig string, solIn uint64, sentAt time.Time, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mint, err := solana.PublicKeyFromBase58(mintStr)
	if err != nil {
		return 0, "", 0, time.Time{}, err
	}
	s, expectedOut, spendBudget, sentAt, err := swapPumpFun(ctx, rpcPumpDirect(), livePrivKey, mint, spendLamports)
	if err != nil {
		return 0, "", 0, time.Time{}, err
	}
	return expectedOut, s.String(), spendBudget, sentAt, nil
}

// PumpDirectSellAll — продажа всего баланса токена; solOutLamports — грубая оценка выхода SOL (до slippage в min_out).
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

// PumpDirectTokenRawBalance — текущий raw-баланс токена в ATA live-кошелька.
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
		// Нет ATA/баланса — считаем что позиции нет.
		return 0, nil
	}
	raw, err := strconv.ParseUint(bal.Value.Amount, 10, 64)
	if err != nil {
		return 0, err
	}
	return raw, nil
}

// PumpDirectEstimateSellSlippage — оценка проскальзывания продажи относительно spot цены.
// Возвращает долю (0.15 = 15% хуже spot).
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
