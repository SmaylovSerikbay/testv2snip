// Боевой режим: ключ из env, без сторонних swap API — сделки только через pump_direct.go.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gagliardetto/solana-go"
)

const (
	// Резерв SOL на комиссии (не включаем в сумму свапа)
	liveReserveSOL = 0.012
)

var (
	livePrivKey solana.PrivateKey
	livePub     solana.PublicKey
)

func liveTradingEnabled() bool {
	return strings.TrimSpace(os.Getenv("LIVE_TRADING")) == "1"
}

func liveReserveSOLValue() float64 {
	if s := strings.TrimSpace(os.Getenv("LIVE_RESERVE_SOL")); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v >= 0.0005 && v <= 0.2 {
			return v
		}
	}
	if ultraFastEntryMode() {
		return 0.002
	}
	return liveReserveSOL
}

func initLiveTrading() error {
	if !liveTradingEnabled() {
		return nil
	}
	s := strings.TrimSpace(os.Getenv("SOLANA_PRIVATE_KEY"))
	if s == "" {
		return fmt.Errorf("LIVE_TRADING=1: задай SOLANA_PRIVATE_KEY (base58 от Phantom Export Private Key)")
	}
	pk, err := solana.PrivateKeyFromBase58(s)
	if err != nil {
		return fmt.Errorf("SOLANA_PRIVATE_KEY: %w", err)
	}
	livePrivKey = pk
	livePub = pk.PublicKey()
	initPumpDirectFromEnv()
	return nil
}

func syncWalletBalanceUSD(w *Wallet) {
	if !liveTradingEnabled() {
		return
	}
	sol, err := rpcGetBalanceSOLCached(livePub.String(), BALANCE_CHECK_INTERVAL)
	if err != nil {
		return
	}
	usd := sol * getSolUSD()
	w.mu.Lock()
	w.Balance = usd
	w.mu.Unlock()
}

func syncWalletBalanceUSDFresh(w *Wallet) {
	if !liveTradingEnabled() {
		return
	}
	sol, err := rpcRefreshBalanceSOLCached(livePub.String())
	if err != nil {
		return
	}
	usd := sol * getSolUSD()
	w.mu.Lock()
	w.Balance = usd
	w.mu.Unlock()
}
