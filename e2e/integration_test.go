package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/eigeninference/d-inference/coordinator/api"
	"github.com/eigeninference/d-inference/coordinator/billing"
	"github.com/eigeninference/d-inference/coordinator/payments"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/store"
	"github.com/eigeninference/d-inference/e2e/testbed"
	tbassert "github.com/eigeninference/d-inference/e2e/testbed/assert"
	"github.com/eigeninference/d-inference/e2e/testbed/deps"
	"github.com/eigeninference/d-inference/e2e/testbed/profile"
)

var (
	envMu    sync.Mutex
	envOnce  sync.Once
	envReady bool
	envErr   error

	envCtx      context.Context
	envCancel   context.CancelFunc
	envLogger   *slog.Logger
	envPg       *deps.PostgresLifecycle
	envPgStore  store.Store
	envCoord    *testbed.CoordinatorLifecycle
	envProvider *testbed.ProviderLifecycle
	envModelID  string
)

func ensureEnvironment(t *testing.T) {
	t.Helper()

	envOnce.Do(func() {
		envCtx, envCancel = context.WithTimeout(context.Background(), 5*time.Minute)

		if os.Getenv("DARKBLOOM_REPO_ROOT") == "" {
			if cwd, err := os.Getwd(); err == nil {
				os.Setenv("DARKBLOOM_REPO_ROOT", cwd+"/../..")
			}
		}

		envLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

		envModelID = os.Getenv("TESTBED_MODEL_ID")
		if envModelID == "" {
			envModelID = "mlx-community/Qwen3.5-0.8B-MLX-4bit"
		}

		envErr = startEnvironment()
		if envErr != nil {
			envLogger.Error("failed to start test environment", "error", envErr)
			stopEnvironment()
		}
		envReady = true
	})

	if envErr != nil {
		t.Fatalf("environment startup failed: %v", envErr)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()

	envMu.Lock()
	if envReady {
		stopEnvironment()
	}
	envMu.Unlock()

	os.Exit(code)
}

func startEnvironment() error {
	envPg = deps.NewPostgresLifecycle(envLogger, 0)
	if err := envPg.Start(envCtx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	envLogger.Info("postgres started", "url", envPg.DatabaseURL)

	var err error
	envPgStore, err = testbed.NewPostgresStore(envCtx, envPg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres store: %w", err)
	}
	if err := envPgStore.Credit("admin", 100_000_000, store.LedgerDeposit, "test-seed"); err != nil {
		return fmt.Errorf("seed balance: %w", err)
	}

	providerBinary, err := testbed.BuildProvider(envCtx, envLogger)
	if err != nil {
		return fmt.Errorf("build provider: %w", err)
	}

	envCoord, err = testbed.NewCoordinatorLifecycle(envCtx, envPgStore, envLogger)
	if err != nil {
		return fmt.Errorf("coordinator create: %w", err)
	}
	envCoord.Registry.SetQueue(registry.NewRequestQueue(100, 120*time.Second))

	ledger := payments.NewLedger(envPgStore)
	billingSvc := billing.NewService(envPgStore, ledger, envLogger, billing.Config{MockMode: true})
	envCoord.Server.SetBilling(billingSvc)
	envCoord.Server.SetRuntimeManifest(&api.RuntimeManifest{})

	if err := envCoord.Start(envCtx); err != nil {
		return fmt.Errorf("coordinator start: %w", err)
	}
	envLogger.Info("coordinator started", "base_url", envCoord.BaseURL())

	envProvider = testbed.NewProviderLifecycle(providerBinary, envCoord.BaseURL(), envLogger)
	if err := envProvider.Start(envCtx, testbed.ProviderConfig{
		ModelID:    envModelID,
		TrustLevel: testbed.TrustNone,
	}); err != nil {
		return fmt.Errorf("provider start: %w", err)
	}
	envLogger.Info("provider started, waiting for registration...")

	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		if envCoord.Registry.ProviderCount() > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if envCoord.Registry.ProviderCount() == 0 {
		return fmt.Errorf("no providers registered after 3m")
	}
	envLogger.Info("provider registered", "count", envCoord.Registry.ProviderCount())

	for _, id := range envCoord.Registry.ProviderIDs() {
		envCoord.Registry.SetTrustLevel(id, registry.TrustSelfSigned)
		envCoord.Registry.RecordChallengeSuccess(id)
	}
	envLogger.Info("provider promoted to self-signed trust")

	return nil
}

func stopEnvironment() {
	if envProvider != nil {
		envProvider.Stop()
	}
	if envCoord != nil {
		envCoord.Stop()
	}
	if envPg != nil {
		envPg.Stop()
	}
	if envCancel != nil {
		envCancel()
	}
}

func postChatCompletions(t *testing.T, prompt string, stream bool, maxTokens int) *http.Response {
	t.Helper()

	body := map[string]any{
		"model":       envModelID,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"stream":      stream,
		"max_tokens":  maxTokens,
		"temperature": 0.0,
	}
	bodyJSON, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(envCtx, http.MethodPost,
		envCoord.BaseURL()+"/v1/chat/completions", strings.NewReader(string(bodyJSON)))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer testbed-admin-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	require.NoError(t, err)
	return resp
}

func assertAccounting(t *testing.T) {
	t.Helper()

	envMu.Lock()
	pgDB := envPg.DatabaseURL
	pgStore := envPgStore
	envMu.Unlock()

	pool, err := pgxpool.New(envCtx, pgDB)
	require.NoError(t, err)
	defer pool.Close()

	pgAsserter := tbassert.NewPostgresAccountingAsserter(pool)
	acctReport := pgAsserter.EvaluateAll(envCtx)
	t.Logf("\n%s", acctReport.SummaryTable())
	require.True(t, acctReport.Passed, "accounting integrity check failed")

	storeAsserter := tbassert.NewAccountingAsserter(pgStore)
	storeReport := storeAsserter.EvaluateAll(envCtx)
	t.Logf("\n%s", storeReport.SummaryTable())
	require.True(t, storeReport.Passed, "store-level accounting check failed")
}

func TestIntegration_NonStreamingInference(t *testing.T) {
	ensureEnvironment(t)
	t.Parallel()

	resp := postChatCompletions(t, "What is 2+2? Answer with just the number.", false, 20)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", string(respBody[:min(len(respBody), 500)]))
	t.Logf("non-streaming response: %s", string(respBody[:min(len(respBody), 200)]))

	assertAccounting(t)
}

func TestIntegration_StreamingInference(t *testing.T) {
	ensureEnvironment(t)
	t.Parallel()

	resp := postChatCompletions(t, "Count from 1 to 5.", true, 50)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var chunks int
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			chunks++
		}
	}
	require.Greater(t, chunks, 0, "expected at least one SSE chunk")
	t.Logf("streaming: received %d SSE chunks", chunks)

	assertAccounting(t)
}

func TestIntegration_MultipleRequestsAccounting(t *testing.T) {
	ensureEnvironment(t)
	t.Parallel()

	buf := testbed.NewEventBuffer()
	inst := testbed.NewInstrument(buf)

	const totalRequests = 3
	var successCount int
	for i := 0; i < totalRequests; i++ {
		ri := inst.NewRequest()
		clientTimer := ri.StartSegment(testbed.SegmentClientToCoordinator)

		resp := postChatCompletions(t, "What is 2+2?", false, 20)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		clientTimer.Stop()

		if resp.StatusCode != 200 {
			ri.Error(fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody[:min(len(respBody), 500)])))
			t.Logf("request %d: status=%d", i+1, resp.StatusCode)
			continue
		}

		ri.EndWithDuration(0)
		successCount++
		t.Logf("request %d: status=200", i+1)
	}

	require.Greater(t, successCount, 0, "no successful requests")

	cfg := testbed.DefaultTestConfig()
	p := profile.NewProfiler(cfg, buf)
	run := p.BuildProfile()
	t.Logf("\n%s", run.SummaryTable())

	assertAccounting(t)
}
