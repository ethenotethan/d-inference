package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/eigeninference/d-inference/e2e/testbed"
	tbassert "github.com/eigeninference/d-inference/e2e/testbed/assert"
	"github.com/eigeninference/d-inference/e2e/testbed/profile"
)

var (
	suiteMu   sync.Mutex
	suiteOnce sync.Once
	suite     *testbed.Suite
	suiteErr  error
	suiteCtx  context.Context
	suiteDone context.CancelFunc
)

func ensureSuite(t *testing.T) {
	t.Helper()

	suiteOnce.Do(func() {
		suiteCtx, suiteDone = context.WithTimeout(context.Background(), 5*time.Minute)
		suite = testbed.NewSuite(testbed.SuiteConfig{})
		suiteErr = suite.Start(suiteCtx)
		if suiteErr != nil {
			suite.Logger.Error("failed to start suite", "error", suiteErr)
			suite.Stop()
		}
	})

	if suiteErr != nil {
		t.Fatalf("suite startup failed: %v", suiteErr)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()

	suiteMu.Lock()
	if suite != nil && suiteErr == nil {
		suite.Stop()
	}
	if suiteDone != nil {
		suiteDone()
	}
	suiteMu.Unlock()

	os.Exit(code)
}

func postChatCompletions(t *testing.T, s *testbed.Suite, prompt string, stream bool, maxTokens int) *http.Response {
	t.Helper()

	body := map[string]any{
		"model":       s.ModelID,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"stream":      stream,
		"max_tokens":  maxTokens,
		"temperature": 0.0,
	}
	bodyJSON, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(s.Ctx, http.MethodPost,
		s.Coordinator.BaseURL()+"/v1/chat/completions", strings.NewReader(string(bodyJSON)))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer testbed-admin-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	require.NoError(t, err)
	return resp
}

func assertAccounting(t *testing.T, s *testbed.Suite) {
	t.Helper()

	pool, err := pgxpool.New(s.Ctx, s.Pg.DatabaseURL)
	require.NoError(t, err)
	defer pool.Close()

	pgAsserter := tbassert.NewPostgresAccountingAsserter(pool)
	acctReport := pgAsserter.EvaluateAll(s.Ctx)
	require.True(t, acctReport.Passed, "accounting integrity check failed\n%s", acctReport.SummaryTable())

	storeAsserter := tbassert.NewAccountingAsserter(s.PgStore)
	storeReport := storeAsserter.EvaluateAll(s.Ctx)
	require.True(t, storeReport.Passed, "store-level accounting check failed\n%s", storeReport.SummaryTable())
}

func TestIntegration_NonStreamingInference(t *testing.T) {
	ensureSuite(t)
	t.Parallel()

	resp := postChatCompletions(t, suite, "What is 2+2? Answer with just the number.", false, 20)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", string(respBody[:min(len(respBody), 500)]))
	t.Logf("non-streaming response: %s", string(respBody[:min(len(respBody), 200)]))

	assertAccounting(t, suite)
}

func TestIntegration_StreamingInference(t *testing.T) {
	ensureSuite(t)
	t.Parallel()

	resp := postChatCompletions(t, suite, "Count from 1 to 5.", true, 50)
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

	assertAccounting(t, suite)
}

func TestIntegration_MultipleRequestsAccounting(t *testing.T) {
	ensureSuite(t)
	t.Parallel()

	buf := testbed.NewEventBuffer()
	inst := testbed.NewInstrument(buf)

	const totalRequests = 3
	var successCount int
	for i := 0; i < totalRequests; i++ {
		ri := inst.NewRequest()
		clientTimer := ri.StartSegment(testbed.SegmentClientToCoordinator)

		resp := postChatCompletions(t, suite, "What is 2+2?", false, 20)
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

	assertAccounting(t, suite)
}
