package testbed

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
)

type ProviderLifecycle struct {
	BinaryPath     string
	ConfigDir      string
	CoordinatorURL string
	Logger         *slog.Logger

	cmd    *exec.Cmd
	cancel context.CancelFunc
}

func NewProviderLifecycle(binaryPath, coordinatorURL string, logger *slog.Logger) *ProviderLifecycle {
	return &ProviderLifecycle{
		BinaryPath:     binaryPath,
		CoordinatorURL: coordinatorURL,
		Logger:         logger,
	}
}

func (p *ProviderLifecycle) Start(ctx context.Context, cfg ProviderConfig) error {
	if p.BinaryPath == "" {
		p.BinaryPath = findProviderBinary()
	}
	if p.BinaryPath == "" {
		return fmt.Errorf("testbed: provider binary not found (set DARKBLOOM_PROVIDER_BINARY or ensure 'darkbloom' is in PATH)")
	}

	ctx, p.cancel = context.WithCancel(ctx)

	wsURL := p.CoordinatorURL
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	if !strings.HasSuffix(wsURL, "/ws/provider") {
		wsURL += "/ws/provider"
	}

	args := []string{"serve", "--coordinator", wsURL}
	if cfg.ModelID != "" {
		args = append(args, "--model", cfg.ModelID)
	}

	p.cmd = exec.CommandContext(ctx, p.BinaryPath, args...)
	p.cmd.Stdout = &logWriter{logger: p.Logger, prefix: "provider:stdout"}
	p.cmd.Stderr = &logWriter{logger: p.Logger, prefix: "provider:stderr"}

	if p.ConfigDir != "" {
		p.cmd.Env = append(os.Environ(),
			"EIGENINFERENCE_CONFIG_DIR="+p.ConfigDir,
		)
	}

	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("testbed: start provider: %w", err)
	}

	p.Logger.Info("provider started", "binary", p.BinaryPath, "pid", p.cmd.Process.Pid)
	return nil
}

func (p *ProviderLifecycle) Wait() error {
	if p.cmd == nil {
		return fmt.Errorf("testbed: provider not started")
	}
	return p.cmd.Wait()
}

func (p *ProviderLifecycle) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.cmd != nil && p.cmd.Process != nil {
		if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
			p.cmd.Process.Kill()
		}
		done := make(chan error, 1)
		go func() {
			done <- p.cmd.Wait()
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			p.cmd.Process.Kill()
		}
	}
	p.Logger.Info("provider stopped")
	return nil
}

func findProviderBinary() string {
	if path := os.Getenv("DARKBLOOM_PROVIDER_BINARY"); path != "" {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	if path, err := exec.LookPath("darkbloom"); err == nil {
		return path
	}
	return ""
}

func BuildProvider(ctx context.Context, logger *slog.Logger) (string, error) {
	repoRoot := os.Getenv("DARKBLOOM_REPO_ROOT")
	if repoRoot == "" {
		repoRoot = "."
	}
	providerDir := repoRoot + "/provider"

	binaryPath := providerDir + "/target/release/darkbloom"
	if _, err := os.Stat(binaryPath); err == nil {
		logger.Info("using cached provider binary", "path", binaryPath)
		return binaryPath, nil
	}

	logger.Info("building provider binary (cargo build --release)", "dir", providerDir)

	cmd := exec.CommandContext(ctx, "cargo", "build", "--release")
	cmd.Dir = providerDir
	cmd.Env = append(os.Environ(),
		"PYO3_PYTHON=python3.12",
		"PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("testbed: cargo build provider: %w: %s", err, string(out))
	}

	if _, err := os.Stat(binaryPath); err != nil {
		return "", fmt.Errorf("testbed: provider binary not found after build: %s", binaryPath)
	}

	logger.Info("provider binary built", "path", binaryPath)
	return binaryPath, nil
}

type logWriter struct {
	logger *slog.Logger
	prefix string
}

func (w *logWriter) Write(p []byte) (int, error) {
	n := len(p)
	for _, line := range strings.Split(string(p), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			w.logger.Info(w.prefix, "line", line)
		}
	}
	return n, nil
}
