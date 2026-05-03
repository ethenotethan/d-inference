package testbed

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
)

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
		return "", fmt.Errorf("cargo build provider: %w: %s", err, string(out))
	}

	if _, err := os.Stat(binaryPath); err != nil {
		return "", fmt.Errorf("provider binary not found after build: %s", binaryPath)
	}

	logger.Info("provider binary built", "path", binaryPath)
	return binaryPath, nil
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
