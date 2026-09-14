//go:build integration

package krabka

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	integrationOptInEnv           = "KRABKA_GO_INTEGRATION"
	integrationGatewayEndpointEnv = "KRABKA_GATEWAY_ENDPOINT"

	// The compose images have no shell, so the harness has no container
	// health check. The smoke polls the gateway from the host instead. The
	// deadline covers the broker format step, the broker startup and the
	// gateway restarts until the broker accepts connections.
	gatewayReadyDeadline = 2 * time.Minute
	gatewayPollInterval  = 500 * time.Millisecond
	gatewayProbeTimeout  = 5 * time.Second
)

func TestComposeGatewaySmoke(t *testing.T) {
	if os.Getenv(integrationOptInEnv) != "1" {
		t.Skipf("set %s=1 and %s to run the live compose gateway smoke", integrationOptInEnv, integrationGatewayEndpointEnv)
	}

	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(integrationGatewayEndpointEnv)), "/")
	if endpoint == "" {
		t.Fatalf("%s=1 requires %s, for example http://127.0.0.1:9500", integrationOptInEnv, integrationGatewayEndpointEnv)
	}

	client := New(endpoint, nil)
	if client.gateway == nil {
		t.Fatalf("%s must name a live gateway endpoint, got %q", integrationGatewayEndpointEnv, endpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), gatewayReadyDeadline)
	defer cancel()
	if err := waitForGatewayHealth(ctx, endpoint); err != nil {
		t.Fatalf("gateway h2c health smoke through SDK transport failed: %v", err)
	}
}

// waitForGatewayHealth retries the health check until it passes or ctx ends.
// On timeout it returns the error of the last attempt.
func waitForGatewayHealth(ctx context.Context, endpoint string) error {
	ticker := time.NewTicker(gatewayPollInterval)
	defer ticker.Stop()
	for attempt := 1; ; attempt++ {
		probeCtx, cancelProbe := context.WithTimeout(ctx, gatewayProbeTimeout)
		err := checkGatewayHealthOverSDKTransport(probeCtx, endpoint)
		cancelProbe()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("gateway not healthy after %d attempts: %w", attempt, err)
		case <-ticker.C:
		}
	}
}

func checkGatewayHealthOverSDKTransport(ctx context.Context, endpoint string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/healthz", nil)
	if err != nil {
		return fmt.Errorf("build health request: %w", err)
	}

	response, err := defaultHTTPClientForEndpoint(endpoint).Do(request)
	if err != nil {
		return fmt.Errorf("GET %s: %w", request.URL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %s, want 200 OK", request.URL, response.Status)
	}
	return nil
}
