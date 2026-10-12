// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/cmn/logger"
	"github.com/dagucloud/dagu/v2/internal/serviceregistry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type stubServiceRegistry struct{}

func (stubServiceRegistry) Register(context.Context, serviceregistry.ServiceName, serviceregistry.HostInfo) error {
	return nil
}
func (stubServiceRegistry) Unregister(context.Context) {}
func (stubServiceRegistry) GetServiceMembers(context.Context, serviceregistry.ServiceName) ([]serviceregistry.HostInfo, error) {
	return nil, nil
}
func (stubServiceRegistry) UpdateStatus(context.Context, serviceregistry.ServiceName, serviceregistry.ServiceStatus) error {
	return nil
}

type callbackLogger struct {
	info func()
}

func (l *callbackLogger) Debug(string, ...slog.Attr) {}

func (l *callbackLogger) Info(string, ...slog.Attr) {
	if l.info != nil {
		l.info()
	}
}

func (l *callbackLogger) Warn(string, ...slog.Attr)  {}
func (l *callbackLogger) Error(string, ...slog.Attr) {}
func (l *callbackLogger) Fatal(string, ...slog.Attr) {}

func (l *callbackLogger) Debugf(string, ...any) {}
func (l *callbackLogger) Infof(string, ...any)  {}
func (l *callbackLogger) Warnf(string, ...any)  {}
func (l *callbackLogger) Errorf(string, ...any) {}
func (l *callbackLogger) Fatalf(string, ...any) {}

func (l *callbackLogger) With(...slog.Attr) logger.Logger {
	return l
}

func (l *callbackLogger) WithGroup(string) logger.Logger {
	return l
}

func (l *callbackLogger) Write(string) {}

func TestClientRecordSuccess_LogsRecoveryWithoutHoldingStateLock(t *testing.T) {
	t.Parallel()

	cli := New(stubServiceRegistry{}, DefaultConfig()).(*clientImpl)
	cli.state.IsConnected = false
	cli.state.ConsecutiveFails = 3

	ctx := logger.WithFixedLogger(context.Background(), &callbackLogger{
		info: func() {
			_ = cli.Metrics()
		},
	})

	done := make(chan struct{})
	go func() {
		cli.recordSuccess(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("recordSuccess deadlocked while logging recovery")
	}

	metrics := cli.Metrics()
	require.True(t, metrics.IsConnected)
	require.Zero(t, metrics.ConsecutiveFails)
	require.Nil(t, metrics.LastError)
}

func TestClientCacheUsesDerivedKeyForEmptyCoordinatorIDs(t *testing.T) {
	t.Parallel()

	cli := &clientImpl{
		config:  DefaultConfig(),
		clients: make(map[string]*client),
	}

	member1 := serviceregistry.HostInfo{Host: "127.0.0.1", Port: 1234}
	member2 := serviceregistry.HostInfo{Host: "127.0.0.1", Port: 5678}

	client1, err := cli.getOrCreateClient(member1)
	require.NoError(t, err)

	client2, err := cli.getOrCreateClient(member2)
	require.NoError(t, err)

	require.NotSame(t, client1, client2)
	require.Len(t, cli.clients, 2)
	require.Contains(t, cli.clients, coordinatorMemberKey(member1))
	require.Contains(t, cli.clients, coordinatorMemberKey(member2))
	require.NoError(t, cli.Cleanup(t.Context()))
}

func TestClientCacheUsesAdvertisedEndpointAcrossCoordinatorProcesses(t *testing.T) {
	t.Parallel()

	cli := &clientImpl{
		config:  DefaultConfig(),
		clients: make(map[string]*client),
	}
	endpoint := serviceregistry.HostInfo{ID: "coord-a", Host: "coordinator", Port: 50055}
	replacement := endpoint
	replacement.ID = "coord-b"

	first, err := cli.getOrCreateClient(endpoint)
	require.NoError(t, err)
	second, err := cli.getOrCreateClient(replacement)
	require.NoError(t, err)

	require.Same(t, first, second)
	require.Len(t, cli.clients, 1)
	require.NoError(t, cli.Cleanup(t.Context()))
}

func TestClientRetiresOnlyFailedConnection(t *testing.T) {
	t.Parallel()
	cli := New(stubServiceRegistry{}, DefaultConfig()).(*clientImpl)
	t.Cleanup(func() { require.NoError(t, cli.Cleanup(context.Background())) })
	owner := serviceregistry.HostInfo{Host: "127.0.0.1", Port: 1234}
	other := serviceregistry.HostInfo{Host: "127.0.0.1", Port: 5678}
	failed, err := cli.getOrCreateClient(owner)
	require.NoError(t, err)
	healthy, err := cli.getOrCreateClient(other)
	require.NoError(t, err)
	cli.retireFailedClient(owner, failed, status.Error(codes.Internal, "application failure"))
	same, err := cli.getOrCreateClient(owner)
	require.NoError(t, err)
	require.Same(t, failed, same)
	cli.retireFailedClient(owner, failed, status.Error(codes.Unavailable, "dead connection"))
	fresh, err := cli.getOrCreateClient(owner)
	require.NoError(t, err)
	require.NotSame(t, failed, fresh)
	cli.retireFailedClient(owner, failed, status.Error(codes.DeadlineExceeded, "late failure"))
	same, err = cli.getOrCreateClient(owner)
	require.NoError(t, err)
	require.Same(t, fresh, same)
	same, err = cli.getOrCreateClient(other)
	require.NoError(t, err)
	require.Same(t, healthy, same)
	require.Eventually(t, func() bool { return failed.conn.GetState() == connectivity.Shutdown }, time.Second, time.Millisecond)
	require.NotEqual(t, connectivity.Shutdown, healthy.conn.GetState())
}

func TestClientOrdinaryRPCFailureKeepsConnection(t *testing.T) {
	t.Parallel()
	cli := New(stubServiceRegistry{}, DefaultConfig()).(*clientImpl)
	t.Cleanup(func() { require.NoError(t, cli.Cleanup(context.Background())) })
	member := serviceregistry.HostInfo{Host: "127.0.0.1", Port: 1234}
	original, err := cli.getOrCreateClient(member)
	require.NoError(t, err)
	for _, code := range []codes.Code{codes.Internal, codes.DeadlineExceeded, codes.Unavailable} {
		err := cli.callOwner(t.Context(), member, false, func(context.Context, serviceregistry.HostInfo, *client) error {
			return status.Error(code, "ordinary RPC failed")
		})
		require.Equal(t, code, status.Code(err))
		cached, err := cli.getOrCreateClient(member)
		require.NoError(t, err)
		require.Same(t, original, cached)
		require.NotEqual(t, connectivity.Shutdown, original.conn.GetState())
	}
}

func TestClientRetiredConnectionDrains(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup=%t", cleanup), func(t *testing.T) {
			t.Parallel()
			cli := New(stubServiceRegistry{}, DefaultConfig()).(*clientImpl)
			t.Cleanup(func() { require.NoError(t, cli.Cleanup(context.Background())) })
			member := serviceregistry.HostInfo{Host: "127.0.0.1", Port: 1234}
			c, err := cli.getOrCreateClient(member)
			require.NoError(t, err)
			c.HandleRPC(t.Context(), &stats.Begin{})
			cli.retireFailedClient(member, c, status.Error(codes.DeadlineExceeded, "health timeout"))
			require.NotEqual(t, connectivity.Shutdown, c.conn.GetState())
			if cleanup {
				require.NoError(t, cli.Cleanup(t.Context()))
			} else {
				c.HandleRPC(t.Context(), &stats.End{})
			}
			require.Eventually(t, func() bool { return c.conn.GetState() == connectivity.Shutdown }, time.Second, time.Millisecond)
			cli.clientsMu.RLock()
			require.Empty(t, cli.retiredClients)
			cli.clientsMu.RUnlock()
		})
	}
}
