package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/footprintai/containarium/internal/auth"
	pb "github.com/footprintai/containarium/pkg/pb/containarium/v1"
)

// assertHealthInvariant fails unless healthy is true exactly when the reason
// is UNSPECIFIED (#3722): a client must never see a reason on a healthy
// backend, or an unhealthy backend without one, from a daemon that sets it.
func assertHealthInvariant(t *testing.T, id string, healthy bool, reason pb.BackendUnhealthyReason) {
	t.Helper()
	if healthy != (reason == pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_UNSPECIFIED) {
		t.Errorf("%s: healthy = %v with unhealthy_reason %s; want healthy exactly when the reason is UNSPECIFIED", id, healthy, reason)
	}
}

func TestBackendUnhealthyReason(t *testing.T) {
	seen := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		healthy bool
		reason  pb.BackendUnhealthyReason
		want    pb.BackendUnhealthyReason
	}{
		{"local healthy", true, localUnhealthyReason(true), pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_UNSPECIFIED},
		{"local probe failing", false, localUnhealthyReason(false), pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_DRIVER_ERROR},
		{"peer healthy", true, peerUnhealthyReason(true, seen), pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_UNSPECIFIED},
		// Not reachable from discovery (a healthy peer always has a
		// last-seen time), but healthy must still win.
		{"peer healthy, no last-seen time", true, peerUnhealthyReason(true, time.Time{}), pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_UNSPECIFIED},
		{"peer was healthy, now is not", false, peerUnhealthyReason(false, seen), pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_NO_RECENT_HEARTBEAT},
		{"peer never healthy", false, peerUnhealthyReason(false, time.Time{}), pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_NEVER_SEEN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.reason != tc.want {
				t.Errorf("unhealthy_reason = %s, want %s", tc.reason, tc.want)
			}
			assertHealthInvariant(t, tc.name, tc.healthy, tc.reason)
		})
	}
}

// TestContainerServer_ListBackends_UnhealthyReason drives every reason through
// ListBackends itself, so the reason is proven to come from the same inputs as
// healthy rather than only from the helpers.
func TestContainerServer_ListBackends_UnhealthyReason(t *testing.T) {
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/system/info" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"info":{"hostname":"peer-host"}}`))
	}))
	defer peerSrv.Close()

	seen := time.Now().Add(-10 * time.Minute)
	pool := NewPeerPool("local-spot", "", nil, "")
	pool.mu.Lock()
	pool.peers["tunnel-up"] = &PeerClient{
		ID:         "tunnel-up",
		Addr:       peerSrv.Listener.Addr().String(),
		Healthy:    true,
		LastSeenAt: seen,
		client:     peerSrv.Client(),
	}
	pool.peers["tunnel-stale"] = &PeerClient{
		ID:         "tunnel-stale",
		Healthy:    false,
		LastSeenAt: seen,
		client:     &http.Client{},
	}
	pool.peers["tunnel-never"] = &PeerClient{
		ID:      "tunnel-never",
		Healthy: false,
		client:  &http.Client{},
	}
	pool.mu.Unlock()

	ctx := auth.ContextWithTestSubject(context.Background(), "ops", auth.RoleAdmin)
	for _, localHealthy := range []bool{true, false} {
		s := &ContainerServer{
			peerPool:           pool,
			startTime:          time.Now().Add(-time.Minute),
			localHealthCheckFn: func() bool { return localHealthy },
		}
		resp, err := s.ListBackends(ctx, &pb.ListBackendsRequest{})
		if err != nil {
			t.Fatalf("ListBackends: %v", err)
		}

		wantLocal := pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_UNSPECIFIED
		if !localHealthy {
			wantLocal = pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_DRIVER_ERROR
		}
		want := map[string]struct {
			healthy bool
			reason  pb.BackendUnhealthyReason
		}{
			"local-spot":   {localHealthy, wantLocal},
			"tunnel-up":    {true, pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_UNSPECIFIED},
			"tunnel-stale": {false, pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_NO_RECENT_HEARTBEAT},
			"tunnel-never": {false, pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_NEVER_SEEN},
		}
		if len(resp.Backends) != len(want) {
			t.Fatalf("local healthy=%v: got %d backends, want %d", localHealthy, len(resp.Backends), len(want))
		}
		for _, b := range resp.Backends {
			w, ok := want[b.Id]
			if !ok {
				t.Fatalf("unexpected backend %q", b.Id)
			}
			if b.Healthy != w.healthy || b.UnhealthyReason != w.reason {
				t.Errorf("local healthy=%v: %s: healthy=%v reason=%s, want healthy=%v reason=%s",
					localHealthy, b.Id, b.Healthy, b.UnhealthyReason, w.healthy, w.reason)
			}
			assertHealthInvariant(t, b.Id, b.Healthy, b.UnhealthyReason)
		}
	}
}

// TestContainerServer_ListBackends_PeerHealthReadUnderPoolLock lists backends
// while a writer keeps changing a peer's health the way discover() writes it:
// under the pool lock, Healthy first and then LastSeenAt when healthy. Every
// listing must describe one discovery result: a healthy peer has a last-seen
// time, and an unhealthy one is NEVER_SEEN exactly when it has none. Under
// -race (the server-race lane) it also fails if ListBackends reads the pair
// without the lock.
func TestContainerServer_ListBackends_PeerHealthReadUnderPoolLock(t *testing.T) {
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"info":{}}`))
	}))
	defer peerSrv.Close()

	pool := NewPeerPool("local-spot", "", nil, "")
	flap := &PeerClient{
		ID:     "tunnel-flap",
		Addr:   peerSrv.Listener.Addr().String(),
		client: peerSrv.Client(),
	}
	pool.mu.Lock()
	pool.peers[flap.ID] = flap
	pool.mu.Unlock()

	s := &ContainerServer{
		peerPool:           pool,
		localHealthCheckFn: func() bool { return true },
	}
	ctx := auth.ContextWithTestSubject(context.Background(), "ops", auth.RoleAdmin)

	// The start barrier makes the writer and the listings overlap; stop keeps
	// the writer flipping until the last listing is done.
	start, stop := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for {
			select {
			case <-stop:
				return
			default:
			}
			pool.mu.Lock()
			flap.Healthy = !flap.Healthy
			if flap.Healthy {
				flap.LastSeenAt = time.Now()
			}
			pool.mu.Unlock()
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	close(start)
	for i := 0; i < 200; i++ {
		resp, err := s.ListBackends(ctx, &pb.ListBackendsRequest{})
		if err != nil {
			t.Fatalf("ListBackends: %v", err)
		}
		found := false
		for _, b := range resp.Backends {
			if b.Id != flap.ID {
				continue
			}
			found = true
			if b.Healthy && b.LastSeenAt == "" {
				t.Fatalf("listing %d: healthy peer has no last_seen_at", i)
			}
			neverSeen := b.UnhealthyReason == pb.BackendUnhealthyReason_BACKEND_UNHEALTHY_REASON_NEVER_SEEN
			if !b.Healthy && neverSeen != (b.LastSeenAt == "") {
				t.Fatalf("listing %d: unhealthy_reason %s with last_seen_at %q; want NEVER_SEEN exactly when last_seen_at is empty",
					i, b.UnhealthyReason, b.LastSeenAt)
			}
			assertHealthInvariant(t, b.Id, b.Healthy, b.UnhealthyReason)
		}
		if !found {
			t.Fatalf("listing %d: %s is missing", i, flap.ID)
		}
	}
}
