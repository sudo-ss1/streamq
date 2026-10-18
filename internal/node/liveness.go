package node

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// registerBackoff is the wait between registration attempts while no controller is reachable.
const registerBackoff = 200 * time.Millisecond

// controllerClient returns an internal-API client for the current controller.
func (n *Node) controllerClient() (streamqv1.StreamQInternalServiceClient, error) {
	leader := n.raft.LeaderID()
	if leader < 0 {
		return nil, fmt.Errorf("no controller elected")
	}
	for _, m := range n.cfg.Cluster {
		if m.ID == leader {
			return n.forwarder.Client(m.Internal)
		}
	}
	return nil, fmt.Errorf("controller %d not in cluster config", leader)
}

// selfInternalAddr returns this broker's internal address from the cluster config.
func (n *Node) selfInternalAddr() string {
	for _, m := range n.cfg.Cluster {
		if m.ID == n.cfg.BrokerID {
			return m.Internal
		}
	}
	return n.cfg.InternalAddr
}

// register registers with the controller, retrying until it succeeds or ctx ends.
func (n *Node) register(ctx context.Context, port int32) (*streamqv1.RegisterBrokerResponse, error) {
	req := &streamqv1.RegisterBrokerRequest{BrokerId: n.cfg.BrokerID, Host: n.cfg.AdvertisedHost, Port: port, InternalAddr: n.selfInternalAddr()}
	for {
		resp, err := n.tryRegister(ctx, req)
		if err == nil {
			return resp, nil
		}
		n.logger.Debug("registration attempt failed", slog.Any("error", err))
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("register with controller: %w (last error: %w)", ctx.Err(), err)
		case <-time.After(registerBackoff):
		}
	}
}

func (n *Node) tryRegister(ctx context.Context, req *streamqv1.RegisterBrokerRequest) (*streamqv1.RegisterBrokerResponse, error) {
	c, err := n.controllerClient()
	if err != nil {
		return nil, err
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.RegisterBroker(rctx, req)
	if err != nil {
		return nil, fmt.Errorf("register RPC: %w", err)
	}
	if resp.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
		return nil, fmt.Errorf("register: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp, nil
}

func (n *Node) setEpoch(epoch int64) { n.epoch.Store(epoch) }

// liveness heartbeats to the controller every broker_heartbeat_interval_ms, reporting the
// metadata index the broker has processed (so the controller can unfence it, D35). A stale-epoch
// answer means the controller lost track of this incarnation: re-register.
func (n *Node) liveness(ctx context.Context, port int32) error {
	interval := time.Duration(n.cfg.BrokerHeartbeatIntervalMs) * time.Millisecond
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		c, err := n.controllerClient()
		if err != nil {
			continue
		}
		hctx, cancel := context.WithTimeout(ctx, interval)
		resp, err := c.BrokerHeartbeat(hctx, &streamqv1.BrokerHeartbeatRequest{
			BrokerId: n.cfg.BrokerID, BrokerEpoch: n.epoch.Load(), AppliedIndex: n.broker.ProcessedIndex(),
		})
		cancel()
		switch {
		case err != nil || ctx.Err() != nil:
			continue
		case resp.GetErrorCode() == streamqv1.ErrorCode_ERROR_CODE_STALE_BROKER_EPOCH:
			n.logger.Warn("controller reports a stale broker epoch; re-registering")
			reg, err := n.register(ctx, port)
			if err != nil {
				continue
			}
			n.setEpoch(reg.GetBrokerEpoch())
		}
	}
}
