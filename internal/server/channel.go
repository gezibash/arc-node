package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/indexstore"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/pkg/envelope"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// streamWriter serializes concurrent sends on a bidi stream.
type streamWriter struct {
	mu     sync.Mutex
	stream nodev1.NodeService_ChannelServer
}

func (w *streamWriter) send(frame *nodev1.ServerFrame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stream.Send(frame)
}

// channelSubs manages per-stream subscription goroutines.
type channelSubs struct {
	mu   sync.Mutex
	subs map[string]context.CancelFunc
}

func newChannelSubs() *channelSubs {
	return &channelSubs{subs: make(map[string]context.CancelFunc)}
}

func (cs *channelSubs) add(channel string, cancel context.CancelFunc) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	// Cancel existing subscription on the same channel.
	if prev, ok := cs.subs[channel]; ok {
		prev()
	}
	cs.subs[channel] = cancel
}

func (cs *channelSubs) remove(channel string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cancel, ok := cs.subs[channel]; ok {
		cancel()
		delete(cs.subs, channel)
	}
}

func (cs *channelSubs) closeAll() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for ch, cancel := range cs.subs {
		cancel()
		delete(cs.subs, ch)
	}
}

func (s *nodeService) Channel(stream nodev1.NodeService_ChannelServer) error {
	ctx := stream.Context()
	writer := &streamWriter{stream: stream}
	subs := newChannelSubs()
	defer subs.closeAll()

	for {
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		reqID := frame.GetRequestId()

		switch f := frame.Frame.(type) {
		case *nodev1.ClientFrame_Put:
			s.handlePut(ctx, writer, reqID, f.Put)
		case *nodev1.ClientFrame_Get:
			s.handleGet(ctx, writer, reqID, f.Get)
		case *nodev1.ClientFrame_Publish:
			s.handlePublish(ctx, writer, reqID, f.Publish)
		case *nodev1.ClientFrame_Query:
			s.handleQuery(ctx, writer, reqID, f.Query)
		case *nodev1.ClientFrame_Subscribe:
			s.handleSubscribe(ctx, writer, reqID, f.Subscribe, subs)
		case *nodev1.ClientFrame_Unsubscribe:
			s.handleUnsubscribe(writer, reqID, f.Unsubscribe, subs)
		case *nodev1.ClientFrame_Ack:
			s.handleAck(ctx, writer, reqID, f.Ack)
		case *nodev1.ClientFrame_Seek:
			s.handleSeek(ctx, writer, reqID, f.Seek)
		case *nodev1.ClientFrame_Federate:
			s.handleFederate(ctx, writer, reqID, f.Federate)
		case *nodev1.ClientFrame_ListPeers:
			s.handleListPeers(ctx, writer, reqID)
		case *nodev1.ClientFrame_ResolveGet:
			s.handleResolveGet(ctx, writer, reqID, f.ResolveGet)
		default:
			sendErr(writer, reqID, codes.InvalidArgument, "unknown frame type")
		}
	}
}

func (s *nodeService) handlePut(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.PutFrame) {
	ref, err := s.doPut(ctx, f.Data)
	if err != nil {
		sendErr(w, reqID, codes.Internal, err.Error())
		return
	}
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{Reference: ref[:], Ok: true}},
	})
}

func (s *nodeService) handleGet(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.GetFrame) {
	ref, err := referenceFromBytes(f.Reference)
	if err != nil {
		sendErr(w, reqID, codes.InvalidArgument, err.Error())
		return
	}
	data, err := s.doGet(ctx, ref)
	if err != nil {
		sendErr(w, reqID, codes.NotFound, err.Error())
		return
	}
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_Response{Response: &nodev1.ResponseFrame{Data: data}},
	})
}

func (s *nodeService) handlePublish(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.PublishFrame) {
	ref, err := s.doPublish(ctx, f.Message, f.Labels, f.Dimensions)
	if err != nil {
		st := publishErrToStatus(err)
		if s, ok := status.FromError(st); ok {
			sendErr(w, reqID, s.Code(), s.Message())
		} else {
			sendErr(w, reqID, codes.Internal, err.Error())
		}
		return
	}
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{Reference: ref[:], Ok: true}},
	})
}

func (s *nodeService) handleQuery(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.QueryFrame) {
	opts := &indexstore.QueryOptions{
		Expression: f.Expression,
		Labels:     f.Labels,
		Limit:      int(f.Limit),
		Cursor:     f.Cursor,
		Descending: f.Order == nodev1.Order_ORDER_DESCENDING,
	}
	result, err := s.doQuery(ctx, opts)
	if err != nil {
		sendErr(w, reqID, codes.Internal, err.Error())
		return
	}
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame: &nodev1.ServerFrame_Response{Response: &nodev1.ResponseFrame{
			Entries:    entriesToProto(result.Entries),
			NextCursor: result.NextCursor,
			HasMore:    result.HasMore,
		}},
	})
}

func (s *nodeService) handleSubscribe(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.SubscribeFrame, cs *channelSubs) {
	channel := f.Channel
	if channel == "" {
		channel = "default"
	}

	// Build expression from labels + expression, same as SubscribeMessages.
	expr := f.Expression
	if expr == "" {
		expr = "true"
	}
	for k, v := range f.Labels {
		expr = fmt.Sprintf(`%s && "%s" in labels && labels["%s"] == "%s"`, expr, k, k, v)
	}

	var callerKey [32]byte
	if caller, ok := envelope.GetCaller(ctx); ok {
		callerKey = caller.PublicKey
	}

	sub, err := s.index.Subscribe(ctx, expr, callerKey)
	if err != nil {
		sendErr(w, reqID, codes.Internal, err.Error())
		return
	}

	// Track this subscriber.
	var tracker *subscriber
	if s.subscribers != nil {
		pubKey := callerKey
		tracker = s.subscribers.Add(pubKey, f.Labels, f.Expression)
	}

	subCtx, cancel := context.WithCancel(ctx)
	cs.add(channel, func() {
		sub.Cancel()
		cancel()
		if tracker != nil {
			s.subscribers.Remove(tracker)
		}
	})

	// Build cursor key for durable subscriptions.
	subID := f.SubscriptionId
	var cursorKey string
	if subID != "" {
		var pubKeyHex string
		if caller, ok := envelope.GetCaller(ctx); ok {
			pubKeyHex = hex.EncodeToString(caller.PublicKey[:])
		} else {
			pubKeyHex = "anonymous"
		}
		cursorKey = pubKeyHex + "/" + subID
	}

	// Ack the subscribe.
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{Ok: true}},
	})

	// Fan out deliveries in a goroutine.
	go func() {
		defer cs.remove(channel)

		// Cursor resume: replay missed entries before switching to live.
		if cursorKey != "" {
			s.replayFromCursor(ctx, w, channel, cursorKey, f.Cursor, f.Labels, f.Expression, tracker)
		}

		// Start redelivery loop.
		redeliverDone := make(chan struct{})
		go func() {
			defer close(redeliverDone)
			s.redeliveryLoop(subCtx, w, channel)
		}()

		for {
			select {
			case <-subCtx.Done():
				<-redeliverDone
				return
			case entry, ok := <-sub.Entries():
				if !ok {
					<-redeliverDone
					return
				}

				var deliveryID int64
				if entry.DeliveryMode == 1 {
					deliveryID = s.index.Deliver(entry, sub.ID())
				}

				if err := w.send(&nodev1.ServerFrame{
					RequestId: 0, // unsolicited push
					Frame: &nodev1.ServerFrame_Delivery{Delivery: &nodev1.DeliveryFrame{
						Channel:    channel,
						Entry:      entryToProto(entry),
						DeliveryId: deliveryID,
					}},
				}); err != nil {
					slog.Error("channel delivery send failed", "error", err)
					<-redeliverDone
					return
				}
				if tracker != nil {
					tracker.entriesSent.Add(1)
				}
			}
		}
	}()
}

// replayFromCursor replays entries from the cursor position.
// Entries are sorted by priority (descending) then timestamp (ascending).
func (s *nodeService) replayFromCursor(ctx context.Context, w *streamWriter, channel, cursorKey, providedCursor string, labels map[string]string, expression string, tracker *subscriber) {
	var afterTS int64

	if providedCursor != "" {
		// Parse cursor as timestamp.
		_, _ = fmt.Sscanf(providedCursor, "%d", &afterTS)
	} else {
		// Load cursor from backend.
		cursor, err := s.index.GetCursor(ctx, cursorKey)
		if err != nil {
			if !errors.Is(err, physical.ErrCursorNotFound) {
				slog.Error("failed to load cursor", "key", cursorKey, "error", err)
			}
			return
		}
		afterTS = cursor.Timestamp
	}

	if afterTS == 0 {
		return
	}

	// Build expression for replay query.
	expr := expression
	if expr == "" {
		expr = "true"
	}
	for k, v := range labels {
		expr = fmt.Sprintf(`%s && "%s" in labels && labels["%s"] == "%s"`, expr, k, k, v)
	}

	result, err := s.doQuery(ctx, &indexstore.QueryOptions{
		Expression: expr,
		After:      afterTS,
	})
	if err != nil {
		slog.Error("cursor replay query failed", "error", err)
		return
	}

	// Sort by priority (descending) then timestamp (ascending).
	sort.Slice(result.Entries, func(i, j int) bool {
		if result.Entries[i].Priority != result.Entries[j].Priority {
			return result.Entries[i].Priority > result.Entries[j].Priority
		}
		return result.Entries[i].Timestamp < result.Entries[j].Timestamp
	})

	for _, entry := range result.Entries {
		var deliveryID int64
		if entry.DeliveryMode == 1 {
			deliveryID = s.index.Deliver(entry, cursorKey)
		}

		if err := w.send(&nodev1.ServerFrame{
			RequestId: 0,
			Frame: &nodev1.ServerFrame_Delivery{Delivery: &nodev1.DeliveryFrame{
				Channel:    channel,
				Entry:      entryToProto(entry),
				DeliveryId: deliveryID,
			}},
		}); err != nil {
			slog.Error("cursor replay send failed", "error", err)
			return
		}
		if tracker != nil {
			tracker.entriesSent.Add(1)
		}
	}
}

// redeliveryLoop periodically checks for expired deliveries and redelivers or dead-letters them.
// Expired deliveries are sorted by priority (descending) before redelivery.
func (s *nodeService) redeliveryLoop(ctx context.Context, w *streamWriter, channel string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	dt := s.index.DeliveryTracker()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expired := dt.Expired()

			// Sort by priority descending before redelivery.
			sort.Slice(expired, func(i, j int) bool {
				pi, pj := int32(0), int32(0)
				if expired[i].Entry != nil {
					pi = expired[i].Entry.Priority
				}
				if expired[j].Entry != nil {
					pj = expired[j].Entry.Priority
				}
				return pi > pj
			})

			for _, d := range expired {
				maxR := dt.MaxRedeliver()
				if d.Entry != nil && d.Entry.MaxRedelivery > 0 {
					maxR = int(d.Entry.MaxRedelivery)
				}
				if d.Attempt >= maxR {
					if err := dt.DeadLetter(ctx, d); err != nil {
						slog.Error("dead letter failed", "error", err)
					}
					continue
				}

				newID := dt.Redeliver(d)
				if d.Entry != nil {
					_ = w.send(&nodev1.ServerFrame{
						RequestId: 0,
						Frame: &nodev1.ServerFrame_Delivery{Delivery: &nodev1.DeliveryFrame{
							Channel:    channel,
							Entry:      entryToProto(d.Entry),
							DeliveryId: newID,
						}},
					})
				}
			}
		}
	}
}

func (s *nodeService) handleUnsubscribe(w *streamWriter, reqID uint64, f *nodev1.UnsubscribeFrame, cs *channelSubs) {
	channel := f.Channel
	if channel == "" {
		channel = "default"
	}
	cs.remove(channel)
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{Ok: true}},
	})
}

func (s *nodeService) handleAck(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.AckFrame) {
	// Prefer delivery_id if set, fall back to reference-based ack for backwards compat.
	if f.DeliveryId > 0 {
		entry, err := s.index.AckDelivery(ctx, f.DeliveryId)
		if err != nil {
			sendErr(w, reqID, codes.Internal, err.Error())
			return
		}

		// Update cursor if we have the entry.
		if entry != nil && entry.Sequence > 0 {
			_ = entry
		}

		_ = w.send(&nodev1.ServerFrame{
			RequestId: reqID,
			Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{Ok: true}},
		})
		return
	}

	// Legacy: ack by reference.
	if len(f.Reference) == 0 {
		sendErr(w, reqID, codes.InvalidArgument, "delivery_id or reference required")
		return
	}
	// No-op for legacy acks since delivery tracking is now ID-based.
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{Ok: true}},
	})
}

func (s *nodeService) handleSeek(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.SeekFrame) {
	channel := f.Channel
	if channel == "" {
		channel = "default"
	}

	// Determine the seek position.
	var afterTS int64
	if f.Cursor != "" {
		_, _ = fmt.Sscanf(f.Cursor, "%d", &afterTS)
	} else if f.Timestamp > 0 {
		afterTS = f.Timestamp
	}

	if afterTS == 0 {
		sendErr(w, reqID, codes.InvalidArgument, "timestamp or cursor required for seek")
		return
	}

	// Ack the seek.
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{Ok: true}},
	})

	// Replay from the seek position.
	go s.replayFromCursor(ctx, w, channel, "", fmt.Sprintf("%d", afterTS), nil, "true", nil)
}

func (s *nodeService) handleFederate(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.FederateFrame) {
	if !s.isAdmin(ctx) {
		sendErr(w, reqID, codes.PermissionDenied, "admin key required")
		return
	}

	if f.Peer == "" {
		sendErr(w, reqID, codes.InvalidArgument, "peer required")
		return
	}
	if s.federator == nil {
		sendErr(w, reqID, codes.FailedPrecondition, "federation unavailable")
		return
	}

	peer := normalizePeerAddr(f.Peer)
	started, err := s.federator.Start(peer, f.Labels)
	if err != nil {
		sendErr(w, reqID, codes.Internal, fmt.Sprintf("start federation: %v", err))
		return
	}

	st := "ok"
	msg := fmt.Sprintf("federating with %s", peer)
	if !started {
		st = "already_federating"
		msg = fmt.Sprintf("already federating with %s", peer)
	}

	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame: &nodev1.ServerFrame_FederateResponse{FederateResponse: &nodev1.FederateResponseFrame{
			Status:  st,
			Message: msg,
		}},
	})
}

func (s *nodeService) handleListPeers(ctx context.Context, w *streamWriter, reqID uint64) {
	if !s.isAdmin(ctx) {
		sendErr(w, reqID, codes.PermissionDenied, "admin key required")
		return
	}

	var peers []*nodev1.PeerInfo

	// Outbound: peers we are federating from.
	if s.federator != nil {
		for _, info := range s.federator.List() {
			peers = append(peers, &nodev1.PeerInfo{
				Address:           info.Address,
				Labels:            info.Labels,
				BytesReceived:     info.BytesReceived,
				EntriesReplicated: info.EntriesReplicated,
				StartedAt:         info.StartedAt.UnixMilli(),
				Direction:         nodev1.PeerDirection_PEER_DIRECTION_OUTBOUND,
			})
		}
	}

	// Inbound: subscribers pulling from us.
	if s.subscribers != nil {
		for _, info := range s.subscribers.List() {
			peers = append(peers, &nodev1.PeerInfo{
				PublicKey:   info.PublicKey[:],
				Labels:      info.Labels,
				EntriesSent: info.EntriesSent,
				StartedAt:   info.StartedAt.UnixMilli(),
				Direction:   nodev1.PeerDirection_PEER_DIRECTION_INBOUND,
			})
		}
	}

	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame: &nodev1.ServerFrame_ListPeersResponse{ListPeersResponse: &nodev1.ListPeersResponseFrame{
			Peers: peers,
		}},
	})
}

func (s *nodeService) handleResolveGet(ctx context.Context, w *streamWriter, reqID uint64, f *nodev1.ResolveGetFrame) {
	resp, err := s.doResolveGet(ctx, f.Prefix)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			sendErr(w, reqID, st.Code(), st.Message())
		} else {
			sendErr(w, reqID, codes.Internal, err.Error())
		}
		return
	}
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame:     &nodev1.ServerFrame_ResolveGetResponse{ResolveGetResponse: resp},
	})
}

func sendErr(w *streamWriter, reqID uint64, code codes.Code, msg string) {
	_ = w.send(&nodev1.ServerFrame{
		RequestId: reqID,
		Frame: &nodev1.ServerFrame_Error{Error: &nodev1.ErrorFrame{
			Code:    int32(code),
			Message: msg,
		}},
	})
}
