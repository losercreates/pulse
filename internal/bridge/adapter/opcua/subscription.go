package opcua

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
)

// MonitoredItemHandle links a client handle to its item definition and configuration.
type MonitoredItemHandle struct {
	ClientHandle    uint32
	MonitoredItemID uint32
	Config          config.MonitoredItemConfig
	NodeID          NodeID
}

// SubscriptionManager handles subscription lifecycle and monitored item registration.
type SubscriptionManager struct {
	channel        *Channel
	subscriptionID uint32
	mu             sync.RWMutex
	items          map[uint32]MonitoredItemHandle
	nextHandle     uint32
}

// NewSubscriptionManager constructs a SubscriptionManager.
func NewSubscriptionManager(ch *Channel) *SubscriptionManager {
	return &SubscriptionManager{
		channel:    ch,
		items:      make(map[uint32]MonitoredItemHandle),
		nextHandle: 1,
	}
}

// SubscriptionID returns active subscription ID.
func (sm *SubscriptionManager) SubscriptionID() uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.subscriptionID
}

// GetItem looks up item handle by client handle.
func (sm *SubscriptionManager) GetItem(clientHandle uint32) (MonitoredItemHandle, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	item, ok := sm.items[clientHandle]
	return item, ok
}

// CreateSubscription sends CreateSubscriptionRequest to the OPC-UA server.
func (sm *SubscriptionManager) CreateSubscription(ctx context.Context, interval time.Duration) (uint32, error) {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}

	var body bytes.Buffer

	// TypeID: CreateSubscriptionRequest (787)
	reqNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDCreateSubscriptionRequest}
	_ = reqNode.Encode(&body)

	reqHeader := RequestHeader{
		AuthenticationToken: sm.channel.authToken,
		Timestamp:           time.Now().UnixNano(),
		RequestHandle:       10,
		TimeoutHint:         10000,
	}
	_ = reqHeader.Encode(&body)

	// RequestedPublishingInterval (float64 ms)
	_ = binary.Write(&body, binary.LittleEndian, float64(interval.Milliseconds()))

	// RequestedLifetimeCount = 60
	_ = binary.Write(&body, binary.LittleEndian, uint32(60))

	// RequestedMaxKeepAliveCount = 10
	_ = binary.Write(&body, binary.LittleEndian, uint32(10))

	// MaxNotificationsPerPublish = 0 (unlimited)
	_ = binary.Write(&body, binary.LittleEndian, uint32(0))

	// PublishingEnabled = true
	_ = binary.Write(&body, binary.LittleEndian, byte(1))

	// Priority = 0
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	if err := sm.channel.writeMessage(body.Bytes()); err != nil {
		return 0, fmt.Errorf("failed to write CreateSubscriptionRequest: %w", err)
	}

	respBytes, err := sm.channel.readMessage()
	if err != nil {
		return 0, fmt.Errorf("failed to read CreateSubscriptionResponse: %w", err)
	}

	pr := bytes.NewReader(respBytes)
	_, _ = DecodeNodeID(pr) // TypeIDCreateSubscriptionResponse
	resHdr, err := DecodeResponseHeader(pr)
	if err != nil {
		return 0, err
	}
	if !resHdr.ServiceResult.IsGood() {
		return 0, fmt.Errorf("create subscription failed: %s", resHdr.ServiceResult)
	}

	var subID uint32
	if err := binary.Read(pr, binary.LittleEndian, &subID); err != nil {
		return 0, err
	}

	sm.mu.Lock()
	sm.subscriptionID = subID
	sm.mu.Unlock()

	return subID, nil
}

// CreateMonitoredItems registers variable nodes for subscription data change notifications.
func (sm *SubscriptionManager) CreateMonitoredItems(ctx context.Context, items []config.MonitoredItemConfig) ([]MonitoredItemHandle, error) {
	if len(items) == 0 {
		return nil, nil
	}

	var body bytes.Buffer

	// TypeID: CreateMonitoredItemsRequest (751)
	reqNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDCreateMonitoredItemsRequest}
	_ = reqNode.Encode(&body)

	reqHeader := RequestHeader{
		AuthenticationToken: sm.channel.authToken,
		Timestamp:           time.Now().UnixNano(),
		RequestHandle:       11,
		TimeoutHint:         10000,
	}
	_ = reqHeader.Encode(&body)

	// SubscriptionID
	_ = binary.Write(&body, binary.LittleEndian, sm.subscriptionID)

	// TimestampsToReturn = 2 (Both: Source and Server)
	_ = binary.Write(&body, binary.LittleEndian, uint32(2))

	// ItemsToCreate count
	_ = binary.Write(&body, binary.LittleEndian, int32(len(items)))

	var handles []MonitoredItemHandle

	sm.mu.Lock()
	for _, it := range items {
		handle := sm.nextHandle
		sm.nextHandle++

		nid, err := ParseNodeID(it.NodeID)
		if err != nil {
			nid = NodeID{Type: NodeIDTypeString, NamespaceIndex: 2, StringID: it.NodeID}
		}

		mHandle := MonitoredItemHandle{
			ClientHandle: handle,
			Config:       it,
			NodeID:       nid,
		}
		handles = append(handles, mHandle)
		sm.items[handle] = mHandle

		// ReadValueId: NodeID, AttributeId (13 = Value), IndexRange (null), DataEncoding (null)
		_ = nid.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, uint32(13)) // Value attribute
		_ = writeOPCUAString(&body, "")
		nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
		_ = nullNode.Encode(&body)

		// MonitoringMode = 1 (Reporting)
		_ = binary.Write(&body, binary.LittleEndian, uint32(1))

		// MonitoringParameters: ClientHandle, SamplingInterval, Filter, QueueSize, DiscardOldest
		_ = binary.Write(&body, binary.LittleEndian, handle)
		samplingMs := float64(it.SamplingInterval.Milliseconds())
		if samplingMs < 0 {
			samplingMs = 0
		}
		_ = binary.Write(&body, binary.LittleEndian, samplingMs)

		// Filter: ExtensionObject (null)
		_ = nullNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0))

		qSize := it.QueueSize
		if qSize == 0 {
			qSize = 10
		}
		_ = binary.Write(&body, binary.LittleEndian, qSize)

		discardOldest := byte(1)
		if !it.DiscardOldest {
			discardOldest = 0
		}
		_ = binary.Write(&body, binary.LittleEndian, discardOldest)
	}
	sm.mu.Unlock()

	if err := sm.channel.writeMessage(body.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write CreateMonitoredItemsRequest: %w", err)
	}

	respBytes, err := sm.channel.readMessage()
	if err != nil {
		return nil, fmt.Errorf("failed to read CreateMonitoredItemsResponse: %w", err)
	}

	pr := bytes.NewReader(respBytes)
	_, _ = DecodeNodeID(pr) // TypeIDCreateMonitoredItemsResponse
	resHdr, err := DecodeResponseHeader(pr)
	if err != nil {
		return nil, err
	}
	if !resHdr.ServiceResult.IsGood() {
		return nil, fmt.Errorf("create monitored items failed: %s", resHdr.ServiceResult)
	}

	// Results count
	var resCount int32
	if err := binary.Read(pr, binary.LittleEndian, &resCount); err != nil {
		return nil, err
	}

	sm.mu.Lock()
	for i := int32(0); i < resCount && int(i) < len(handles); i++ {
		var st, monitoredItemID uint32
		var revisedSampling float64
		var revisedQueue uint32
		_ = binary.Read(pr, binary.LittleEndian, &st)
		_ = binary.Read(pr, binary.LittleEndian, &monitoredItemID)
		_ = binary.Read(pr, binary.LittleEndian, &revisedSampling)
		_ = binary.Read(pr, binary.LittleEndian, &revisedQueue)
		// Skip filter result
		_, _ = DecodeNodeID(pr)
		var enc byte
		_ = binary.Read(pr, binary.LittleEndian, &enc)

		h := handles[i]
		h.MonitoredItemID = monitoredItemID
		handles[i] = h
		sm.items[h.ClientHandle] = h
	}
	sm.mu.Unlock()

	return handles, nil
}

