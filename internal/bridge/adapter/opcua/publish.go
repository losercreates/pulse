package opcua

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/losercreates/pulse/internal/bridge/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// ExtensionObject TypeIDs
const (
	TypeIDDataChangeNotification uint32 = 811
)

// PublishLoop executes continuous PublishRequest pipelining and dispatches incoming readings.
func PublishLoop(
	ctx context.Context,
	ch *Channel,
	sm *SubscriptionManager,
	out chan<- *telemetryv1.TelemetryReading,
	m *metrics.BridgeMetrics,
	adapterName string,
) error {
	var lastSeq uint32
	var hasAck bool

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 1. Build PublishRequest
		var body bytes.Buffer

		// TypeID: PublishRequest (826)
		reqNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDPublishRequest}
		_ = reqNode.Encode(&body)

		reqHeader := RequestHeader{
			AuthenticationToken: ch.authToken,
			Timestamp:           time.Now().UnixNano(),
			RequestHandle:       20,
			TimeoutHint:         15000,
		}
		_ = reqHeader.Encode(&body)

		// SubscriptionAcknowledgements array
		if hasAck && sm.SubscriptionID() > 0 {
			_ = binary.Write(&body, binary.LittleEndian, int32(1))
			_ = binary.Write(&body, binary.LittleEndian, sm.SubscriptionID())
			_ = binary.Write(&body, binary.LittleEndian, lastSeq)
		} else {
			_ = binary.Write(&body, binary.LittleEndian, int32(0))
		}

		if err := ch.writeMessage(body.Bytes()); err != nil {
			return fmt.Errorf("publish request write failed: %w", err)
		}

		// 2. Read PublishResponse
		respBytes, err := ch.readMessage()
		if err != nil {
			return fmt.Errorf("publish response read failed: %w", err)
		}

		// Decode Response
		pr := bytes.NewReader(respBytes)
		_, _ = DecodeNodeID(pr) // TypeIDPublishResponse
		resHdr, err := DecodeResponseHeader(pr)
		if err != nil {
			return fmt.Errorf("failed to decode publish response header: %w", err)
		}

		if !resHdr.ServiceResult.IsGood() {
			if resHdr.ServiceResult == StatusBadTimeout {
				// Keep-alive or request timeout, safe to continue loop
				continue
			}
			return fmt.Errorf("publish response returned service error: %s", resHdr.ServiceResult)
		}

		// SubscriptionID
		var subID uint32
		if err := binary.Read(pr, binary.LittleEndian, &subID); err != nil {
			return err
		}

		// AvailableSequenceNumbers
		var availCount int32
		if err := binary.Read(pr, binary.LittleEndian, &availCount); err == nil && availCount > 0 {
			for i := int32(0); i < availCount; i++ {
				var seq uint32
				_ = binary.Read(pr, binary.LittleEndian, &seq)
			}
		}

		// MoreNotifications (bool)
		var moreNotifs byte
		_ = binary.Read(pr, binary.LittleEndian, &moreNotifs)

		// NotificationMessage: SequenceNumber (4), PublishTime (8), NotificationData array
		var msgSeq uint32
		if err := binary.Read(pr, binary.LittleEndian, &msgSeq); err != nil {
			return err
		}
		lastSeq = msgSeq
		hasAck = true

		_, _ = decodeDateTime(pr) // PublishTime

		var dataCount int32
		if err := binary.Read(pr, binary.LittleEndian, &dataCount); err != nil {
			return err
		}

		// Keep-alive empty notification
		if dataCount <= 0 {
			continue
		}

		for d := int32(0); d < dataCount; d++ {
			extType, err := DecodeNodeID(pr)
			if err != nil {
				break
			}
			var enc byte
			_ = binary.Read(pr, binary.LittleEndian, &enc)

			// If DataChangeNotification
			if extType.NumericID == TypeIDDataChangeNotification {
				var innerLen int32
				_ = binary.Read(pr, binary.LittleEndian, &innerLen)

				var itemsCount int32
				if err := binary.Read(pr, binary.LittleEndian, &itemsCount); err != nil {
					break
				}

				for i := int32(0); i < itemsCount; i++ {
					var clientHandle uint32
					if err := binary.Read(pr, binary.LittleEndian, &clientHandle); err != nil {
						break
					}

					dv, err := DecodeDataValue(pr)
					if err != nil {
						break
					}

					item, ok := sm.GetItem(clientHandle)
					if !ok {
						continue
					}

					val, err := dv.Value.Float64()
					if err != nil {
						if m != nil {
							m.IngestionErrorsTotal.WithLabelValues(adapterName, "opcua", "type_conversion").Inc()
						}
						continue
					}

					tsNs := dv.SourceTimestamp.UnixNano()
					if tsNs <= 0 {
						tsNs = dv.ServerTimestamp.UnixNano()
					}
					if tsNs <= 0 {
						tsNs = time.Now().UnixNano()
					}

					reading := &telemetryv1.TelemetryReading{
						DeviceId:   item.Config.DeviceID,
						MetricName: item.Config.MetricName,
						Value:      val,
						TimestampNs: tsNs,
						Metadata: map[string]string{
							"protocol":    "opcua",
							"node_id":     item.Config.NodeID,
							"status_code": dv.Status.String(),
						},
					}

					select {
					case out <- reading:
						if m != nil {
							m.ReadingsIngestedTotal.WithLabelValues(adapterName, "opcua").Inc()
							m.OPCUANotificationsTotal.WithLabelValues(adapterName, dv.Status.String()).Inc()
							if dv.Status.IsBad() {
								m.IngestionErrorsTotal.WithLabelValues(adapterName, "opcua", "bad_status").Inc()
							}
						}
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	}
}

