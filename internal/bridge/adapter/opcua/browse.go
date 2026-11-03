package opcua

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
)

// DiscoveredVariable holds discovered node information.
type DiscoveredVariable struct {
	NodeID      NodeID
	BrowsePath  string
	DisplayName string
}

func (a *Adapter) runDiscovery(ctx context.Context) ([]config.MonitoredItemConfig, error) {
	start := time.Now()
	defer func() {
		if a.metrics != nil {
			a.metrics.OPCUABrowseDurationSeconds.WithLabelValues(a.cfg.Name).Observe(time.Since(start).Seconds())
		}
	}()

	rootNode, err := ParseNodeID(a.cfg.Discovery.RootNodeID)
	if err != nil {
		rootNode = NodeID{Type: NodeIDTypeTwoByte, NamespaceIndex: 0, NumericID: 85} // Objects
	}

	maxDepth := a.cfg.Discovery.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 10
	}

	visited := make(map[string]bool)
	var discovered []DiscoveredVariable

	err = a.browseRecursive(ctx, rootNode, "Objects", 1, maxDepth, visited, &discovered)
	if err != nil {
		return nil, err
	}

	var items []config.MonitoredItemConfig
	pattern := a.cfg.Discovery.PathFilter
	if pattern == "" {
		pattern = "*"
	}

	for _, dv := range discovered {
		matched, _ := filepath.Match(pattern, dv.BrowsePath)
		if !matched && !strings.Contains(pattern, "*") {
			matched = strings.HasPrefix(dv.BrowsePath, pattern)
		}
		if matched {
			deviceID := a.cfg.Discovery.DefaultDeviceID
			if deviceID == "" {
				// Derive from browse path: e.g. Objects/Turbine-01/Temp -> Turbine-01
				parts := strings.Split(dv.BrowsePath, "/")
				if len(parts) >= 2 {
					deviceID = strings.ToLower(parts[len(parts)-2])
				} else {
					deviceID = "opcua-discovered"
				}
			}

			metricName := strings.ToLower(dv.DisplayName)
			metricName = strings.ReplaceAll(metricName, " ", "_")
			metricName = strings.ReplaceAll(metricName, ".", "_")

			items = append(items, config.MonitoredItemConfig{
				NodeID:           dv.NodeID.String(),
				DeviceID:         deviceID,
				MetricName:       metricName,
				SamplingInterval: 100 * time.Millisecond,
				QueueSize:        10,
				DiscardOldest:    true,
			})
		}
	}

	return items, nil
}

func (a *Adapter) browseRecursive(
	ctx context.Context,
	node NodeID,
	currentPath string,
	depth int,
	maxDepth int,
	visited map[string]bool,
	results *[]DiscoveredVariable,
) error {
	if depth > maxDepth {
		return nil
	}

	nodeKey := node.String()
	if visited[nodeKey] {
		return nil // Cycle detected, skip
	}
	visited[nodeKey] = true

	// Build BrowseRequest
	var body bytes.Buffer

	// TypeID: BrowseRequest (527)
	reqNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDBrowseRequest}
	_ = reqNode.Encode(&body)

	reqHeader := RequestHeader{
		AuthenticationToken: a.channel.authToken,
		Timestamp:           time.Now().UnixNano(),
		RequestHandle:       15,
		TimeoutHint:         10000,
	}
	_ = reqHeader.Encode(&body)

	// ViewDescription: ViewId (null), Timestamp (0), ViewVersion (0)
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, int64(0))
	_ = binary.Write(&body, binary.LittleEndian, uint32(0))

	// RequestedMaxReferencesPerNode = 1000
	_ = binary.Write(&body, binary.LittleEndian, uint32(1000))

	// NodesToBrowse array count: 1
	_ = binary.Write(&body, binary.LittleEndian, int32(1))

	// BrowseDescription: NodeID, BrowseDirection=0 (Forward), ReferenceTypeId (33=HierarchicalReferences),
	// IncludeSubtypes=1, NodeClassMask=0 (all), ResultMask=63 (all)
	_ = node.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, uint32(0)) // Forward
	hierRef := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 33}
	_ = hierRef.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(1))  // IncludeSubtypes
	_ = binary.Write(&body, binary.LittleEndian, uint32(0)) // NodeClassMask (all)
	_ = binary.Write(&body, binary.LittleEndian, uint32(63))

	if err := a.channel.writeMessage(body.Bytes()); err != nil {
		return err
	}

	respBytes, err := a.channel.readMessage()
	if err != nil {
		return err
	}

	pr := bytes.NewReader(respBytes)
	_, _ = DecodeNodeID(pr) // TypeIDBrowseResponse
	resHdr, err := DecodeResponseHeader(pr)
	if err != nil {
		return err
	}
	if !resHdr.ServiceResult.IsGood() {
		return fmt.Errorf("browse failed: %s", resHdr.ServiceResult)
	}

	var resCount int32
	if err := binary.Read(pr, binary.LittleEndian, &resCount); err != nil || resCount <= 0 {
		return nil
	}

	var st uint32
	_ = binary.Read(pr, binary.LittleEndian, &st)
	_, _ = readByteString(pr) // continuation point

	var refCount int32
	if err := binary.Read(pr, binary.LittleEndian, &refCount); err != nil || refCount <= 0 {
		return nil
	}

	type childRef struct {
		targetNode  NodeID
		browseName  string
		displayName string
		nodeClass   uint32
	}

	var children []childRef
	for i := int32(0); i < refCount; i++ {
		_, _ = DecodeNodeID(pr) // ReferenceTypeId
		var isForward byte
		_ = binary.Read(pr, binary.LittleEndian, &isForward)

		targetNode, err := DecodeNodeID(pr)
		if err != nil {
			break
		}

		var ns uint16
		_ = binary.Read(pr, binary.LittleEndian, &ns)
		browseName, _ := readOPCUAString(pr)

		var enc byte
		_ = binary.Read(pr, binary.LittleEndian, &enc)
		displayName, _ := readOPCUAString(pr)

		var nodeClass uint32
		_ = binary.Read(pr, binary.LittleEndian, &nodeClass)

		_, _ = DecodeNodeID(pr) // TypeDefinition

		children = append(children, childRef{
			targetNode:  targetNode,
			browseName:  browseName,
			displayName: displayName,
			nodeClass:   nodeClass,
		})
	}

	for _, child := range children {
		childPath := currentPath + "/" + child.browseName
		if child.nodeClass == 2 { // Variable node
			*results = append(*results, DiscoveredVariable{
				NodeID:      child.targetNode,
				BrowsePath:  childPath,
				DisplayName: child.displayName,
			})
		} else if child.nodeClass == 1 { // Object node
			_ = a.browseRecursive(ctx, child.targetNode, childPath, depth+1, maxDepth, visited, results)
		}
	}

	return nil
}

