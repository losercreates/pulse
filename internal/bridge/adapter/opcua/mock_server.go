package opcua

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// MockOPCUAServer is an in-memory TCP mock OPC-UA server for deterministic unit and integration tests.
type MockOPCUAServer struct {
	listener net.Listener
	addr     string
	mu       sync.Mutex
	closed   atomic.Bool
	conns    []net.Conn

	// Configurable hooks / responses
	OnPublishRequest func(w io.Writer, seq uint32, reqID uint32, subAckCount int32)
	OnBrowseRequest  func(w io.Writer, seq uint32, reqID uint32, rootNode NodeID)
}

// StartMockServer starts an in-memory mock OPC-UA TCP server on an ephemeral port.
func StartMockServer(tb any) *MockOPCUAServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	s := &MockOPCUAServer{
		listener: ln,
		addr:     ln.Addr().String(),
	}

	go s.serve()
	return s
}

func startMockOPCUAServer(t any) *MockOPCUAServer {
	return StartMockServer(t)
}

func (s *MockOPCUAServer) Endpoint() string {
	return "opc.tcp://" + s.addr
}

func (s *MockOPCUAServer) Close() {
	if s.closed.CompareAndSwap(false, true) {
		_ = s.listener.Close()
		s.mu.Lock()
		for _, c := range s.conns {
			_ = c.Close()
		}
		s.conns = nil
		s.mu.Unlock()
	}
}

func (s *MockOPCUAServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.handleConn(conn)
	}
}

func (s *MockOPCUAServer) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		hdr, err := ReadHeader(conn)
		if err != nil {
			return
		}

		switch hdr.MessageType {
		case "HEL":
			_, _ = DecodeHello(conn)
			ack := AcknowledgeMessage{
				ProtocolVersion:   0,
				ReceiveBufferSize: 65536,
				SendBufferSize:    65536,
				MaxMessageSize:    16777216,
				MaxChunkCount:     5000,
			}
			_ = ack.Encode(conn)

		case "OPN":
			buf := make([]byte, hdr.MessageSize-8)
			if _, err := io.ReadFull(conn, buf); err != nil {
				return
			}
			s.sendOpenSecureChannelResponse(conn)

		case "MSG":
			buf := make([]byte, hdr.MessageSize-8)
			if _, err := io.ReadFull(conn, buf); err != nil {
				return
			}
			s.handleServiceMessage(conn, buf)

		case "CLO":
			return
		}
	}
}

func (s *MockOPCUAServer) sendOpenSecureChannelResponse(w io.Writer) {
	var body bytes.Buffer

	_ = binary.Write(&body, binary.LittleEndian, uint32(1))
	_ = writeOPCUAString(&body, "http://opcfoundation.org/UA/SecurityPolicy#None")
	_ = writeByteString(&body, nil)
	_ = writeByteString(&body, nil)

	_ = binary.Write(&body, binary.LittleEndian, uint32(1))
	_ = binary.Write(&body, binary.LittleEndian, uint32(1))

	respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 449}
	_ = respNode.Encode(&body)

	resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
	_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
	_ = binary.Write(&body, binary.LittleEndian, uint32(1))
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	_ = binary.Write(&body, binary.LittleEndian, uint32(0))

	_ = binary.Write(&body, binary.LittleEndian, uint32(1))
	_ = binary.Write(&body, binary.LittleEndian, uint32(100))
	_ = binary.Write(&body, binary.LittleEndian, int64(0))
	_ = binary.Write(&body, binary.LittleEndian, uint32(300000))

	nonce := make([]byte, 32)
	_ = writeByteString(&body, nonce)

	msgSize := uint32(8 + body.Len())
	h := Header{MessageType: "OPN", ChunkType: 'F', MessageSize: msgSize}
	_ = h.Encode(w)
	_, _ = w.Write(body.Bytes())
}

func (s *MockOPCUAServer) handleServiceMessage(w io.Writer, payload []byte) {
	if len(payload) < 16 {
		return
	}
	seq := binary.LittleEndian.Uint32(payload[8:12])
	reqID := binary.LittleEndian.Uint32(payload[12:16])
	bodyReader := bytes.NewReader(payload[16:])

	typeNode, err := DecodeNodeID(bodyReader)
	if err != nil {
		return
	}

	switch typeNode.NumericID {
	case TypeIDCreateSessionRequest:
		s.sendCreateSessionResponse(w, seq, reqID)
	case TypeIDActivateSessionRequest:
		s.sendActivateSessionResponse(w, seq, reqID)
	case TypeIDCreateSubscriptionRequest:
		s.sendCreateSubscriptionResponse(w, seq, reqID)
	case TypeIDCreateMonitoredItemsRequest:
		s.sendCreateMonitoredItemsResponse(w, seq, reqID, bodyReader)
	case TypeIDPublishRequest:
		if s.OnPublishRequest != nil {
			s.OnPublishRequest(w, seq, reqID, 0)
		} else {
			s.sendEmptyKeepAlive(w, seq, reqID)
		}
	case TypeIDBrowseRequest:
		if s.OnBrowseRequest != nil {
			s.OnBrowseRequest(w, seq, reqID, NodeID{Type: NodeIDTypeTwoByte, NumericID: 85})
		} else {
			s.sendDefaultBrowseResponse(w, seq, reqID)
		}
	}
}

func (s *MockOPCUAServer) sendCreateSessionResponse(w io.Writer, seq, reqID uint32) {
	var body bytes.Buffer

	respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDCreateSessionResponse}
	_ = respNode.Encode(&body)

	resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
	_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
	_ = binary.Write(&body, binary.LittleEndian, reqID)
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	sessID := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 1, NumericID: 5001}
	_ = sessID.Encode(&body)

	authToken := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 1, NumericID: 9001}
	_ = authToken.Encode(&body)

	_ = binary.Write(&body, binary.LittleEndian, float64(1200000))

	sNonce := make([]byte, 32)
	_ = writeByteString(&body, sNonce)
	_ = writeByteString(&body, nil)
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	_ = writeOPCUAString(&body, "")
	_ = writeByteString(&body, nil)
	_ = binary.Write(&body, binary.LittleEndian, uint32(16777216))

	s.sendMSG(w, seq, reqID, body.Bytes())
}

func (s *MockOPCUAServer) sendActivateSessionResponse(w io.Writer, seq, reqID uint32) {
	var body bytes.Buffer

	respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDActivateSessionResponse}
	_ = respNode.Encode(&body)

	resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
	_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
	_ = binary.Write(&body, binary.LittleEndian, reqID)
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	sNonce := make([]byte, 32)
	_ = writeByteString(&body, sNonce)
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))

	s.sendMSG(w, seq, reqID, body.Bytes())
}

func (s *MockOPCUAServer) sendCreateSubscriptionResponse(w io.Writer, seq, reqID uint32) {
	var body bytes.Buffer

	respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDCreateSubscriptionResponse}
	_ = respNode.Encode(&body)

	resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
	_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
	_ = binary.Write(&body, binary.LittleEndian, reqID)
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	_ = binary.Write(&body, binary.LittleEndian, uint32(101))
	_ = binary.Write(&body, binary.LittleEndian, float64(100.0))
	_ = binary.Write(&body, binary.LittleEndian, uint32(60))
	_ = binary.Write(&body, binary.LittleEndian, uint32(10))

	s.sendMSG(w, seq, reqID, body.Bytes())
}

func (s *MockOPCUAServer) sendCreateMonitoredItemsResponse(w io.Writer, seq, reqID uint32, r io.Reader) {
	var body bytes.Buffer

	respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDCreateMonitoredItemsResponse}
	_ = respNode.Encode(&body)

	resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
	_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
	_ = binary.Write(&body, binary.LittleEndian, reqID)
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	_ = binary.Write(&body, binary.LittleEndian, int32(1))
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, uint32(1001))
	_ = binary.Write(&body, binary.LittleEndian, float64(50.0))
	_ = binary.Write(&body, binary.LittleEndian, uint32(10))

	nullObj := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullObj.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))

	s.sendMSG(w, seq, reqID, body.Bytes())
}

func (s *MockOPCUAServer) sendEmptyKeepAlive(w io.Writer, seq, reqID uint32) {
	var body bytes.Buffer

	respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDPublishResponse}
	_ = respNode.Encode(&body)

	resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
	_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
	_ = binary.Write(&body, binary.LittleEndian, reqID)
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	_ = binary.Write(&body, binary.LittleEndian, uint32(101))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	_ = binary.Write(&body, binary.LittleEndian, uint32(1))
	_ = encodeDateTime(&body, time.Now().UTC())
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))

	s.sendMSG(w, seq, reqID, body.Bytes())
}

func (s *MockOPCUAServer) sendDefaultBrowseResponse(w io.Writer, seq, reqID uint32) {
	var body bytes.Buffer

	respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDBrowseResponse}
	_ = respNode.Encode(&body)

	resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
	_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
	_ = binary.Write(&body, binary.LittleEndian, reqID)
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = binary.Write(&body, binary.LittleEndian, byte(0))
	_ = binary.Write(&body, binary.LittleEndian, int32(0))
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	_ = nullNode.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(0))

	_ = binary.Write(&body, binary.LittleEndian, int32(1))
	_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
	_ = writeByteString(&body, nil)

	_ = binary.Write(&body, binary.LittleEndian, int32(2))

	ref1Type := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 35}
	baseVar := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 63}

	_ = ref1Type.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(1))
	target1 := NodeID{Type: NodeIDTypeString, NamespaceIndex: 2, StringID: "Turbine.Speed"}
	_ = target1.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, uint16(2))
	_ = writeOPCUAString(&body, "Turbine.Speed")
	_ = binary.Write(&body, binary.LittleEndian, byte(0x02))
	_ = writeOPCUAString(&body, "Turbine.Speed")
	_ = binary.Write(&body, binary.LittleEndian, uint32(2))
	_ = baseVar.Encode(&body)

	_ = ref1Type.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, byte(1))
	target2 := NodeID{Type: NodeIDTypeString, NamespaceIndex: 2, StringID: "Turbine.Temp"}
	_ = target2.Encode(&body)
	_ = binary.Write(&body, binary.LittleEndian, uint16(2))
	_ = writeOPCUAString(&body, "Turbine.Temp")
	_ = binary.Write(&body, binary.LittleEndian, byte(0x02))
	_ = writeOPCUAString(&body, "Turbine.Temp")
	_ = binary.Write(&body, binary.LittleEndian, uint32(2))
	_ = baseVar.Encode(&body)

	_ = binary.Write(&body, binary.LittleEndian, int32(0))

	s.sendMSG(w, seq, reqID, body.Bytes())
}

func (s *MockOPCUAServer) SendMSG(w io.Writer, seq, reqID uint32, payload []byte) {
	s.sendMSG(w, seq, reqID, payload)
}

func (s *MockOPCUAServer) sendMSG(w io.Writer, seq, reqID uint32, payload []byte) {
	var frame bytes.Buffer
	_ = binary.Write(&frame, binary.LittleEndian, uint32(1))
	_ = binary.Write(&frame, binary.LittleEndian, uint32(100))
	_ = binary.Write(&frame, binary.LittleEndian, seq)
	_ = binary.Write(&frame, binary.LittleEndian, reqID)
	_, _ = frame.Write(payload)

	msgSize := uint32(8 + frame.Len())
	h := Header{MessageType: "MSG", ChunkType: 'F', MessageSize: msgSize}
	_ = h.Encode(w)
	_, _ = w.Write(frame.Bytes())
}

