package opcua

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
)

// SessionState represents the state of the OPC-UA connection session.
type SessionState int32

const (
	StateDisconnected SessionState = 0
	StateConnecting   SessionState = 1
	StateConnected    SessionState = 2
	StateSubscribed   SessionState = 3
)

func (s SessionState) String() string {
	switch s {
	case StateDisconnected:
		return "Disconnected"
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateSubscribed:
		return "Subscribed"
	default:
		return "Unknown"
	}
}

// Channel manages the low-level TCP connection, SecureChannel, and Session state machine.
type Channel struct {
	cfg       config.OPCUAAdapterConfig
	conn      net.Conn
	mu        sync.Mutex
	state     atomic.Int32
	seqNumber atomic.Uint32
	requestID atomic.Uint32

	channelID uint32
	tokenID   uint32

	sessionID   NodeID
	authToken   NodeID
	serverNonce []byte

	lastKeepAlive atomic.Int64 // Unix nanoseconds
}

// NewChannel constructs a new Channel instance.
func NewChannel(cfg config.OPCUAAdapterConfig) *Channel {
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 3 * time.Second
	}
	ch := &Channel{
		cfg: cfg,
	}
	ch.state.Store(int32(StateDisconnected))
	ch.seqNumber.Store(1)
	ch.requestID.Store(1)
	return ch
}

// State returns current SessionState.
func (c *Channel) State() SessionState {
	return SessionState(c.state.Load())
}

// LastKeepAliveTime returns the timestamp of last received keep-alive or message.
func (c *Channel) LastKeepAliveTime() time.Time {
	ns := c.lastKeepAlive.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// IsKeepAliveExpired reports whether more than maxDuration has elapsed since last heartbeat.
func (c *Channel) IsKeepAliveExpired(maxDuration time.Duration) bool {
	last := c.LastKeepAliveTime()
	if last.IsZero() {
		return false
	}
	return time.Since(last) > maxDuration
}

func (c *Channel) recordKeepAlive() {
	c.lastKeepAlive.Store(time.Now().UnixNano())
}

// Connect dials the endpoint, performs HEL/ACK, OpenSecureChannel, CreateSession, and ActivateSession.
func (c *Channel) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state.Load() == int32(StateConnected) || c.state.Load() == int32(StateSubscribed) {
		return nil
	}
	c.state.Store(int32(StateConnecting))

	endpointAddr := strings.TrimPrefix(c.cfg.Endpoint, "opc.tcp://")
	d := net.Dialer{Timeout: c.cfg.ConnectTimeout}
	conn, err := d.DialContext(ctx, "tcp", endpointAddr)
	if err != nil {
		c.state.Store(int32(StateDisconnected))
		return fmt.Errorf("failed to dial OPC-UA endpoint %s: %w", c.cfg.Endpoint, err)
	}
	c.conn = conn

	// 1. Hello / Acknowledge Handshake
	if err := c.sendHello(); err != nil {
		_ = c.conn.Close()
		c.state.Store(int32(StateDisconnected))
		return fmt.Errorf("hello handshake failed: %w", err)
	}

	// 2. Open Secure Channel
	if err := c.openSecureChannel(); err != nil {
		_ = c.conn.Close()
		c.state.Store(int32(StateDisconnected))
		return fmt.Errorf("open secure channel failed: %w", err)
	}

	// 3. Create Session
	if err := c.createSession(); err != nil {
		_ = c.conn.Close()
		c.state.Store(int32(StateDisconnected))
		return fmt.Errorf("create session failed: %w", err)
	}

	// 4. Activate Session
	if err := c.activateSession(); err != nil {
		_ = c.conn.Close()
		c.state.Store(int32(StateDisconnected))
		return fmt.Errorf("activate session failed: %w", err)
	}

	c.state.Store(int32(StateConnected))
	c.recordKeepAlive()
	return nil
}

func (c *Channel) sendHello() error {
	hello := HelloMessage{
		ProtocolVersion:   0,
		ReceiveBufferSize: 65536,
		SendBufferSize:    65536,
		MaxMessageSize:    16777216,
		MaxChunkCount:     5000,
		EndpointURL:       c.cfg.Endpoint,
	}
	if err := hello.Encode(c.conn); err != nil {
		return err
	}

	h, err := ReadHeader(c.conn)
	if err != nil {
		return err
	}
	if h.MessageType != "ACK" {
		if h.MessageType == "ERR" {
			em, _ := DecodeErrorMessage(c.conn)
			return fmt.Errorf("server returned error: %s (%s)", em.ErrorCode, em.Reason)
		}
		return fmt.Errorf("unexpected handshake message type: %s", h.MessageType)
	}

	_, err = DecodeAcknowledge(c.conn)
	return err
}

func (c *Channel) openSecureChannel() error {
	var body bytes.Buffer

	// SecurityPolicyURI
	secPolicy := "http://opcfoundation.org/UA/SecurityPolicy#None"
	if c.cfg.SecurityPolicy == "Basic256Sha256" {
		secPolicy = "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256"
	}
	_ = writeOPCUAString(&body, secPolicy)
	_ = writeByteString(&body, nil) // SenderCertificate
	_ = writeByteString(&body, nil) // ReceiverCertificateThumbprint

	// SequenceHeader
	seq := c.seqNumber.Add(1)
	reqID := c.requestID.Add(1)
	_ = binary.Write(&body, binary.LittleEndian, seq)
	_ = binary.Write(&body, binary.LittleEndian, reqID)

	// TypeId: OpenSecureChannelRequest (446)
	openReqNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 446}
	_ = openReqNode.Encode(&body)

	// RequestHeader
	reqHeader := RequestHeader{
		AuthenticationToken: NodeID{Type: NodeIDTypeTwoByte, NumericID: 0},
		Timestamp:           time.Now().UnixNano(),
		RequestHandle:       1,
		TimeoutHint:         10000,
	}
	_ = reqHeader.Encode(&body)

	// ClientProtocolVersion, RequestType (0=Issue), SecurityMode (1=None)
	_ = binary.Write(&body, binary.LittleEndian, uint32(0))
	_ = binary.Write(&body, binary.LittleEndian, uint32(0))
	secMode := uint32(1) // None
	if c.cfg.SecurityMode == "Sign" {
		secMode = 2
	} else if c.cfg.SecurityMode == "SignAndEncrypt" {
		secMode = 3
	}
	_ = binary.Write(&body, binary.LittleEndian, secMode)

	// ClientNonce (32 random bytes)
	nonce := make([]byte, 32)
	_, _ = rand.Read(nonce)
	_ = writeByteString(&body, nonce)

	// RequestedLifetime: 300,000 ms
	_ = binary.Write(&body, binary.LittleEndian, uint32(300000))

	// Write OPN Header + ChannelId (0) + Body
	msgSize := uint32(8 + 4 + body.Len())
	h := Header{MessageType: "OPN", ChunkType: 'F', MessageSize: msgSize}
	if err := h.Encode(c.conn); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.LittleEndian, uint32(0)); err != nil {
		return err
	}
	if _, err := c.conn.Write(body.Bytes()); err != nil {
		return err
	}

	// Read Response
	respHeader, err := ReadHeader(c.conn)
	if err != nil {
		return err
	}
	if respHeader.MessageType != "OPN" {
		return fmt.Errorf("expected OPN response, got %s", respHeader.MessageType)
	}

	payload := make([]byte, respHeader.MessageSize-8)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return err
	}
	pr := bytes.NewReader(payload)

	var channelID uint32
	if err := binary.Read(pr, binary.LittleEndian, &channelID); err != nil {
		return err
	}
	c.channelID = channelID

	// Skip SecurityPolicyURI, SenderCertificate, ReceiverCertificateThumbprint
	_, _ = readOPCUAString(pr)
	_, _ = readByteString(pr)
	_, _ = readByteString(pr)

	// SequenceHeader
	var respSeq, respReq uint32
	_ = binary.Read(pr, binary.LittleEndian, &respSeq)
	_ = binary.Read(pr, binary.LittleEndian, &respReq)

	// Response TypeID (449 = OpenSecureChannelResponse)
	_, _ = DecodeNodeID(pr)
	_, _ = DecodeResponseHeader(pr)

	// ServerProtocolVersion
	var protoVer uint32
	_ = binary.Read(pr, binary.LittleEndian, &protoVer)

	// SecurityToken (ChannelId, TokenId, CreatedAt, RevisedLifetime)
	var tokChannelID, tokenID uint32
	_ = binary.Read(pr, binary.LittleEndian, &tokChannelID)
	_ = binary.Read(pr, binary.LittleEndian, &tokenID)
	c.tokenID = tokenID

	return nil
}

func (c *Channel) createSession() error {
	var body bytes.Buffer

	// TypeId: CreateSessionRequest (461)
	reqNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDCreateSessionRequest}
	_ = reqNode.Encode(&body)

	reqHeader := RequestHeader{
		AuthenticationToken: NodeID{Type: NodeIDTypeTwoByte, NumericID: 0},
		Timestamp:           time.Now().UnixNano(),
		RequestHandle:       2,
		TimeoutHint:         10000,
	}
	_ = reqHeader.Encode(&body)

	// ClientDescription (ApplicationURI, ProductURI, ApplicationName, ApplicationType=1, GatewayServerURI, DiscoveryProfileURI, DiscoveryURLs)
	_ = writeOPCUAString(&body, "urn:pulse:bridge")
	_ = writeOPCUAString(&body, "urn:pulse:bridge:client")
	// LocalizedText (EncodingMask byte, Locale string, Text string)
	_ = binary.Write(&body, binary.LittleEndian, byte(0x02)) // Text only
	_ = writeOPCUAString(&body, "Pulse Bridge Client")
	_ = binary.Write(&body, binary.LittleEndian, uint32(1)) // ApplicationType Client
	_ = writeOPCUAString(&body, "")
	_ = writeOPCUAString(&body, "")
	_ = binary.Write(&body, binary.LittleEndian, int32(0)) // 0 Discovery URLs

	_ = writeOPCUAString(&body, "")               // ServerURI
	_ = writeOPCUAString(&body, c.cfg.Endpoint)    // EndpointURL
	_ = writeOPCUAString(&body, "PulseSession-01") // SessionName

	// ClientNonce (32 random bytes)
	nonce := make([]byte, 32)
	_, _ = rand.Read(nonce)
	_ = writeByteString(&body, nonce)
	_ = writeByteString(&body, nil) // ClientCertificate

	_ = binary.Write(&body, binary.LittleEndian, float64(1200000)) // RequestedSessionTimeout 1200s
	_ = binary.Write(&body, binary.LittleEndian, uint32(16777216)) // MaxResponseMessageSize

	if err := c.writeMessage(body.Bytes()); err != nil {
		return err
	}

	respBytes, err := c.readMessage()
	if err != nil {
		return err
	}

	pr := bytes.NewReader(respBytes)
	_, _ = DecodeNodeID(pr) // TypeIDCreateSessionResponse
	resHdr, err := DecodeResponseHeader(pr)
	if err != nil {
		return err
	}
	if !resHdr.ServiceResult.IsGood() {
		return fmt.Errorf("create session failed with service result: %s", resHdr.ServiceResult)
	}

	sessionID, err := DecodeNodeID(pr)
	if err != nil {
		return err
	}
	c.sessionID = sessionID

	authToken, err := DecodeNodeID(pr)
	if err != nil {
		return err
	}
	c.authToken = authToken

	// RevisedSessionTimeout
	var revisedTimeout float64
	_ = binary.Read(pr, binary.LittleEndian, &revisedTimeout)

	// ServerNonce
	sNonce, _ := readByteString(pr)
	c.serverNonce = sNonce

	return nil
}

func (c *Channel) activateSession() error {
	var body bytes.Buffer

	// TypeId: ActivateSessionRequest (467)
	reqNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDActivateSessionRequest}
	_ = reqNode.Encode(&body)

	reqHeader := RequestHeader{
		AuthenticationToken: c.authToken,
		Timestamp:           time.Now().UnixNano(),
		RequestHandle:       3,
		TimeoutHint:         10000,
	}
	_ = reqHeader.Encode(&body)

	// ClientSignature (Algorithm, Signature)
	_ = writeOPCUAString(&body, "")
	_ = writeByteString(&body, nil)

	// ClientSoftwareCertificates (array length 0)
	_ = binary.Write(&body, binary.LittleEndian, int32(0))

	// LocaleIDs (array length 0)
	_ = binary.Write(&body, binary.LittleEndian, int32(0))

	// UserIdentityToken
	if c.cfg.AuthMode == "username" && c.cfg.Username != "" {
		// UserNameIdentityToken TypeID = 324 (FourByte ns=0;i=324)
		tokenTypeNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 324}
		_ = tokenTypeNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01)) // byte encoding
		var tokenBody bytes.Buffer
		_ = writeOPCUAString(&tokenBody, "username") // PolicyID
		_ = writeOPCUAString(&tokenBody, c.cfg.Username)
		_ = writeByteString(&tokenBody, []byte(c.cfg.Password))
		_ = writeOPCUAString(&tokenBody, "") // EncryptionAlgorithm
		_ = writeByteString(&body, tokenBody.Bytes())
	} else {
		// AnonymousIdentityToken TypeID = 321 (FourByte ns=0;i=321)
		tokenTypeNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 321}
		_ = tokenTypeNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01)) // byte encoding
		var tokenBody bytes.Buffer
		_ = writeOPCUAString(&tokenBody, "anonymous") // PolicyID
		_ = writeByteString(&body, tokenBody.Bytes())
	}

	// UserTokenSignature
	_ = writeOPCUAString(&body, "")
	_ = writeByteString(&body, nil)

	if err := c.writeMessage(body.Bytes()); err != nil {
		return err
	}

	respBytes, err := c.readMessage()
	if err != nil {
		return err
	}

	pr := bytes.NewReader(respBytes)
	_, _ = DecodeNodeID(pr) // TypeIDActivateSessionResponse
	resHdr, err := DecodeResponseHeader(pr)
	if err != nil {
		return err
	}
	if !resHdr.ServiceResult.IsGood() {
		return fmt.Errorf("activate session failed with service result: %s", resHdr.ServiceResult)
	}

	return nil
}

// writeMessage writes a standard MSG frame containing SecureChannelId, TokenId, SequenceHeader, and payload.
func (c *Channel) writeMessage(payload []byte) error {
	var frame bytes.Buffer

	// SecureChannelId (uint32), TokenId (uint32)
	_ = binary.Write(&frame, binary.LittleEndian, c.channelID)
	_ = binary.Write(&frame, binary.LittleEndian, c.tokenID)

	// SequenceHeader
	seq := c.seqNumber.Add(1)
	reqID := c.requestID.Add(1)
	_ = binary.Write(&frame, binary.LittleEndian, seq)
	_ = binary.Write(&frame, binary.LittleEndian, reqID)

	_, _ = frame.Write(payload)

	msgSize := uint32(8 + frame.Len())
	h := Header{MessageType: "MSG", ChunkType: 'F', MessageSize: msgSize}

	if err := h.Encode(c.conn); err != nil {
		return err
	}
	_, err := c.conn.Write(frame.Bytes())
	return err
}

// readMessage reads a single MSG frame and returns the inner service payload.
func (c *Channel) readMessage() ([]byte, error) {
	if c.conn != nil && c.cfg.KeepAliveInterval > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.KeepAliveInterval * 3))
	}
	h, err := ReadHeader(c.conn)
	if err != nil {
		return nil, err
	}
	if h.MessageType != "MSG" {
		if h.MessageType == "ERR" {
			em, _ := DecodeErrorMessage(c.conn)
			return nil, fmt.Errorf("server error: %s (%s)", em.ErrorCode, em.Reason)
		}
		return nil, fmt.Errorf("expected MSG frame, got %s", h.MessageType)
	}

	buf := make([]byte, h.MessageSize-8)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, err
	}

	c.recordKeepAlive()

	// Strip SecureChannelId (4), TokenId (4), SequenceNumber (4), RequestId (4) -> 16 bytes
	if len(buf) < 16 {
		return nil, fmt.Errorf("message frame too short: %d bytes", len(buf))
	}
	return buf[16:], nil
}

// Close closes the TCP connection and transitions state to StateDisconnected.
func (c *Channel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.state.Store(int32(StateDisconnected))
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}
