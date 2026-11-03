package opcua

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Header represents the 8-byte OPC-UA TCP message header.
type Header struct {
	MessageType string // "HEL", "ACK", "ERR", "OPN", "CLO", "MSG"
	ChunkType   byte   // 'F', 'C', 'A'
	MessageSize uint32
}

// EncodeHeader writes the 8-byte header.
func (h Header) Encode(w io.Writer) error {
	if len(h.MessageType) != 3 {
		return fmt.Errorf("invalid message type: %s", h.MessageType)
	}
	buf := make([]byte, 8)
	copy(buf[0:3], h.MessageType)
	buf[3] = h.ChunkType
	binary.LittleEndian.PutUint32(buf[4:8], h.MessageSize)
	_, err := w.Write(buf)
	return err
}

// ReadHeader reads the 8-byte header from r.
func ReadHeader(r io.Reader) (Header, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Header{}, err
	}
	return Header{
		MessageType: string(buf[0:3]),
		ChunkType:   buf[3],
		MessageSize: binary.LittleEndian.Uint32(buf[4:8]),
	}, nil
}

// HelloMessage represents the client HEL handshake.
type HelloMessage struct {
	ProtocolVersion   uint32
	ReceiveBufferSize uint32
	SendBufferSize    uint32
	MaxMessageSize    uint32
	MaxChunkCount     uint32
	EndpointURL       string
}

// Encode writes the HEL message including header.
func (m HelloMessage) Encode(w io.Writer) error {
	var body bytes.Buffer
	_ = binary.Write(&body, binary.LittleEndian, m.ProtocolVersion)
	_ = binary.Write(&body, binary.LittleEndian, m.ReceiveBufferSize)
	_ = binary.Write(&body, binary.LittleEndian, m.SendBufferSize)
	_ = binary.Write(&body, binary.LittleEndian, m.MaxMessageSize)
	_ = binary.Write(&body, binary.LittleEndian, m.MaxChunkCount)
	_ = writeOPCUAString(&body, m.EndpointURL)

	h := Header{
		MessageType: "HEL",
		ChunkType:   'F',
		MessageSize: uint32(8 + body.Len()),
	}
	if err := h.Encode(w); err != nil {
		return err
	}
	_, err := w.Write(body.Bytes())
	return err
}

// DecodeHello reads a HEL message body.
func DecodeHello(r io.Reader) (HelloMessage, error) {
	var m HelloMessage
	if err := binary.Read(r, binary.LittleEndian, &m.ProtocolVersion); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.ReceiveBufferSize); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.SendBufferSize); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.MaxMessageSize); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.MaxChunkCount); err != nil {
		return m, err
	}
	url, err := readOPCUAString(r)
	if err != nil {
		return m, err
	}
	m.EndpointURL = url
	return m, nil
}

// AcknowledgeMessage represents the server ACK handshake.
type AcknowledgeMessage struct {
	ProtocolVersion   uint32
	ReceiveBufferSize uint32
	SendBufferSize    uint32
	MaxMessageSize    uint32
	MaxChunkCount     uint32
}

// Encode writes an ACK message including header.
func (m AcknowledgeMessage) Encode(w io.Writer) error {
	var body bytes.Buffer
	_ = binary.Write(&body, binary.LittleEndian, m.ProtocolVersion)
	_ = binary.Write(&body, binary.LittleEndian, m.ReceiveBufferSize)
	_ = binary.Write(&body, binary.LittleEndian, m.SendBufferSize)
	_ = binary.Write(&body, binary.LittleEndian, m.MaxMessageSize)
	_ = binary.Write(&body, binary.LittleEndian, m.MaxChunkCount)

	h := Header{
		MessageType: "ACK",
		ChunkType:   'F',
		MessageSize: uint32(8 + body.Len()),
	}
	if err := h.Encode(w); err != nil {
		return err
	}
	_, err := w.Write(body.Bytes())
	return err
}

// DecodeAcknowledge reads an ACK message body.
func DecodeAcknowledge(r io.Reader) (AcknowledgeMessage, error) {
	var m AcknowledgeMessage
	if err := binary.Read(r, binary.LittleEndian, &m.ProtocolVersion); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.ReceiveBufferSize); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.SendBufferSize); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.MaxMessageSize); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.LittleEndian, &m.MaxChunkCount); err != nil {
		return m, err
	}
	return m, nil
}

// ErrorMessage represents an OPC-UA ERR message.
type ErrorMessage struct {
	ErrorCode StatusCode
	Reason    string
}

// Encode writes an ERR message.
func (m ErrorMessage) Encode(w io.Writer) error {
	var body bytes.Buffer
	_ = binary.Write(&body, binary.LittleEndian, uint32(m.ErrorCode))
	_ = writeOPCUAString(&body, m.Reason)

	h := Header{
		MessageType: "ERR",
		ChunkType:   'F',
		MessageSize: uint32(8 + body.Len()),
	}
	if err := h.Encode(w); err != nil {
		return err
	}
	_, err := w.Write(body.Bytes())
	return err
}

// DecodeErrorMessage reads an ERR message body.
func DecodeErrorMessage(r io.Reader) (ErrorMessage, error) {
	var errCode uint32
	if err := binary.Read(r, binary.LittleEndian, &errCode); err != nil {
		return ErrorMessage{}, err
	}
	reason, _ := readOPCUAString(r)
	return ErrorMessage{
		ErrorCode: StatusCode(errCode),
		Reason:    reason,
	}, nil
}

// SequenceHeader is included in secure channel messages.
type SequenceHeader struct {
	SequenceNumber uint32
	RequestID      uint32
}

// RequestHeader represents the standard OPC-UA request header passed inside service payloads.
type RequestHeader struct {
	AuthenticationToken NodeID
	Timestamp           int64
	RequestHandle       uint32
	ReturnDiagnostics   uint32
	AuditEntryID        string
	TimeoutHint         uint32
}

// Encode writes RequestHeader to w.
func (h RequestHeader) Encode(w io.Writer) error {
	if err := h.AuthenticationToken.Encode(w); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Timestamp); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.RequestHandle); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.ReturnDiagnostics); err != nil {
		return err
	}
	if err := writeOPCUAString(w, h.AuditEntryID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.TimeoutHint); err != nil {
		return err
	}
	// AdditionalHeader (Null NodeID + byte 0)
	nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
	if err := nullNode.Encode(w); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, byte(0))
}

// ResponseHeader represents standard OPC-UA response header.
type ResponseHeader struct {
	Timestamp          int64
	RequestHandle      uint32
	ServiceResult      StatusCode
	ServiceDiagnostics byte
}

// DecodeResponseHeader reads ResponseHeader from r.
func DecodeResponseHeader(r io.Reader) (ResponseHeader, error) {
	var h ResponseHeader
	if err := binary.Read(r, binary.LittleEndian, &h.Timestamp); err != nil {
		return h, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.RequestHandle); err != nil {
		return h, err
	}
	var res uint32
	if err := binary.Read(r, binary.LittleEndian, &res); err != nil {
		return h, err
	}
	h.ServiceResult = StatusCode(res)
	if err := binary.Read(r, binary.LittleEndian, &h.ServiceDiagnostics); err != nil {
		return h, err
	}

	// StringTable length
	var strTableLen int32
	if err := binary.Read(r, binary.LittleEndian, &strTableLen); err == nil && strTableLen > 0 {
		for i := int32(0); i < strTableLen; i++ {
			_, _ = readOPCUAString(r)
		}
	}

	// AdditionalHeader (NodeId + byte)
	_, _ = DecodeNodeID(r)
	var enc byte
	_ = binary.Read(r, binary.LittleEndian, &enc)

	return h, nil
}

