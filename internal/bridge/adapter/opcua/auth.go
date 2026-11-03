package opcua

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// IdentityToken TypeIDs
const (
	TypeIDAnonymousIdentityToken uint32 = 321
	TypeIDUserNameIdentityToken  uint32 = 324
	TypeIDX509IdentityToken      uint32 = 327
)

// BuildUserIdentityToken encodes an OPC-UA UserIdentityToken.
func BuildUserIdentityToken(mode string, username, password string, certBytes []byte) ([]byte, error) {
	var body bytes.Buffer

	switch mode {
	case "username":
		// UserNameIdentityToken
		typeNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDUserNameIdentityToken}
		if err := typeNode.Encode(&body); err != nil {
			return nil, err
		}
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01)) // byte encoding

		var tokenBody bytes.Buffer
		_ = writeOPCUAString(&tokenBody, "username")
		_ = writeOPCUAString(&tokenBody, username)
		_ = writeByteString(&tokenBody, []byte(password))
		_ = writeOPCUAString(&tokenBody, "") // EncryptionAlgorithm

		_ = writeByteString(&body, tokenBody.Bytes())
		return body.Bytes(), nil

	case "certificate":
		// X509IdentityToken
		typeNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDX509IdentityToken}
		if err := typeNode.Encode(&body); err != nil {
			return nil, err
		}
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01)) // byte encoding

		var tokenBody bytes.Buffer
		_ = writeOPCUAString(&tokenBody, "certificate")
		_ = writeByteString(&tokenBody, certBytes)

		_ = writeByteString(&body, tokenBody.Bytes())
		return body.Bytes(), nil

	default: // "anonymous"
		// AnonymousIdentityToken
		typeNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDAnonymousIdentityToken}
		if err := typeNode.Encode(&body); err != nil {
			return nil, err
		}
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01)) // byte encoding

		var tokenBody bytes.Buffer
		_ = writeOPCUAString(&tokenBody, "anonymous")

		_ = writeByteString(&body, tokenBody.Bytes())
		return body.Bytes(), nil
	}
}

// DecodeUserIdentityToken inspects a UserIdentityToken from a stream.
func DecodeUserIdentityToken(r io.Reader) (string, string, error) {
	typeNode, err := DecodeNodeID(r)
	if err != nil {
		return "", "", err
	}
	var enc byte
	_ = binary.Read(r, binary.LittleEndian, &enc)

	rawToken, err := readByteString(r)
	if err != nil {
		return "", "", err
	}

	tr := bytes.NewReader(rawToken)
	policyID, _ := readOPCUAString(tr)

	switch typeNode.NumericID {
	case TypeIDAnonymousIdentityToken:
		return "anonymous", policyID, nil
	case TypeIDUserNameIdentityToken:
		user, _ := readOPCUAString(tr)
		return "username", user, nil
	case TypeIDX509IdentityToken:
		return "certificate", policyID, nil
	default:
		return "unknown", policyID, fmt.Errorf("unknown token type: %d", typeNode.NumericID)
	}
}

