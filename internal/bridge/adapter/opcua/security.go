package opcua

import (
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

// Security Policies supported by the adapter.
const (
	SecurityPolicyNone           = "http://opcfoundation.org/UA/SecurityPolicy#None"
	SecurityPolicyBasic256Sha256 = "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256"
)

// SecurityManager encapsulates cryptographic operations for OPC-UA SecureChannels.
type SecurityManager struct {
	PolicyURI   string
	Certificate []byte // Raw DER bytes
	PrivateKey  *rsa.PrivateKey
}

// NewSecurityManager initializes a SecurityManager from certificate and key files.
func NewSecurityManager(policy string, certFile, keyFile string) (*SecurityManager, error) {
	if policy == "" || policy == "None" {
		return &SecurityManager{PolicyURI: SecurityPolicyNone}, nil
	}

	if policy != "Basic256Sha256" {
		return nil, fmt.Errorf("unsupported security policy: %s", policy)
	}

	sm := &SecurityManager{PolicyURI: SecurityPolicyBasic256Sha256}

	if certFile != "" && keyFile != "" {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate pair: %w", err)
		}
		if len(pair.Certificate) > 0 {
			sm.Certificate = pair.Certificate[0]
		}
		if pk, ok := pair.PrivateKey.(*rsa.PrivateKey); ok {
			sm.PrivateKey = pk
		} else {
			return nil, errors.New("private key must be an RSA key for Basic256Sha256")
		}
	}

	return sm, nil
}

// Thumbprint computes the 20-byte SHA-1 thumbprint of the client certificate.
func (sm *SecurityManager) Thumbprint() []byte {
	if len(sm.Certificate) == 0 {
		return nil
	}
	hash := sha1.Sum(sm.Certificate)
	return hash[:]
}

// SignAsymmetric signs data with the client RSA private key using SHA-256 PKCS#1 v1.5.
func (sm *SecurityManager) SignAsymmetric(data []byte) ([]byte, error) {
	if sm.PrivateKey == nil {
		return nil, errors.New("private key not configured")
	}
	hashed := sha256.Sum256(data)
	return rsa.SignPKCS1v15(rand.Reader, sm.PrivateKey, crypto.SHA256, hashed[:])
}

// VerifyAsymmetric verifies a signature against server certificate DER bytes.
func (sm *SecurityManager) VerifyAsymmetric(data, signature, certDER []byte) error {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}
	rsaPub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return errors.New("server public key is not RSA")
	}
	hashed := sha256.Sum256(data)
	return rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, hashed[:], signature)
}

// P_SHA256 implements pseudo-random function (PRF) key expansion (RFC 5246).
func P_SHA256(secret, seed []byte, requiredBytes int) []byte {
	var result []byte
	a := seed

	for len(result) < requiredBytes {
		h := hmac.New(sha256.New, secret)
		h.Write(a)
		a = h.Sum(nil)

		h2 := hmac.New(sha256.New, secret)
		h2.Write(a)
		h2.Write(seed)
		result = append(result, h2.Sum(nil)...)
	}

	return result[:requiredBytes]
}

// DeriveKeys computes symmetric encryption and signing keys from client and server nonces.
func (sm *SecurityManager) DeriveKeys(serverNonce, clientNonce []byte, keyLen int) ([]byte, []byte, error) {
	if len(serverNonce) == 0 || len(clientNonce) == 0 {
		return nil, nil, errors.New("nonces must be non-empty")
	}

	seed := append(clientNonce, serverNonce...)
	keys := P_SHA256(serverNonce, seed, keyLen*2)
	return keys[:keyLen], keys[keyLen:], nil
}

