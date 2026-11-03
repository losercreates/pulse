package opcua

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateTestCert(t *testing.T) (*rsa.PrivateKey, []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Pulse OPC-UA Test Client",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	return key, certDER
}

func TestUserIdentityToken_Encoding(t *testing.T) {
	// Anonymous
	anonBytes, err := BuildUserIdentityToken("anonymous", "", "", nil)
	require.NoError(t, err)
	mode, user, err := DecodeUserIdentityToken(bytes.NewReader(anonBytes))
	require.NoError(t, err)
	assert.Equal(t, "anonymous", mode)
	assert.Equal(t, "anonymous", user)

	// Username
	userBytes, err := BuildUserIdentityToken("username", "operator1", "secret123", nil)
	require.NoError(t, err)
	mode, user, err = DecodeUserIdentityToken(bytes.NewReader(userBytes))
	require.NoError(t, err)
	assert.Equal(t, "username", mode)
	assert.Equal(t, "operator1", user)

	// Certificate
	_, certDER := generateTestCert(t)
	certTokenBytes, err := BuildUserIdentityToken("certificate", "", "", certDER)
	require.NoError(t, err)
	mode, policy, err := DecodeUserIdentityToken(bytes.NewReader(certTokenBytes))
	require.NoError(t, err)
	assert.Equal(t, "certificate", mode)
	assert.Equal(t, "certificate", policy)
}

func TestSecurityManager_AsymmetricSignAndVerify(t *testing.T) {
	key, certDER := generateTestCert(t)

	sm := &SecurityManager{
		PolicyURI:   SecurityPolicyBasic256Sha256,
		Certificate: certDER,
		PrivateKey:  key,
	}

	thumbprint := sm.Thumbprint()
	assert.Len(t, thumbprint, 20) // SHA-1 thumbprint

	data := []byte("OPC-UA Telemetry Integrity Verification Payload")
	sig, err := sm.SignAsymmetric(data)
	require.NoError(t, err)
	assert.NotEmpty(t, sig)

	// Verify valid signature
	err = sm.VerifyAsymmetric(data, sig, certDER)
	require.NoError(t, err)

	// Verify tampered data fails
	tampered := []byte("Tampered Telemetry Payload")
	err = sm.VerifyAsymmetric(tampered, sig, certDER)
	require.Error(t, err)
}

func TestSecurityManager_KeyDerivation(t *testing.T) {
	sm := &SecurityManager{PolicyURI: SecurityPolicyBasic256Sha256}

	serverNonce := make([]byte, 32)
	clientNonce := make([]byte, 32)
	_, _ = rand.Read(serverNonce)
	_, _ = rand.Read(clientNonce)

	key1, key2, err := sm.DeriveKeys(serverNonce, clientNonce, 32)
	require.NoError(t, err)
	assert.Len(t, key1, 32)
	assert.Len(t, key2, 32)
	assert.NotEqual(t, key1, key2)

	// Verify deterministic PRF
	key1Repeat, key2Repeat, err := sm.DeriveKeys(serverNonce, clientNonce, 32)
	require.NoError(t, err)
	assert.Equal(t, key1, key1Repeat)
	assert.Equal(t, key2, key2Repeat)
}

