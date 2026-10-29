#!/usr/bin/env bash
# ==============================================================================
# Pulse Platform — Local TLS & mTLS Test Certificate Generator
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CERTS_DIR="$ROOT_DIR/certs"

mkdir -p "$CERTS_DIR"

echo "Generating Pulse Root CA..."
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$CERTS_DIR/ca.key" \
    -out "$CERTS_DIR/ca.crt" \
    -days 365 \
    -subj "/CN=Pulse Test Root CA/O=Pulse/OU=Security"

echo "Generating Ingestion Gateway Server Certificate..."
cat > "$CERTS_DIR/gateway_ext.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost
O = Pulse
OU = Gateway

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = pulse-gateway
IP.1 = 127.0.0.1
EOF

openssl req -new -newkey rsa:2048 -nodes \
    -keyout "$CERTS_DIR/gateway.key" \
    -out "$CERTS_DIR/gateway.csr" \
    -config "$CERTS_DIR/gateway_ext.cnf"

openssl x509 -req \
    -in "$CERTS_DIR/gateway.csr" \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERTS_DIR/gateway.crt" \
    -days 365 \
    -extfile "$CERTS_DIR/gateway_ext.cnf" \
    -extensions v3_req

rm -f "$CERTS_DIR/gateway.csr" "$CERTS_DIR/gateway_ext.cnf"

# Function to generate client certificate
generate_client_cert() {
    local DEVICE_ID="$1"
    echo "Generating client certificate for device: $DEVICE_ID..."

    cat > "$CERTS_DIR/${DEVICE_ID}_ext.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = $DEVICE_ID
O = Pulse
OU = Devices

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

    openssl req -new -newkey rsa:2048 -nodes \
        -keyout "$CERTS_DIR/${DEVICE_ID}.key" \
        -out "$CERTS_DIR/${DEVICE_ID}.csr" \
        -config "$CERTS_DIR/${DEVICE_ID}_ext.cnf"

    openssl x509 -req \
        -in "$CERTS_DIR/${DEVICE_ID}.csr" \
        -CA "$CERTS_DIR/ca.crt" \
        -CAkey "$CERTS_DIR/ca.key" \
        -CAcreateserial \
        -out "$CERTS_DIR/${DEVICE_ID}.crt" \
        -days 365 \
        -extfile "$CERTS_DIR/${DEVICE_ID}_ext.cnf" \
        -extensions v3_req

    rm -f "$CERTS_DIR/${DEVICE_ID}.csr" "$CERTS_DIR/${DEVICE_ID}_ext.cnf"
}

generate_client_cert "turbine-01"
generate_client_cert "turbine-02"

chmod 0600 "$CERTS_DIR"/*.key
chmod 0644 "$CERTS_DIR"/*.crt

echo "Test certificates successfully generated in $CERTS_DIR:"
ls -l "$CERTS_DIR"

