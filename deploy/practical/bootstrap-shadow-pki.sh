#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: bootstrap-shadow-pki.sh [output-dir] [predictor-dns] [predictor-ip]

Creates a private CA, predictor server certificate, executor client certificate,
and operations-console client certificate for Shadow/Testnet deployment only.
Existing files are never overwritten.
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then usage; exit 0; fi
command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 2; }

output=${1:-"$(cd "$(dirname "$0")" && pwd)/pki"}
predictor_dns=${2:-predictor-paper.internal}
predictor_ip=${3:-10.70.0.1}

if [[ -e "$output" ]]; then
  echo "Refusing to overwrite existing PKI directory: $output" >&2
  exit 3
fi

umask 077
mkdir -p "$output"/{ca,predictor,executor,ops}

openssl genrsa -out "$output/ca/ca.key" 4096
openssl req -x509 -new -nodes -key "$output/ca/ca.key" -sha256 -days 825 \
  -subj "/CN=ai-bybit-shadow-ca" -out "$output/ca/ca.crt"

cat > "$output/server.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:${predictor_dns},IP:${predictor_ip}
EOF
openssl genrsa -out "$output/predictor/control-plane.key" 3072
openssl req -new -key "$output/predictor/control-plane.key" \
  -subj "/CN=${predictor_dns}" -out "$output/predictor/control-plane.csr"
openssl x509 -req -in "$output/predictor/control-plane.csr" \
  -CA "$output/ca/ca.crt" -CAkey "$output/ca/ca.key" -CAcreateserial \
  -out "$output/predictor/control-plane.crt" -days 397 -sha256 -extfile "$output/server.ext"

issue_client() {
  local cn=$1 destination=$2
  cat > "$output/client.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
  openssl genrsa -out "$destination/${cn}.key" 3072
  openssl req -new -key "$destination/${cn}.key" -subj "/CN=${cn}" \
    -out "$destination/${cn}.csr"
  openssl x509 -req -in "$destination/${cn}.csr" \
    -CA "$output/ca/ca.crt" -CAkey "$output/ca/ca.key" -CAcreateserial \
    -out "$destination/${cn}.crt" -days 397 -sha256 -extfile "$output/client.ext"
}

issue_client executor-paper-01 "$output/executor"
issue_client ops-console "$output/ops"

cp "$output/ca/ca.crt" "$output/predictor/executor-ca.crt"
cp "$output/ca/ca.crt" "$output/predictor/control-plane-ca.crt"
cp "$output/ca/ca.crt" "$output/executor/control-plane-ca.crt"
cp "$output/ca/ca.crt" "$output/ops/control-plane-ca.crt"
cp "$output/ops/ops-console.crt" "$output/predictor/ops-console.crt"
cp "$output/ops/ops-console.key" "$output/predictor/ops-console.key"

rm -f "$output"/*.ext "$output"/*/*.csr "$output/ca/ca.srl"
chmod 600 "$output"/*/*.key
chmod 644 "$output"/*/*.crt

cat <<EOF
PKI created at: $output

Predictor compose mount: $output/predictor
Executor compose mount:  $output/executor

Keep $output/ca/ca.key offline. Copy only the executor directory to the executor host.
These certificates are for Shadow/Testnet practical validation, not automatic mainnet authorization.
EOF
