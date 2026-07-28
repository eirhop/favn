#!/bin/sh
set -eu

root=/certificates
postgres_output="$root/postgres"
control_plane_output="$root/control-plane"
runner_output="$root/runner"
umask 077

mkdir -p "$postgres_output" "$control_plane_output" "$runner_output"

required_files="
$postgres_output/ca.crt
$postgres_output/postgres.crt
$postgres_output/postgres.key
$control_plane_output/ca.crt
$control_plane_output/control-plane.crt
$control_plane_output/control-plane.key
$control_plane_output/control-plane-ssl-dist.config
$runner_output/ca.crt
$runner_output/runner.crt
$runner_output/runner.key
$runner_output/runner-ssl-dist.config
"

initialized=false
for file in $required_files; do
  if [ -e "$file" ]; then
    initialized=true
    break
  fi
done

if [ "$initialized" = "true" ]; then
  for file in $required_files; do
    if [ ! -f "$file" ]; then
      echo "certificate volumes are incomplete: missing $file" >&2
      exit 1
    fi
  done

  echo "certificate volumes already initialized"
  exit 0
fi

authority=$(mktemp -d)
trap 'rm -rf "$authority"' EXIT HUP INT TERM

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$authority/ca.key"
openssl req -x509 -new -sha256 -days 7 \
  -key "$authority/ca.key" \
  -subj "/CN=Favn local simulation CA" \
  -out "$authority/ca.crt"

issue_certificate() {
  output=$1
  name=$2
  common_name=$3
  san=$4
  usage=$5

  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$output/$name.key"
  openssl req -new -sha256 \
    -key "$output/$name.key" \
    -subj "/CN=$common_name" \
    -out "$authority/$name.csr"

  cat >"$authority/$name.ext" <<EOF
subjectAltName=$san
extendedKeyUsage=$usage
keyUsage=digitalSignature,keyEncipherment
EOF

  openssl x509 -req -sha256 -days 7 \
    -in "$authority/$name.csr" \
    -CA "$authority/ca.crt" \
    -CAkey "$authority/ca.key" \
    -CAcreateserial \
    -extfile "$authority/$name.ext" \
    -out "$output/$name.crt"

  install -m 0444 "$authority/ca.crt" "$output/ca.crt"
}

issue_certificate "$postgres_output" postgres postgres DNS:postgres serverAuth
issue_certificate \
  "$control_plane_output" \
  control-plane \
  control-plane.favn.local \
  DNS:control-plane,DNS:control-plane.favn.local \
  serverAuth,clientAuth
issue_certificate \
  "$runner_output" \
  runner \
  runner.favn.local \
  DNS:runner,DNS:runner.favn.local,DNS:*.favn.local \
  serverAuth,clientAuth

cat >"$control_plane_output/control-plane-ssl-dist.config" <<'EOF'
[
  {server, [
    {certfile, "/etc/favn/tls/control-plane.crt"},
    {keyfile, "/etc/favn/tls/control-plane.key"},
    {cacertfile, "/etc/favn/tls/ca.crt"},
    {verify, verify_peer},
    {fail_if_no_peer_cert, true}
  ]},
  {client, [
    {certfile, "/etc/favn/tls/control-plane.crt"},
    {keyfile, "/etc/favn/tls/control-plane.key"},
    {cacertfile, "/etc/favn/tls/ca.crt"},
    {verify, verify_peer}
  ]}
].
EOF

cat >"$runner_output/runner-ssl-dist.config" <<'EOF'
[
  {server, [
    {certfile, "/etc/favn/tls/runner.crt"},
    {keyfile, "/etc/favn/tls/runner.key"},
    {cacertfile, "/etc/favn/tls/ca.crt"},
    {verify, verify_peer},
    {fail_if_no_peer_cert, true}
  ]},
  {client, [
    {certfile, "/etc/favn/tls/runner.crt"},
    {keyfile, "/etc/favn/tls/runner.key"},
    {cacertfile, "/etc/favn/tls/ca.crt"},
    {verify, verify_peer}
  ]}
].
EOF

chmod 0600 "$postgres_output/postgres.key"
chown 10001:10001 \
  "$control_plane_output/control-plane.key" \
  "$runner_output/runner.key"
chmod 0400 \
  "$control_plane_output/control-plane.key" \
  "$runner_output/runner.key"
chmod 0444 \
  "$postgres_output/postgres.crt" \
  "$control_plane_output/control-plane.crt" \
  "$runner_output/runner.crt" \
  "$control_plane_output/control-plane-ssl-dist.config" \
  "$runner_output/runner-ssl-dist.config"

trap - EXIT HUP INT TERM
rm -rf "$authority"

echo "generated isolated seven-day certificates for the local simulation"
