#!/bin/sh
set -eu

result_file=/results/trusted-proxy-security.json
headers_file=$(mktemp)
body_file=$(mktemp)
trap 'rm -f "$headers_file" "$body_file"' EXIT HUP INT TERM

pass() {
  printf '%s PASS %s\n' "$1" "$2"
}

fail() {
  printf '%s FAIL %s\n' "$1" "$2" >&2
  exit 1
}

proxy_status=$(
  curl \
    --silent \
    --show-error \
    --insecure \
    --retry 30 \
    --retry-all-errors \
    --retry-delay 1 \
    --resolve favn.localhost:443:172.31.58.2 \
    --output "$body_file" \
    --write-out '%{http_code}' \
    https://favn.localhost/api/web/v1/health/ready
)

[ "$proxy_status" = "200" ] ||
  fail TP-001 "HTTPS proxy health returned $proxy_status"
pass TP-001 "real HTTPS proxy reaches View without a redirect loop"

sanitized_status=$(
  curl \
    --silent \
    --show-error \
    --insecure \
    --resolve favn.localhost:443:172.31.58.2 \
    --header 'Forwarded: for=198.51.100.7;host=attacker.example;proto=http' \
    --header 'X-Forwarded-For: 198.51.100.8' \
    --header 'X-Forwarded-Host: attacker.example' \
    --header 'X-Forwarded-Port: 80' \
    --header 'X-Forwarded-Proto: http' \
    --output "$body_file" \
    --write-out '%{http_code}' \
    https://favn.localhost/__proxy_header_probe
)

[ "$sanitized_status" = "204" ] ||
  fail TP-002 "proxy did not sanitize forged headers; status $sanitized_status"

upstream_request=/results/proxy-upstream-request.txt

grep -qi '^X-Forwarded-For: 172\.31\.58\.4[[:space:]]*$' "$upstream_request" ||
  fail TP-002 "upstream did not receive the proxy-observed client address"
grep -qi '^X-Forwarded-Proto: https[[:space:]]*$' "$upstream_request" ||
  fail TP-002 "upstream did not receive the proxy-owned HTTPS scheme"

if grep -Eqi '^(Forwarded|X-Forwarded-Host|X-Forwarded-Port):' "$upstream_request" ||
    grep -Eqi 'attacker\.example|198\.51\.100\.' "$upstream_request"; then
  fail TP-002 "upstream received client-controlled forwarding identity"
fi

pass TP-002 "real proxy removes client forwarding identity and writes its own"

for direct_case in \
  'localhost|/api/web/v1/health/ready' \
  '127.0.0.1|/api/web/v1/health/ready' \
  'attacker.example|/robots.txt' \
  'attacker.example|/live/websocket?vsn=2.0.0' \
  'attacker.example|/api/web/v1/health/ready'
do
  direct_host=${direct_case%%|*}
  direct_path=${direct_case#*|}

  direct_status=$(
    curl \
      --silent \
      --show-error \
      --dump-header "$headers_file" \
      --header "Host: $direct_host" \
      --header 'Connection: upgrade' \
      --header 'Upgrade: websocket' \
      --header 'Forwarded: for=198.51.100.7;host=attacker.example;proto=https' \
      --header 'X-Forwarded-For: 198.51.100.8' \
      --header 'X-Forwarded-Host: attacker.example' \
      --header 'X-Forwarded-Port: 443' \
      --header 'X-Forwarded-Proto: https' \
      --output "$body_file" \
      --write-out '%{http_code}' \
      "http://view:4000$direct_path"
  )

  [ "$direct_status" = "301" ] ||
    fail TP-003 "direct $direct_host$direct_path returned $direct_status instead of 301"
done

pass TP-003 "non-proxy peer cannot make plaintext HTTP appear to be HTTPS"

location=$(
  grep -i '^location:' "$headers_file" |
    head -n 1 |
    cut -d ' ' -f 2- |
    tr -d '\r'
)

[ "$location" = "https://favn.localhost/api/web/v1/health/ready" ] ||
  fail TP-004 "redirect used unexpected authority: $location"
pass TP-004 "redirect authority comes from the configured public origin"

cat >"$result_file" <<'EOF'
{"status":"pass","assertions":["TP-001","TP-002","TP-003","TP-004"]}
EOF

printf 'trusted proxy security evidence: %s\n' "$result_file"
