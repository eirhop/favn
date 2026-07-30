#!/bin/sh
set -eu

request_file=/results/proxy-upstream-request.txt
carriage_return=$(printf '\r')
: >"$request_file"

while IFS= read -r line; do
  printf '%s\n' "$line" >>"$request_file"
  [ "$line" = "$carriage_return" ] && break
done

printf 'HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'
