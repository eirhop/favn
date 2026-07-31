#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
env_file=${1:-"$script_dir/.env.local"}

if [ ! -f "$env_file" ]; then
  echo "$env_file does not exist; refusing to prune without a resolved image tag" >&2
  exit 1
fi

current_tag=$(sed -n 's/^FAVN_IMAGE_TAG=//p' "$env_file" | head -n 1)
if ! printf '%s\n' "$current_tag" |
    grep -Eq '^(pr565-[0-9a-f]{12}|diagnostic-[0-9a-f]{12}-[0-9]{14}-[0-9]+)$'; then
  echo "refusing to prune an invalid qualification image tag" >&2
  exit 1
fi

protected_clean_tag=$current_tag
case "$current_tag" in
  diagnostic-*)
    protected_revision=${current_tag#diagnostic-}
    protected_revision=${protected_revision%%-*}
    protected_clean_tag="pr565-$protected_revision"
    ;;
esac

repositories="
favn-control-plane
favn-qualification-certificates
favn-qualification-postgres
favn-elastic-simulation-operator
favn-elastic-simulation-runner
favn-elastic-simulation-scaler
favn-security-probe
"

for repository in $repositories; do
  retained_clean_tags=0
  docker image ls "$repository" --format '{{.Repository}}:{{.Tag}}' |
    while IFS= read -r image; do
      tag=${image#*:}
      if printf '%s\n' "$tag" | grep -Eq '^pr565-[0-9a-f]{12}$'; then
        retained_clean_tags=$((retained_clean_tags + 1))
        if [ "$tag" = "$protected_clean_tag" ] || [ "$retained_clean_tags" -le 3 ]; then
          continue
        fi
      elif printf '%s\n' "$tag" |
          grep -Eq '^diagnostic-[0-9a-f]{12}-[0-9]{14}-[0-9]+$'; then
        [ "$tag" = "$current_tag" ] || continue
      else
        continue
      fi

      if ! docker image rm "$image" >/dev/null; then
        echo "kept in-use qualification image $image" >&2
      fi
    done
done
