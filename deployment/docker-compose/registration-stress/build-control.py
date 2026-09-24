#!/usr/bin/env python3
"""Build a source-pinned local control-plane candidate, keeping runner fixtures fixed."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[3]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--revision", default="HEAD")
args = parser.parse_args()
revision = subprocess.check_output(
    ["git", "rev-parse", "--verify", args.revision + "^{commit}"], cwd=ROOT, text=True).strip()
if not re.fullmatch(r"[a-f0-9]{40}", revision):
    raise SystemExit("Expected a full commit identity")
context = ROOT / ".favn/registration-stress" / ("control-" + revision[:12])
if context.exists():
    raise SystemExit(f"Source snapshot already exists; inspect its build evidence: {context}")
context.mkdir(parents=True)
archive = subprocess.check_output(["git", "archive", revision], cwd=ROOT)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    source.extractall(context, filter="data")
dockerfile = context / "rel/control_plane/Dockerfile"
dockerfile.write_text(dockerfile.read_text().replace(
    "ENV MIX_ENV=prod", 'ENV MIX_ENV=prod ERL_FLAGS="+JMsingle true"', 1))
recipe_hash = hashlib.sha256(dockerfile.read_bytes()).hexdigest()
image = "favn-763-control:" + revision[:12] + "-" + recipe_hash[:8]
metadata = dict(line.split("=", 1) for line in subprocess.check_output(
    ["bash", "scripts/release_metadata.sh"], cwd=context, text=True).splitlines())
metadata["FAVN_SOURCE_REVISION"] = revision
metadata["FAVN_BUILD_TIMESTAMP"] = subprocess.check_output(
    ["git", "show", "-s", "--format=%cI", revision], cwd=ROOT, text=True).strip()
command = ["docker", "--context", "orbstack", "buildx", "build", "--builder",
           "favn-qualification-v1", "--platform", "linux/amd64", "--provenance=false",
           "--load", "--tag", image, "--file", str(dockerfile)]
for key, value in metadata.items():
    command.extend(["--build-arg", key + "=" + value])
command.append(str(context))
record = {"source_revision": revision, "dockerfile_sha256": recipe_hash,
          "local_build_adjustment": "ERL_FLAGS=+JMsingle true", "image": image,
          "context": str(context), "command": command}
evidence = context / "local-build.json"
evidence.write_text(json.dumps(record, indent=2) + "\n")
print(json.dumps(record), flush=True)
subprocess.run(command, cwd=ROOT, check=True)
record["image_id"] = subprocess.check_output(
    ["docker", "--context", "orbstack", "image", "inspect", "--format", "{{.Id}}", image],
    text=True).strip()
evidence.write_text(json.dumps(record, indent=2) + "\n")
print(json.dumps(record), flush=True)
