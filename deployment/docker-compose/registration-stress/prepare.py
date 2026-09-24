#!/usr/bin/env python3
"""Stage a source-pinned CRM tutorial with a disposable shared DuckLake profile."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--revision", default="HEAD")
args = parser.parse_args()
revision = subprocess.check_output(["git", "rev-parse", "--verify", args.revision + "^{commit}"], cwd=ROOT, text=True).strip()
state = ROOT / ".favn" / "registration-stress"
harness_hash = hashlib.sha256(b"".join(p.read_bytes() for p in (HERE / name for name in ["prepare.py", "config.exs", "runtime.exs", "operator.sh"]))).hexdigest()
context = state / ("source-" + revision[:12] + "-" + harness_hash[:8])
if context.exists():
    raise SystemExit(f"Refusing to overwrite existing source snapshot: {context}")
context.mkdir(parents=True)
archive = subprocess.check_output(["git", "archive", revision], cwd=ROOT)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    source.extractall(context, filter="data")

project = context / "examples/basic-workflow-tutorial"
deploy = project / "deploy/favn"
deploy.mkdir(parents=True)
templates = context / "apps/favn/priv/templates/deployment"
for name in ["mix.exs", "env.sh.eex"]:
    shutil.copyfile(templates / name, deploy / name)
with (project / "config/config.exs").open("w") as f:
    f.write('import Config\nimport_config "registration_stress.exs"\n')
shutil.copyfile(HERE / "config.exs", project / "config/registration_stress.exs")
shutil.copyfile(HERE / "runtime.exs", project / "config/runtime.exs")
sql = project / "priv/duckdb"
(sql / "stress_startup.sql").write_text("SET threads=1; SET memory_limit='384MB'; LOAD postgres; LOAD ducklake; LOAD json;\n")
for catalog in ["source", "core", "mart"]:
    (sql / f"{catalog}_catalog.sql").write_text(
        f"ATTACH @metadata AS {catalog} (METADATA_SCHEMA 'stress_{catalog}', DATA_PATH '/var/lib/favn/stress/{catalog}');\n")

assets = []
modules = []
for number in range(1, 36):
    name = f"CrmDemo.RegistrationStress.Target{number:02d}"
    assets.append('{' + name + ', :asset}')
    modules.append(f'''defmodule {name} do
  @moduledoc "Local initial-registration stress target {number}."
  use Favn.SQLAsset
  relation(connection: :warehouse, catalog: "source", schema: "stress", name: "target_{number:02d}")
  materialized(:table)
  execution_pool(:duckdb)
  contract do
    column(:id, :integer, null: false)
  end
  query do
    ~SQL"SELECT i::BIGINT AS id FROM range(1000) AS t(i)"
  end
end
''')
modules.append('defmodule CrmDemo.RegistrationStress.Pipeline do\n  @moduledoc "Local 35-target registration reproduction."\n  use Favn.Pipeline\n  pipeline :registration_stress do\n    assets([' + ', '.join(assets) + '])\n    max_concurrency(5)\n    execution_pool(:duckdb)\n  end\nend\n')
(project / "lib/registration_stress.ex").write_text("\n".join(modules))

dockerfile = (templates / "runner.Dockerfile").read_text()
dockerfile = dockerfile.replace("ENV MIX_ENV=prod", "ENV MIX_ENV=prod ERL_FLAGS=\"+JMsingle true\"")
dockerfile += '''
FROM builder AS local-operator
COPY . /build
WORKDIR /build/examples/basic-workflow-tutorial
ARG FAVN_RUNNER_RELEASE_ID
ENV DUCKDB_ADBC_DRIVER=/opt/duckdb/1.5.5/libduckdb.so
RUN mix deps.get --only prod --check-locked && mix deps.compile && mix compile --warnings-as-errors && mix favn.build.manifest --runner-release "default=$FAVN_RUNNER_RELEASE_ID"
COPY LocalOperator.sh /usr/local/bin/favn-simulation-operator
RUN chmod 0555 /usr/local/bin/favn-simulation-operator
ENTRYPOINT ["/usr/local/bin/favn-simulation-operator"]
CMD ["help"]
'''
(context / "RunnerDockerfile").write_text(dockerfile)
shutil.copyfile(HERE / "operator.sh", context / "LocalOperator.sh")
profile_hash = hashlib.sha256(harness_hash.encode() + "\n".join(modules).encode()).hexdigest()
tag = revision[:12] + "-" + profile_hash[:8]
release = "rr_" + hashlib.sha256((revision + profile_hash).encode()).hexdigest()
metadata = {"source_revision": revision, "profile_sha256": profile_hash, "runner_release_id": release, "image_tag": tag, "context": str(context)}
state.mkdir(exist_ok=True)
(state / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")
(state / "build.env").write_text(
    f"FAVN_SOURCE_REVISION={revision}\nFAVN_STRESS_CONTEXT={context}\nFAVN_STRESS_TAG={tag}\nFAVN_RUNNER_RELEASE_ID={release}\n"
    "FAVN_STRESS_CONTROL_IMAGE=ghcr.io/eirhop/favn-control-plane@sha256:641d01af54cc11264b5a16e459d460ba48f2ac9b80709b81e935a4e7399a2beb\n")
print(json.dumps(metadata, indent=2))
