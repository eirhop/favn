defmodule Favn.Dev.Paths do
  @moduledoc false

  @spec root_dir(keyword()) :: Path.t()
  def root_dir(opts) when is_list(opts) do
    Keyword.get_lazy(opts, :root_dir, &File.cwd!/0)
  end

  @spec favn_dir(Path.t()) :: Path.t()
  def favn_dir(root_dir), do: Path.join(root_dir, ".favn")

  @spec logs_dir(Path.t()) :: Path.t()
  def logs_dir(root_dir), do: Path.join(favn_dir(root_dir), "logs")

  @spec install_dir(Path.t()) :: Path.t()
  def install_dir(root_dir), do: Path.join(favn_dir(root_dir), "install")

  @spec build_dir(Path.t()) :: Path.t()
  def build_dir(root_dir), do: Path.join(favn_dir(root_dir), "build")

  @spec build_target_dir(Path.t(), String.t()) :: Path.t()
  def build_target_dir(root_dir, target) when is_binary(target),
    do: Path.join(build_dir(root_dir), target)

  @spec dist_dir(Path.t()) :: Path.t()
  def dist_dir(root_dir), do: Path.join(favn_dir(root_dir), "dist")

  @spec dist_target_dir(Path.t(), String.t()) :: Path.t()
  def dist_target_dir(root_dir, target) when is_binary(target),
    do: Path.join(dist_dir(root_dir), target)

  @spec dist_runner_dir(Path.t(), String.t()) :: Path.t()
  def dist_runner_dir(root_dir, build_id),
    do: Path.join(dist_target_dir(root_dir, "runner"), build_id)

  @spec dist_manifest_dir(Path.t(), String.t()) :: Path.t()
  def dist_manifest_dir(root_dir, manifest_version_id),
    do: Path.join(dist_target_dir(root_dir, "manifest"), manifest_version_id)

  @spec data_dir(Path.t()) :: Path.t()
  def data_dir(root_dir), do: Path.join(favn_dir(root_dir), "data")

  @spec manifests_dir(Path.t()) :: Path.t()
  def manifests_dir(root_dir), do: Path.join(favn_dir(root_dir), "manifests")

  @spec history_dir(Path.t()) :: Path.t()
  def history_dir(root_dir), do: Path.join(favn_dir(root_dir), "history")

  @spec runtime_path(Path.t()) :: Path.t()
  def runtime_path(root_dir), do: Path.join(favn_dir(root_dir), "runtime.json")

  @spec lock_path(Path.t()) :: Path.t()
  def lock_path(root_dir), do: Path.join(root_dir, ".favn.lock")

  @spec latest_manifest_path(Path.t()) :: Path.t()
  def latest_manifest_path(root_dir), do: Path.join(manifests_dir(root_dir), "latest.json")

  @spec manifest_cache_dir(Path.t()) :: Path.t()
  def manifest_cache_dir(root_dir), do: Path.join(manifests_dir(root_dir), "cache")

  @spec last_failure_path(Path.t()) :: Path.t()
  def last_failure_path(root_dir), do: Path.join(history_dir(root_dir), "last_failure.json")

  @spec failures_dir(Path.t()) :: Path.t()
  def failures_dir(root_dir), do: Path.join(history_dir(root_dir), "failures")

  @spec install_path(Path.t()) :: Path.t()
  def install_path(root_dir), do: Path.join(install_dir(root_dir), "control-plane.json")

  @spec compose_dir(Path.t()) :: Path.t()
  def compose_dir(root_dir), do: Path.join(favn_dir(root_dir), "compose")

  @spec compose_path(Path.t()) :: Path.t()
  def compose_path(root_dir), do: Path.join(compose_dir(root_dir), "compose.yml")

  @spec compose_env_path(Path.t()) :: Path.t()
  def compose_env_path(root_dir), do: Path.join(compose_dir(root_dir), ".env")

  @spec compose_runner_env_path(Path.t()) :: Path.t()
  def compose_runner_env_path(root_dir), do: Path.join(compose_dir(root_dir), "runner.env")

  @spec compose_postgres_init_path(Path.t()) :: Path.t()
  def compose_postgres_init_path(root_dir),
    do: Path.join(compose_dir(root_dir), "postgres-init.sh")

  @spec runner_latest_path(Path.t()) :: Path.t()
  def runner_latest_path(root_dir),
    do: Path.join(dist_target_dir(root_dir, "runner"), "latest.json")

  @spec secrets_path(Path.t()) :: Path.t()
  def secrets_path(root_dir), do: Path.join(favn_dir(root_dir), "secrets.json")

  @spec maintenance_path(Path.t()) :: Path.t()
  def maintenance_path(root_dir), do: Path.join(favn_dir(root_dir), "maintenance.json")

  @spec compose_failure_log_path(Path.t()) :: Path.t()
  def compose_failure_log_path(root_dir), do: Path.join(logs_dir(root_dir), "compose-failure.log")
end
