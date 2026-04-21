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

  @spec install_cache_dir(Path.t()) :: Path.t()
  def install_cache_dir(root_dir), do: Path.join(install_dir(root_dir), "cache")

  @spec install_cache_npm_dir(Path.t()) :: Path.t()
  def install_cache_npm_dir(root_dir), do: Path.join(install_cache_dir(root_dir), "npm")

  @spec install_runtimes_dir(Path.t()) :: Path.t()
  def install_runtimes_dir(root_dir), do: Path.join(install_dir(root_dir), "runtimes")

  @spec install_runtime_web_dir(Path.t()) :: Path.t()
  def install_runtime_web_dir(root_dir), do: Path.join(install_runtimes_dir(root_dir), "web")

  @spec install_runtime_orchestrator_dir(Path.t()) :: Path.t()
  def install_runtime_orchestrator_dir(root_dir),
    do: Path.join(install_runtimes_dir(root_dir), "orchestrator")

  @spec install_runtime_runner_dir(Path.t()) :: Path.t()
  def install_runtime_runner_dir(root_dir),
    do: Path.join(install_runtimes_dir(root_dir), "runner")

  @spec build_dir(Path.t()) :: Path.t()
  def build_dir(root_dir), do: Path.join(favn_dir(root_dir), "build")

  @spec build_target_dir(Path.t(), String.t()) :: Path.t()
  def build_target_dir(root_dir, target) when is_binary(target),
    do: Path.join(build_dir(root_dir), target)

  @spec build_runner_dir(Path.t(), String.t()) :: Path.t()
  def build_runner_dir(root_dir, build_id),
    do: Path.join(build_target_dir(root_dir, "runner"), build_id)

  @spec build_web_dir(Path.t(), String.t()) :: Path.t()
  def build_web_dir(root_dir, build_id),
    do: Path.join(build_target_dir(root_dir, "web"), build_id)

  @spec build_orchestrator_dir(Path.t(), String.t()) :: Path.t()
  def build_orchestrator_dir(root_dir, build_id),
    do: Path.join(build_target_dir(root_dir, "orchestrator"), build_id)

  @spec build_single_dir(Path.t(), String.t()) :: Path.t()
  def build_single_dir(root_dir, build_id),
    do: Path.join(build_target_dir(root_dir, "single"), build_id)

  @spec dist_dir(Path.t()) :: Path.t()
  def dist_dir(root_dir), do: Path.join(favn_dir(root_dir), "dist")

  @spec dist_target_dir(Path.t(), String.t()) :: Path.t()
  def dist_target_dir(root_dir, target) when is_binary(target),
    do: Path.join(dist_dir(root_dir), target)

  @spec dist_runner_dir(Path.t(), String.t()) :: Path.t()
  def dist_runner_dir(root_dir, build_id),
    do: Path.join(dist_target_dir(root_dir, "runner"), build_id)

  @spec dist_web_dir(Path.t(), String.t()) :: Path.t()
  def dist_web_dir(root_dir, build_id), do: Path.join(dist_target_dir(root_dir, "web"), build_id)

  @spec dist_orchestrator_dir(Path.t(), String.t()) :: Path.t()
  def dist_orchestrator_dir(root_dir, build_id),
    do: Path.join(dist_target_dir(root_dir, "orchestrator"), build_id)

  @spec dist_single_dir(Path.t(), String.t()) :: Path.t()
  def dist_single_dir(root_dir, build_id),
    do: Path.join(dist_target_dir(root_dir, "single"), build_id)

  @spec data_dir(Path.t()) :: Path.t()
  def data_dir(root_dir), do: Path.join(favn_dir(root_dir), "data")

  @spec manifests_dir(Path.t()) :: Path.t()
  def manifests_dir(root_dir), do: Path.join(favn_dir(root_dir), "manifests")

  @spec history_dir(Path.t()) :: Path.t()
  def history_dir(root_dir), do: Path.join(favn_dir(root_dir), "history")

  @spec runtime_path(Path.t()) :: Path.t()
  def runtime_path(root_dir), do: Path.join(favn_dir(root_dir), "runtime.json")

  @spec secrets_path(Path.t()) :: Path.t()
  def secrets_path(root_dir), do: Path.join(favn_dir(root_dir), "secrets.json")

  @spec lock_path(Path.t()) :: Path.t()
  def lock_path(root_dir), do: Path.join(favn_dir(root_dir), "lock")

  @spec latest_manifest_path(Path.t()) :: Path.t()
  def latest_manifest_path(root_dir), do: Path.join(manifests_dir(root_dir), "latest.json")

  @spec manifest_cache_dir(Path.t()) :: Path.t()
  def manifest_cache_dir(root_dir), do: Path.join(manifests_dir(root_dir), "cache")

  @spec last_failure_path(Path.t()) :: Path.t()
  def last_failure_path(root_dir), do: Path.join(history_dir(root_dir), "last_failure.json")

  @spec failures_dir(Path.t()) :: Path.t()
  def failures_dir(root_dir), do: Path.join(history_dir(root_dir), "failures")

  @spec install_path(Path.t()) :: Path.t()
  def install_path(root_dir), do: Path.join(install_dir(root_dir), "install.json")

  @spec toolchain_path(Path.t()) :: Path.t()
  def toolchain_path(root_dir), do: Path.join(install_dir(root_dir), "toolchain.json")

  @spec web_log_path(Path.t()) :: Path.t()
  def web_log_path(root_dir), do: Path.join(logs_dir(root_dir), "web.log")

  @spec orchestrator_log_path(Path.t()) :: Path.t()
  def orchestrator_log_path(root_dir), do: Path.join(logs_dir(root_dir), "orchestrator.log")

  @spec runner_log_path(Path.t()) :: Path.t()
  def runner_log_path(root_dir), do: Path.join(logs_dir(root_dir), "runner.log")
end
