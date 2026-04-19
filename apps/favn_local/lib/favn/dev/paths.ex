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

  @spec last_failure_path(Path.t()) :: Path.t()
  def last_failure_path(root_dir), do: Path.join(history_dir(root_dir), "last_failure.json")

  @spec web_log_path(Path.t()) :: Path.t()
  def web_log_path(root_dir), do: Path.join(logs_dir(root_dir), "web.log")

  @spec orchestrator_log_path(Path.t()) :: Path.t()
  def orchestrator_log_path(root_dir), do: Path.join(logs_dir(root_dir), "orchestrator.log")

  @spec runner_log_path(Path.t()) :: Path.t()
  def runner_log_path(root_dir), do: Path.join(logs_dir(root_dir), "runner.log")
end
