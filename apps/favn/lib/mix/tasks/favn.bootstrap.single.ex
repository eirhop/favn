defmodule Mix.Tasks.Favn.Bootstrap.Single do
  use Mix.Task

  @shortdoc "Bootstraps a single-node backend through orchestrator APIs"

  @moduledoc """
  Bootstraps a SQLite single-node backend by registering and activating a
  manifest through the orchestrator API, then registering that manifest with the
  single-node runner.

  Required options can be passed as flags or environment variables:

  - `--manifest` or `FAVN_BOOTSTRAP_MANIFEST_PATH`
  - `--orchestrator-url` or `FAVN_WEB_ORCHESTRATOR_BASE_URL`
  - `--service-token` or `FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN` /
    `FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN`
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)

    case Dev.bootstrap_single(opts) do
      {:ok, summary} ->
        IO.puts("Favn single-node bootstrap complete")
        IO.puts("manifest version: #{summary.manifest_version_id}")
        IO.puts("activated: #{summary.activated?}")

        IO.puts(
          "active manifest verification: #{format_active_manifest_verification(summary.active_manifest_verification)}"
        )

      {:error, {:missing_required_option, key, _value}} ->
        Mix.raise("bootstrap failed: missing required option #{option_name(key)}")

      {:error, %{operation: :publish_manifest, reason: {:http_error, 409, _body}}} ->
        Mix.raise("bootstrap failed: manifest version conflict")

      {:error, %{operation: :register_runner, reason: {:http_error, 409, _body}}} ->
        Mix.raise("bootstrap failed: runner registration conflict")

      {:error, %{operation: :verify_service_token, reason: {:http_error, 401, _body}}} ->
        Mix.raise("bootstrap failed: service token was rejected by orchestrator")

      {:error, %{operation: operation, reason: reason}} ->
        Mix.raise("bootstrap failed during #{operation}: #{inspect(reason)}")

      {:error, reason} ->
        Mix.raise("bootstrap failed: #{inspect(reason)}")
    end
  end

  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) when is_list(args) do
    "favn.bootstrap.single"
    |> CLIArgs.parse_no_args!(args,
      manifest: :string,
      orchestrator_url: :string,
      service_token: :string,
      activate: :boolean
    )
    |> with_env_defaults()
    |> normalize_activation()
    |> validate_required!()
  end

  defp normalize_activation(opts) do
    case Keyword.fetch(opts, :activate) do
      {:ok, value} -> opts |> Keyword.put(:activate?, value) |> Keyword.delete(:activate)
      :error -> opts
    end
  end

  defp with_env_defaults(opts) do
    opts
    |> put_default(
      :manifest_path,
      Keyword.get(opts, :manifest) || env("FAVN_BOOTSTRAP_MANIFEST_PATH")
    )
    |> Keyword.delete(:manifest)
    |> put_default(
      :orchestrator_url,
      Keyword.get(opts, :orchestrator_url) || env("FAVN_WEB_ORCHESTRATOR_BASE_URL")
    )
    |> put_default(
      :service_token,
      Keyword.get(opts, :service_token) || env("FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN") ||
        env("FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN") || env("FAVN_ORCHESTRATOR_SERVICE_TOKEN")
    )
  end

  defp put_default(opts, key, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, key, value)

  defp put_default(opts, _key, _value), do: opts

  defp validate_required!(opts) do
    missing =
      [:manifest_path, :orchestrator_url, :service_token]
      |> Enum.reject(fn key -> present?(Keyword.get(opts, key)) end)
      |> Enum.map(&option_name/1)

    case missing do
      [] -> opts
      _ -> Mix.raise("missing required option(s): #{Enum.join(missing, ", ")}")
    end
  end

  defp present?(value), do: is_binary(value) and value != ""

  defp option_name(:manifest_path), do: "--manifest"
  defp option_name(:orchestrator_url), do: "--orchestrator-url"
  defp option_name(:service_token), do: "--service-token"
  defp option_name(key), do: "--" <> (key |> Atom.to_string() |> String.replace("_", "-"))

  defp format_active_manifest_verification(:matched), do: "matched"
  defp format_active_manifest_verification(value), do: inspect(value)

  defp env(name) do
    case System.get_env(name) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end
end
