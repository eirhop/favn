defmodule FavnView.Readiness do
  @moduledoc """
  Web liveness and readiness checks for the Phoenix boundary.

  The web app talks to the orchestrator through the public `FavnOrchestrator`
  facade because the supported production web placement runs in the same BEAM as
  the backend apps.
  """

  alias FavnView.ProductionRuntimeConfig

  @type check :: %{
          required(:name) => atom(),
          required(:status) => :ok | :error,
          optional(:details) => map(),
          optional(:error) => term()
        }

  @doc """
  Returns process-only web liveness diagnostics.
  """
  @spec liveness() :: map()
  def liveness do
    %{status: :ok, checks: [%{name: :process, status: :ok}]}
  end

  @doc """
  Returns web readiness diagnostics.
  """
  @spec readiness(keyword()) :: map()
  def readiness(opts \\ []) when is_list(opts) do
    checks = [web_config_check(), orchestrator_check(opts)]
    status = if Enum.all?(checks, &(&1.status == :ok)), do: :ready, else: :not_ready

    %{status: status, checks: checks}
  end

  @doc """
  Normalizes readiness payloads for JSON responses.
  """
  @spec normalize(term()) :: term()
  def normalize(%DateTime{} = value), do: DateTime.to_iso8601(value)

  def normalize(value) when is_map(value) do
    Map.new(value, fn {key, val} -> {normalize_key(key), normalize(val)} end)
  end

  def normalize(value) when is_list(value), do: Enum.map(value, &normalize/1)

  def normalize(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&normalize/1)

  def normalize(value) when is_atom(value), do: Atom.to_string(value)
  def normalize(value), do: value

  defp web_config_check do
    diagnostics =
      Application.get_env(:favn_view, :production_runtime_diagnostics, %{
        status: :ok,
        public_origin: %{
          configured?: is_binary(Application.get_env(:favn_view, :public_origin)),
          redacted: true
        },
        orchestrator: %{
          boundary: :same_beam_facade,
          readiness_timeout_ms: ProductionRuntimeConfig.configured_timeout_ms()
        }
      })

    case diagnostics do
      %{status: :ok} ->
        ok(:web_config, diagnostics)

      %{status: :invalid, error: error} ->
        error(:web_config, error)

      other ->
        error(:web_config, %{kind: :invalid_web_config_diagnostics, details: redact(other)})
    end
  end

  defp orchestrator_check(opts) do
    orchestrator =
      Keyword.get(opts, :orchestrator) ||
        Application.get_env(:favn_view, :orchestrator_facade, FavnOrchestrator)

    timeout_ms = Keyword.get(opts, :timeout_ms, ProductionRuntimeConfig.configured_timeout_ms())

    task = Task.async(fn -> orchestrator.readiness() end)

    case Task.yield(task, timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, %{status: :ready} = readiness} ->
        ok(:orchestrator, %{boundary: :same_beam_facade, upstream: upstream_summary(readiness)})

      {:ok, %{status: :not_ready} = readiness} ->
        error(:orchestrator, %{
          kind: :orchestrator_not_ready,
          boundary: :same_beam_facade,
          upstream: upstream_summary(readiness)
        })

      {:ok, other} ->
        error(:orchestrator, %{kind: :invalid_orchestrator_readiness, details: redact(other)})

      {:exit, reason} ->
        error(:orchestrator, %{kind: :exited, reason: redact_untrusted(reason)})

      nil ->
        error(:orchestrator, %{
          kind: :timeout,
          boundary: :same_beam_facade,
          timeout_ms: timeout_ms
        })
    end
  rescue
    exception ->
      error(:orchestrator, %{kind: :raised, exception: exception.__struct__})
  catch
    :exit, reason ->
      error(:orchestrator, %{kind: :exited, reason: redact_untrusted(reason)})

    kind, reason ->
      error(:orchestrator, %{kind: kind, reason: redact_untrusted(reason)})
  end

  defp upstream_summary(readiness) do
    %{
      status: Map.get(readiness, :status),
      checks: readiness |> Map.get(:checks, []) |> Enum.map(&upstream_check_summary/1)
    }
  end

  defp upstream_check_summary(check) when is_map(check) do
    %{
      name: Map.get(check, :name),
      status: Map.get(check, :status)
    }
  end

  defp upstream_check_summary(_check), do: %{name: :unknown, status: :unknown}

  defp ok(name, details), do: %{name: name, status: :ok, details: redact(details)}
  defp error(name, reason), do: %{name: name, status: :error, error: redact(reason)}

  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: key

  defp redact(value) when is_map(value) do
    Map.new(value, fn {key, val} -> {key, redact(key, val)} end)
  end

  defp redact(value) when is_list(value), do: Enum.map(value, &redact/1)

  defp redact(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&redact/1) |> List.to_tuple()

  defp redact(%_struct{} = value), do: value.__struct__
  defp redact(value), do: value

  defp redact(key, _value) when key in [:token, :secret, :password, :authorization, :cookie],
    do: "[REDACTED]"

  defp redact(key, value) when is_atom(key) do
    key
    |> Atom.to_string()
    |> redact(value)
  end

  defp redact(key, value) when is_binary(key) do
    if sensitive_key?(key), do: "[REDACTED]", else: redact(value)
  end

  defp redact(_key, value), do: redact(value)

  defp redact_untrusted(value) when is_atom(value) or is_integer(value) or is_boolean(value),
    do: value

  defp redact_untrusted(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&redact_untrusted/1) |> List.to_tuple()

  defp redact_untrusted(value) when is_list(value), do: Enum.map(value, &redact_untrusted/1)

  defp redact_untrusted(value) when is_map(value),
    do: Map.new(value, fn {key, val} -> {key, redact_untrusted(val)} end)

  defp redact_untrusted(_value), do: "[REDACTED]"

  defp sensitive_key?(key) do
    key = String.downcase(key)

    Enum.any?(
      ~w(token password secret authorization cookie credential dsn url uri),
      &String.contains?(key, &1)
    )
  end
end
