defmodule Favn.RuntimeValue do
  @moduledoc """
  A deferred, provider-owned value resolved at a supported runtime boundary.

  Runtime values let an integration describe a short-lived value without
  placing the resolved secret in application configuration. For example,
  `favn_azure` uses this contract to inject a cached access token into a DuckDB
  session script immediately before a physical session is prepared.

  A ref is inert data. Only boundaries that explicitly document runtime-value
  support resolve it. Consumer code normally calls the integration's public API
  directly instead of resolving refs itself.
  """

  alias Favn.RuntimeValue.{Error, Ref}

  @provider_timeout 15_000

  @doc """
  Builds a deferred runtime value.

  Set `:secret?` when the resolved value must be redacted. The default is
  `false` so integrations must make the security decision explicitly.
  """
  @spec new(module(), term(), keyword()) :: Ref.t()
  def new(provider, request, opts \\ []) when is_atom(provider) and is_list(opts) do
    %Ref{
      provider: provider,
      request: request,
      secret?: Keyword.get(opts, :secret?, false)
    }
    |> Ref.validate!()
  end

  @doc false
  @spec resolve(Ref.t()) :: {:ok, term()} | {:error, Error.t()}
  def resolve(%Ref{} = ref) do
    case validate_provider(ref.provider) do
      :ok ->
        ref.provider
        |> safely_fetch(ref.request)
        |> normalize_result(ref.provider)

      {:error, %Error{}} = error ->
        error
    end
  end

  defp validate_provider(provider) do
    with {:module, ^provider} <- Code.ensure_loaded(provider),
         true <- function_exported?(provider, :fetch_runtime_value, 1) do
      :ok
    else
      _other -> {:error, error(:invalid_provider, provider, false)}
    end
  end

  defp safely_fetch(provider, request) do
    parent = self()
    result_ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result =
          try do
            provider.fetch_runtime_value(request)
          rescue
            _error -> {:provider_failure, :raised}
          catch
            :exit, _reason -> {:provider_failure, :exited}
            _kind, _reason -> {:provider_failure, :threw}
          end

        send(parent, {result_ref, result})
      end)

    receive do
      {^result_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^pid, _reason} ->
        {:provider_failure, :exited}
    after
      @provider_timeout ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^monitor_ref, :process, ^pid, _reason} -> :ok
        end

        {:provider_failure, :timeout}
    end
  end

  defp normalize_result({:ok, value}, _provider), do: {:ok, value}

  defp normalize_result({:error, %Error{} = error}, _provider), do: {:error, error}

  defp normalize_result({:error, reason}, provider) do
    retryable? = is_map(reason) and Map.get(reason, :retryable?, false) == true
    {:error, error(:provider_error, provider, retryable?)}
  end

  defp normalize_result({:provider_failure, kind}, provider),
    do: {:error, error(kind, provider, kind == :timeout)}

  defp normalize_result(_other, provider),
    do: {:error, error(:invalid_provider_result, provider, false)}

  defp error(reason, provider, retryable?) do
    %Error{
      reason: reason,
      provider: provider,
      retryable?: retryable?,
      message: "runtime value provider failed"
    }
  end
end
