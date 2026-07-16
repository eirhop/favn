defmodule Favn.Azure.Credentials.Source do
  @moduledoc false

  alias Favn.Azure.Credentials.{AzureCLI, ManagedIdentity, Request}
  alias Favn.Azure.{Token, TokenError}

  @spec fetch_token(Request.t(), keyword()) :: {:ok, Favn.Azure.Token.t()} | {:error, TokenError.t()}
  def fetch_token(%Request{provider: provider} = request, opts) do
    provider = provider_module(provider)

    with {:module, ^provider} <- Code.ensure_loaded(provider),
         true <- function_exported?(provider, :fetch_token, 2) do
      provider
      |> then(& &1.fetch_token(request, opts))
      |> normalize_result()
    else
      _other -> unsupported_provider(provider)
    end
  rescue
    _error -> provider_failure(provider, :raised)
  catch
    :exit, _reason -> provider_failure(provider, :exited)
    _kind, _reason -> provider_failure(provider, :threw)
  end

  defp provider_module(:azure_cli), do: AzureCLI
  defp provider_module(:managed_identity), do: ManagedIdentity
  defp provider_module(provider), do: provider

  defp normalize_result({:ok, %Token{} = token}) do
    if Token.valid_for?(token, 0) do
      {:ok, token}
    else
      {:error,
       %TokenError{
         type: :authentication_error,
         message: "Azure credential provider returned an expired token",
         retryable?: true,
         details: %{reason: :expired_token}
       }}
    end
  end

  defp normalize_result({:error, %TokenError{} = error}), do: {:error, error}

  defp normalize_result(_result) do
    {:error,
     %TokenError{
       type: :execution_error,
       message: "Azure credential provider returned an invalid result",
       details: %{reason: :invalid_provider_result}
     }}
  end

  defp unsupported_provider(provider) do
    {:error,
     %TokenError{
       type: :invalid_config,
       message: "unsupported Azure credential provider",
       details: %{provider: inspect(provider)}
     }}
  end

  defp provider_failure(provider, kind) do
    {:error,
     %TokenError{
       type: :execution_error,
       message: "Azure credential provider failed",
       details: %{provider: inspect(provider), reason: kind}
     }}
  end
end
