defmodule Favn.Azure.CredentialsDefaultCacheTest do
  use ExUnit.Case, async: false

  alias Favn.Azure.Credentials
  alias Favn.Azure.Credentials.Request
  alias Favn.Azure.TokenError

  defmodule UnexpectedProvider do
    @behaviour Favn.Azure.CredentialProvider

    @impl true
    def fetch_token(_request, opts) do
      send(Keyword.fetch!(opts, :owner), :unexpected_provider_fetch)
      Process.sleep(:infinity)
    end
  end

  setup do
    previous = Application.get_env(:favn, :runner_plugins)
    Application.put_env(:favn, :runner_plugins, [Favn.Azure.RunnerPlugin])

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous)
      end
    end)

    :ok
  end

  test "a configured default cache fails closed while its process is unavailable" do
    refute Process.whereis(Favn.Azure.Credentials.Cache)
    request = Request.new!("https://vault.azure.net", provider: UnexpectedProvider)

    assert {:error,
            %TokenError{
              type: :connection_error,
              retryable?: true,
              details: %{reason: :cache_unavailable}
            }} = Credentials.fetch_token(request, provider_options: [owner: self()])

    refute_receive :unexpected_provider_fetch
  end
end
