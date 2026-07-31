defmodule FavnStoragePostgres.Authentication do
  @moduledoc false

  alias FavnStoragePostgres.ConnectionConfig

  @failure_event [:favn, :storage_postgres, :authentication, :failure]

  @type mode :: ConnectionConfig.authentication()

  @spec validate(mode()) :: :ok | {:error, term()}
  def validate(:password), do: :ok

  def validate({:dynamic, provider, options})
      when is_atom(provider) and is_list(options) do
    required = [
      applications: 1,
      child_specs: 1,
      connection_reference: 1,
      connection_password: 1,
      status: 1
    ]

    loaded? = Code.ensure_loaded?(provider)

    callbacks? =
      loaded? and
        Enum.all?(required, fn {function, arity} ->
          function_exported?(provider, function, arity)
        end)

    if Keyword.keyword?(options) and callbacks? do
      :ok
    else
      {:error, :invalid_database_authentication_provider}
    end
  end

  def validate(_authentication), do: {:error, :invalid_database_authentication}

  @spec applications(mode()) :: {:ok, [atom()]} | {:error, term()}
  def applications(:password), do: {:ok, []}

  def applications({:dynamic, provider, options}) do
    case apply(provider, :applications, [options]) do
      {:ok, applications} when is_list(applications) and applications != [] ->
        if Enum.all?(applications, &is_atom/1),
          do: {:ok, applications},
          else: {:error, :invalid_database_authentication_provider_applications}

      {:error, reason} ->
        {:error, reason}

      _invalid ->
        {:error, :invalid_database_authentication_provider_applications}
    end
  end

  @spec child_specs(mode()) :: {:ok, [Supervisor.child_spec()]} | {:error, term()}
  def child_specs(:password), do: {:ok, []}

  def child_specs({:dynamic, provider, options}) do
    case apply(provider, :child_specs, [options]) do
      {:ok, child_specs} when is_list(child_specs) -> {:ok, child_specs}
      {:error, reason} -> {:error, reason}
      _invalid -> {:error, :invalid_database_authentication_provider_children}
    end
  end

  @spec install(keyword(), mode()) :: {:ok, keyword()} | {:error, term()}
  def install(options, :password), do: {:ok, options}

  def install(options, {:dynamic, provider, provider_options}) do
    case apply(provider, :connection_reference, [provider_options]) do
      {:ok, reference} when is_list(reference) ->
        if Keyword.keyword?(reference) do
          {:ok,
           Keyword.put(
             options,
             :configure,
             {__MODULE__, :configure_connection, [provider, reference]}
           )}
        else
          {:error, :invalid_database_authentication_provider_reference}
        end

      {:error, reason} ->
        {:error, reason}

      _invalid ->
        {:error, :invalid_database_authentication_provider_reference}
    end
  end

  @doc false
  @spec configure_connection(keyword(), module(), keyword()) :: keyword()
  def configure_connection(options, provider, provider_options) do
    case apply(provider, :connection_password, [provider_options]) do
      {:ok, password} when is_binary(password) and password != "" ->
        Keyword.put(options, :password, password)

      {:error, reason} ->
        emit_failure(reason)

        Keyword.put(
          options,
          :password,
          "favn-managed-identity-unavailable-" <>
            Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
        )

      _invalid ->
        raise "database authentication provider returned an invalid result"
    end
  end

  @spec status(mode()) :: map()
  def status(:password), do: %{mode: :password, lifecycle_ready?: true}

  def status({:dynamic, provider, options}) do
    case apply(provider, :status, [options]) do
      status when is_map(status) -> Map.put(status, :mode, :azure_managed_identity)
      _invalid -> %{mode: :azure_managed_identity, lifecycle_ready?: false}
    end
  end

  defp emit_failure(reason) do
    :telemetry.execute(
      @failure_event,
      %{system_time: System.system_time()},
      %{class: failure_class(reason), retryable?: retryable?(reason)}
    )
  end

  defp failure_class(%{class: class}) when is_atom(class), do: class
  defp failure_class(%{type: type}) when is_atom(type), do: type
  defp failure_class(_reason), do: :provider_error

  defp retryable?(%{retryable?: retryable?}) when is_boolean(retryable?), do: retryable?
  defp retryable?(_reason), do: false
end
