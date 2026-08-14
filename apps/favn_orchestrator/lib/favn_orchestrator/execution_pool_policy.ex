defmodule FavnOrchestrator.ExecutionPoolPolicy do
  @moduledoc """
  Resolves manifest execution-pool defaults into one persisted deployment policy.

  Operator overrides are durable deployment configuration. A later manifest
  activation preserves matching overrides, keeps removed-pool overrides
  inactive for inspection, and never silently revives an inactive override.
  """

  alias Favn.ExecutionPool.PolicySet
  alias Favn.Manifest

  @configuration_key "execution_pool_policy"
  @max_deployment_configuration_bytes 262_144
  @allowed_request_keys [
    :approve_manifest_defaults,
    :discard_orphaned,
    :overrides,
    :reset,
    "approve_manifest_defaults",
    "discard_orphaned",
    "overrides",
    "reset"
  ]

  @type request :: %{
          optional(:approve_manifest_defaults) => boolean(),
          optional(:overrides) => keyword() | map(),
          optional(:reset) => [atom() | String.t()]
        }

  @doc "Resolves one approved effective policy and its durable provenance snapshot."
  @spec resolve(Manifest.t(), map(), map(), request() | nil) ::
          {:ok, %{configuration: map(), effective: PolicySet.t()}} | {:error, term()}
  def resolve(
        %Manifest{execution_pools: defaults},
        configuration,
        previous_configuration,
        request
      )
      when is_map(configuration) and is_map(previous_configuration) do
    with {:ok, defaults} <- PolicySet.new(defaults),
         {:ok, request} <- normalize_request(request),
         :ok <- require_approval(defaults, request),
         {:ok, previous_active, previous_orphaned} <- previous_overrides(previous_configuration),
         {:ok, requested_overrides} <- PolicySet.new(field(request, :overrides, %{})),
         {:ok, reset} <- normalize_reset(field(request, :reset, [])),
         {:ok, discard_orphaned} <-
           normalize_reset(field(request, :discard_orphaned, [])),
         :ok <- validate_requested_names(defaults, requested_overrides, reset),
         :ok <-
           validate_discarded_names(
             defaults,
             previous_active,
             previous_orphaned,
             discard_orphaned
           ) do
      {preserved, newly_orphaned} = partition_overrides(previous_active, defaults)

      orphaned =
        previous_orphaned
        |> Enum.reject(fn {name, _policy} -> Map.has_key?(defaults, name) end)
        |> Map.new()
        |> Map.merge(newly_orphaned)
        |> Map.drop(Map.keys(requested_overrides) ++ reset ++ discard_orphaned)

      active_overrides =
        preserved
        |> Map.merge(requested_overrides)
        |> Map.drop(reset)

      effective = Map.merge(defaults, active_overrides)

      sources =
        Map.new(effective, fn {name, _policy} -> {name, source(name, active_overrides)} end)

      policy_configuration = %{
        "schema_version" => 1,
        "manifest_fingerprint" => PolicySet.fingerprint(defaults),
        "effective_fingerprint" => PolicySet.fingerprint(effective),
        "operator_overrides" => PolicySet.to_map(active_overrides),
        "orphaned_overrides" => PolicySet.to_map(orphaned),
        "effective" => PolicySet.to_map(effective),
        "sources" => sources
      }

      resolved_configuration =
        configuration
        |> Map.delete(:execution_pool_policy)
        |> Map.put(@configuration_key, policy_configuration)

      with :ok <- validate_configuration_size(resolved_configuration) do
        {:ok,
         %{
           configuration: resolved_configuration,
           effective: effective
         }}
      end
    end
  end

  @doc "Returns the normalized effective policy from persisted deployment configuration."
  @spec effective(map()) :: {:ok, PolicySet.t()} | {:error, term()}
  def effective(configuration) when is_map(configuration) do
    configuration
    |> policy_configuration()
    |> field(:effective, %{})
    |> PolicySet.new()
  end

  @doc "Returns bounded non-sensitive effective-policy diagnostics."
  @spec diagnostics(map()) :: {:ok, [map()]} | {:error, term()}
  def diagnostics(configuration) when is_map(configuration) do
    policy_configuration = policy_configuration(configuration)
    sources = field(policy_configuration, :sources, %{})

    with {:ok, effective} <- effective(configuration) do
      {:ok,
       effective
       |> PolicySet.diagnostics()
       |> Enum.map(fn item -> Map.put(item, :source, Map.get(sources, item.name, "manifest")) end)}
    end
  end

  @doc "Returns the JSON key used inside immutable deployment configuration."
  @spec configuration_key() :: String.t()
  def configuration_key, do: @configuration_key

  defp normalize_request(nil), do: {:ok, %{}}

  defp normalize_request(request) when is_map(request) do
    unknown = request |> Map.keys() |> Enum.reject(&(&1 in @allowed_request_keys))

    if unknown == [],
      do: {:ok, request},
      else:
        {:error, {:unknown_execution_pool_policy_request_keys, Enum.sort_by(unknown, &inspect/1)}}
  end

  defp normalize_request(request), do: {:error, {:invalid_execution_pool_policy_request, request}}

  defp require_approval(defaults, request) when map_size(defaults) == 0 do
    case field(request, :approve_manifest_defaults, false) do
      value when value in [true, false] -> :ok
      value -> {:error, {:invalid_execution_pool_policy_approval, value}}
    end
  end

  defp require_approval(_defaults, request) do
    case field(request, :approve_manifest_defaults, false) do
      true -> :ok
      false -> {:error, :execution_pool_policy_approval_required}
      value -> {:error, {:invalid_execution_pool_policy_approval, value}}
    end
  end

  defp previous_overrides(configuration) do
    policy = policy_configuration(configuration)

    with {:ok, active} <- PolicySet.new(field(policy, :operator_overrides, %{})),
         {:ok, orphaned} <- PolicySet.new(field(policy, :orphaned_overrides, %{})) do
      {:ok, active, orphaned}
    end
  end

  defp policy_configuration(configuration) do
    field(configuration, @configuration_key, %{})
  end

  defp partition_overrides(overrides, defaults) do
    Enum.reduce(overrides, {%{}, %{}}, fn {name, policy}, {active, orphaned} ->
      if Map.has_key?(defaults, name),
        do: {Map.put(active, name, policy), orphaned},
        else: {active, Map.put(orphaned, name, policy)}
    end)
  end

  defp normalize_reset(reset) when is_list(reset) do
    reset
    |> Enum.reduce_while({:ok, []}, fn
      name, {:ok, acc} when is_atom(name) or is_binary(name) ->
        {:cont, {:ok, [to_string(name) | acc]}}

      invalid, _acc ->
        {:halt, {:error, {:invalid_execution_pool_policy_reset, invalid}}}
    end)
    |> case do
      {:ok, names} -> {:ok, names |> Enum.uniq() |> Enum.sort()}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_reset(reset), do: {:error, {:invalid_execution_pool_policy_reset, reset}}

  defp validate_requested_names(defaults, requested_overrides, reset) do
    unknown =
      (Map.keys(requested_overrides) ++ reset)
      |> Enum.uniq()
      |> Enum.reject(&Map.has_key?(defaults, &1))
      |> Enum.sort()

    if unknown == [], do: :ok, else: {:error, {:unknown_execution_pool_overrides, unknown}}
  end

  defp validate_discarded_names(defaults, previous_active, previous_orphaned, discarded) do
    available =
      previous_active
      |> Map.merge(previous_orphaned)
      |> Map.keys()
      |> Enum.reject(&Map.has_key?(defaults, &1))
      |> MapSet.new()

    unknown = Enum.reject(discarded, &MapSet.member?(available, &1))

    if unknown == [],
      do: :ok,
      else: {:error, {:unknown_orphaned_execution_pool_overrides, unknown}}
  end

  defp source(name, overrides) do
    if Map.has_key?(overrides, name), do: "operator_override", else: "manifest"
  end

  defp validate_configuration_size(configuration) do
    case Jason.encode(configuration) do
      {:ok, encoded} when byte_size(encoded) <= @max_deployment_configuration_bytes -> :ok
      {:ok, encoded} -> {:error, {:execution_pool_policy_too_large, byte_size(encoded)}}
      {:error, reason} -> {:error, {:invalid_execution_pool_policy, reason}}
    end
  end

  defp field(map, key, default) do
    atom_key = if is_atom(key), do: key, else: nil
    string_key = if is_binary(key), do: key, else: Atom.to_string(key)

    cond do
      not is_nil(atom_key) and Map.has_key?(map, atom_key) -> Map.get(map, atom_key)
      Map.has_key?(map, string_key) -> Map.get(map, string_key)
      true -> default
    end
  end
end
