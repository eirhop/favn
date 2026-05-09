defmodule FavnOrchestrator.RefreshPolicy do
  @moduledoc """
  Normalized refresh policy for orchestrator run submissions.

  This module only normalizes submission intent and expands forced nodes within an
  already-built `%Favn.Plan{}`. It does not decide freshness or execute runs.

  ## Accepted Input Values

  - `nil` or `:auto`: use each asset's manifest freshness policy.
  - `:force`: run every planned node regardless of stored freshness state.
  - `:missing`: skip any node with a prior successful freshness state, including
    assets declared with `@freshness :always`.
  - `{:force_assets, refs}`: force planned nodes whose refs are listed.
  - `{:force_assets, refs, include_upstream: true}`: force selected refs and their
    planned transitive upstream dependencies.
  - `%{mode: :force_assets, refs: refs, include_upstream?: true}` and string-keyed
    map equivalents for JSON/API-shaped callers.

  `refs` must be canonical asset refs such as `{MyApp.Warehouse.Raw.Orders,
  :asset}`. Module shorthand is resolved before this boundary.

  Backfill child pipeline runs default to `:missing` unless callers pass an
  explicit `refresh` or `refresh_policy` option.
  """

  alias Favn.Plan
  alias Favn.Ref

  @type mode :: :auto | :force | :missing | :force_assets

  @type t :: %__MODULE__{
          mode: mode(),
          refs: [Ref.t()],
          include_upstream?: boolean()
        }

  defstruct mode: :auto,
            refs: [],
            include_upstream?: false

  @doc """
  Normalizes refresh policy options from backend submission opts.
  """
  @spec from_opts(keyword() | map()) :: {:ok, t()} | {:error, term()}
  def from_opts(opts) when is_list(opts) do
    opts
    |> Keyword.get(:refresh_policy, Keyword.get(opts, :refresh))
    |> from_value()
  end

  def from_opts(%{} = opts) do
    opts
    |> Map.get(:refresh_policy, Map.get(opts, :refresh))
    |> from_value()
  end

  def from_opts(opts), do: {:error, {:invalid_refresh_policy_opts, opts}}

  @doc """
  Normalizes a refresh policy value.
  """
  @spec from_value(term()) :: {:ok, t()} | {:error, term()}
  def from_value(nil), do: {:ok, %__MODULE__{mode: :auto}}

  def from_value(%__MODULE__{} = policy), do: validate_policy(policy)

  def from_value(mode) when mode in [:auto, :force, :missing] do
    {:ok, %__MODULE__{mode: mode}}
  end

  def from_value(mode) when is_binary(mode) do
    mode
    |> String.trim()
    |> String.downcase()
    |> mode_from_string()
  end

  def from_value(value) when is_map(value) do
    mode = Map.get(value, :mode, Map.get(value, "mode"))
    refs = Map.get(value, :refs, Map.get(value, "refs", []))

    include_upstream? =
      Map.get(value, :include_upstream?, Map.get(value, "include_upstream?", false))

    case normalize_mode(mode) do
      {:ok, mode} when mode in [:auto, :force, :missing] ->
        validate_policy(%__MODULE__{mode: mode})

      {:ok, :force_assets} ->
        with {:ok, refs} <- normalize_refs(refs),
             {:ok, include_upstream?} <- normalize_include_upstream(include_upstream?) do
          validate_policy(%__MODULE__{
            mode: :force_assets,
            refs: refs,
            include_upstream?: include_upstream?
          })
        end

      {:error, _reason} = error ->
        error
    end
  end

  def from_value({:force_assets, refs}) do
    normalize_force_assets(refs, [])
  end

  def from_value({:force_assets, refs, opts}) when is_list(opts) do
    normalize_force_assets(refs, opts)
  end

  def from_value(value), do: {:error, {:invalid_refresh_policy, value}}

  @doc """
  Normalizes a refresh policy value, raising when invalid.
  """
  @spec from_value!(term()) :: t()
  def from_value!(value) do
    case from_value(value) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid refresh policy: #{inspect(reason)}"
    end
  end

  @doc """
  Expands the policy into the forced node key set for a planned graph.

  `:force` selects every planned node. `:force_assets` selects planned nodes whose
  `:ref` is listed in the policy, and optionally walks transitive planned upstream
  edges from those selected nodes.
  """
  @spec expand_force_set(t(), Plan.t()) :: MapSet.t(Plan.node_key())
  def expand_force_set(%__MODULE__{mode: :force}, %Plan{nodes: nodes}) do
    nodes
    |> Map.keys()
    |> MapSet.new()
  end

  def expand_force_set(%__MODULE__{mode: :force_assets} = policy, %Plan{nodes: nodes}) do
    refs = MapSet.new(policy.refs)

    selected =
      nodes
      |> Enum.filter(fn {_node_key, node} -> Map.get(node, :ref) in refs end)
      |> Enum.map(fn {node_key, _node} -> node_key end)
      |> MapSet.new()

    if policy.include_upstream? do
      expand_upstream(selected, nodes)
    else
      selected
    end
  end

  def expand_force_set(%__MODULE__{}, %Plan{}), do: MapSet.new()

  defp mode_from_string("auto"), do: from_value(:auto)
  defp mode_from_string("force"), do: from_value(:force)
  defp mode_from_string("missing"), do: from_value(:missing)
  defp mode_from_string(value), do: {:error, {:invalid_refresh_policy, value}}

  defp normalize_mode(mode) when mode in [:auto, :force, :missing, :force_assets], do: {:ok, mode}
  defp normalize_mode("auto"), do: {:ok, :auto}
  defp normalize_mode("force"), do: {:ok, :force}
  defp normalize_mode("missing"), do: {:ok, :missing}
  defp normalize_mode("force_assets"), do: {:ok, :force_assets}
  defp normalize_mode(mode), do: {:error, {:invalid_refresh_policy, mode}}

  defp normalize_force_assets(refs, opts) do
    with {:ok, refs} <- normalize_refs(refs),
         {:ok, include_upstream?} <- normalize_include_upstream(opts) do
      {:ok, %__MODULE__{mode: :force_assets, refs: refs, include_upstream?: include_upstream?}}
    end
  end

  defp normalize_refs(refs) when is_list(refs) do
    Enum.reduce_while(refs, {:ok, []}, fn ref, {:ok, acc} ->
      case normalize_ref(ref) do
        {:ok, ref} -> {:cont, {:ok, [ref | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, refs} -> {:ok, Enum.reverse(refs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_refs(refs), do: {:error, {:invalid_refresh_refs, refs}}

  defp normalize_ref({module, name} = ref) when is_atom(module) and is_atom(name), do: {:ok, ref}
  defp normalize_ref(ref), do: {:error, {:invalid_refresh_ref, ref}}

  defp normalize_include_upstream(opts) when is_list(opts) do
    case Keyword.get(opts, :include_upstream, false) do
      value when is_boolean(value) -> {:ok, value}
      value -> {:error, {:invalid_include_upstream, value}}
    end
  end

  defp normalize_include_upstream(value) when is_boolean(value), do: {:ok, value}
  defp normalize_include_upstream(value), do: {:error, {:invalid_include_upstream, value}}

  defp validate_policy(%__MODULE__{mode: mode, refs: refs, include_upstream?: include_upstream?})
       when mode in [:auto, :force, :missing] and refs == [] and include_upstream? == false do
    {:ok, %__MODULE__{mode: mode}}
  end

  defp validate_policy(%__MODULE__{
         mode: :force_assets,
         refs: refs,
         include_upstream?: include_upstream?
       })
       when is_boolean(include_upstream?) do
    with {:ok, refs} <- normalize_refs(refs) do
      {:ok, %__MODULE__{mode: :force_assets, refs: refs, include_upstream?: include_upstream?}}
    end
  end

  defp validate_policy(policy), do: {:error, {:invalid_refresh_policy, policy}}

  defp expand_upstream(selected, nodes) do
    selected
    |> MapSet.to_list()
    |> do_expand_upstream(nodes, selected)
  end

  defp do_expand_upstream([], _nodes, forced), do: forced

  defp do_expand_upstream([node_key | rest], nodes, forced) do
    upstream =
      nodes
      |> Map.get(node_key, %{})
      |> Map.get(:upstream, [])
      |> Enum.filter(&Map.has_key?(nodes, &1))
      |> Enum.reject(&MapSet.member?(forced, &1))

    forced = Enum.reduce(upstream, forced, &MapSet.put(&2, &1))

    do_expand_upstream(rest ++ upstream, nodes, forced)
  end
end
