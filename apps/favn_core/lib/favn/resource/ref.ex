defmodule Favn.Resource.Ref do
  @moduledoc """
  Redaction-safe identity for a shared runtime resource.

  Workspace identity is added by the orchestrator persistence boundary. The
  public resource identity contains only the kind and configured resource name.
  """

  @kinds [:execution_pool, :connection]

  @enforce_keys [:kind, :name]
  defstruct [:kind, :name]

  @type kind :: :execution_pool | :connection
  @type t :: %__MODULE__{kind: kind(), name: String.t()}

  @doc "Builds and validates a resource reference."
  @spec new(kind() | String.t(), atom() | String.t()) :: {:ok, t()} | {:error, term()}
  def new(kind, name) do
    with {:ok, kind} <- normalize_kind(kind),
         {:ok, name} <- normalize_name(name) do
      {:ok, %__MODULE__{kind: kind, name: name}}
    end
  end

  @doc "Builds a resource reference or raises `ArgumentError`."
  @spec new!(kind() | String.t(), atom() | String.t()) :: t()
  def new!(kind, name) do
    case new(kind, name) do
      {:ok, ref} -> ref
      {:error, reason} -> raise ArgumentError, "invalid resource reference: #{inspect(reason)}"
    end
  end

  @doc "Normalizes a struct or map representation."
  @spec from_value(term()) :: {:ok, t()} | {:error, term()}
  def from_value(%__MODULE__{} = ref), do: new(ref.kind, ref.name)

  def from_value(value) when is_map(value) do
    new(field(value, :kind), field(value, :name))
  end

  def from_value(value), do: {:error, {:invalid_resource_ref, value}}

  @doc "Returns the supported resource kinds."
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  defp normalize_kind(kind) when kind in @kinds, do: {:ok, kind}
  defp normalize_kind("execution_pool"), do: {:ok, :execution_pool}
  defp normalize_kind("connection"), do: {:ok, :connection}
  defp normalize_kind(kind), do: {:error, {:invalid_resource_kind, kind}}

  defp normalize_name(name) when is_atom(name) and not is_nil(name),
    do: normalize_name(Atom.to_string(name))

  defp normalize_name(name) when is_binary(name) do
    name = String.trim(name)

    if name == "",
      do: {:error, {:invalid_resource_name, name}},
      else: {:ok, name}
  end

  defp normalize_name(name), do: {:error, {:invalid_resource_name, name}}

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
