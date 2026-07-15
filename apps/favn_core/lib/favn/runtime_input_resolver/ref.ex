defmodule Favn.RuntimeInputResolver.Ref do
  @moduledoc """
  Serializable manifest reference to a runtime SQL input resolver module.

  The manifest stores this stable module reference only. Resolver functions,
  captured environments, and resolved parameter payloads are never serialized.
  """

  @enforce_keys [:module]
  defstruct [:module]

  @type t :: %__MODULE__{module: module()}

  @doc "Builds and validates a resolver module reference."
  @spec new(module()) :: {:ok, t()} | {:error, :invalid_module}
  def new(module) when is_atom(module) do
    if module_atom?(module),
      do: {:ok, %__MODULE__{module: module}},
      else: {:error, :invalid_module}
  end

  def new(_module), do: {:error, :invalid_module}

  @doc "Builds a resolver reference or raises for an invalid module."
  @spec new!(module()) :: t()
  def new!(module) do
    case new(module) do
      {:ok, ref} ->
        ref

      {:error, :invalid_module} ->
        raise ArgumentError, "invalid runtime input resolver module #{inspect(module)}"
    end
  end

  @doc "Validates an existing resolver reference."
  @spec validate(t()) :: :ok | {:error, :invalid_module}
  def validate(%__MODULE__{module: module}) do
    case new(module) do
      {:ok, _ref} -> :ok
      {:error, :invalid_module} -> {:error, :invalid_module}
    end
  end

  defp module_atom?(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end
end
