defmodule Favn.RuntimeValue.Ref do
  @moduledoc """
  Inert reference to a provider-owned value resolved at runtime.

  Inspect output never includes the provider request because integrations may
  place identity or endpoint information in it. Resolved values are never held
  in this struct.
  """

  @enforce_keys [:provider, :request, :secret?]
  defstruct [:provider, :request, :secret?]

  @type t :: %__MODULE__{
          provider: module(),
          request: term(),
          secret?: boolean()
        }

  @doc false
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{provider: provider, secret?: secret?} = ref)
      when is_atom(provider) and not is_nil(provider) and is_boolean(secret?),
      do: ref

  def validate!(ref), do: raise(ArgumentError, "invalid runtime value ref: #{inspect(ref)}")
end

defimpl Inspect, for: Favn.RuntimeValue.Ref do
  import Inspect.Algebra

  def inspect(ref, opts) do
    concat([
      "#Favn.RuntimeValue.Ref<",
      to_doc([provider: ref.provider, secret?: ref.secret?], opts),
      ">"
    ])
  end
end
