defmodule FavnStoragePostgres.Types.JsonValue do
  @moduledoc false

  use Ecto.Type

  @type t :: map() | list() | String.t() | number() | boolean() | nil

  @impl true
  def type, do: :map

  @impl true
  def cast(value), do: validate(value)

  @impl true
  def load(value), do: validate(value)

  @impl true
  def dump(value), do: validate(value)

  defp validate(value) do
    if json_value?(value), do: {:ok, value}, else: :error
  end

  defp json_value?(value)
       when is_nil(value) or is_binary(value) or is_boolean(value) or is_number(value),
       do: true

  defp json_value?(value) when is_list(value), do: Enum.all?(value, &json_value?/1)

  defp json_value?(value) when is_map(value) and not is_struct(value) do
    Enum.all?(value, fn {key, child} ->
      (is_binary(key) or is_atom(key)) and json_value?(child)
    end)
  end

  defp json_value?(_value), do: false
end
