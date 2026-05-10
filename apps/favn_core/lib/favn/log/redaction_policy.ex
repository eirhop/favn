defmodule Favn.Log.RedactionPolicy do
  @moduledoc """
  Redaction settings for backend logs.

  The default policy is declared-mode redaction with a conservative metadata key
  list. Messages are preserved unless an explicit configured value or pattern
  matches them.
  """

  @default_redact_keys [:password, :token, :secret, :api_key, :access_token, :refresh_token]

  @type mode :: :none | :declared
  @type t :: %__MODULE__{
          mode: mode(),
          redact_keys: [atom() | String.t()],
          redact_key_patterns: [Regex.t()],
          redact_values: [String.t()],
          redact_patterns: [Regex.t()]
        }

  defstruct mode: :declared,
            redact_keys: @default_redact_keys,
            redact_key_patterns: [],
            redact_values: [],
            redact_patterns: []

  @doc """
  Normalizes a map, keyword list, mode atom, or policy struct.
  """
  @spec normalize(t() | map() | keyword() | mode() | nil) :: t()
  def normalize(nil), do: %__MODULE__{}
  def normalize(%__MODULE__{} = policy), do: policy
  def normalize(mode) when mode in [:none, :declared], do: %__MODULE__{mode: mode}
  def normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

  def normalize(attrs) when is_map(attrs) do
    attrs = atomize_known_keys(attrs)

    struct!(__MODULE__, %{
      mode: Map.get(attrs, :mode, :declared),
      redact_keys: Map.get(attrs, :redact_keys, @default_redact_keys),
      redact_key_patterns: Map.get(attrs, :redact_key_patterns, []),
      redact_values: Map.get(attrs, :redact_values, []),
      redact_patterns: Map.get(attrs, :redact_patterns, [])
    })
  end

  defp atomize_known_keys(attrs) do
    known_keys = Map.keys(%__MODULE__{})

    Enum.reduce(attrs, %{}, fn {key, value}, acc ->
      normalized_key =
        if key in known_keys, do: key, else: normalize_known_string_key(key, known_keys)

      Map.put(acc, normalized_key, value)
    end)
  end

  defp normalize_known_string_key(key, known_keys) when is_binary(key) do
    Enum.find(known_keys, key, &(Atom.to_string(&1) == key))
  end

  defp normalize_known_string_key(key, _known_keys), do: key
end
