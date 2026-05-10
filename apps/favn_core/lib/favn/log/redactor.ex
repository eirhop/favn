defmodule Favn.Log.Redactor do
  @moduledoc """
  Applies declared log redaction policy.
  """

  alias Favn.Log.Entry
  alias Favn.Log.RedactionPolicy

  @redacted "[REDACTED]"

  @type result :: {Entry.t(), boolean()}

  @doc """
  Redacts an entry and returns whether any redaction occurred.
  """
  @spec redact(
          Entry.t() | map() | keyword(),
          RedactionPolicy.t() | map() | keyword() | RedactionPolicy.mode() | nil
        ) ::
          result()
  def redact(entry, policy \\ nil) do
    entry = Entry.normalize(entry)
    policy = RedactionPolicy.normalize(policy)

    if policy.mode == :none do
      {entry, false}
    else
      {message, message_redacted?} = redact_message(entry.message, policy)
      {metadata, metadata_redacted?} = redact_metadata(entry.metadata, policy)

      {%{entry | message: message, metadata: metadata}, message_redacted? or metadata_redacted?}
    end
  end

  defp redact_message(message, policy) do
    message
    |> redact_configured_values(policy.redact_values)
    |> redact_patterns(policy.redact_patterns)
  end

  defp redact_metadata(value, policy), do: redact_metadata_value(value, policy)

  defp redact_metadata_value(%DateTime{} = value, _policy), do: {value, false}

  defp redact_metadata_value(value, policy) when is_map(value) do
    Enum.reduce(value, {%{}, false}, fn {key, child}, {acc, redacted?} ->
      if redact_key?(key, policy) do
        {Map.put(acc, key, @redacted), true}
      else
        {redacted_child, child_redacted?} = redact_metadata_value(child, policy)
        {Map.put(acc, key, redacted_child), redacted? or child_redacted?}
      end
    end)
  end

  defp redact_metadata_value(values, policy) when is_list(values) do
    values
    |> Enum.map_reduce(false, fn value, redacted? ->
      {redacted_value, value_redacted?} = redact_metadata_value(value, policy)
      {redacted_value, redacted? or value_redacted?}
    end)
  end

  defp redact_metadata_value(value, policy) when is_tuple(value) do
    {values, redacted?} = value |> Tuple.to_list() |> redact_metadata_value(policy)
    {List.to_tuple(values), redacted?}
  end

  defp redact_metadata_value(value, policy) when is_binary(value) do
    value
    |> redact_configured_values(policy.redact_values)
    |> redact_patterns(policy.redact_patterns)
  end

  defp redact_metadata_value(value, _policy), do: {value, false}

  defp redact_key?(key, policy) do
    normalized_key = normalize_key(key)

    Enum.any?(policy.redact_keys, &(normalize_key(&1) == normalized_key)) or
      Enum.any?(policy.redact_key_patterns, &Regex.match?(&1, normalized_key))
  end

  defp normalize_key(key) when is_atom(key), do: key |> Atom.to_string() |> String.downcase()
  defp normalize_key(key) when is_binary(key), do: String.downcase(key)
  defp normalize_key(key), do: key |> inspect() |> String.downcase()

  defp redact_configured_values(value, redact_values) do
    Enum.reduce(redact_values, {value, false}, fn secret, {acc, redacted?} ->
      if is_binary(secret) and secret != "" and String.contains?(acc, secret) do
        {String.replace(acc, secret, @redacted), true}
      else
        {acc, redacted?}
      end
    end)
  end

  defp redact_patterns({value, redacted?}, redact_patterns) do
    Enum.reduce(redact_patterns, {value, redacted?}, fn pattern, {acc, acc_redacted?} ->
      redacted = Regex.replace(pattern, acc, @redacted)
      {redacted, acc_redacted? or redacted != acc}
    end)
  end
end
