defmodule Favn.Manifest.Labels do
  @moduledoc """
  Normalization helpers for manifest selector labels.

  Tags and categories are user-facing labels in the manifest contract. They are
  persisted and matched as strings so authored atoms and strings behave the same
  before and after JSON persistence without creating atoms from persisted label
  data. Boolean, nil, and module atoms are rejected because they are not stable
  user-facing classification labels.
  """

  @type label_input :: atom() | String.t()
  @type label :: String.t()
  @type error :: {:invalid_manifest_label, term()} | {:invalid_manifest_labels, term()}

  @doc """
  Normalizes one authored or persisted selector label to a string.
  """
  @spec normalize_label(label_input()) :: {:ok, label()} | {:error, error()}
  def normalize_label(value) when value in [nil, true, false],
    do: {:error, {:invalid_manifest_label, value}}

  def normalize_label(value) when is_atom(value) do
    label = Atom.to_string(value)

    if String.starts_with?(label, "Elixir.") do
      {:error, {:invalid_manifest_label, value}}
    else
      {:ok, label}
    end
  end

  def normalize_label(value) when is_binary(value), do: {:ok, value}
  def normalize_label(value), do: {:error, {:invalid_manifest_label, value}}

  @doc """
  Normalizes one selector label or raises `ArgumentError`.
  """
  @spec normalize_label!(label_input()) :: label()
  def normalize_label!(value) do
    case normalize_label(value) do
      {:ok, label} -> label
      {:error, {:invalid_manifest_label, invalid}} -> raise_invalid_label!(invalid)
    end
  end

  @doc """
  Normalizes a list of selector labels while preserving order.
  """
  @spec normalize_labels([label_input()]) :: {:ok, [label()]} | {:error, error()}
  def normalize_labels(values) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case normalize_label(value) do
        {:ok, label} -> {:cont, {:ok, [label | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, labels} -> {:ok, Enum.reverse(labels)}
      {:error, _reason} = error -> error
    end
  end

  def normalize_labels(value), do: {:error, {:invalid_manifest_labels, value}}

  @doc """
  Normalizes a list of selector labels or raises `ArgumentError`.
  """
  @spec normalize_labels!([label_input()]) :: [label()]
  def normalize_labels!(values) do
    case normalize_labels(values) do
      {:ok, labels} -> labels
      {:error, {:invalid_manifest_label, invalid}} -> raise_invalid_label!(invalid)
      {:error, {:invalid_manifest_labels, invalid}} -> raise_invalid_labels!(invalid)
    end
  end

  @doc """
  Returns true when two label inputs normalize to the same manifest label.
  """
  @spec match_label?(label_input(), label_input()) :: boolean()
  def match_label?(left, right) do
    with {:ok, left_label} <- normalize_label(left),
         {:ok, right_label} <- normalize_label(right) do
      left_label == right_label
    else
      {:error, _reason} -> false
    end
  end

  defp raise_invalid_label!(value) do
    raise ArgumentError, "manifest label must be an atom or string, got: #{inspect(value)}"
  end

  defp raise_invalid_labels!(value) do
    raise ArgumentError, "manifest labels must be a list, got: #{inspect(value)}"
  end
end
