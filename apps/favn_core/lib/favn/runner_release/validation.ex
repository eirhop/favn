defmodule Favn.RunnerRelease.Validation do
  @moduledoc false

  @sha256 ~r/\A[0-9a-f]{64}\z/
  @identifier ~r/\A[[:alnum:]_.:@!?$-]+\z/u

  def fetch(value, field) when is_map(value) do
    string_field = Atom.to_string(field)

    cond do
      Map.has_key?(value, field) -> {:ok, Map.get(value, field)}
      Map.has_key?(value, string_field) -> {:ok, Map.get(value, string_field)}
      true -> {:error, {:missing_runner_release_field, field}}
    end
  end

  def fetch(_value, field), do: {:error, {:missing_runner_release_field, field}}

  def fetch_optional(value, field, default \\ nil)

  def fetch_optional(value, field, default) when is_map(value) do
    string_field = Atom.to_string(field)

    cond do
      Map.has_key?(value, field) -> Map.get(value, field)
      Map.has_key?(value, string_field) -> Map.get(value, string_field)
      true -> default
    end
  end

  def fetch_optional(_value, _field, default), do: default

  def string(value, field, max_bytes) when is_atom(value),
    do: string(Atom.to_string(value), field, max_bytes)

  def string(value, field, max_bytes) when is_binary(value) do
    cond do
      value == "" -> {:error, {:invalid_runner_release_field, field, :empty}}
      not String.valid?(value) -> {:error, {:invalid_runner_release_field, field, :invalid_utf8}}
      value != String.trim(value) -> {:error, {:invalid_runner_release_field, field, :whitespace}}
      byte_size(value) > max_bytes -> {:error, {:invalid_runner_release_field, field, :too_long}}
      true -> {:ok, value}
    end
  end

  def string(_value, field, _max_bytes),
    do: {:error, {:invalid_runner_release_field, field, :expected_string}}

  def identifier(value, field, max_bytes) do
    with {:ok, value} <- string(value, field, max_bytes),
         true <- Regex.match?(@identifier, value) do
      {:ok, value}
    else
      false -> {:error, {:invalid_runner_release_field, field, :invalid_identifier}}
      {:error, _reason} = error -> error
    end
  end

  def digest(value, field) when is_binary(value) do
    if Regex.match?(@sha256, value) do
      {:ok, value}
    else
      {:error, {:invalid_runner_release_field, field, :invalid_sha256}}
    end
  end

  def digest(_value, field),
    do: {:error, {:invalid_runner_release_field, field, :invalid_sha256}}

  def positive_integer(value, _field) when is_integer(value) and value > 0, do: {:ok, value}

  def positive_integer(_value, field),
    do: {:error, {:invalid_runner_release_field, field, :expected_positive_integer}}

  def list(value, _field) when is_list(value), do: {:ok, value}
  def list(_value, field), do: {:error, {:invalid_runner_release_field, field, :expected_list}}

  def module_name(value, field \\ :module) do
    with {:ok, value} <- string(value, field, 255),
         true <- Regex.match?(@identifier, value) do
      {:ok, value}
    else
      false -> {:error, {:invalid_runner_release_field, field, :invalid_module_name}}
      {:error, _reason} = error -> error
    end
  end
end
