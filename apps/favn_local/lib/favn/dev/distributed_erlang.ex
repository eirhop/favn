defmodule Favn.Dev.DistributedErlang do
  @moduledoc false

  @max_node_name_bytes 255
  @max_node_part_bytes 128
  @max_cookie_bytes 255
  @node_part_pattern ~r/^[A-Za-z0-9_-]+$/
  @cookie_pattern ~r/^[A-Za-z0-9_]+$/

  @spec cookie_to_atom(String.t()) :: {:ok, atom()} | {:error, term()}
  def cookie_to_atom(cookie) do
    with :ok <- validate_cookie(cookie) do
      {:ok, String.to_atom(cookie)}
    end
  end

  @spec node_name_to_atom(String.t()) :: {:ok, atom()} | {:error, term()}
  def node_name_to_atom(node_name) do
    with :ok <- validate_node_name(node_name) do
      {:ok, String.to_atom(node_name)}
    end
  end

  @spec short_node_name_to_atom(String.t()) :: {:ok, atom()} | {:error, term()}
  def short_node_name_to_atom(node_name) do
    with :ok <- validate_short_node_name(node_name) do
      {:ok, String.to_atom(node_name)}
    end
  end

  @spec validate_cookie(term()) :: :ok | {:error, term()}
  def validate_cookie(cookie) when is_binary(cookie) do
    if valid_cookie?(cookie) do
      :ok
    else
      {:error, {:invalid_rpc_cookie, cookie}}
    end
  end

  def validate_cookie(cookie), do: {:error, {:invalid_rpc_cookie, cookie}}

  @spec validate_node_name(term()) :: :ok | {:error, term()}
  def validate_node_name(node_name) when is_binary(node_name) do
    case String.split(node_name, "@", parts: 2) do
      [short_name, host] ->
        if byte_size(node_name) <= @max_node_name_bytes and valid_node_part?(short_name) and
             valid_short_host?(host) do
          :ok
        else
          {:error, {:invalid_node_name, node_name}}
        end

      _other ->
        validate_short_node_name(node_name)
    end
  end

  def validate_node_name(node_name), do: {:error, {:invalid_node_name, node_name}}

  @spec validate_short_node_name(term()) :: :ok | {:error, term()}
  def validate_short_node_name(node_name) when is_binary(node_name) do
    if byte_size(node_name) <= @max_node_part_bytes and valid_node_part?(node_name) do
      :ok
    else
      {:error, {:invalid_node_name, node_name}}
    end
  end

  def validate_short_node_name(node_name), do: {:error, {:invalid_node_name, node_name}}

  @spec valid_short_host?(term()) :: boolean()
  def valid_short_host?(host) when is_binary(host) do
    not String.contains?(host, ".") and valid_node_part?(host)
  end

  def valid_short_host?(_host), do: false

  defp valid_cookie?(cookie) do
    byte_size(cookie) in 1..@max_cookie_bytes and String.match?(cookie, @cookie_pattern)
  end

  defp valid_node_part?(part) when is_binary(part) do
    byte_size(part) in 1..@max_node_part_bytes and String.match?(part, @node_part_pattern)
  end
end
