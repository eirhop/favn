defmodule Favn.Dev.NodeControl do
  @moduledoc false

  @spec ensure_local_node_started(String.t(), keyword()) :: :ok | {:error, term()}
  def ensure_local_node_started(cookie, opts \\ []) when is_binary(cookie) and is_list(opts) do
    case Node.alive?() do
      true ->
        Node.set_cookie(String.to_atom(cookie))
        :ok

      false ->
        name =
          opts
          |> Keyword.get(:name, "favn_local_ctl_#{:erlang.unique_integer([:positive])}")
          |> String.to_atom()

        case Node.start(name, name_domain: :shortnames) do
          {:ok, _pid} ->
            Node.set_cookie(String.to_atom(cookie))
            :ok

          {:error, {:already_started, _pid}} ->
            Node.set_cookie(String.to_atom(cookie))
            :ok

          {:error, reason} ->
            {:error, {:node_start_failed, reason}}
        end
    end
  end
end
