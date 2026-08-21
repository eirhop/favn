defmodule FavnRunner.RuntimeBootstrap do
  @moduledoc false

  use GenServer

  alias FavnRunner.Lifecycle

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    case bootstrap(Keyword.get(opts, :mark_accepting?, true)) do
      :ok -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end

  defp bootstrap(true), do: Lifecycle.mark_accepting()
  defp bootstrap(false), do: :ok
end
