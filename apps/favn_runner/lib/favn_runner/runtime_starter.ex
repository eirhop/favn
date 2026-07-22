defmodule FavnRunner.RuntimeStarter do
  @moduledoc false

  use GenServer

  alias FavnRunner.Lifecycle

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    case Lifecycle.mark_accepting() do
      :ok -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end
end
