defmodule Favn.Dev.Init do
  @moduledoc """
  Dispatches the two explicit Favn initialization modes.

  DuckDB authoring samples and consumer-owned deployment templates have
  separate ownership and overwrite contracts, so callers must select one
  mode explicitly.
  """

  alias Favn.Dev.Init.{Compose, Runner, Sample}

  @type result :: Sample.result() | Compose.result() | Runner.result()

  @doc "Initializes the selected authoring sample or deployment template."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    case Keyword.get(opts, :target) do
      :compose -> Compose.run(opts)
      "compose" -> Compose.run(opts)
      :runner -> Runner.run(opts)
      "runner" -> Runner.run(opts)
      nil -> Sample.run(opts)
      target -> {:error, {:unsupported_init_target, target}}
    end
  end
end
