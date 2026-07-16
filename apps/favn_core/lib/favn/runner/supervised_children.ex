defmodule Favn.Runner.SupervisedChildren do
  @moduledoc """
  Simple runner plugin for supervising ordinary OTP child specifications.

  Use this when a dedicated plugin module would add no useful logic:

      config :favn,
        runner_plugins: [
          {Favn.Runner.SupervisedChildren,
           children: [MyApp.RuntimeCache, {MyApp.SessionPool, size: 4}]}
        ]

  For option validation or computed children, implement `Favn.Runner.Plugin`
  directly instead.
  """

  @behaviour Favn.Runner.Plugin

  @impl true
  def child_specs(opts) when is_list(opts) do
    with :ok <- validate_options(opts),
         children when is_list(children) <- Keyword.get(opts, :children, []) do
      {:ok, children}
    else
      {:error, reason} -> {:error, reason}
      _other -> {:error, {:invalid_option, :children, :expected_list}}
    end
  end

  def child_specs(_opts), do: {:error, :expected_keyword_options}

  defp validate_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, :expected_keyword_options}

      Keyword.keys(opts) -- [:children] != [] ->
        {:error, {:unknown_options, Keyword.keys(opts) -- [:children]}}

      true ->
        :ok
    end
  end
end
