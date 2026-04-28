defmodule Mix.Tasks.Favn.Doctor do
  use Mix.Task

  @shortdoc "Validates local Favn project setup"

  @moduledoc """
  Validates local Favn project setup before running the local stack.

      mix favn.doctor
  """

  alias Favn.Dev

  @switches [skip_compile: :boolean]

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest} do
      {[], []} -> doctor(opts)
      {_invalid, _rest} -> Mix.raise("usage: mix favn.doctor")
    end
  end

  defp doctor(opts) do
    unless Keyword.get(opts, :skip_compile, false), do: Mix.Task.run("compile")

    case Dev.doctor(opts) do
      {:ok, checks} ->
        print_checks(checks)
        IO.puts("Favn doctor passed")

      {:error, checks} ->
        print_checks(checks)
        Mix.raise("Favn doctor found #{failed_count(checks)} problem(s)")
    end
  end

  defp print_checks(checks) do
    Enum.each(checks, fn
      %{status: :ok, name: name, message: message} ->
        IO.puts("ok: #{name} - #{message}")

      %{status: :error, name: name, message: message} ->
        IO.puts("error: #{name} - #{message}")
    end)
  end

  defp failed_count(checks), do: Enum.count(checks, &(&1.status == :error))
end
