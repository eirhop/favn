defmodule FavnLocal.Distribution do
  @moduledoc false

  @spec start(node(), String.t()) :: :ok | {:error, term()}
  def start(name, cookie) when is_atom(name) and is_binary(cookie) do
    with :ok <- ensure_epmd(),
         {:ok, _pid} <- Node.start(name, :longnames) do
      Node.set_cookie(String.to_atom(cookie))
      :ok
    end
  end

  defp ensure_epmd do
    case System.find_executable("epmd") do
      nil ->
        {:error, :epmd_not_found}

      executable ->
        case System.cmd(executable, ["-daemon"], stderr_to_stdout: true) do
          {_output, 0} -> :ok
          {output, status} -> {:error, {:epmd_start_failed, status, String.trim(output)}}
        end
    end
  end
end
