defmodule FavnStoragePostgres.AdminSecret do
  @moduledoc false

  import Bitwise
  require Record

  Record.defrecordp(:file_info, Record.extract(:file_info, from_lib: "kernel/include/file.hrl"))

  @max_password_bytes 1_024

  @spec read(keyword(), String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def read(opts, prompt) when is_list(opts) and is_binary(prompt) do
    case Keyword.get(opts, :password_file) do
      nil -> read_stdin(prompt)
      path when is_binary(path) -> read_protected_file(path)
    end
  end

  @spec read_protected_file(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def read_protected_file(path) when is_binary(path) do
    with :ok <- supported_file_platform(),
         {:ok, password} <- read_verified_file(path) do
      normalize(password)
    else
      {:error, :enoent} -> {:error, "password file does not exist"}
      {:error, :eacces} -> {:error, "password file is not readable"}
      {:error, message} when is_binary(message) -> {:error, message}
      {:error, _reason} -> {:error, "password file could not be read safely"}
    end
  end

  defp read_verified_file(path) do
    case File.open(path, [:read, :binary]) do
      {:ok, io} ->
        try do
          with {:ok, stat} <- :file.read_file_info(io, time: :posix),
               :ok <- regular_file(file_info(stat, :type)),
               :ok <- private_mode(file_info(stat, :mode)),
               :ok <- bounded_size(file_info(stat, :size)),
               password when is_binary(password) <- IO.binread(io, @max_password_bytes + 3) do
            {:ok, password}
          else
            :eof -> {:error, "password file must contain 1..1024 bytes"}
            {:error, reason} -> {:error, reason}
          end
        after
          File.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_stdin(prompt) do
    IO.write(:stderr, prompt <> ": ")

    case :io.get_password() do
      password when is_list(password) -> password |> List.to_string() |> normalize()
      :eof -> {:error, "password input ended before a password was provided"}
      {:error, _reason} -> {:error, "password could not be read from stdin"}
    end
  end

  defp supported_file_platform do
    case :os.type() do
      {:unix, _name} ->
        :ok

      {:win32, _name} ->
        {:error, "use stdin on Windows; portable ACL verification is unavailable"}
    end
  end

  defp regular_file(:regular), do: :ok
  defp regular_file(_type), do: {:error, "password file must be a regular file"}

  defp private_mode(mode) when is_integer(mode) do
    if band(mode, 0o077) == 0,
      do: :ok,
      else: {:error, "password file must not be readable, writable, or executable by group/other"}
  end

  defp bounded_size(size) when size >= 1 and size <= @max_password_bytes + 2, do: :ok
  defp bounded_size(_size), do: {:error, "password file must contain 1..1024 bytes"}

  defp normalize(password) when is_binary(password) do
    password = password |> String.trim_trailing("\n") |> String.trim_trailing("\r")

    cond do
      password == "" -> {:error, "password must not be empty"}
      byte_size(password) > @max_password_bytes -> {:error, "password must not exceed 1024 bytes"}
      String.contains?(password, <<0>>) -> {:error, "password must not contain a null byte"}
      true -> {:ok, password}
    end
  end
end
