defmodule Favn.Dev.Logs do
  @moduledoc """
  Thin helper for reading project-local service logs under `.favn/logs`.
  """

  alias Favn.Dev.Paths

  @type root_opt :: [root_dir: Path.t()]
  @type logs_opt :: [
          root_dir: Path.t(),
          service: :all | :web | :orchestrator | :runner,
          tail: pos_integer(),
          follow: boolean()
        ]

  @spec run(logs_opt()) :: :ok
  def run(opts \\ []) when is_list(opts) do
    writer = Keyword.get(opts, :writer, &IO.write/1)
    follow? = Keyword.get(opts, :follow, false)
    tail_lines = max(1, Keyword.get(opts, :tail, 100))

    services = selected_services(opts)
    root_dir = Paths.root_dir(opts)

    Enum.each(services, fn service ->
      output_tail(service, log_path(service, root_dir), tail_lines, writer, length(services) > 1)
    end)

    if follow? do
      follow_logs(services, root_dir, writer, opts)
    else
      :ok
    end
  end

  @spec selected_services(keyword()) :: [:web | :orchestrator | :runner]
  def selected_services(opts) do
    case Keyword.get(opts, :service, :all) do
      :web -> [:web]
      :orchestrator -> [:orchestrator]
      :runner -> [:runner]
      _ -> [:web, :orchestrator, :runner]
    end
  end

  defp output_tail(service, path, tail_lines, writer, include_prefix?) do
    lines = read_tail_lines(path, tail_lines)

    Enum.each(lines, fn line ->
      writer.(format_line(service, line, include_prefix?))
    end)
  end

  defp read_tail_lines(path, tail_lines) do
    case File.read(path) do
      {:ok, content} ->
        content
        |> String.split("\n", trim: true)
        |> Enum.take(-tail_lines)
        |> Enum.map(&(&1 <> "\n"))

      {:error, :enoent} ->
        []

      {:error, _reason} ->
        []
    end
  end

  defp follow_logs(services, root_dir, writer, opts) do
    include_prefix? = length(services) > 1
    sleep_ms = Keyword.get(opts, :follow_sleep_ms, 200)
    ticks = Keyword.get(opts, :follow_ticks, :infinity)

    initial_offsets =
      Map.new(services, fn service ->
        path = log_path(service, root_dir)
        {service, file_size(path)}
      end)

    do_follow(services, root_dir, writer, include_prefix?, initial_offsets, ticks, sleep_ms)
  end

  defp do_follow(_services, _root_dir, _writer, _include_prefix?, _offsets, 0, _sleep_ms), do: :ok

  defp do_follow(services, root_dir, writer, include_prefix?, offsets, ticks, sleep_ms) do
    next_offsets =
      Enum.reduce(services, offsets, fn service, acc ->
        path = log_path(service, root_dir)
        previous = Map.get(acc, service, 0)

        case read_append(path, previous) do
          {:ok, "", offset} ->
            Map.put(acc, service, offset)

          {:ok, appended, offset} ->
            appended
            |> String.split("\n", trim: false)
            |> Enum.reject(&(&1 == ""))
            |> Enum.each(fn line ->
              writer.(format_line(service, line <> "\n", include_prefix?))
            end)

            Map.put(acc, service, offset)

          {:error, _reason} ->
            acc
        end
      end)

    Process.sleep(sleep_ms)

    next_ticks =
      case ticks do
        :infinity -> :infinity
        number -> number - 1
      end

    do_follow(services, root_dir, writer, include_prefix?, next_offsets, next_ticks, sleep_ms)
  end

  defp read_append(path, offset) do
    case File.read(path) do
      {:ok, content} ->
        size = byte_size(content)

        cond do
          size < offset -> {:ok, content, size}
          size == offset -> {:ok, "", offset}
          true -> {:ok, binary_part(content, offset, size - offset), size}
        end

      {:error, :enoent} ->
        {:ok, "", offset}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, stat} -> stat.size
      {:error, _reason} -> 0
    end
  end

  defp format_line(service, line, true), do: "[#{service}] " <> line
  defp format_line(_service, line, false), do: line

  defp log_path(:web, root_dir), do: Paths.web_log_path(root_dir)
  defp log_path(:orchestrator, root_dir), do: Paths.orchestrator_log_path(root_dir)
  defp log_path(:runner, root_dir), do: Paths.runner_log_path(root_dir)
end
