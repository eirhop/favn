defmodule Favn.Catalog do
  @moduledoc """
  Publishes immutable manifest and semantic catalogs directly from precompiled CI.

  `publish/2` accepts parsed options and trusted dedicated configuration, without
  reading global runtime configuration, starting the customer application or an
  orchestrator/runner. Targets name an explicit connection, catalog and schema.
  `connection_modules` is a name-to-module keyword/map for scoped resolution.

  `rebuild/2` regenerates derived metadata from retained artifacts in the same
  target without changing publication selections or receipts.

  Inputs must come from trusted builds. Publishing definitions never proves that
  compatible data is served: every receipt reports compatibility as `unknown`.
  The overall deadline defaults to five minutes and cannot exceed fifteen.
  """
  alias Favn.Catalog.{Artifact, Runtime}
  alias Favn.Connection.{Loader, Registry}
  alias Favn.Semantic.Artifact, as: SemanticArtifact
  alias Favn.SQL.Catalog.{Publisher, Request}
  alias Favn.SQL.Deadline

  @doc "Publishes selected artifacts, or reads a receipt when `:reconcile` is true."
  @spec publish(keyword(), keyword()) :: {:ok, map()} | {:error, map()}
  def publish(opts, config) when is_list(opts) and is_list(config),
    do: run(opts, config, if(opts[:reconcile], do: :reconcile, else: :publish))

  @doc """
  Rebuilds derived catalog tables from all retained artifacts in one target.

  Accepts `:target` and optional `:timeout_ms` (300000 by default, at most 900000),
  using the same dedicated configuration as `publish/2`. Preserves releases,
  selections, receipts and macros. Stop publishers during maintenance. An uncertain
  commit returns `rebuild_outcome_unknown`; no write is automatically retried.
  """
  @spec rebuild(keyword(), keyword()) :: {:ok, map()} | {:error, map()}
  def rebuild(opts, config) when is_list(opts) and is_list(config) do
    if Keyword.keys(opts) -- [:target, :config, :timeout_ms] == [],
      do: run(opts, config, :rebuild),
      else: failure(:invalid_rebuild_request)
  end

  defp run(opts, config, mode) do
    timeout = Keyword.get(opts, :timeout_ms, 300_000)

    if is_integer(timeout) and timeout in 1..900_000 do
      case Runtime.start() do
        {:ok, runtime} -> publish_with_runtime(opts, config, timeout, runtime, mode)
        {:error, _} -> failure(:catalog_runtime_busy)
      end
    else
      failure(:invalid_timeout)
    end
  end

  defp publish_with_runtime(opts, config, timeout, runtime, mode) do
    deadline = Deadline.new(timeout)
    {:ok, supervisor} = Task.Supervisor.start_link()
    owner = self()
    ref = make_ref()

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        prepare(opts, config, deadline, runtime, {owner, ref}, mode)
      end)

    try do
      case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
        {:ok, result} ->
          result

        _ ->
          receive do
            {^ref, request} ->
              {:error,
               %{
                 "outcome" => "error",
                 "reason" =>
                   if(mode == :rebuild,
                     do: "rebuild_outcome_unknown",
                     else: "publication_outcome_unknown"
                   ),
                 "operation_id" => request.operation_id,
                 "target" => request.target,
                 "compatibility" => "unknown"
               }}
          after
            0 -> failure(:deadline_exceeded)
          end
      end
    after
      Supervisor.stop(supervisor, :normal, 1000)
      Runtime.close(runtime)

      receive do
        {^ref, _} -> :ok
      after
        0 -> :ok
      end
    end
  end

  defp prepare(opts, config, deadline, runtime_owner, {owner, ref}, mode) do
    with {:ok, target, settings} <- target(config[:catalog_targets], opts[:target]),
         {:ok, request} <- request(mode, target, settings, opts),
         _ <- send(owner, {ref, request}),
         {:ok, module} <- entry(config[:connection_modules], request.connection),
         {:ok, runtime} <- entry(config[:connections], request.connection),
         {:ok, resolved} <- Loader.resolve_selected(request.connection, module, runtime),
         true <-
           Code.ensure_loaded?(resolved.adapter) and
             function_exported?(resolved.adapter, :catalog_publication_backend, 0) do
      backend = resolved.adapter.catalog_publication_backend()

      with :ok <-
             Runtime.start_applications(
               runtime_owner,
               [:favn_sql_runtime | backend.applications()],
               Deadline.remaining_ms(deadline)
             ) do
        {:ok, registry} =
          Registry.start_link(name: nil, connections: %{request.connection => resolved})

        try do
          Publisher.run(
            request,
            registry,
            deadline,
            mode,
            Keyword.take(config, [:duckdb_adbc])
          )
        after
          if Process.alive?(registry), do: GenServer.stop(registry, :normal, 1000)
        end
      else
        {:error, reason} -> failure(reason)
      end
    else
      {:error, reason} when is_atom(reason) -> failure(reason)
      _ -> failure(:invalid_catalog_configuration)
    end
  rescue
    _ -> failure(:invalid_catalog_configuration)
  end

  defp request(:rebuild, target, settings, _opts), do: Request.rebuild(target, settings)

  defp request(_, target, settings, opts) do
    with {:ok, artifacts} <- artifacts(opts),
         {:ok, expectations} <- expectations(opts),
         do: Request.new(target, settings, artifacts, expectations)
  end

  defp target(entries, name) when is_binary(name) do
    entries = entries || []
    pairs = Enum.to_list(entries)
    selected = Enum.filter(pairs, fn {key, _} -> to_string(key) == name end)

    boundaries =
      Enum.map(pairs, fn {_, value} -> {value[:connection], value[:catalog], value[:schema]} end)

    case selected do
      [{_, value}] when is_list(value) ->
        if boundaries == Enum.uniq(boundaries),
          do: {:ok, name, value},
          else: {:error, :duplicate_catalog_target}

      _ ->
        {:error, :unknown_catalog_target}
    end
  end

  defp target(_, _), do: {:error, :unknown_catalog_target}

  defp entry(entries, name) when is_map(entries), do: Map.fetch(entries, name)
  defp entry(entries, name) when is_list(entries), do: Keyword.fetch(entries, name)
  defp entry(_, _), do: {:error, :invalid_catalog_configuration}

  defp artifacts(opts) do
    Enum.reduce_while([{:manifest, Artifact}, {:semantics, SemanticArtifact}], {:ok, []}, fn {key,
                                                                                              module},
                                                                                             {:ok,
                                                                                              acc} ->
      if path = opts[key] do
        case module.read(path) do
          {:ok, artifact} -> {:cont, {:ok, [artifact | acc]}}
          _ -> {:halt, {:error, :invalid_catalog_input}}
        end
      else
        {:cont, {:ok, acc}}
      end
    end)
  end

  defp expectations(opts) do
    Enum.reduce_while(
      [{:expect_manifest, "manifest"}, {:expect_semantics, "semantic"}],
      {:ok, %{}},
      fn {key, kind}, {:ok, acc} ->
        if value = opts[key] do
          case Request.expectation(value) do
            {:ok, item} -> {:cont, {:ok, Map.put(acc, kind, item)}}
            error -> {:halt, error}
          end
        else
          {:cont, {:ok, acc}}
        end
      end
    )
  end

  defp failure(code),
    do:
      {:error, %{"outcome" => "error", "reason" => to_string(code), "compatibility" => "unknown"}}
end
