defmodule Favn.Dev.Build.RunnerReleaseInput do
  @moduledoc false

  alias Favn.Dev.Build.Artifact
  alias Favn.RunnerRelease

  @builder_image "hexpm/elixir:1.20.2-erlang-28.3.3-debian-bookworm-20260713-slim@sha256:874b36d3e432c42a4f78e12fbe251c5e6c3b1342c8f1072e25dc418b823c31ba"
  @runtime_image "debian:bookworm-slim@sha256:63a496b5d3b99214b39f5ed70eb71a61e590a77979c79cbee4faf991f8c0783e"
  @release_version "1.0.0"
  @spec write(Path.t(), map(), keyword()) :: :ok | {:error, term()}
  def write(artifact_dir, inputs, _opts \\ []) when is_binary(artifact_dir) and is_map(inputs) do
    release_input = Path.join(artifact_dir, "release-input")

    with :ok <- File.mkdir_p(release_input),
         :ok <- copy_applications(release_input, inputs.application_sources),
         :ok <- copy_customer_beams(release_input, inputs),
         :ok <- copy_descriptor(release_input, artifact_dir),
         :ok <- write_mix_project(release_input, inputs),
         :ok <- write_dependency_lock(release_input, inputs),
         :ok <-
           write_runtime_config(
             release_input,
             inputs.packaged_config,
             inputs.current_application
           ),
         :ok <- write_stamp_script(release_input, inputs),
         :ok <- write_release_env(release_input),
         :ok <- write_dockerfile(artifact_dir, inputs.descriptor) do
      :ok
    end
  end

  defp write_dependency_lock(release_input, inputs) do
    Artifact.write_json(Path.join(release_input, "dependency-lock.json"), %{
      "schema_version" => 1,
      "applications" =>
        Enum.map(inputs.applications, fn application ->
          %{
            "application" => application.application,
            "version" => application.version,
            "lock_fingerprint" => application.lock_fingerprint
          }
        end)
    })
  end

  defp copy_applications(release_input, sources) do
    Enum.reduce_while(sources, :ok, fn {app, source}, :ok ->
      case Artifact.copy_tree(source, Path.join([release_input, "apps", app])) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp copy_customer_beams(release_input, inputs) do
    application_names = MapSet.new(inputs.applications, & &1.application)
    target = Path.join([release_input, "apps", "favn_runner", "priv", "customer_ebin"])

    with :ok <- File.mkdir_p(target) do
      inputs.closure.modules
      |> Enum.reduce_while(:ok, fn fingerprint, :ok ->
        entry = Map.fetch!(inputs.inventory, fingerprint.module)

        if MapSet.member?(application_names, Atom.to_string(entry.app)) do
          {:cont, :ok}
        else
          case File.write(Path.join(target, fingerprint.module <> ".beam"), entry.beam) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end
      end)
    end
  end

  defp copy_descriptor(release_input, artifact_dir) do
    File.cp(
      Path.join(artifact_dir, "runner-release.json"),
      Path.join([release_input, "apps", "favn_runner", "priv", "runner-release.json"])
    )
  end

  defp write_mix_project(release_input, inputs) do
    build_only = MapSet.new(inputs.build_only_applications)

    deps =
      inputs.application_sources
      |> Map.keys()
      |> Enum.sort()
      |> Enum.map_join(",\n", fn app ->
        runtime = if MapSet.member?(build_only, app), do: ", runtime: false", else: ""
        "      {:#{app}, path: \"apps/#{app}\", override: true#{runtime}}"
      end)

    extra_applications =
      inputs.applications
      |> Enum.map(& &1.application)
      |> Enum.reject(&(&1 == "favn_runner"))
      |> Enum.sort()
      |> Enum.map_join(", ", &(":" <> &1))

    source = """
    defmodule FavnCustomerRunnerRelease.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_customer_runner_release,
          version: "#{@release_version}",
          elixir: "~> 1.20",
          consolidate_protocols: false,
          start_permanent: Mix.env() == :prod,
          deps: deps(),
          releases: [
            favn_runner: [
              applications: [favn_customer_runner_release: :permanent, favn_runner: :permanent],
              strip_beams: true
            ]
          ]
        ]
      end

      def application do
        [extra_applications: [#{extra_applications}]]
      end

      defp deps do
        [
    #{deps}
        ]
      end
    end
    """

    with :ok <- File.write(Path.join(release_input, "mix.exs"), source),
         :ok <- File.mkdir_p(Path.join(release_input, "lib")) do
      File.write(
        Path.join(release_input, "lib/favn_customer_runner_release.ex"),
        "defmodule FavnCustomerRunnerRelease do\n  @moduledoc false\nend\n"
      )
    end
  end

  defp write_runtime_config(release_input, config, current_application) do
    encoded = config |> :erlang.term_to_binary() |> Base.encode64()

    source = """
    import Config

    runner_config =
      "#{encoded}"
      |> Base.decode64!()
      |> :erlang.binary_to_term()

    Enum.each(runner_config.compile_env, fn {app, key, value} ->
      config app, key, value
    end)

    config :favn, runner_config.favn
    """

    path = Path.join(release_input, "config/config.exs")

    case Code.string_to_quoted(source, file: "release-input/config/config.exs") do
      {:ok, _quoted} ->
        with :ok <- File.mkdir_p(Path.dirname(path)),
             :ok <- File.write(path, source) do
          write_customer_runtime_config(release_input, current_application)
        end

      {:error, _reason} ->
        {:error, :unsupported_runner_config_term}
    end
  end

  defp write_customer_runtime_config(release_input, current_application) do
    customer_runtime =
      Path.join([release_input, "apps", current_application, "config/runtime.exs"])

    if File.regular?(customer_runtime) do
      overlay =
        Path.join([
          release_input,
          "rel/overlays/releases",
          @release_version,
          "customer_config"
        ])

      with {:ok, source} <- File.read(customer_runtime),
           :ok <- validate_customer_runtime_config(source),
           :ok <- File.mkdir_p(overlay),
           :ok <- File.write(Path.join(overlay, "runtime.exs"), source) do
        File.write(
          Path.join(release_input, "config/runtime.exs"),
          "import Config\n\nimport_config \"customer_config/runtime.exs\"\n"
        )
      end
    else
      :ok
    end
  end

  defp validate_customer_runtime_config(source) do
    with {:ok, quoted} <- Code.string_to_quoted(source, file: "customer config/runtime.exs"),
         false <- contains_import_config?(quoted) do
      :ok
    else
      true -> {:error, :customer_runtime_config_imports_unsupported}
      {:error, _reason} -> {:error, :invalid_customer_runtime_config}
    end
  end

  defp contains_import_config?(quoted) do
    {_quoted, found?} =
      Macro.prewalk(quoted, false, fn
        {:import_config, _metadata, _arguments} = node, _found -> {node, true}
        node, found -> {node, found}
      end)

    found?
  end

  defp write_stamp_script(release_input, inputs) do
    entries =
      inputs.applications
      |> Enum.map(fn app -> {String.to_atom(app.application), app.lock_fingerprint} end)
      |> inspect(pretty: true, limit: :infinity)

    current_application = String.to_atom(inputs.current_application)

    selected_current_modules =
      inputs.closure.modules
      |> Enum.filter(fn module ->
        Map.fetch!(inputs.inventory, module.module).app == current_application
      end)
      |> Enum.map(&String.to_atom(&1.module))
      |> Enum.sort()
      |> inspect(pretty: true, limit: :infinity)

    build_only_applications =
      inputs.build_only_applications
      |> Enum.map(&String.to_atom/1)
      |> inspect(pretty: true, limit: :infinity)

    source = """
    applications = #{entries}
    current_application = #{inspect(current_application)}
    selected_current_modules = #{selected_current_modules}
    build_only_applications = #{build_only_applications}

    Enum.each(applications, fn {application, fingerprint} ->
      path = Path.join([Mix.Project.build_path(), "lib", Atom.to_string(application), "ebin", "\#{application}.app"])
      {:ok, [{:application, ^application, properties}]} = :file.consult(String.to_charlist(path))
      properties =
        properties
        |> Keyword.put(:favn_runner_lock_fingerprint, fingerprint)
        |> Keyword.update(:applications, [], &(&1 -- build_only_applications))
        |> Keyword.update(:included_applications, [], &(&1 -- build_only_applications))

      properties =
        if application == current_application do
          ebin = Path.dirname(path)
          selected_beams = MapSet.new(selected_current_modules, &(Atom.to_string(&1) <> ".beam"))

          ebin
          |> Path.join("*.beam")
          |> Path.wildcard()
          |> Enum.reject(&MapSet.member?(selected_beams, Path.basename(&1)))
          |> Enum.each(&File.rm!/1)

          properties
          |> Keyword.put(:modules, selected_current_modules)
        else
          properties
        end

      contents = :io_lib.format("~p.~n", [{:application, application, properties}])
      File.write!(path, contents)
    end)
    """

    File.write(Path.join(release_input, "stamp_apps.exs"), source)
  end

  defp write_release_env(release_input) do
    path = Path.join(release_input, "rel/env.sh.eex")

    with :ok <- File.mkdir_p(Path.dirname(path)) do
      File.write(
        path,
        """
        #!/bin/sh
        set -eu

        : "${FAVN_RUNNER_NODE:?FAVN_RUNNER_NODE is required}"
        : "${FAVN_DISTRIBUTION_COOKIE:?FAVN_DISTRIBUTION_COOKIE is required}"
        : "${FAVN_BEAM_DISTRIBUTION_PORT:?FAVN_BEAM_DISTRIBUTION_PORT is required}"

        case "$FAVN_BEAM_DISTRIBUTION_PORT" in
          *[!0-9]*) echo "invalid FAVN_BEAM_DISTRIBUTION_PORT" >&2; exit 1 ;;
        esac

        if [ "$FAVN_BEAM_DISTRIBUTION_PORT" -lt 1 ] || [ "$FAVN_BEAM_DISTRIBUTION_PORT" -gt 65535 ]; then
          echo "invalid FAVN_BEAM_DISTRIBUTION_PORT" >&2
          exit 1
        fi

        export RELEASE_DISTRIBUTION=name
        export RELEASE_NODE="$FAVN_RUNNER_NODE"
        export RELEASE_COOKIE="$FAVN_DISTRIBUTION_COOKIE"
        export ERL_AFLAGS="${ERL_AFLAGS:-} -kernel inet_dist_listen_min $FAVN_BEAM_DISTRIBUTION_PORT inet_dist_listen_max $FAVN_BEAM_DISTRIBUTION_PORT"
        """
      )
    end
  end

  defp write_dockerfile(artifact_dir, %RunnerRelease{} = descriptor) do
    revision = Map.get(descriptor.build_metadata, "source_revision", "unknown")

    dockerfile = """
    FROM --platform=linux/amd64 #{@builder_image} AS builder

    ENV MIX_ENV=prod
    WORKDIR /build
    RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates git cmake pkg-config && rm -rf /var/lib/apt/lists/*
    RUN mix local.hex --force && mix local.rebar --force
    COPY release-input/ ./
    RUN mix deps.get --only prod && mix deps.compile && mix compile
    RUN mix run --no-start stamp_apps.exs
    RUN mix release favn_runner

    FROM --platform=linux/amd64 #{@runtime_image} AS runtime
    ARG BUILD_DATE=unknown
    ARG RUNNER_DIST_PORT=9100
    RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libgcc-s1 openssl libncurses6 && rm -rf /var/lib/apt/lists/* \\
      && groupadd --system --gid 10001 favn \\
      && useradd --system --uid 10001 --gid favn --home-dir /opt/favn --shell /usr/sbin/nologin favn \\
      && mkdir -p /opt/favn /tmp/favn \\
      && chown -R favn:favn /opt/favn /tmp/favn
    WORKDIR /opt/favn
    COPY --from=builder --chown=favn:favn /build/_build/prod/rel/favn_runner/ ./
    ENV HOME=/tmp/favn TMPDIR=/tmp/favn RELEASE_TMP=/tmp/favn
    LABEL org.opencontainers.image.title="Favn customer runner" \\
      org.opencontainers.image.created="${BUILD_DATE}" \\
      org.opencontainers.image.revision="#{revision}" \\
      io.favn.runner-release-id="#{descriptor.runner_release_id}" \\
      io.favn.version="#{descriptor.favn_version}" \\
      io.favn.runner-contract-version="#{descriptor.runner_contract_version}" \\
      io.favn.target="#{descriptor.target}"
    USER 10001:10001
    VOLUME ["/tmp/favn"]
    EXPOSE 4369 ${RUNNER_DIST_PORT}
    ENTRYPOINT ["/opt/favn/bin/favn_runner"]
    CMD ["start"]
    """

    File.write(Path.join(artifact_dir, "Dockerfile"), dockerfile)
  end
end
