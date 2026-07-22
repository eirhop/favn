defmodule Favn.Dev.Build.RunnerReleaseInput do
  @moduledoc false

  alias Favn.Dev.Build.Artifact
  alias Favn.RunnerRelease

  @builder_image "hexpm/elixir:1.20.2-erlang-29.0.3-debian-trixie-20260713-slim@sha256:6fcd8ea864221b960c1ec418e3b10fa488298ff9e70c9e0f3db18070e610fb8a"
  @runtime_image "debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd"
  @debian_snapshot "20260713T000000Z"
  @elixir_version "1.20.2"
  @otp_version "29.0.3"
  @otp_release "29"
  @hex_version "2.5.1"
  @rebar_url "https://github.com/erlang/rebar3/releases/download/3.27.0/rebar3"
  @rebar_sha512 "0d00494d849fdc521a55142278d1f6ba552954fbd65b80d40df8022f594f05d6c99ed1d731bc263691a04176e11d4c6e126c56ba20dca19c5e42d4ffab2e7e36"
  @release_version "1.0.0"
  @builder_packages ~w(build-essential ca-certificates git cmake pkg-config)

  @type toolchain :: %{
          required(:elixir_version) => String.t(),
          required(:otp_release) => String.t()
        }

  @doc false
  @spec expected_toolchain() :: toolchain()
  def expected_toolchain do
    %{elixir_version: @elixir_version, otp_release: @otp_release}
  end

  @doc false
  @spec validate_host_toolchain() ::
          :ok | {:error, {:runner_build_toolchain_mismatch, toolchain(), toolchain()}}
  def validate_host_toolchain, do: validate_host_toolchain([])

  @doc false
  @spec validate_host_toolchain(keyword()) ::
          :ok | {:error, {:runner_build_toolchain_mismatch, toolchain(), toolchain()}}
  def validate_host_toolchain(opts) when is_list(opts) do
    actual =
      case {Mix.env(), Keyword.get(opts, :host_toolchain)} do
        {:test, %{elixir_version: elixir_version, otp_release: otp_release}} ->
          %{elixir_version: elixir_version, otp_release: otp_release}

        _runtime ->
          %{
            elixir_version: System.version(),
            otp_release: to_string(:erlang.system_info(:otp_release))
          }
      end

    validate_toolchain(actual)
  end

  @doc false
  @spec validate_toolchain(toolchain()) ::
          :ok | {:error, {:runner_build_toolchain_mismatch, toolchain(), toolchain()}}
  def validate_toolchain(actual) when is_map(actual) do
    expected = expected_toolchain()

    if actual == expected do
      :ok
    else
      {:error, {:runner_build_toolchain_mismatch, expected, actual}}
    end
  end

  @spec write(Path.t(), map(), keyword()) :: :ok | {:error, term()}
  def write(artifact_dir, inputs, _opts \\ []) when is_binary(artifact_dir) and is_map(inputs) do
    dependency_input = Path.join(artifact_dir, "dependency-input")
    application_input = Path.join(artifact_dir, "application-input")

    with :ok <- File.mkdir_p(dependency_input),
         :ok <- File.mkdir_p(application_input),
         :ok <- copy_dependency_applications(dependency_input, inputs),
         :ok <- copy_current_application(application_input, inputs),
         :ok <- copy_customer_beams(application_input, inputs),
         :ok <- copy_descriptor(application_input, artifact_dir),
         :ok <- write_mix_project(dependency_input, inputs, :dependencies),
         :ok <- write_mix_project(application_input, inputs, :release),
         :ok <- write_mix_lock(dependency_input, inputs.dependency_lock),
         :ok <- write_dependency_lock(application_input, inputs),
         :ok <-
           write_runtime_config(
             dependency_input,
             inputs.packaged_config,
             application_input,
             inputs.current_application
           ),
         :ok <- write_stamp_script(application_input, inputs),
         :ok <- write_release_env(application_input),
         :ok <- write_dependency_identity(dependency_input),
         :ok <-
           write_dockerfile(artifact_dir, inputs.descriptor, inputs.current_application) do
      :ok
    end
  end

  defp write_mix_lock(release_input, dependency_lock) do
    entries =
      dependency_lock
      |> Enum.sort_by(fn {app, _entry} -> app end)
      |> Enum.map(fn {app, entry} ->
        encoded =
          inspect(entry,
            pretty: false,
            limit: :infinity,
            printable_limit: :infinity,
            width: :infinity,
            charlists: :as_lists
          )

        ~s(  "#{app}": #{encoded},\n)
      end)

    File.write(Path.join(release_input, "mix.lock"), ["%{\n", entries, "}\n"])
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

  defp copy_dependency_applications(dependency_input, inputs) do
    inputs.application_sources
    |> Map.delete(inputs.current_application)
    |> Enum.reduce_while(:ok, fn {app, source}, :ok ->
      case Artifact.copy_tree(source, Path.join([dependency_input, "apps", app])) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp copy_current_application(application_input, inputs) do
    case Map.fetch(inputs.application_sources, inputs.current_application) do
      {:ok, source} ->
        Artifact.copy_tree(
          source,
          Path.join([application_input, "apps", inputs.current_application])
        )

      :error ->
        {:error, {:runner_application_source_missing, inputs.current_application}}
    end
  end

  defp copy_customer_beams(application_input, inputs) do
    application_names = MapSet.new(inputs.applications, & &1.application)
    target = Path.join([application_input, "runner-priv", "customer_ebin"])

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

  defp copy_descriptor(application_input, artifact_dir) do
    File.cp(
      Path.join(artifact_dir, "runner-release.json"),
      Path.join([application_input, "runner-priv", "runner-release.json"])
    )
  end

  defp write_mix_project(input_dir, inputs, kind) when kind in [:dependencies, :release] do
    build_only = MapSet.new(inputs.build_only_applications)

    deps =
      inputs.application_sources
      |> maybe_without_current(inputs.current_application, kind)
      |> Map.keys()
      |> Enum.sort()
      |> Enum.map_join(",\n", fn app ->
        runtime = if MapSet.member?(build_only, app), do: ", runtime: false", else: ""
        "      {:#{app}, path: \"apps/#{app}\", override: true#{runtime}}"
      end)

    extra_applications =
      inputs.applications
      |> Enum.map(& &1.application)
      |> maybe_reject_current(inputs.current_application, kind)
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

    with :ok <- File.write(Path.join(input_dir, "mix.exs"), source) do
      maybe_write_release_module(input_dir, kind)
    end
  end

  defp maybe_without_current(sources, current_application, :dependencies),
    do: Map.delete(sources, current_application)

  defp maybe_without_current(sources, _current_application, :release), do: sources

  defp maybe_reject_current(applications, current_application, :dependencies),
    do: Enum.reject(applications, &(&1 == current_application))

  defp maybe_reject_current(applications, _current_application, :release), do: applications

  defp maybe_write_release_module(_input_dir, :dependencies), do: :ok

  defp maybe_write_release_module(input_dir, :release) do
    with :ok <- File.mkdir_p(Path.join(input_dir, "lib")) do
      File.write(
        Path.join(input_dir, "lib/favn_customer_runner_release.ex"),
        "defmodule FavnCustomerRunnerRelease do\n  @moduledoc false\nend\n"
      )
    end
  end

  defp write_runtime_config(dependency_input, config, application_input, current_application) do
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

    path = Path.join(dependency_input, "config/config.exs")

    case Code.string_to_quoted(source, file: "dependency-input/config/config.exs") do
      {:ok, _quoted} ->
        with :ok <- File.mkdir_p(Path.dirname(path)),
             :ok <- File.write(path, source) do
          write_customer_runtime_config(application_input, current_application)
        end

      {:error, _reason} ->
        {:error, :unsupported_runner_config_term}
    end
  end

  defp write_customer_runtime_config(application_input, current_application) do
    customer_runtime =
      Path.join([application_input, "apps", current_application, "config/runtime.exs"])

    runtime_config = Path.join(application_input, "config/runtime.exs")

    if File.regular?(customer_runtime) do
      overlay =
        Path.join([
          application_input,
          "rel/overlays/releases",
          @release_version,
          "customer_config"
        ])

      with {:ok, source} <- File.read(customer_runtime),
           :ok <- validate_customer_runtime_config(source),
           :ok <- File.mkdir_p(overlay),
           :ok <- File.write(Path.join(overlay, "runtime.exs"), source) do
        with :ok <- File.mkdir_p(Path.dirname(runtime_config)) do
          File.write(
            runtime_config,
            "import Config\n\nimport_config \"customer_config/runtime.exs\"\n"
          )
        end
      end
    else
      with :ok <- File.mkdir_p(Path.dirname(runtime_config)) do
        File.write(runtime_config, "import Config\n")
      end
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

  defp write_stamp_script(application_input, inputs) do
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

    File.write(Path.join(application_input, "stamp_apps.exs"), source)
  end

  defp write_release_env(application_input) do
    path = Path.join(application_input, "rel/env.sh.eex")

    with :ok <- File.mkdir_p(Path.dirname(path)) do
      File.write(
        path,
        """
        #!/bin/sh
        set -eu

        : "${FAVN_RUNNER_NODE:?FAVN_RUNNER_NODE is required}"
        : "${FAVN_DISTRIBUTION_COOKIE:?FAVN_DISTRIBUTION_COOKIE is required}"
        : "${FAVN_BEAM_DISTRIBUTION_PORT:?FAVN_BEAM_DISTRIBUTION_PORT is required}"

        validate_node_name() {
          value=$1

          case "$value" in
            *@*@*|@*|*@|*[!A-Za-z0-9_.@-]*) return 1 ;;
          esac

          local_name=${value%%@*}
          host=${value#*@}
          [ "$local_name" != "$value" ] || return 1
          [ "${#local_name}" -le 255 ] || return 1
          [ "${#host}" -le 255 ] || return 1

          normalized_host=$(printf '%s' "$host" | tr '[:upper:]' '[:lower:]')

          case "$normalized_host" in
            localhost|nohost|127.*|::1|*.localhost) return 1 ;;
          esac
        }

        if ! validate_node_name "$FAVN_RUNNER_NODE"; then
          echo "invalid FAVN_RUNNER_NODE" >&2
          exit 1
        fi

        case "$FAVN_DISTRIBUTION_COOKIE" in
          *[[:space:]]*) echo "invalid FAVN_DISTRIBUTION_COOKIE" >&2; exit 1 ;;
        esac

        cookie_length=${#FAVN_DISTRIBUTION_COOKIE}
        unique_cookie_bytes=$(printf '%s' "$FAVN_DISTRIBUTION_COOKIE" | LC_ALL=C fold -w 1 | LC_ALL=C sort -u | wc -l | tr -d ' ')

        if [ "$cookie_length" -lt 32 ] || [ "$cookie_length" -gt 255 ] || [ "$unique_cookie_bytes" -lt 12 ]; then
          echo "invalid FAVN_DISTRIBUTION_COOKIE" >&2
          exit 1
        fi

        case "$FAVN_BEAM_DISTRIBUTION_PORT" in
          *[!0-9]*) echo "invalid FAVN_BEAM_DISTRIBUTION_PORT" >&2; exit 1 ;;
        esac

        if [ "$FAVN_BEAM_DISTRIBUTION_PORT" -lt 1 ] || [ "$FAVN_BEAM_DISTRIBUTION_PORT" -gt 65535 ]; then
          echo "invalid FAVN_BEAM_DISTRIBUTION_PORT" >&2
          exit 1
        fi

        ERL_EPMD_PORT="${ERL_EPMD_PORT:-4369}"
        case "$ERL_EPMD_PORT" in
          *[!0-9]*) echo "invalid ERL_EPMD_PORT" >&2; exit 1 ;;
        esac

        if [ "$ERL_EPMD_PORT" -lt 1 ] || [ "$ERL_EPMD_PORT" -gt 65535 ]; then
          echo "invalid ERL_EPMD_PORT" >&2
          exit 1
        fi

        export ERL_EPMD_PORT
        export RELEASE_DISTRIBUTION=name
        export RELEASE_NODE="$FAVN_RUNNER_NODE"
        export RELEASE_COOKIE="$FAVN_DISTRIBUTION_COOKIE"

        case "${RELEASE_COMMAND:-}" in
          start|start_iex|daemon|daemon_iex)
            export ERL_AFLAGS="${ERL_AFLAGS:-} -kernel inet_dist_listen_min $FAVN_BEAM_DISTRIBUTION_PORT inet_dist_listen_max $FAVN_BEAM_DISTRIBUTION_PORT"
            ;;
        esac
        """
      )
    end
  end

  defp write_dependency_identity(dependency_input) do
    files =
      dependency_input
      |> Path.join("**/*")
      |> Path.wildcard(match_dot: true)
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(fn path ->
        bytes = File.read!(path)

        %{
          "path" => Path.relative_to(path, dependency_input),
          "sha256" => sha256(bytes),
          "size" => byte_size(bytes)
        }
      end)
      |> Enum.sort_by(& &1["path"])

    inputs = %{
      "builder_image" => @builder_image,
      "builder_packages" => @builder_packages,
      "debian_snapshot" => @debian_snapshot,
      "files" => files,
      "hex_version" => @hex_version,
      "rebar_sha512" => @rebar_sha512,
      "rebar_url" => @rebar_url,
      "schema_version" => 1
    }

    Artifact.write_json(
      Path.join(dependency_input, "dependency-input.json"),
      Map.put(inputs, "digest", dependency_digest(inputs))
    )
  end

  defp dependency_digest(inputs) do
    inputs
    |> :erlang.term_to_binary([:deterministic])
    |> sha256()
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp write_dockerfile(artifact_dir, %RunnerRelease{} = descriptor, current_application) do
    with {:ok, revision} <- source_revision(descriptor.build_metadata) do
      dockerfile = """
      # syntax=docker/dockerfile:1.7
      FROM --platform=linux/amd64 #{@builder_image} AS toolchain

      ENV MIX_ENV=prod
      WORKDIR /build
      RUN sed -i \\
        -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/#{@debian_snapshot}|' \\
        -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/#{@debian_snapshot}|' \\
        /etc/apt/sources.list.d/debian.sources \\
        && apt-get -o Acquire::Check-Valid-Until=false update \\
        && apt-get -o Acquire::Check-Valid-Until=false install -y --no-install-recommends build-essential ca-certificates git cmake pkg-config \\
        && rm -rf /var/lib/apt/lists/*
      RUN mix local.hex #{@hex_version} --force \\
        && mix local.rebar rebar3 #{@rebar_url} --sha512 #{@rebar_sha512} --force
      FROM toolchain AS dependencies
      COPY dependency-input/ ./
      RUN --mount=type=cache,target=/root/.hex \
        --mount=type=cache,target=/root/.cache/rebar3 \
        mix deps.get --only prod --check-locked \
        && mix deps.compile

      FROM dependencies AS builder
      COPY application-input/mix.exs ./mix.exs
      COPY application-input/apps/ ./apps/
      COPY application-input/lib/ ./lib/
      COPY application-input/config/ ./config/
      COPY application-input/rel/ ./rel/
      COPY application-input/stamp_apps.exs ./stamp_apps.exs
      COPY application-input/dependency-lock.json ./dependency-lock.json
      RUN --mount=type=cache,target=/root/.hex \
        --mount=type=cache,target=/root/.cache/rebar3 \
        mix deps.get --only prod --check-locked \
        && mix deps.compile #{current_application} \
        && mix compile
      COPY application-input/runner-priv/ /build/_build/prod/lib/favn_runner/priv/
      RUN mix run --no-start stamp_apps.exs
      RUN mix release favn_runner
      RUN mkdir -p /build/runtime-versions \\
        && elixir -e 'IO.write(System.version())' > /build/runtime-versions/ELIXIR_VERSION \\
        && cp /usr/local/lib/erlang/releases/#{@otp_release}/OTP_VERSION /build/runtime-versions/OTP_VERSION \\
        && test "$(cat /build/runtime-versions/ELIXIR_VERSION)" = "#{@elixir_version}" \\
        && test "$(cat /build/runtime-versions/OTP_VERSION)" = "#{@otp_version}"

      FROM --platform=linux/amd64 #{@runtime_image} AS runtime
      ARG BUILD_DATE=unknown
      ARG RUNNER_DIST_PORT=9100
      RUN sed -i \\
        -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/#{@debian_snapshot}|' \\
        -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/#{@debian_snapshot}|' \\
        /etc/apt/sources.list.d/debian.sources \\
        && apt-get -o Acquire::Check-Valid-Until=false update \\
        && apt-get -o Acquire::Check-Valid-Until=false install -y --no-install-recommends ca-certificates libstdc++6 libgcc-s1 openssl libncurses6 \\
        && rm -rf /var/lib/apt/lists/* \\
        && groupadd --system --gid 10001 favn \\
        && useradd --system --uid 10001 --gid favn --home-dir /opt/favn --shell /usr/sbin/nologin favn \\
        && mkdir -p /opt/favn /tmp/favn \\
        && chown -R favn:favn /opt/favn /tmp/favn
      RUN mkdir -p /var/lib/favn/data && chown -R favn:favn /var/lib/favn/data
      WORKDIR /opt/favn
      COPY --from=builder --chown=favn:favn /build/_build/prod/rel/favn_runner/ ./
      COPY --from=builder --chown=favn:favn /build/runtime-versions/ /opt/favn/runtime-versions/
      RUN rm -f /opt/favn/releases/COOKIE
      ENV HOME=/tmp/favn TMPDIR=/tmp/favn RELEASE_TMP=/tmp/favn LANG=C.UTF-8 LC_ALL=C.UTF-8
      LABEL org.opencontainers.image.title="Favn customer runner" \\
        org.opencontainers.image.created="${BUILD_DATE}" \\
        org.opencontainers.image.revision="#{revision}" \\
        io.favn.runner-release-id="#{descriptor.runner_release_id}" \\
        io.favn.version="#{descriptor.favn_version}" \\
        io.favn.runner-contract-version="#{descriptor.runner_contract_version}" \\
        io.favn.elixir-version="#{@elixir_version}" \\
        io.favn.otp-version="#{@otp_version}" \\
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

  @doc false
  @spec source_revision(map()) :: {:ok, String.t()} | {:error, :invalid_runner_source_revision}
  def source_revision(metadata) when is_map(metadata) do
    case Map.get(metadata, "source_revision", "unknown") do
      "unknown" ->
        {:ok, "unknown"}

      revision when is_binary(revision) and byte_size(revision) in [40, 64] ->
        if Regex.match?(~r/\A[0-9a-f]+\z/, revision),
          do: {:ok, revision},
          else: {:error, :invalid_runner_source_revision}

      _invalid ->
        {:error, :invalid_runner_source_revision}
    end
  end
end
