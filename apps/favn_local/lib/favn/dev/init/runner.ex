defmodule Favn.Dev.Init.Runner do
  @moduledoc """
  Scaffolds an editable customer-owned runner image build.

  The generated files are written once and never overwritten after customer
  modification. They are a recommended starting point, not a deployment
  contract or Favn-owned production build pipeline.
  """

  alias Favn.Dev.Build.SourceInputSet
  alias Favn.Dev.Paths

  @default_output "deploy/runner"
  @safe_path_segment ~r/\A[A-Za-z0-9][A-Za-z0-9._-]*\z/
  @builder_image "hexpm/elixir:1.20.2-erlang-29.0.3-debian-trixie-20260713-slim"
  @runtime_image "debian:trixie-slim"
  @release_version "1.0.0"
  @duckdb_adbc_version "1.5.4"
  @duckdb_adbc_checksum "838d98a85e697bab9935010c88a8c67d3312ccedcab4cb4a0ba01da65113bb70"
  @duckdb_adbc_dependency :favn_duckdb_adbc

  @type result :: %{
          created: [Path.t()],
          existing: [Path.t()],
          includes: [String.t()],
          output: Path.t(),
          target: :runner
        }

  @doc "Writes the recommended runner Dockerfile and release wrapper."
  @spec run(keyword()) :: {:ok, result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    with {:ok, root_dir} <- project_root(opts),
         {:ok, app} <- project_application(opts),
         {:ok, includes} <- includes(opts),
         :ok <- validate_include_dependencies(includes, opts),
         {:ok, runtime_config} <- SourceInputSet.runtime_config(root_dir),
         {:ok, output, output_relative, project_root_relative} <- output_path(root_dir, opts),
         targets <-
           targets(
             output,
             app,
             output_relative,
             project_root_relative,
             includes,
             runtime_config.entries
           ),
         {:ok, statuses} <- preflight(targets),
         :ok <- write_missing(targets, statuses) do
      {:ok,
       %{
         created: relative_paths(root_dir, targets, statuses, :missing),
         existing: relative_paths(root_dir, targets, statuses, :identical),
         includes: Enum.map(includes, &include_name/1),
         output: Path.relative_to(output, root_dir),
         target: :runner
       }}
    end
  end

  defp project_root(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()

    if File.regular?(Path.join(root_dir, "mix.exs")),
      do: {:ok, root_dir},
      else: {:error, {:missing_mix_project, root_dir}}
  end

  defp project_application(opts) do
    case Keyword.get(opts, :app, Mix.Project.config()[:app]) do
      app when is_atom(app) and not is_nil(app) -> {:ok, app}
      _missing -> {:error, :mix_project_application_required}
    end
  end

  defp output_path(root_dir, opts) do
    value = Keyword.get(opts, :output, @default_output)

    with true <- is_binary(value) and String.trim(value) != "",
         output <- Path.expand(value, root_dir),
         true <- inside_root?(output, root_dir),
         output_relative <- docker_path(Path.relative_to(output, root_dir)),
         true <- safe_output_relative?(output_relative),
         :ok <- safe_output_directory(output),
         :ok <- safe_parent(output, root_dir) do
      project_root_relative =
        root_dir
        |> Path.relative_to(output, force: true)
        |> docker_path()

      {:ok, output, output_relative, project_root_relative}
    else
      _invalid -> {:error, {:unsafe_runner_output, value}}
    end
  end

  defp targets(
         output,
         app,
         output_relative,
         project_root_relative,
         includes,
         runtime_config_entries
       ) do
    [
      {Path.join(output, "Dockerfile"),
       dockerfile(output_relative, project_root_relative, includes, runtime_config_entries)},
      {Path.join(output, "Dockerfile.dockerignore"), dockerignore()},
      {Path.join(output, "mix.exs"), release_project(app, project_root_relative)},
      {Path.join(output, "env.sh.eex"), release_env()}
    ]
  end

  defp includes(opts) do
    opts
    |> Keyword.get_values(:include)
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, includes} ->
      case parse_include(value) do
        {:ok, include} ->
          if Enum.any?(includes, &(&1.capability == include.capability)) do
            {:halt, {:error, {:duplicate_runner_include, include.capability}}}
          else
            {:cont, {:ok, [include | includes]}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, includes} -> {:ok, Enum.reverse(includes)}
      {:error, _reason} = error -> error
    end
  end

  defp parse_include("duckdb-adbc"),
    do: {:ok, %{capability: :duckdb_adbc, version: @duckdb_adbc_version}}

  defp parse_include("duckdb-adbc@" <> version) do
    if version == @duckdb_adbc_version do
      {:ok, %{capability: :duckdb_adbc, version: version}}
    else
      {:error,
       {:unsupported_runner_include_version, "duckdb-adbc", version, [@duckdb_adbc_version]}}
    end
  end

  defp parse_include(value),
    do: {:error, {:unsupported_runner_include, value, ["duckdb-adbc"]}}

  defp validate_include_dependencies(includes, opts) do
    if Enum.any?(includes, &(&1.capability == :duckdb_adbc)) and
         @duckdb_adbc_dependency not in project_dependencies(opts) do
      {:error, {:runner_include_dependency_missing, "duckdb-adbc", @duckdb_adbc_dependency}}
    else
      :ok
    end
  end

  defp project_dependencies(opts) do
    opts
    |> Keyword.get_lazy(:project_dependencies, fn -> Mix.Project.config()[:deps] || [] end)
    |> Enum.flat_map(fn
      {app, _requirement} when is_atom(app) -> [app]
      {app, _requirement, _options} when is_atom(app) -> [app]
      _invalid -> []
    end)
  end

  defp include_name(%{capability: :duckdb_adbc, version: version}),
    do: "duckdb-adbc@#{version}"

  defp preflight(targets) do
    Enum.reduce_while(targets, {:ok, %{}}, fn {path, content}, {:ok, statuses} ->
      case File.lstat(path) do
        {:error, :enoent} ->
          {:cont, {:ok, Map.put(statuses, path, :missing)}}

        {:ok, %{type: :regular}} ->
          case File.read(path) do
            {:ok, ^content} -> {:cont, {:ok, Map.put(statuses, path, :identical)}}
            {:ok, _modified} -> {:halt, {:error, {:runner_scaffold_modified, path}}}
            {:error, reason} -> {:halt, {:error, {:runner_scaffold_read_failed, path, reason}}}
          end

        {:ok, _other} ->
          {:halt, {:error, {:unsafe_runner_scaffold_target, path}}}

        {:error, reason} ->
          {:halt, {:error, {:runner_scaffold_read_failed, path, reason}}}
      end
    end)
  end

  defp write_missing(targets, statuses) do
    Enum.reduce_while(targets, :ok, fn {path, content}, :ok ->
      if statuses[path] == :missing do
        with :ok <- File.mkdir_p(Path.dirname(path)),
             :ok <- File.write(path, content, [:binary, :exclusive]),
             :ok <- File.chmod(path, 0o644) do
          {:cont, :ok}
        else
          {:error, reason} ->
            {:halt, {:error, {:runner_scaffold_write_failed, path, reason}}}
        end
      else
        {:cont, :ok}
      end
    end)
  end

  defp relative_paths(root_dir, targets, statuses, status) do
    targets
    |> Enum.filter(fn {path, _content} -> statuses[path] == status end)
    |> Enum.map(fn {path, _content} -> Path.relative_to(path, root_dir) end)
  end

  defp safe_parent(output, root_dir) do
    output
    |> Path.dirname()
    |> existing_ancestors(root_dir)
    |> Enum.reduce_while(:ok, fn path, :ok ->
      case File.lstat(path) do
        {:ok, %{type: :directory}} -> {:cont, :ok}
        {:error, :enoent} -> {:cont, :ok}
        _unsafe -> {:halt, {:error, :unsafe_parent}}
      end
    end)
  end

  defp safe_output_directory(output) do
    case File.lstat(output) do
      {:ok, %{type: :directory}} -> :ok
      {:error, :enoent} -> :ok
      _unsafe -> {:error, :unsafe_output}
    end
  end

  defp existing_ancestors(path, root_dir) do
    Stream.iterate(path, &Path.dirname/1)
    |> Enum.take_while(&inside_root_or_equal?(&1, root_dir))
  end

  defp inside_root?(path, root_dir) do
    relative = Path.relative_to(path, root_dir)

    Path.type(relative) == :relative and relative not in ["", ".", ".."] and
      not String.starts_with?(relative, "../")
  end

  defp inside_root_or_equal?(path, root_dir),
    do: path == root_dir or inside_root?(path, root_dir)

  defp safe_output_relative?(relative) do
    relative
    |> Path.split()
    |> Enum.all?(&Regex.match?(@safe_path_segment, &1))
  end

  defp docker_path(path), do: String.replace(path, "\\", "/")

  defp release_project(app, project_root_relative) do
    """
    defmodule FavnCustomerRunner.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_customer_runner,
          version: "#{@release_version}",
          elixir: "~> 1.20",
          config_path: "#{project_root_relative}/config/config.exs",
          lockfile: "#{project_root_relative}/mix.lock",
          deps_path: "#{project_root_relative}/deps",
          start_permanent: Mix.env() == :prod,
          deps: [{#{inspect(app)}, path: "#{project_root_relative}"}],
          releases: [
            favn_runner: [
              applications: [
                {:favn_customer_runner, :load},
                {#{inspect(app)}, :load},
                {:favn_runner, :permanent}
              ],
              include_executables_for: [:unix],
              rel_templates_path: ".",
              strip_beams: true
            ] ++ runtime_config()
          ]
        ]
      end

      def application do
        []
      end

      defp runtime_config do
        path = Path.expand("#{project_root_relative}/config/runtime.exs", __DIR__)
        if File.regular?(path), do: [runtime_config_path: path], else: []
      end
    end
    """
  end

  defp dockerfile(
         output_relative,
         project_root_relative,
         includes,
         runtime_config_entries
       ) do
    """
    # syntax=docker/dockerfile:1.7

    ARG FAVN_BUILD_SOURCE=project-source

    # Empty during normal builds. Maintainer mode replaces this stage with the
    # validated local Favn checkout through Docker's named-context interface.
    FROM scratch AS favn-checkout

    # Build the customer project and its Favn runner release. Keep this image
    # aligned with the Elixir and Erlang versions supported by Favn.
    FROM --platform=linux/amd64 #{@builder_image} AS build-base

    ENV MIX_ENV=prod
    WORKDIR /build

    RUN apt-get update \\
        && apt-get install -y --no-install-recommends build-essential ca-certificates git cmake pkg-config \\
        && rm -rf /var/lib/apt/lists/* \\
        && mix local.hex --force \\
        && mix local.rebar --force

    FROM build-base AS project-source
    COPY . /build

    # Favn selects this stage only for `mix favn.maintainer.dev`. Validated
    # source files from the local Favn checkout are supplied as a separate
    # Docker build context and become the FAVN_CHECKOUT used by mix.exs.
    FROM project-source AS maintainer-source
    COPY --from=favn-checkout / /favn-checkout
    ENV FAVN_CHECKOUT=/favn-checkout

    FROM ${FAVN_BUILD_SOURCE} AS builder
    ARG FAVN_PROJECT_ROOT=.

    WORKDIR /build/${FAVN_PROJECT_ROOT}/#{output_relative}
    RUN mix deps.get --only prod --check-locked \\
        && mix deps.compile \\
    #{runtime_config_overlay(project_root_relative, runtime_config_entries)}
        && mix release favn_runner \\
        && cp -R _build/prod/rel/favn_runner /runner-release

    #{optional_build_stages(includes)}
    # Run only the compiled release and the native libraries selected by this
    # project. `docker build --pull` refreshes this tagged base image.
    FROM --platform=linux/amd64 #{@runtime_image} AS runtime

    # Favn supplies a new local release ID automatically. Production CI may
    # supply its own ID while building this same customer-owned Dockerfile.
    ARG FAVN_RUNNER_RELEASE_ID
    RUN case "$FAVN_RUNNER_RELEASE_ID" in \\
          rr_*) release_hex="${FAVN_RUNNER_RELEASE_ID#rr_}" ;; \\
          *) echo "FAVN_RUNNER_RELEASE_ID must be an immutable rr_ ID" >&2; exit 1 ;; \\
        esac \\
        && [ "${#release_hex}" -eq 64 ] \\
        && case "$release_hex" in \\
          *[!0-9a-f]*) echo "FAVN_RUNNER_RELEASE_ID must use lowercase hexadecimal" >&2; exit 1 ;; \\
          *) ;; \\
        esac \\
        && apt-get update \\
        && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libgcc-s1 openssl libncurses6 \\
        && rm -rf /var/lib/apt/lists/* \\
        && groupadd --system --gid 10001 favn \\
        && useradd --system --uid 10001 --gid favn --home-dir /var/lib/favn/data --shell /usr/sbin/nologin favn \\
        && install -d -o 10001 -g 10001 /opt/favn /tmp/favn /var/lib/favn/data

    WORKDIR /opt/favn
    COPY --from=builder --chown=10001:10001 /runner-release/ ./
    #{optional_runtime_copies(includes)}
    RUN rm -f /opt/favn/releases/COOKIE

    ENV FAVN_RUNNER_RELEASE_ID=$FAVN_RUNNER_RELEASE_ID \\
        HOME=/var/lib/favn/data \\
        TMPDIR=/tmp/favn \\
        RELEASE_TMP=/tmp/favn \\
        LANG=C.UTF-8 \\
        LC_ALL=C.UTF-8

    # Favn validates these labels before starting the image and aligns the
    # generated manifest with the exact runner release ID.
    LABEL org.opencontainers.image.title="Customer Favn runner" \\
          io.favn.runner-release-id="$FAVN_RUNNER_RELEASE_ID" \\
          io.favn.version="#{Favn.RunnerRelease.current_favn_version()}" \\
          io.favn.runner-contract-version="#{Favn.Manifest.Compatibility.current_runner_contract_version()}" \\
          io.favn.target="linux/amd64"

    USER 10001:10001
    EXPOSE 4369 9100
    ENTRYPOINT ["/opt/favn/bin/favn_runner"]
    CMD ["start"]
    """
  end

  defp runtime_config_overlay(project_root_relative, entries) do
    Enum.map_join(entries, "\n", fn entry ->
      relative = Path.relative_to(entry.path, "config/runtime")
      source = Path.join(project_root_relative, entry.path) |> docker_path() |> shell_quote()

      destination =
        Path.join(["overlays/releases", @release_version, "runtime", relative])
        |> docker_path()
        |> shell_quote()

      "    && install -D -m 0644 #{source} #{destination} \\\\"
    end)
  end

  defp shell_quote(value), do: "'" <> String.replace(value, "'", "'\"'\"'") <> "'"

  defp optional_build_stages(includes) do
    if Enum.any?(includes, &(&1.capability == :duckdb_adbc)) do
      """
      # Optional DuckDB ADBC native driver requested with:
      #   mix favn.init --include duckdb-adbc@#{@duckdb_adbc_version}
      #
      # Configure the plugin with:
      #   config :favn, :duckdb_adbc,
      #     driver: "/opt/duckdb/#{@duckdb_adbc_version}/libduckdb.so",
      #     entrypoint: "duckdb_adbc_init"
      #
      # Remove this stage and its COPY if the favn_duckdb_adbc runner plugin is
      # removed from the customer project.
      FROM --platform=linux/amd64 #{@runtime_image} AS duckdb-adbc-driver

      ADD --checksum=sha256:#{@duckdb_adbc_checksum} \\
        https://github.com/duckdb/duckdb/releases/download/v#{@duckdb_adbc_version}/libduckdb-linux-amd64.zip \\
        /tmp/libduckdb.zip

      RUN apt-get update \\
          && apt-get install -y --no-install-recommends unzip \\
          && mkdir -p /opt/duckdb/#{@duckdb_adbc_version} \\
          && unzip -j /tmp/libduckdb.zip libduckdb.so -d /opt/duckdb/#{@duckdb_adbc_version} \\
          && chmod 0444 /opt/duckdb/#{@duckdb_adbc_version}/libduckdb.so \\
          && rm -rf /var/lib/apt/lists/* /tmp/libduckdb.zip
      """
    else
      """
      # Add optional native runtime dependencies in a separate stage here.
      """
    end
  end

  defp optional_runtime_copies(includes) do
    if Enum.any?(includes, &(&1.capability == :duckdb_adbc)) do
      """
      COPY --from=duckdb-adbc-driver --chown=10001:10001 /opt/duckdb/ /opt/duckdb/
      """
    else
      ""
    end
  end

  defp dockerignore do
    """
    .git
    .favn
    .data
    .env
    .env.*
    _build
    deps
    cover
    doc
    test
    tmp
    """
  end

  defp release_env do
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

    export RELEASE_DISTRIBUTION=name
    export RELEASE_NODE="$FAVN_RUNNER_NODE"
    export RELEASE_COOKIE="$FAVN_DISTRIBUTION_COOKIE"
    export ERL_EPMD_PORT

    case "${RELEASE_COMMAND:-}" in
      start|start_iex|daemon|daemon_iex)
        export ERL_AFLAGS="${ERL_AFLAGS:-} -kernel inet_dist_listen_min $FAVN_BEAM_DISTRIBUTION_PORT inet_dist_listen_max $FAVN_BEAM_DISTRIBUTION_PORT"
        ;;
    esac
    """
  end
end
