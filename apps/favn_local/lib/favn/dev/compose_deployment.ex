defmodule Favn.Dev.ComposeDeployment do
  @moduledoc """
  Typed, semantically validated local Docker Compose deployment.

  Service names are discovered from versioned Favn role labels. Consumer-owned
  services and ordinary Compose configuration remain outside this contract.
  """

  alias Favn.Dev.{Docker, Paths}

  @contract_version 1
  @roles %{
    "postgres" => :postgres,
    "control-plane-ops" => :control_plane_ops,
    "control-plane-verify" => :control_plane_verify,
    "runner" => :runner,
    "control-plane" => :control_plane
  }
  @local_roles Map.values(@roles)
  @single_host_roles @local_roles -- [:postgres]

  @enforce_keys [
    :root_dir,
    :compose_file,
    :env_file,
    :project_name,
    :contract_version,
    :profile,
    :services,
    :workspace_id,
    :view_url,
    :orchestrator_url,
    :control_plane_image,
    :runner_image
  ]
  defstruct @enforce_keys

  @type role ::
          :postgres | :control_plane_ops | :control_plane_verify | :runner | :control_plane
  @type profile :: :local | :single_host
  @type t :: %__MODULE__{
          root_dir: Path.t(),
          compose_file: Path.t(),
          env_file: Path.t(),
          project_name: String.t(),
          contract_version: pos_integer(),
          profile: profile(),
          services: %{required(role()) => String.t()},
          workspace_id: String.t(),
          view_url: String.t(),
          orchestrator_url: String.t(),
          control_plane_image: String.t(),
          runner_image: String.t()
        }

  @doc "Returns the currently supported template contract version."
  @spec contract_version() :: pos_integer()
  def contract_version, do: @contract_version

  @doc "Renders and validates one selected deployment before lifecycle mutation."
  @spec resolve(map(), map(), map(), keyword()) :: {:ok, t()} | {:error, term()}
  def resolve(project, install, runner, opts)
      when is_map(project) and is_map(install) and is_map(runner) and is_list(opts) do
    with :ok <- Docker.probe_compose(opts),
         {output, 0} <-
           Docker.render_compose(
             project["project_name"],
             project["compose_path"],
             project["env_path"],
             opts
           ),
         {:ok, rendered} <- decode_rendered(output),
         {:ok, profile, services} <- discover_services(rendered),
         :ok <- require_profile(profile, Keyword.get(opts, :required_profile)),
         :ok <- validate_images(rendered, services, install, runner, project, opts) do
      {:ok,
       %__MODULE__{
         root_dir: opts |> Paths.root_dir() |> Path.expand(),
         compose_file: project["compose_path"],
         env_file: project["env_path"],
         project_name: project["project_name"],
         contract_version: @contract_version,
         profile: profile,
         services: services,
         workspace_id: project["workspace_id"],
         view_url: project["view_url"],
         orchestrator_url: project["orchestrator_url"],
         control_plane_image: install["image_reference"],
         runner_image: runner.image_reference
       }}
    else
      {render_output, status} when is_integer(status) ->
        {:error, {:compose_render_failed, status, bounded(render_output)}}

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Rehydrates the recorded deployment identity without reading install state."
  @spec from_runtime(map(), keyword()) :: {:ok, t()} | {:error, term()}
  def from_runtime(
        %{
          "kind" => "docker_compose",
          "compose_file" => compose_file,
          "compose_project" => project_name,
          "compose_contract_version" => @contract_version,
          "compose_profile" => profile,
          "compose_services" => encoded_services,
          "workspace_id" => workspace_id,
          "view_url" => view_url,
          "orchestrator_url" => orchestrator_url,
          "control_plane_image_reference" => control_plane_image,
          "runner_image_reference" => runner_image
        },
        opts
      )
      when is_binary(compose_file) and is_binary(project_name) and is_map(encoded_services) and
             is_binary(workspace_id) and is_binary(view_url) and is_binary(orchestrator_url) and
             is_binary(control_plane_image) and is_binary(runner_image) and is_list(opts) do
    with {:ok, profile} <- decode_profile(profile),
         {:ok, services} <- decode_services(encoded_services, profile) do
      {:ok,
       %__MODULE__{
         root_dir: opts |> Paths.root_dir() |> Path.expand(),
         compose_file: Path.expand(compose_file),
         env_file: opts |> Paths.root_dir() |> Paths.compose_env_path() |> Path.expand(),
         project_name: project_name,
         contract_version: @contract_version,
         profile: profile,
         services: services,
         workspace_id: workspace_id,
         view_url: view_url,
         orchestrator_url: orchestrator_url,
         control_plane_image: control_plane_image,
         runner_image: runner_image
       }}
    end
  end

  def from_runtime(_runtime, _opts), do: {:error, :stale_pre_migration_runtime_state}

  @doc "Encodes role names for the non-secret runtime record."
  @spec encoded_services(t()) :: %{String.t() => String.t()}
  def encoded_services(%__MODULE__{services: services}) do
    Map.new(services, fn {role, service} -> {role_name(role), service} end)
  end

  @doc "Returns the rendered service name for a required Favn role."
  @spec service!(t(), role()) :: String.t()
  def service!(%__MODULE__{services: services}, role), do: Map.fetch!(services, role)

  @doc "Returns the consumer-facing project-relative Compose path."
  @spec relative_compose_file(t()) :: Path.t()
  def relative_compose_file(%__MODULE__{root_dir: root_dir, compose_file: compose_file}),
    do: Path.relative_to(compose_file, root_dir)

  defp decode_rendered(output) do
    case JSON.decode(output) do
      {:ok, %{"services" => services} = rendered} when is_map(services) -> {:ok, rendered}
      {:ok, _invalid} -> {:error, :invalid_rendered_compose}
      {:error, reason} -> {:error, {:invalid_rendered_compose_json, reason}}
    end
  end

  defp discover_services(%{"services" => rendered_services}) do
    rendered_services
    |> Enum.reduce_while({:ok, nil, %{}}, fn {service_name, service}, {:ok, profile, roles} ->
      case service_role(service_name, service) do
        :consumer ->
          {:cont, {:ok, profile, roles}}

        {:ok, service_profile, role} ->
          cond do
            profile != nil and profile != service_profile ->
              {:halt, {:error, {:inconsistent_compose_profile, service_name}}}

            Map.has_key?(roles, role) ->
              {:halt, {:error, {:duplicate_compose_role, role}}}

            true ->
              {:cont, {:ok, service_profile, Map.put(roles, role, service_name)}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, nil, _roles} -> {:error, :missing_favn_compose_roles}
      {:ok, profile, roles} -> require_roles(profile, roles)
      {:error, _reason} = error -> error
    end
  end

  defp service_role(service_name, %{"labels" => labels}) when is_map(labels) do
    version = labels["io.favn.compose.contract-version"]
    profile = labels["io.favn.compose.profile"]
    role = labels["io.favn.compose.role"]
    values = [version, profile, role]

    cond do
      Enum.all?(values, &is_nil/1) ->
        :consumer

      Enum.any?(values, &is_nil/1) ->
        {:error, {:incomplete_favn_compose_labels, service_name}}

      version != Integer.to_string(@contract_version) ->
        {:error, {:unsupported_compose_contract_version, version}}

      true ->
        with {:ok, normalized_profile} <- decode_profile(profile),
             {:ok, normalized_role} <- decode_role(role) do
          {:ok, normalized_profile, normalized_role}
        end
    end
  end

  defp service_role(_service_name, _service), do: :consumer

  defp require_roles(profile, roles) do
    required = if profile == :local, do: @local_roles, else: @single_host_roles
    missing = required -- Map.keys(roles)
    unexpected = Map.keys(roles) -- required

    cond do
      missing != [] -> {:error, {:missing_compose_roles, Enum.sort(missing)}}
      unexpected != [] -> {:error, {:unexpected_compose_roles, Enum.sort(unexpected)}}
      true -> {:ok, profile, roles}
    end
  end

  defp require_profile(_profile, nil), do: :ok
  defp require_profile(profile, profile), do: :ok

  defp require_profile(actual, expected),
    do: {:error, {:unsupported_compose_profile, expected, actual}}

  defp validate_images(rendered, services, install, runner, _project, opts) do
    expected_control = install["image_reference"]
    expected_runner = runner.image_reference

    with :ok <- immutable_control_plane_reference(expected_control, install),
         :ok <-
           validate_role_images(
             rendered,
             services,
             [:control_plane_ops, :control_plane_verify, :control_plane],
             expected_control
           ),
         :ok <- validate_role_images(rendered, services, [:runner], expected_runner),
         {:ok, image} <- Docker.inspect_image(expected_runner, opts),
         true <- image.id == runner.image_id,
         true <- image.labels["io.favn.runner-release-id"] == runner.runner_release_id do
      :ok
    else
      false -> {:error, :runner_image_identity_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp validate_role_images(rendered, services, roles, expected) do
    Enum.reduce_while(roles, :ok, fn role, :ok ->
      service_name = Map.fetch!(services, role)
      image = get_in(rendered, ["services", service_name, "image"])

      if image == expected,
        do: {:cont, :ok},
        else: {:halt, {:error, {:compose_role_image_mismatch, role, image, expected}}}
    end)
  end

  defp immutable_control_plane_reference(reference, %{"source" => "official"})
       when is_binary(reference) do
    if Regex.match?(~r/@sha256:[0-9a-f]{64}\z/, reference),
      do: :ok,
      else: {:error, :mutable_control_plane_reference}
  end

  defp immutable_control_plane_reference(reference, %{"source" => "candidate"}) do
    if Mix.env() == :test and is_binary(reference) and String.starts_with?(reference, "sha256:"),
      do: :ok,
      else: {:error, :mutable_control_plane_reference}
  end

  defp immutable_control_plane_reference(reference, %{"source" => "maintainer"})
       when is_binary(reference) do
    if Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, reference),
      do: :ok,
      else: {:error, :mutable_control_plane_reference}
  end

  defp immutable_control_plane_reference(_reference, _install),
    do: {:error, :mutable_control_plane_reference}

  defp decode_services(encoded, profile) do
    encoded
    |> Enum.reduce_while({:ok, %{}}, fn {role, service}, {:ok, services} ->
      with {:ok, role} <- decode_role(role),
           true <- is_binary(service) and service != "" and not Map.has_key?(services, role) do
        {:cont, {:ok, Map.put(services, role, service)}}
      else
        _invalid -> {:halt, {:error, :invalid_runtime_compose_services}}
      end
    end)
    |> case do
      {:ok, services} ->
        case require_roles(profile, services) do
          {:ok, _profile, services} -> {:ok, services}
          {:error, _reason} -> {:error, :invalid_runtime_compose_services}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_profile("local"), do: {:ok, :local}
  defp decode_profile(:local), do: {:ok, :local}
  defp decode_profile("single-host"), do: {:ok, :single_host}
  defp decode_profile(:single_host), do: {:ok, :single_host}
  defp decode_profile(value), do: {:error, {:unsupported_compose_profile, value}}

  defp decode_role(role) when is_binary(role) do
    case Map.fetch(@roles, role) do
      {:ok, normalized} -> {:ok, normalized}
      :error -> {:error, {:unknown_compose_role, role}}
    end
  end

  defp decode_role(role) when role in @local_roles, do: {:ok, role}
  defp decode_role(role), do: {:error, {:unknown_compose_role, role}}

  defp role_name(role) do
    @roles |> Enum.find_value(fn {name, value} -> if value == role, do: name end)
  end

  defp bounded(output) when is_binary(output),
    do: output |> String.trim() |> String.slice(-8_192, 8_192)
end
