defmodule Favn.Dev.Build.Web do
  @moduledoc """
  Project-local web build target.
  """

  alias Favn.Dev.Install
  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @schema_version 1
  @target "web"

  @type root_opt :: [root_dir: Path.t()]

  @spec run(root_opt()) :: {:ok, map()} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with :ok <- Install.ensure_ready(opts),
         :ok <- State.ensure_layout(opts),
         {:ok, install} <- State.read_install(opts),
         web_source_root when is_binary(web_source_root) <- runtime_source_root(install, "web"),
         {build_id, root_dir} <- {build_id(), Paths.root_dir(opts)},
         build_dir <- Paths.build_web_dir(root_dir, build_id),
         dist_dir <- Paths.dist_web_dir(root_dir, build_id),
         :ok <- File.mkdir_p(build_dir),
         :ok <- File.mkdir_p(dist_dir),
         build_json <- build_json(build_id, web_source_root, opts),
         metadata_json <- metadata_json(build_id, web_source_root, opts),
         :ok <- write_json(Path.join(build_dir, "build.json"), build_json),
         :ok <- write_json(Path.join(dist_dir, "metadata.json"), metadata_json),
         :ok <-
           write_json(
             Path.join(dist_dir, "bundle.json"),
             web_bundle_json(build_id, web_source_root, opts)
           ) do
      {:ok, %{build_id: build_id, build_dir: build_dir, dist_dir: dist_dir}}
    else
      nil -> {:error, :missing_install_runtime_input}
      {:error, :not_found} -> {:error, :install_required}
      {:error, reason} -> {:error, reason}
    end
  end

  defp runtime_source_root(install, target) when is_map(install) do
    get_in(install, ["runtime_inputs", target, "source_root"])
  end

  defp build_json(build_id, source_root, opts) do
    base(build_id, opts)
    |> Map.put("phase", "build")
    |> Map.put("target", @target)
    |> Map.put("web_source_root", source_root)
  end

  defp metadata_json(build_id, source_root, opts) do
    base(build_id, opts)
    |> Map.put("phase", "dist")
    |> Map.put("target", @target)
    |> Map.put("web_source_root", source_root)
    |> Map.put("compatibility", %{
      "orchestrator_api_version" => "v1"
    })
    |> Map.put("required_env", [
      "FAVN_ORCHESTRATOR_BASE_URL",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_WEB_SESSION_SECRET"
    ])
  end

  defp web_bundle_json(build_id, source_root, opts) do
    %{
      "schema_version" => @schema_version,
      "target" => @target,
      "build_id" => build_id,
      "source_root" => source_root,
      "generated_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "project_root" => Paths.root_dir(opts)
    }
  end

  defp base(build_id, opts) do
    %{
      "schema_version" => @schema_version,
      "build_id" => build_id,
      "built_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "favn_version" => to_string(Application.spec(:favn, :vsn) || "unknown"),
      "install_fingerprint" => read_install_fingerprint(opts),
      "elixir_version" => System.version(),
      "otp_release" => :erlang.system_info(:otp_release) |> List.to_string()
    }
  end

  defp read_install_fingerprint(opts) do
    case State.read_install(opts) do
      {:ok, %{"fingerprint" => fingerprint}} when is_map(fingerprint) -> fingerprint
      _ -> %{}
    end
  end

  defp write_json(path, map) when is_map(map) do
    encoded = JSON.encode_to_iodata!(map)
    File.write(path, [encoded, "\n"])
  end

  defp build_id do
    stamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d%H%M%S")
    unique = System.unique_integer([:positive])
    "wb_#{stamp}_#{unique}"
  end
end
