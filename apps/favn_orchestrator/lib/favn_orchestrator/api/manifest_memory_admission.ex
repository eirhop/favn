defmodule FavnOrchestrator.API.ManifestMemoryAdmission do
  @moduledoc """
  Acquires node memory before any low-level manifest publication body is read.
  """

  @behaviour Plug

  import Plug.Conn

  alias Favn.Manifest.ArchiveLimits
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget
  alias FavnOrchestrator.MemoryCapacity.Error

  @manifest_raw_maximum ArchiveLimits.current().manifest_index_bytes

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: "POST", request_path: path} = conn, plug_opts) do
    case admission(path, conn) do
      :skip ->
        conn

      {bytes, admission_opts} ->
        server = Keyword.get(plug_opts, :server, MemoryCapacity.Coordinator)
        admission_opts = Keyword.put(admission_opts, :server, server)

        with :ok <- Authentication.ensure_service(conn),
             {:ok, token} <- MemoryCapacity.acquire(bytes, admission_opts) do
          conn
          |> put_private(:favn_manifest_memory_token, token)
          |> register_before_send(fn conn ->
            :ok = MemoryCapacity.release(token, server: server)
            conn
          end)
        else
          {:error, :service_unauthorized} -> conn
          {:error, %Error{} = error} -> capacity_error(conn, error)
        end
    end
  end

  def call(conn, _opts), do: conn

  defp admission("/api/orchestrator/v1/execution-packages", _conn),
    do: {Budget.manifest_base(), [kind: :execution_package_publication, exclusive: true]}

  defp admission("/api/orchestrator/v1/execution-packages/missing", _conn),
    do: {Budget.missing_hashes(), [kind: :execution_package_missing]}

  defp admission("/api/orchestrator/v1/manifests", conn) do
    bytes =
      if compressed_or_unknown?(conn) do
        Budget.index_max()
      else
        conn
        |> content_length()
        |> min(@manifest_raw_maximum)
        |> Budget.index()
      end

    {bytes, [kind: :manifest_publication, exclusive: true]}
  end

  defp admission(_path, _conn), do: :skip

  defp compressed_or_unknown?(conn) do
    get_req_header(conn, "content-encoding") != [] or is_nil(content_length(conn))
  end

  defp content_length(conn) do
    case get_req_header(conn, "content-length") do
      [value] ->
        case Integer.parse(String.trim(value)) do
          {size, ""} when size >= 0 -> size
          _invalid -> nil
        end

      _other ->
        nil
    end
  end

  defp capacity_error(conn, %Error{code: :manifest_capacity_busy}) do
    conn
    |> put_resp_header("retry-after", "5")
    |> Response.error(429, "manifest_capacity_busy", "Manifest capacity is busy")
    |> halt()
  end

  defp capacity_error(conn, %Error{code: :manifest_capacity_unavailable}) do
    conn
    |> put_resp_header("retry-after", "5")
    |> Response.error(503, "manifest_capacity_unavailable", "Manifest capacity is unavailable")
    |> halt()
  end

  defp capacity_error(conn, %Error{code: :memory_capacity_unknown}) do
    conn
    |> put_resp_header("retry-after", "5")
    |> Response.error(503, "memory_capacity_unknown", "Memory capacity cannot be measured")
    |> halt()
  end
end
