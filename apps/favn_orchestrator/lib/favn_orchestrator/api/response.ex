defmodule FavnOrchestrator.API.Response do
  @moduledoc """
  Encodes the private orchestrator API's JSON response envelope.

  Keeping envelope construction here gives every router and streaming endpoint
  the same content type, request-id, and error shape.
  """

  import Plug.Conn, only: [get_resp_header: 2, put_resp_content_type: 2, send_resp: 3]

  alias FavnOrchestrator.API.DTO

  @type status :: 100..599

  @doc "Returns a JSON success envelope containing `payload`."
  @spec data(Plug.Conn.t(), status(), term()) :: Plug.Conn.t()
  def data(conn, status, payload) do
    send_json(conn, status, %{data: payload})
  end

  @doc "Returns the stable JSON error envelope used by the private API."
  @spec error(Plug.Conn.t(), status(), String.t(), String.t(), map()) :: Plug.Conn.t()
  def error(conn, status, code, message, details \\ %{}) do
    error(conn, status, code, message, details, false)
  end

  @doc "Returns the stable JSON error envelope with an explicit retryability flag."
  @spec error(Plug.Conn.t(), status(), String.t(), String.t(), map(), boolean()) :: Plug.Conn.t()
  def error(conn, status, code, message, details, retryable?) when is_boolean(retryable?) do
    send_json(conn, status, %{
      error: %{
        code: code,
        message: message,
        status: status,
        request_id: request_id(conn),
        retryable: retryable?,
        details: DTO.normalize(details)
      }
    })
  end

  @doc "Normalizes a page using the supplied item mapper."
  @spec page(struct() | map(), (term() -> term())) :: map()
  def page(page, mapper) when is_function(mapper, 1), do: DTO.page(page, mapper)

  defp send_json(conn, status, payload) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(payload))
  end

  defp request_id(conn) do
    case get_resp_header(conn, "x-request-id") do
      [value | _] -> value
      _ -> conn.assigns[:request_id]
    end
  end
end
