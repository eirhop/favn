defmodule FavnView.Dev.DesignSystem.Query do
  @moduledoc """
  Parses the design-system URL into a render request.

  The URL is the whole interface. Asking for one button renders one button, so a
  screenshot of the result is a screenshot of that button and nothing else. There
  is no navigation to perform and no state to set up first.

  ## Parameters

  | Parameter | Meaning | Default |
  | --- | --- | --- |
  | `id` | entry id, repeatable or comma-separated | every entry |
  | `group` | `element`, `component`, or `page` | all groups |
  | `q` | substring match on the entry id | none |
  | `mode` | `examples`, `defaults`, `matrix`, or `all` | `examples` |
  | `axis` | attr to walk in `matrix` mode | the first attr with `values` |
  | `example` | substring match on the example id | every example |
  | `theme` | `dark` or `light` | `dark` |
  | `width` | content width in CSS pixels | fluid |
  | `scale` | page zoom, for crisp screenshots | `1` |
  | `chrome` | `0` hides labels and padding | `1` |
  | `format` | `html` or `json` | `html` |

  Unrecognised values are ignored and reported as warnings on the page rather
  than failing the request: a typo should still render something, and it should
  say what it ignored.
  """

  alias FavnView.Dev.DesignSystem.Entry

  @type mode :: :examples | :defaults | :matrix | :all

  @type t :: %__MODULE__{
          ids: [String.t()],
          group: Entry.group() | nil,
          search: String.t() | nil,
          mode: mode(),
          axis: atom() | nil,
          example: String.t() | nil,
          theme: String.t(),
          width: pos_integer() | nil,
          scale: float(),
          chrome?: boolean(),
          format: :html | :json,
          warnings: [String.t()]
        }

  defstruct ids: [],
            group: nil,
            search: nil,
            mode: :examples,
            axis: nil,
            example: nil,
            theme: "favn-dark",
            width: nil,
            scale: 1.0,
            chrome?: true,
            format: :html,
            warnings: []

  @modes %{"examples" => :examples, "defaults" => :defaults, "matrix" => :matrix, "all" => :all}
  @groups %{"element" => :element, "component" => :component, "page" => :page}
  @themes %{"dark" => "favn-dark", "light" => "favn-light"}
  @formats %{"html" => :html, "json" => :json}

  @doc """
  Parses request params.
  """
  @spec parse(map()) :: t()
  def parse(params) when is_map(params) do
    %__MODULE__{}
    |> put_ids(params)
    |> put_enum(params, "group", @groups, :group)
    |> put_search(params)
    |> put_enum(params, "mode", @modes, :mode)
    |> put_axis(params)
    |> put_example(params)
    |> put_enum(params, "theme", @themes, :theme)
    |> put_width(params)
    |> put_scale(params)
    |> put_chrome(params)
    |> put_enum(params, "format", @formats, :format)
    |> finish_warnings()
  end

  defp put_ids(query, params) do
    ids =
      params
      |> Map.get("id", [])
      |> List.wrap()
      |> Enum.flat_map(&String.split(&1, ",", trim: true))
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    %{query | ids: ids}
  end

  defp put_search(query, params) do
    case Map.get(params, "q") do
      value when is_binary(value) and value != "" -> %{query | search: value}
      _other -> query
    end
  end

  defp put_example(query, params) do
    case Map.get(params, "example") do
      value when is_binary(value) and value != "" -> %{query | example: value}
      _other -> query
    end
  end

  defp put_axis(query, params) do
    case Map.get(params, "axis") do
      value when is_binary(value) and value != "" ->
        case safe_atom(value) do
          nil -> warn(query, "axis #{inspect(value)} is not an attr of any component")
          axis -> %{query | axis: axis}
        end

      _other ->
        query
    end
  end

  defp put_width(query, params) do
    with value when is_binary(value) <- Map.get(params, "width"),
         {width, ""} when width > 0 and width <= 4000 <- Integer.parse(value) do
      %{query | width: width}
    else
      nil -> query
      _invalid -> warn(query, "width must be an integer between 1 and 4000")
    end
  end

  defp put_scale(query, params) do
    with value when is_binary(value) <- Map.get(params, "scale"),
         {scale, ""} when scale >= 0.25 and scale <= 4.0 <- Float.parse(value) do
      %{query | scale: scale}
    else
      nil -> query
      _invalid -> warn(query, "scale must be a number between 0.25 and 4")
    end
  end

  defp put_chrome(query, params) do
    case Map.get(params, "chrome") do
      value when value in ["0", "false"] -> %{query | chrome?: false}
      value when value in ["1", "true", nil] -> query
      _other -> warn(query, "chrome must be 0 or 1")
    end
  end

  defp put_enum(query, params, key, allowed, field) do
    case Map.get(params, key) do
      nil ->
        query

      value ->
        case Map.fetch(allowed, value) do
          {:ok, parsed} ->
            Map.put(query, field, parsed)

          :error ->
            warn(query, "#{key} must be one of: #{Enum.join(Map.keys(allowed), ", ")}")
        end
    end
  end

  # An unknown axis must not create an atom from user input, and an axis that
  # names no attr is reported by the caller once the entry is known.
  defp safe_atom(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  defp warn(query, message), do: %{query | warnings: [message | query.warnings]}

  defp finish_warnings(query), do: %{query | warnings: Enum.reverse(query.warnings)}
end
