defmodule FavnView.Dev.DesignSystem.Color do
  @moduledoc """
  Colour-space maths for the palette gate.

  ## Why this exists

  Favn's palette is authored in `oklch`, and its surfaces are built with
  `color-mix(in oklab, …)`. A contrast ratio, however, is defined on sRGB
  relative luminance. Without a conversion, checking the palette means rendering
  it in a browser first — which is exactly the loop the palette gate is meant to
  remove.

  This module is the browser's colour pipeline, in Elixir: parse, mix in oklab,
  convert to sRGB. `FavnView.Dev.DesignSystem.Audit` then judges the result, so
  a palette regression is caught by `mix test` rather than by an operator
  noticing that a label went grey.

  The conversion is verified against the browser rather than trusted. Chrome
  resolves `color-mix(in oklab, oklch(87% 0.23 130) 12%, oklch(12% 0.035 260))`
  to `oklab(0.21 -0.0230893 -0.00918926)`, which is what `mix/3` returns below.

  ## Scope

  Only what the palette uses: `oklch()`, `oklab()`, hex, and `rgb()`. Gradients,
  named colours, and `currentColor` are not colours a token can hold, and a
  value this module cannot parse is returned as an error rather than guessed.
  """

  @type t :: %{l: float(), a: float(), b: float(), alpha: float()}
  @type srgb :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @doc """
  Parses a CSS colour into oklab.

  ## Examples

      iex> alias FavnView.Dev.DesignSystem.Color
      iex> {:ok, color} = Color.parse("oklch(87% 0.23 130)")
      iex> {Float.round(color.l, 3), Float.round(color.a, 4), Float.round(color.b, 4)}
      {0.87, -0.1478, 0.1762}

      iex> alias FavnView.Dev.DesignSystem.Color
      iex> {:ok, black} = Color.parse("#000000")
      iex> Color.to_srgb(black)
      {0, 0, 0}

      iex> FavnView.Dev.DesignSystem.Color.parse("linear-gradient(red, blue)")
      {:error, :unsupported}
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, :unsupported}
  def parse(value) when is_binary(value) do
    value = String.trim(value)

    cond do
      String.starts_with?(value, "oklch(") -> from_oklch(value)
      String.starts_with?(value, "oklab(") -> from_oklab(value)
      String.starts_with?(value, "#") -> from_hex(value)
      String.starts_with?(value, "rgb") -> from_rgb(value)
      true -> {:error, :unsupported}
    end
  end

  @doc """
  Parses a colour, raising when the value is not one this module supports.
  """
  @spec parse!(String.t()) :: t()
  def parse!(value) do
    case parse(value) do
      {:ok, color} -> color
      {:error, :unsupported} -> raise ArgumentError, "unsupported colour: #{inspect(value)}"
    end
  end

  @doc """
  Mixes two colours in oklab, `weight` being the proportion of the first.

  This is `color-mix(in oklab, first <weight>%, second)`, which is how every
  Favn surface is built.

  ## Examples

      iex> alias FavnView.Dev.DesignSystem.Color
      iex> mixed = Color.mix(Color.parse!("oklch(87% 0.23 130)"), Color.parse!("oklch(12% 0.035 260)"), 0.12)
      iex> {Float.round(mixed.l, 4), Float.round(mixed.a, 7), Float.round(mixed.b, 7)}
      {0.21, -0.0230893, -0.0091893}
  """
  @spec mix(t(), t(), float()) :: t()
  def mix(first, second, weight) when weight >= 0 and weight <= 1 do
    rest = 1 - weight

    %{
      l: first.l * weight + second.l * rest,
      a: first.a * weight + second.a * rest,
      b: first.b * weight + second.b * rest,
      alpha: first.alpha * weight + second.alpha * rest
    }
  end

  @doc """
  Converts oklab to sRGB, channels 0..255, clamped to gamut.

  ## Examples

      iex> alias FavnView.Dev.DesignSystem.Color
      iex> Color.to_srgb(Color.parse!("oklch(100% 0 0)"))
      {255, 255, 255}

      iex> alias FavnView.Dev.DesignSystem.Color
      iex> Color.to_srgb(Color.parse!("oklch(0% 0 0)"))
      {0, 0, 0}
  """
  @spec to_srgb(t()) :: srgb()
  def to_srgb(%{l: lightness, a: a_star, b: b_star}) do
    long = :math.pow(lightness + 0.3963377774 * a_star + 0.2158037573 * b_star, 3)
    medium = :math.pow(lightness - 0.1055613458 * a_star - 0.0638541728 * b_star, 3)
    short = :math.pow(lightness - 0.0894841775 * a_star - 1.2914855480 * b_star, 3)

    {
      channel(4.0767416621 * long - 3.3077115913 * medium + 0.2309699292 * short),
      channel(-1.2684380046 * long + 2.6097574011 * medium - 0.3413193965 * short),
      channel(-0.0041960863 * long - 0.7034186147 * medium + 1.7076147010 * short)
    }
  end

  @doc """
  Composites a colour over an opaque backdrop, honouring its alpha.

  Browsers composite backgrounds in sRGB, so this does too.
  """
  @spec over(t(), t()) :: srgb()
  def over(%{alpha: alpha} = color, backdrop) do
    {red, green, blue} = to_srgb(color)
    {backdrop_red, backdrop_green, backdrop_blue} = to_srgb(backdrop)

    {
      blend(red, backdrop_red, alpha),
      blend(green, backdrop_green, alpha),
      blend(blue, backdrop_blue, alpha)
    }
  end

  defp blend(over, under, alpha), do: round(over * alpha + under * (1 - alpha))

  defp from_oklch(value) do
    with {:ok, [lightness, chroma, hue | rest]} <- components(value, "oklch") do
      radians = hue * :math.pi() / 180

      {:ok,
       %{
         l: lightness,
         a: chroma * :math.cos(radians),
         b: chroma * :math.sin(radians),
         alpha: alpha(rest)
       }}
    end
  end

  defp from_oklab(value) do
    with {:ok, [lightness, a_star, b_star | rest]} <- components(value, "oklab") do
      {:ok, %{l: lightness, a: a_star, b: b_star, alpha: alpha(rest)}}
    end
  end

  defp from_rgb(value) do
    with {:ok, [red, green, blue | rest]} <- components(value, "rgb") do
      {:ok, from_channels(red, green, blue, alpha(rest))}
    end
  end

  defp from_hex("#" <> digits) do
    digits =
      case digits do
        <<r::binary-1, g::binary-1, b::binary-1>> -> r <> r <> g <> g <> b <> b
        <<value::binary-6>> -> value
        _other -> nil
      end

    case digits && parse_hex_channels(digits) do
      {:ok, [red, green, blue]} -> {:ok, from_channels(red, green, blue, 1.0)}
      _other -> {:error, :unsupported}
    end
  end

  defp parse_hex_channels(digits) do
    digits
    |> String.upcase()
    |> String.to_charlist()
    |> Enum.chunk_every(2)
    |> Enum.reduce_while({:ok, []}, fn pair, {:ok, acc} ->
      case Integer.parse(to_string(pair), 16) do
        {value, ""} -> {:cont, {:ok, acc ++ [value]}}
        _other -> {:halt, {:error, :unsupported}}
      end
    end)
  end

  # sRGB to oklab, for the hex and rgb() values the palette uses for anchors.
  defp from_channels(red, green, blue, alpha) do
    long =
      cube_root(
        0.4122214708 * linear(red) + 0.5363325363 * linear(green) + 0.0514459929 * linear(blue)
      )

    medium =
      cube_root(
        0.2119034982 * linear(red) + 0.6806995451 * linear(green) + 0.1073969566 * linear(blue)
      )

    short =
      cube_root(
        0.0883024619 * linear(red) + 0.2817188376 * linear(green) + 0.6299787005 * linear(blue)
      )

    %{
      l: 0.2104542553 * long + 0.7936177850 * medium - 0.0040720468 * short,
      a: 1.9779984951 * long - 2.4285922050 * medium + 0.4505937099 * short,
      b: 0.0259040371 * long + 0.7827717662 * medium - 0.8086757660 * short,
      alpha: alpha
    }
  end

  # A CSS colour function's arguments, with percentages scaled and `/ alpha`
  # treated as one more component.
  defp components(value, function) do
    with [_all, inner] <- Regex.run(~r/^#{function}a?\(([^)]*)\)$/, value) do
      numbers =
        inner
        |> String.split(~r/[\s,\/]+/, trim: true)
        |> Enum.map(&number/1)

      if Enum.all?(numbers, &is_float/1) and length(numbers) >= 3 do
        {:ok, numbers}
      else
        {:error, :unsupported}
      end
    else
      _other -> {:error, :unsupported}
    end
  end

  defp number(token) do
    percentage? = String.ends_with?(token, "%")

    case Float.parse(String.trim_trailing(token, "%")) do
      {value, ""} -> if percentage?, do: value / 100, else: value
      _other -> :error
    end
  end

  defp alpha([]), do: 1.0
  defp alpha([alpha | _rest]), do: alpha

  defp channel(linear) do
    linear
    |> gamma()
    |> Kernel.*(255)
    |> round()
    |> max(0)
    |> min(255)
  end

  defp gamma(channel) when channel <= 0.0031308, do: 12.92 * channel
  defp gamma(channel), do: 1.055 * :math.pow(channel, 1 / 2.4) - 0.055

  defp linear(channel) do
    ratio = channel / 255

    if ratio <= 0.04045 do
      ratio / 12.92
    else
      :math.pow((ratio + 0.055) / 1.055, 2.4)
    end
  end

  defp cube_root(value) when value < 0, do: -:math.pow(-value, 1 / 3)
  defp cube_root(value), do: :math.pow(value, 1 / 3)
end
