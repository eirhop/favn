defmodule FavnView.Dev.DesignSystem.Theme do
  @moduledoc """
  The design tokens of each theme, read from the stylesheet that defines them.

  The tokens are not restated here. `app.css` is the single definition, and this
  module parses it, so a token added or changed there is visible to the palette
  gate without anyone remembering to update a list. The stylesheet is an
  `@external_resource`, so editing it recompiles this module.

  Two kinds of block are collected per theme:

    * the DaisyUI `@plugin` block whose `name:` matches the theme, which holds
      `--color-*`;
    * any `[data-theme="<theme>"]` rule, which holds Favn's own tokens such as
      `--favn-action`.
  """

  @stylesheet Path.expand("../../assets/css/app.css", __DIR__)
  @external_resource @stylesheet
  @css File.read!(@stylesheet)

  @themes ["favn-dark", "favn-light"]

  @doc """
  Every theme the stylesheet defines.
  """
  @spec themes() :: [String.t()]
  def themes, do: @themes

  @doc """
  The tokens of one theme, keyed by custom-property name.

  ## Examples

      iex> tokens = FavnView.Dev.DesignSystem.Theme.tokens("favn-dark")
      iex> tokens["--color-base-100"]
      "oklch(12% 0.035 260)"

      iex> FavnView.Dev.DesignSystem.Theme.tokens("favn-dark")["--favn-action"]
      "oklch(87% 0.23 130)"
  """
  @spec tokens(String.t()) :: %{String.t() => String.t()}
  def tokens(theme) when theme in @themes do
    plugin_block(theme)
    |> Kernel.<>("\n")
    |> Kernel.<>(data_theme_blocks(theme))
    |> declarations()
  end

  @doc """
  Fetches one token, raising when the theme does not define it.
  """
  @spec fetch!(String.t(), String.t()) :: String.t()
  def fetch!(theme, token) do
    case Map.fetch(tokens(theme), token) do
      {:ok, value} ->
        value

      :error ->
        raise KeyError,
              "#{theme} does not define #{token} in #{Path.relative_to_cwd(@stylesheet)}"
    end
  end

  # The `@plugin` block for a theme is found by its `name:` declaration rather
  # than by position, so reordering the stylesheet cannot silently swap themes.
  defp plugin_block(theme) do
    @css
    |> blocks_starting_with("@plugin")
    |> Enum.find(fn block -> Regex.match?(~r/name:\s*"#{Regex.escape(theme)}"/, block) end)
    |> Kernel.||("")
  end

  defp data_theme_blocks(theme) do
    @css
    |> blocks_starting_with(~s([data-theme="#{theme}"]))
    |> Enum.join("\n")
  end

  # Brace matching rather than a regex: a CSS block can nest, and a non-greedy
  # match to the first `}` would truncate at the first nested rule.
  defp blocks_starting_with(css, prefix) do
    css
    |> String.split(prefix)
    |> Enum.drop(1)
    |> Enum.map(&balanced_block/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp balanced_block(text) do
    case String.split(text, "{", parts: 2) do
      [_before, rest] -> take_until_close(rest, 1, "")
      _other -> ""
    end
  end

  defp take_until_close("", _depth, acc), do: acc
  defp take_until_close(_rest, 0, acc), do: acc

  defp take_until_close(<<char::utf8, rest::binary>>, depth, acc) do
    case char do
      ?{ -> take_until_close(rest, depth + 1, acc <> "{")
      ?} when depth == 1 -> acc
      ?} -> take_until_close(rest, depth - 1, acc <> "}")
      _other -> take_until_close(rest, depth, acc <> <<char::utf8>>)
    end
  end

  defp declarations(block) do
    ~r/(--[a-z0-9-]+)\s*:\s*([^;]+);/
    |> Regex.scan(block)
    |> Map.new(fn [_all, name, value] -> {name, String.trim(value)} end)
  end
end
