defmodule Favn.SQL.Template do
  @moduledoc """
  Normalized SQL template representation used by SQL asset authoring.
  """

  @relation_prefix_regex ~r/(?:^|\s)(?:from|join|inner\s+join|left(?:\s+outer)?\s+join|right(?:\s+outer)?\s+join|full(?:\s+outer)?\s+join|cross\s+join)\s*$/i

  @enforce_keys [:source, :inputs, :runtime_inputs, :param_inputs]
  defstruct [
    :source,
    :inputs,
    :runtime_inputs,
    :param_inputs,
    calls: [],
    asset_refs: []
  ]

  @type context :: :expression | :relation

  defmodule Call do
    @moduledoc false
    @enforce_keys [:name, :arity, :context, :line]
    defstruct [:name, :arity, :context, :line]

    @type t :: %__MODULE__{
            name: atom(),
            arity: non_neg_integer(),
            context: Favn.SQL.Template.context(),
            line: pos_integer()
          }
  end

  defmodule AssetRef do
    @moduledoc false
    @enforce_keys [:module, :line]
    defstruct [:module, :line]

    @type t :: %__MODULE__{module: module(), line: pos_integer()}
  end

  @type t :: %__MODULE__{
          source: String.t(),
          inputs: MapSet.t(atom()),
          runtime_inputs: MapSet.t(atom()),
          param_inputs: MapSet.t(atom()),
          calls: [Call.t()],
          asset_refs: [AssetRef.t()]
        }

  @spec reserved_runtime_inputs() :: [atom()]
  def reserved_runtime_inputs, do: [:window_start, :window_end]

  @spec compile!(String.t(), keyword()) :: t()
  def compile!(sql, opts \\ []) when is_binary(sql) and is_list(opts) do
    known_definitions = Keyword.get(opts, :known_definitions, %{})
    file = Keyword.fetch!(opts, :file)
    line = Keyword.fetch!(opts, :line)
    module = Keyword.get(opts, :module)

    calls = extract_calls(sql, known_definitions)
    validate_calls!(calls, known_definitions, file, line)

    asset_refs = extract_asset_refs(sql)
    validate_asset_refs!(asset_refs, module, file, line)

    inputs = extract_inputs(sql)
    reserved_inputs = MapSet.new(reserved_runtime_inputs())
    runtime_inputs = MapSet.intersection(inputs, reserved_inputs)
    param_inputs = MapSet.difference(inputs, reserved_inputs)

    %__MODULE__{
      source: sql,
      inputs: inputs,
      runtime_inputs: runtime_inputs,
      param_inputs: param_inputs,
      calls: calls,
      asset_refs: asset_refs
    }
  end

  defp extract_inputs(sql) do
    ~r/@([a-z][a-z0-9_]*)/
    |> Regex.scan(sql, capture: :all_but_first)
    |> List.flatten()
    |> Enum.map(&String.to_atom/1)
    |> MapSet.new()
  end

  defp extract_calls(sql, known_definitions) do
    known_definitions
    |> Map.keys()
    |> Enum.map(&elem(&1, 0))
    |> Enum.uniq()
    |> Enum.flat_map(&extract_named_calls(sql, &1))
    |> Enum.uniq_by(fn %Call{name: name, arity: arity, context: context, line: line} ->
      {name, arity, context, line}
    end)
  end

  defp extract_named_calls(sql, name) do
    pattern = ~r/\b#{name}\s*\(/

    pattern
    |> Regex.scan(sql, return: :index)
    |> Enum.flat_map(fn [{start_idx, _len}] ->
      case parse_call(sql, start_idx, name) do
        {:ok, call} -> [call]
        :error -> []
      end
    end)
  end

  defp parse_call(sql, start_idx, name) do
    name_string = Atom.to_string(name)
    after_name_idx = start_idx + byte_size(name_string)
    open_idx = skip_spaces(sql, after_name_idx)

    if open_idx >= byte_size(sql) do
      :error
    else
      prefix = String.slice(sql, 0, start_idx)
      line = line_for_index(prefix)

      if String.at(sql, open_idx) == "(" do
        args_source = String.slice(sql, (open_idx + 1)..-1//1)

        case parse_arity(args_source) do
          {:ok, arity} ->
            context = if relation_prefix?(prefix), do: :relation, else: :expression
            {:ok, %Call{name: name, arity: arity, context: context, line: line}}

          :error ->
            :error
        end
      else
        :error
      end
    end
  end

  defp parse_arity(source) do
    chars = String.to_charlist(source)

    do_parse_arity(chars, 0, :outside, false, 0)
  end

  defp do_parse_arity([], _depth, _quote_state, _seen_nonspace, _count), do: :error

  defp do_parse_arity([char | rest], depth, quote_state, _seen_nonspace, count)
       when quote_state in [:single, :double] do
    case {quote_state, char} do
      {:single, ?'} -> do_parse_arity(rest, depth, :outside, true, count)
      {:double, ?"} -> do_parse_arity(rest, depth, :outside, true, count)
      _ -> do_parse_arity(rest, depth, quote_state, true, count)
    end
  end

  defp do_parse_arity([char | rest], depth, :outside, seen_nonspace, count) do
    cond do
      char == ?' ->
        do_parse_arity(rest, depth, :single, true, count)

      char == ?" ->
        do_parse_arity(rest, depth, :double, true, count)

      char == ?( ->
        do_parse_arity(rest, depth + 1, :outside, true, count)

      char == ?) and depth > 0 ->
        do_parse_arity(rest, depth - 1, :outside, true, count)

      char == ?) and depth == 0 ->
        final_count = if seen_nonspace, do: count + 1, else: 0
        {:ok, final_count}

      char == ?, and depth == 0 ->
        do_parse_arity(rest, depth, :outside, false, count + 1)

      char in [32, 9, 10, 13] ->
        do_parse_arity(rest, depth, :outside, seen_nonspace, count)

      true ->
        do_parse_arity(rest, depth, :outside, true, count)
    end
  end

  defp relation_prefix?(prefix) do
    line_prefix =
      prefix
      |> String.split("\n")
      |> List.last()
      |> to_string()
      |> String.trim_leading()

    Regex.match?(@relation_prefix_regex, line_prefix)
  end

  defp line_for_index(prefix) do
    prefix
    |> String.split("\n")
    |> length()
  end

  defp skip_spaces(sql, index) do
    case String.at(sql, index) do
      value when value in [" ", "\t", "\n", "\r"] -> skip_spaces(sql, index + 1)
      _ -> index
    end
  end

  defp validate_calls!(calls, known_definitions, file, fallback_line) do
    Enum.each(calls, fn %Call{name: name, arity: arity, context: context, line: line} ->
      case Map.fetch(known_definitions, {name, arity}) do
        {:ok, definition} ->
          validate_call_context!(definition.shape, context, name, arity, file, line)

        :error ->
          arities =
            known_definitions
            |> Map.keys()
            |> Enum.filter(fn {candidate_name, _candidate_arity} -> candidate_name == name end)
            |> Enum.map(&elem(&1, 1))
            |> Enum.sort()

          if arities != [] do
            compile_error!(
              file,
              line || fallback_line,
              "invalid SQL call #{name}/#{arity}; expected one of arities #{inspect(arities)}"
            )
          end
      end
    end)
  end

  defp validate_call_context!(:expression, :relation, name, arity, file, line) do
    compile_error!(
      file,
      line,
      "invalid SQL call #{name}/#{arity} in relation position; expected a relation SQL macro"
    )
  end

  defp validate_call_context!(:relation, :expression, name, arity, file, line) do
    compile_error!(
      file,
      line,
      "invalid SQL call #{name}/#{arity} in expression position; expected an expression SQL macro"
    )
  end

  defp validate_call_context!(_shape, _context, _name, _arity, _file, _line), do: :ok

  defp extract_asset_refs(sql) do
    pattern =
      ~r/\b(?:from|join|inner\s+join|left(?:\s+outer)?\s+join|right(?:\s+outer)?\s+join|full(?:\s+outer)?\s+join|cross\s+join)\s+([A-Z][A-Za-z0-9_]*(?:\.[A-Z][A-Za-z0-9_]*)+)/i

    pattern
    |> Regex.scan(sql, capture: :all_but_first, return: :index)
    |> Enum.map(fn [{name_start, name_len}] ->
      name = String.slice(sql, name_start, name_len)
      module = name |> String.split(".") |> Module.concat()
      line = line_for_index(String.slice(sql, 0, name_start))
      %AssetRef{module: module, line: line}
    end)
  end

  defp validate_asset_refs!(asset_refs, current_module, file, fallback_line) do
    Enum.each(asset_refs, fn %AssetRef{module: module, line: line} ->
      if module == current_module do
        compile_error!(
          file,
          line || fallback_line,
          "SQL asset cannot reference itself as a relation"
        )
      else
        validate_compiled_asset_module!(module, file, line || fallback_line)
      end
    end)
  end

  defp validate_compiled_asset_module!(module, file, line) do
    case Code.ensure_compiled(module) do
      {:module, _module} ->
        if function_exported?(module, :__favn_single_asset__, 0) and
             module.__favn_single_asset__() do
          :ok
        else
          compile_error!(
            file,
            line,
            "invalid SQL asset reference #{inspect(module)}; expected a compiled single-asset module"
          )
        end

      {:error, _reason} ->
        :ok
    end
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end
end
