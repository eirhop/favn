defmodule Favn.SQL.Template do
  @moduledoc """
  Ordered SQL template IR used by Phase 3 SQL authoring.
  """

  alias Favn.Assets.Compiler
  alias Favn.RelationRef
  alias Favn.SQL.Definition
  alias MapSet

  @reserved_runtime_inputs [:window_start, :window_end]
  @join_prefixes ["inner", "left", "right", "full", "cross"]
  @outer_join_prefixes ["left", "right", "full"]
  @join_entry_prefixes [
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "left outer",
    "right outer",
    "full outer"
  ]

  @type root_kind :: :query | :expression
  @type placeholder_source :: :runtime | :query_param | {:local_arg, non_neg_integer()}
  @type context :: :expression | :relation

  defmodule Position do
    @moduledoc false
    @enforce_keys [:offset, :line, :column]
    defstruct [:offset, :line, :column]

    @type t :: %__MODULE__{offset: non_neg_integer(), line: pos_integer(), column: pos_integer()}
  end

  defmodule Span do
    @moduledoc false
    @enforce_keys [
      :start_offset,
      :end_offset,
      :start_line,
      :start_column,
      :end_line,
      :end_column
    ]
    defstruct [
      :start_offset,
      :end_offset,
      :start_line,
      :start_column,
      :end_line,
      :end_column
    ]

    @type t :: %__MODULE__{
            start_offset: non_neg_integer(),
            end_offset: non_neg_integer(),
            start_line: pos_integer(),
            start_column: pos_integer(),
            end_line: pos_integer(),
            end_column: pos_integer()
          }
  end

  defmodule Requirements do
    @moduledoc false
    @enforce_keys [:runtime_inputs, :query_params]
    defstruct [:runtime_inputs, :query_params]

    @type t :: %__MODULE__{runtime_inputs: MapSet.t(atom()), query_params: MapSet.t(atom())}
  end

  defmodule Fragment do
    @moduledoc false
    @enforce_keys [:nodes, :span]
    defstruct [:nodes, :span]

    @type t :: %__MODULE__{nodes: [Favn.SQL.Template.ir_node()], span: Favn.SQL.Template.Span.t()}
  end

  defmodule Text do
    @moduledoc false
    @enforce_keys [:sql, :span]
    defstruct [:sql, :span]

    @type t :: %__MODULE__{sql: String.t(), span: Favn.SQL.Template.Span.t()}
  end

  defmodule Placeholder do
    @moduledoc false
    @enforce_keys [:name, :source, :span]
    defstruct [:name, :source, :span]

    @type t :: %__MODULE__{
            name: atom(),
            source: Favn.SQL.Template.placeholder_source(),
            span: Favn.SQL.Template.Span.t()
          }
  end

  defmodule DefinitionRef do
    @moduledoc false
    @enforce_keys [:provider, :name, :arity, :kind]
    defstruct [:provider, :name, :arity, :kind]

    @type t :: %__MODULE__{
            provider: module(),
            name: atom(),
            arity: non_neg_integer(),
            kind: :expression | :relation
          }
  end

  defmodule Call do
    @moduledoc false
    @enforce_keys [:definition, :args, :context, :span]
    defstruct [:definition, :args, :context, :span]

    @type t :: %__MODULE__{
            definition: Favn.SQL.Template.DefinitionRef.t(),
            args: [Favn.SQL.Template.Fragment.t()],
            context: Favn.SQL.Template.context(),
            span: Favn.SQL.Template.Span.t()
          }
  end

  defmodule AssetRef do
    @moduledoc false
    @enforce_keys [:module, :asset_ref, :produced_relation, :resolution, :span]
    defstruct [:module, :asset_ref, :produced_relation, :resolution, :span]

    @type t :: %__MODULE__{
            module: module(),
            asset_ref: {module(), :asset},
            produced_relation: RelationRef.t() | nil,
            resolution: :resolved | :deferred,
            span: Favn.SQL.Template.Span.t()
          }
  end

  defmodule Relation do
    @moduledoc false
    @enforce_keys [:raw, :segments, :span]
    defstruct [:raw, :segments, :span]

    @type t :: %__MODULE__{
            raw: String.t(),
            segments: [String.t()],
            span: Favn.SQL.Template.Span.t()
          }
  end

  @type ir_node :: Text.t() | Placeholder.t() | Call.t() | AssetRef.t() | Relation.t()

  @enforce_keys [:source, :root_kind, :nodes, :span, :requires]
  defstruct [:source, :root_kind, :nodes, :span, :requires]

  @type t :: %__MODULE__{
          source: String.t(),
          root_kind: root_kind(),
          nodes: [ir_node()],
          span: Span.t(),
          requires: Requirements.t()
        }

  @type compile_opt ::
          {:known_definitions, %{optional({atom(), non_neg_integer()}) => Definition.t()}}
          | {:file, String.t()}
          | {:line, pos_integer()}
          | {:column, pos_integer()}
          | {:offset, non_neg_integer()}
          | {:module, module() | nil}
          | {:scope, :query | :definition | :fragment}
          | {:local_args, [atom()]}
          | {:local_arg_index, %{optional(atom()) => non_neg_integer()}}
          | {:enforce_query_root, boolean()}

  @spec reserved_runtime_inputs() :: [atom()]
  def reserved_runtime_inputs, do: @reserved_runtime_inputs

  @spec infer_root_kind!(String.t(), keyword()) :: root_kind()
  def infer_root_kind!(sql, opts) when is_binary(sql) and is_list(opts) do
    file = Keyword.fetch!(opts, :file)
    line = Keyword.fetch!(opts, :line)
    column = Keyword.get(opts, :column, 1)
    offset = Keyword.get(opts, :offset, 0)
    start_pos = %Position{offset: offset, line: line, column: column}
    infer_root_kind!(sql, file, line, start_pos)
  end

  @spec compile!(String.t(), [compile_opt()]) :: t()
  def compile!(sql, opts \\ []) when is_binary(sql) and is_list(opts) do
    file = Keyword.fetch!(opts, :file)
    line = Keyword.fetch!(opts, :line)
    column = Keyword.get(opts, :column, 1)
    offset = Keyword.get(opts, :offset, 0)
    known_definitions = Keyword.get(opts, :known_definitions, %{})
    module = Keyword.get(opts, :module)
    scope = Keyword.get(opts, :scope, :query)

    local_arg_index =
      Keyword.get_lazy(opts, :local_arg_index, fn ->
        opts
        |> Keyword.get(:local_args, [])
        |> local_arg_index()
      end)

    enforce_query_root = Keyword.get(opts, :enforce_query_root, false)

    start_pos = %Position{offset: offset, line: line, column: column}
    root_kind = infer_root_kind!(sql, file, line, start_pos)

    if enforce_query_root and root_kind != :query do
      compile_error!(file, line, "query body must start with select or with")
    end

    parser_state = %{
      file: file,
      module: module,
      known_definitions: known_definitions,
      local_args: local_arg_index,
      position: start_pos,
      relation_entry?: false,
      join_prefix: nil,
      paren_depth: 0,
      statement_kind: top_level_statement_kind(root_kind),
      scope: scope,
      root_kind: root_kind,
      cte_names: MapSet.new(),
      cte_active?: false,
      cte_expect_alias?: false,
      cte_waiting_query?: false,
      cte_query_depth: nil,
      cte_ready_for_next?: false
    }

    {nodes, end_state} = parse_nodes(String.to_charlist(sql), parser_state, [])
    span = span(start_pos, end_state.position)

    %__MODULE__{
      source: sql,
      root_kind: root_kind,
      nodes: nodes,
      span: span,
      requires: gather_requirements(nodes)
    }
  end

  @spec called_definition_keys(t()) :: [{atom(), non_neg_integer()}]
  def called_definition_keys(%__MODULE__{nodes: nodes}),
    do: called_definition_keys_from_nodes(nodes)

  @spec runtime_inputs(t()) :: MapSet.t(atom())
  def runtime_inputs(%__MODULE__{requires: %Requirements{runtime_inputs: runtime_inputs}}),
    do: runtime_inputs

  @spec query_params(t()) :: MapSet.t(atom())
  def query_params(%__MODULE__{requires: %Requirements{query_params: query_params}}),
    do: query_params

  @spec asset_refs(t()) :: [AssetRef.t()]
  def asset_refs(%__MODULE__{nodes: nodes}), do: collect_asset_refs(nodes)

  @spec relation_refs(t()) :: [Relation.t()]
  def relation_refs(%__MODULE__{nodes: nodes}), do: collect_relation_refs(nodes)

  @spec calls(t()) :: [Call.t()]
  def calls(%__MODULE__{nodes: nodes}), do: collect_calls(nodes)

  defp called_definition_keys_from_nodes(nodes) do
    nodes
    |> Enum.flat_map(fn
      %Call{definition: %DefinitionRef{name: name, arity: arity}, args: args} ->
        [{name, arity} | Enum.flat_map(args, &called_definition_keys_from_nodes(&1.nodes))]

      _other ->
        []
    end)
    |> Enum.uniq()
  end

  defp collect_asset_refs(nodes) do
    Enum.flat_map(nodes, fn
      %AssetRef{} = asset_ref -> [asset_ref]
      %Call{args: args} -> Enum.flat_map(args, &collect_asset_refs(&1.nodes))
      _other -> []
    end)
  end

  defp collect_calls(nodes) do
    Enum.flat_map(nodes, fn
      %Call{} = call -> [call | Enum.flat_map(call.args, &collect_calls(&1.nodes))]
      _other -> []
    end)
  end

  defp collect_relation_refs(nodes) do
    Enum.flat_map(nodes, fn
      %Relation{} = relation_ref -> [relation_ref]
      %Call{args: args} -> Enum.flat_map(args, &collect_relation_refs(&1.nodes))
      _other -> []
    end)
  end

  defp infer_root_kind!(sql, file, line, _start_pos) do
    case first_top_level_token(String.to_charlist(sql), :code, 0) do
      nil -> compile_error!(file, line, "SQL body cannot be empty")
      "select" -> :query
      "with" -> :query
      _other -> :expression
    end
  end

  defp first_top_level_token([], _lex_state, _depth), do: nil

  defp first_top_level_token([?-, ?- | rest], :code, depth),
    do: first_top_level_token(rest, :line_comment, depth)

  defp first_top_level_token([?/, ?* | rest], :code, depth),
    do: first_top_level_token(rest, :block_comment, depth)

  defp first_top_level_token([?' | rest], :code, depth),
    do: first_top_level_token(rest, :single_quote, depth)

  defp first_top_level_token([?" | rest], :code, depth),
    do: first_top_level_token(rest, :double_quote, depth)

  defp first_top_level_token([char | rest], :line_comment, depth) do
    if char == ?\n,
      do: first_top_level_token(rest, :code, depth),
      else: first_top_level_token(rest, :line_comment, depth)
  end

  defp first_top_level_token([?*, ?/ | rest], :block_comment, depth),
    do: first_top_level_token(rest, :code, depth)

  defp first_top_level_token([_char | rest], :block_comment, depth),
    do: first_top_level_token(rest, :block_comment, depth)

  defp first_top_level_token([?', ?' | rest], :single_quote, depth),
    do: first_top_level_token(rest, :single_quote, depth)

  defp first_top_level_token([?' | rest], :single_quote, depth),
    do: first_top_level_token(rest, :code, depth)

  defp first_top_level_token([_char | rest], :single_quote, depth),
    do: first_top_level_token(rest, :single_quote, depth)

  defp first_top_level_token([?", ?" | rest], :double_quote, depth),
    do: first_top_level_token(rest, :double_quote, depth)

  defp first_top_level_token([?" | rest], :double_quote, depth),
    do: first_top_level_token(rest, :code, depth)

  defp first_top_level_token([_char | rest], :double_quote, depth),
    do: first_top_level_token(rest, :double_quote, depth)

  defp first_top_level_token([char | rest], :code, depth) when char in [32, 9, 10, 13],
    do: first_top_level_token(rest, :code, depth)

  defp first_top_level_token([?( | rest], :code, depth),
    do: first_top_level_token(rest, :code, depth + 1)

  defp first_top_level_token([?) | rest], :code, depth) when depth > 0,
    do: first_top_level_token(rest, :code, depth - 1)

  defp first_top_level_token([char | _rest] = chars, :code, 0)
       when (char >= ?A and char <= ?Z) or (char >= ?a and char <= ?z) or char == ?_ do
    {word, _tail} = read_identifier(chars)
    String.downcase(word)
  end

  defp first_top_level_token([_char | rest], :code, depth),
    do: first_top_level_token(rest, :code, depth)

  defp parse_nodes([], state, acc), do: {Enum.reverse(acc), state}

  defp parse_nodes([?-, ?- | rest], state, acc) do
    {text, tail, next_pos} = consume_line_comment(rest, state.position, ~c"--")

    parse_nodes(tail, %{state | position: next_pos}, [
      text_node(text, state.position, next_pos) | acc
    ])
  end

  defp parse_nodes([?/, ?* | rest], state, acc) do
    {text, tail, next_pos} = consume_block_comment(rest, state.position, ~c"/*")

    parse_nodes(tail, %{state | position: next_pos}, [
      text_node(text, state.position, next_pos) | acc
    ])
  end

  defp parse_nodes([?' | rest], state, acc) do
    {text, tail, next_pos} = consume_quoted(rest, state.position, ?')

    parse_nodes(tail, %{state | position: next_pos}, [
      text_node(text, state.position, next_pos) | acc
    ])
  end

  defp parse_nodes([?" | rest], state, acc) do
    {text, tail, next_pos} = consume_quoted(rest, state.position, ?")

    parse_nodes(tail, %{state | position: next_pos}, [
      text_node(text, state.position, next_pos) | acc
    ])
  end

  defp parse_nodes([?@ | rest], state, acc) do
    parse_placeholder(rest, state, acc)
  end

  defp parse_nodes([char | _rest] = chars, state, acc)
       when (char >= ?A and char <= ?Z) or (char >= ?a and char <= ?z) or char == ?_ do
    parse_identifier(chars, state, acc)
  end

  defp parse_nodes([?, | rest], state, acc) do
    next_state =
      state
      |> advance_state(~c",")
      |> maybe_continue_cte_sequence_after_comma()

    parse_nodes(rest, next_state, [text_node(",", state.position, next_state.position) | acc])
  end

  defp parse_nodes([char | rest], state, acc) when char not in [?(, ?), ?;] do
    state = maybe_close_cte_sequence(state, char)
    {next_state, text} = consume_single_char(char, state)
    parse_nodes(rest, next_state, [text_node(text, state.position, next_state.position) | acc])
  end

  defp parse_nodes([?( | rest], state, acc) do
    next_state =
      state
      |> advance_state(~c"(")
      |> Map.put(:paren_depth, state.paren_depth + 1)
      |> Map.put(:relation_entry?, false)
      |> Map.put(:join_prefix, nil)
      |> maybe_enter_cte_query()

    parse_nodes(rest, next_state, [text_node("(", state.position, next_state.position) | acc])
  end

  defp parse_nodes([?) | rest], state, acc) do
    next_state =
      state
      |> advance_state(~c")")
      |> Map.put(:paren_depth, max(state.paren_depth - 1, 0))
      |> Map.put(:relation_entry?, false)
      |> Map.put(:join_prefix, nil)
      |> maybe_exit_cte_query(state)

    parse_nodes(rest, next_state, [text_node(")", state.position, next_state.position) | acc])
  end

  defp parse_nodes([?; | rest], %{paren_depth: 0} = state, acc) do
    next_state =
      state
      |> advance_state(~c";")
      |> Map.put(:relation_entry?, false)
      |> Map.put(:join_prefix, nil)
      |> Map.put(:statement_kind, top_level_statement_kind(state.root_kind))
      |> Map.put(:cte_names, MapSet.new())
      |> Map.put(:cte_active?, false)
      |> Map.put(:cte_expect_alias?, false)
      |> Map.put(:cte_waiting_query?, false)
      |> Map.put(:cte_query_depth, nil)
      |> Map.put(:cte_ready_for_next?, false)

    parse_nodes(rest, next_state, [text_node(";", state.position, next_state.position) | acc])
  end

  defp parse_nodes([?; | rest], %{paren_depth: depth} = state, acc) when depth > 0 do
    next_state = advance_state(state, ~c";")
    parse_nodes(rest, next_state, [text_node(";", state.position, next_state.position) | acc])
  end

  defp parse_placeholder([next | _] = rest, state, acc)
       when (next >= ?a and next <= ?z) or next == ?_ or (next >= ?A and next <= ?Z) or
              (next >= ?0 and next <= ?9) do
    case rest do
      [valid | _] when valid >= ?a and valid <= ?z ->
        {name_string, tail} = read_identifier(rest)
        name = String.to_atom(name_string)
        next_state = advance_state(state, ~c"@" ++ String.to_charlist(name_string))
        placeholder = build_placeholder(name, state, next_state)
        parse_nodes(tail, next_state, [placeholder | acc])

      _other ->
        compile_error!(
          state.file,
          state.position.line,
          "invalid SQL placeholder at line #{state.position.line}, column #{state.position.column}"
        )
    end
  end

  defp parse_placeholder(rest, state, acc) do
    next_state = advance_state(state, ~c"@")
    parse_nodes(rest, next_state, [text_node("@", state.position, next_state.position) | acc])
  end

  defp parse_identifier(chars, state, acc) do
    {word, tail} = read_identifier(chars)
    state = update_cte_state_for_identifier(state, word)
    context = if state.relation_entry?, do: :relation, else: :expression

    cond do
      asset_ref_candidate?(word, tail, state) ->
        parse_asset_ref(word, tail, state, acc)

      call_candidate?(word, tail, state) ->
        parse_call(word, tail, state, context, acc)

      relation_ref_candidate?(word, tail, state) ->
        parse_relation_ref(word, tail, state, acc)

      true ->
        next_state =
          advance_state(state, String.to_charlist(word))
          |> update_relation_context(String.downcase(word))

        parse_nodes(tail, next_state, [text_node(word, state.position, next_state.position) | acc])
    end
  end

  defp parse_relation_ref(word, tail, state, acc) do
    {segments, tail_after_relation} = read_relation_chain(tail, [word])

    if length(segments) in 1..3 do
      raw = Enum.join(segments, ".")

      next_state =
        advance_state(state, String.to_charlist(raw))
        |> Map.put(:relation_entry?, false)
        |> Map.put(:join_prefix, nil)

      node = build_relation_ref(raw, segments, state, next_state)
      parse_nodes(tail_after_relation, next_state, [node | acc])
    else
      raw = Enum.join(segments, ".")
      next_state = advance_state(state, String.to_charlist(raw))

      parse_nodes(tail_after_relation, next_state, [
        text_node(raw, state.position, next_state.position) | acc
      ])
    end
  end

  defp parse_asset_ref(word, tail, state, acc) do
    {segments, tail_after_module} = read_alias_chain(tail, [word])
    next_char = peek_nonspace_char(tail_after_module)

    if length(segments) >= 2 and uppercase_alias_segments?(segments) and next_char != ?( do
      raw = Enum.join(segments, ".")

      next_state =
        advance_state(state, String.to_charlist(raw))
        |> Map.put(:relation_entry?, false)
        |> Map.put(:join_prefix, nil)

      node = build_asset_ref(Module.concat(segments), state, next_state)
      parse_nodes(tail_after_module, next_state, [node | acc])
    else
      next_state =
        advance_state(state, String.to_charlist(word))
        |> update_relation_context(String.downcase(word))

      parse_nodes(tail, next_state, [text_node(word, state.position, next_state.position) | acc])
    end
  end

  defp parse_call(word, tail, state, context, acc) do
    {space_chars, after_spaces} = take_horizontal_space(tail, [])

    case after_spaces do
      [?( | rest_after_open] ->
        start_pos = state.position
        open_state = advance_state(state, String.to_charlist(word) ++ space_chars ++ ~c"(")

        {args, tail_after_call, end_pos} =
          parse_call_arguments(rest_after_open, open_state.position, state, [])

        arity = length(args)
        visible_arities = visible_arities(word, state.known_definitions)

        case Map.fetch(state.known_definitions, {String.to_atom(word), arity}) do
          {:ok, definition} ->
            call = build_call(definition, args, context, start_pos, end_pos)

            next_state = %{
              state
              | position: end_pos,
                relation_entry?: false,
                join_prefix: nil,
                paren_depth: state.paren_depth
            }

            validate_call_context!(call, state.file)
            parse_nodes(tail_after_call, next_state, [call | acc])

          :error when visible_arities != [] ->
            compile_error!(
              state.file,
              start_pos.line,
              "invalid SQL call #{word}/#{arity}; expected one of arities #{inspect(visible_arities)}"
            )

          :error ->
            next_state =
              advance_state(state, String.to_charlist(word))
              |> update_relation_context(String.downcase(word))

            parse_nodes(tail, next_state, [
              text_node(word, state.position, next_state.position) | acc
            ])
        end

      _other ->
        next_state =
          advance_state(state, String.to_charlist(word))
          |> update_relation_context(String.downcase(word))

        parse_nodes(tail, next_state, [text_node(word, state.position, next_state.position) | acc])
    end
  end

  defp parse_call_arguments(chars, current_pos, state, acc) do
    do_parse_call_arguments(chars, current_pos, current_pos, :code, 0, [], state, acc)
  end

  defp do_parse_call_arguments(
         [],
         _current_pos,
         _arg_start,
         _lex_state,
         _depth,
         _buffer,
         state,
         _acc
       ) do
    compile_error!(state.file, state.position.line, "unterminated SQL call arguments")
  end

  defp do_parse_call_arguments(
         [?-, ?- | rest],
         current_pos,
         arg_start,
         :code,
         depth,
         buffer,
         state,
         acc
       ),
       do: consume_arg_line_comment(rest, current_pos, arg_start, depth, buffer, state, acc)

  defp do_parse_call_arguments(
         [?/, ?* | rest],
         current_pos,
         arg_start,
         :code,
         depth,
         buffer,
         state,
         acc
       ),
       do: consume_arg_block_comment(rest, current_pos, arg_start, depth, buffer, state, acc)

  defp do_parse_call_arguments(
         [?' | rest],
         current_pos,
         arg_start,
         :code,
         depth,
         buffer,
         state,
         acc
       ),
       do: consume_arg_quoted(rest, current_pos, arg_start, depth, buffer, state, acc, ?')

  defp do_parse_call_arguments(
         [?" | rest],
         current_pos,
         arg_start,
         :code,
         depth,
         buffer,
         state,
         acc
       ),
       do: consume_arg_quoted(rest, current_pos, arg_start, depth, buffer, state, acc, ?")

  defp do_parse_call_arguments(
         [?( | rest],
         current_pos,
         arg_start,
         :code,
         depth,
         buffer,
         state,
         acc
       ) do
    next_pos = advance_position(current_pos, ~c"(")

    do_parse_call_arguments(
      rest,
      next_pos,
      arg_start,
      :code,
      depth + 1,
      [?( | buffer],
      state,
      acc
    )
  end

  defp do_parse_call_arguments([?) | rest], current_pos, arg_start, :code, 0, buffer, state, acc) do
    end_pos = advance_position(current_pos, ~c")")
    fragment = build_argument_fragment(Enum.reverse(buffer), arg_start, current_pos, state)
    {Enum.reverse([fragment | acc]), rest, end_pos}
  end

  defp do_parse_call_arguments(
         [?) | rest],
         current_pos,
         arg_start,
         :code,
         depth,
         buffer,
         state,
         acc
       ) do
    next_pos = advance_position(current_pos, ~c")")

    do_parse_call_arguments(
      rest,
      next_pos,
      arg_start,
      :code,
      depth - 1,
      [?) | buffer],
      state,
      acc
    )
  end

  defp do_parse_call_arguments([?, | rest], current_pos, arg_start, :code, 0, buffer, state, acc) do
    comma_pos = advance_position(current_pos, ~c",")
    fragment = build_argument_fragment(Enum.reverse(buffer), arg_start, current_pos, state)
    do_parse_call_arguments(rest, comma_pos, comma_pos, :code, 0, [], state, [fragment | acc])
  end

  defp do_parse_call_arguments(
         [char | rest],
         current_pos,
         arg_start,
         :code,
         depth,
         buffer,
         state,
         acc
       ) do
    next_pos = advance_position(current_pos, [char])
    do_parse_call_arguments(rest, next_pos, arg_start, :code, depth, [char | buffer], state, acc)
  end

  defp consume_arg_line_comment(rest, current_pos, arg_start, depth, buffer, state, acc) do
    {text, tail, next_pos} = consume_line_comment(rest, current_pos, ~c"--")

    do_parse_call_arguments(
      tail,
      next_pos,
      arg_start,
      :code,
      depth,
      Enum.reverse(String.to_charlist(text)) ++ buffer,
      state,
      acc
    )
  end

  defp consume_arg_block_comment(rest, current_pos, arg_start, depth, buffer, state, acc) do
    {text, tail, next_pos} = consume_block_comment(rest, current_pos, ~c"/*")

    do_parse_call_arguments(
      tail,
      next_pos,
      arg_start,
      :code,
      depth,
      Enum.reverse(String.to_charlist(text)) ++ buffer,
      state,
      acc
    )
  end

  defp consume_arg_quoted(rest, current_pos, arg_start, depth, buffer, state, acc, quote_char) do
    {text, tail, next_pos} = consume_quoted(rest, current_pos, [quote_char])

    do_parse_call_arguments(
      tail,
      next_pos,
      arg_start,
      :code,
      depth,
      Enum.reverse(String.to_charlist(text)) ++ buffer,
      state,
      acc
    )
  end

  defp build_argument_fragment(chars, start_pos, end_pos, state) do
    source = chars |> to_string()

    template =
      compile!(source,
        known_definitions: state.known_definitions,
        file: state.file,
        line: start_pos.line,
        column: start_pos.column,
        offset: start_pos.offset,
        module: state.module,
        scope: state.scope,
        local_arg_index: state.local_args,
        enforce_query_root: false
      )

    %Fragment{nodes: template.nodes, span: span(start_pos, end_pos)}
  end

  defp build_placeholder(name, state, next_state) do
    source = classify_placeholder_source(name, state)
    %Placeholder{name: name, source: source, span: span(state.position, next_state.position)}
  end

  defp build_call(definition, args, context, start_pos, end_pos) do
    %Call{
      definition: %DefinitionRef{
        provider: definition.module,
        name: definition.name,
        arity: definition.arity,
        kind: definition.shape
      },
      args: args,
      context: context,
      span: span(start_pos, end_pos)
    }
  end

  defp build_asset_ref(module, state, next_state) do
    {resolution, produced_relation} =
      resolve_asset_reference(module, state.file, state.position.line, state.module)

    %AssetRef{
      module: module,
      asset_ref: {module, :asset},
      produced_relation: produced_relation,
      resolution: resolution,
      span: span(state.position, next_state.position)
    }
  end

  defp build_relation_ref(raw, segments, state, next_state) do
    %Relation{raw: raw, segments: segments, span: span(state.position, next_state.position)}
  end

  defp classify_placeholder_source(name, _state) when name in @reserved_runtime_inputs,
    do: :runtime

  defp classify_placeholder_source(name, %{scope: :definition, local_args: local_args} = state) do
    case Map.fetch(local_args, name) do
      {:ok, index} ->
        {:local_arg, index}

      :error ->
        compile_error!(
          state.file,
          state.position.line,
          "undefined defsql placeholder @#{name}; expected one of #{inspect(Map.keys(local_args))}"
        )
    end
  end

  defp classify_placeholder_source(_name, _state), do: :query_param

  defp asset_ref_candidate?(word, tail, state) do
    state.statement_kind not in [:update, :merge] and
      state.relation_entry? and
      String.match?(word, ~r/^[A-Z][A-Za-z0-9_]*$/) and
      List.first(tail) == ?.
  end

  defp relation_ref_candidate?(word, tail, state) do
    state.statement_kind not in [:update, :merge] and
      state.relation_entry? and
      String.match?(word, ~r/^[a-z_][A-Za-z0-9_]*$/) and
      not cte_name?(state, word) and
      peek_nonspace_char(tail) != ?(
  end

  defp cte_name?(state, word) do
    MapSet.member?(state.cte_names, String.downcase(word))
  end

  defp call_candidate?(word, tail, state) do
    state.known_definitions != %{} and
      visible_arities(word, state.known_definitions) != [] and
      peek_nonspace_char(tail) == ?(
  end

  defp visible_arities(word, known_definitions) do
    name = String.to_atom(word)

    known_definitions
    |> Map.keys()
    |> Enum.filter(fn {candidate_name, _arity} -> candidate_name == name end)
    |> Enum.map(&elem(&1, 1))
    |> Enum.sort()
  end

  defp validate_call_context!(
         %Call{
           definition: %DefinitionRef{kind: :expression, name: name, arity: arity},
           context: :relation,
           span: span
         },
         file
       ) do
    compile_error!(
      file,
      span.start_line,
      "invalid SQL call #{name}/#{arity} in relation position; expected a relation SQL macro"
    )
  end

  defp validate_call_context!(
         %Call{
           definition: %DefinitionRef{kind: :relation, name: name, arity: arity},
           context: :expression,
           span: span
         },
         file
       ) do
    compile_error!(
      file,
      span.start_line,
      "invalid SQL call #{name}/#{arity} in expression position; expected an expression SQL macro"
    )
  end

  defp validate_call_context!(_call, _file), do: :ok

  defp resolve_asset_reference(module, file, line, current_module) do
    if module == current_module do
      compile_error!(file, line, "SQL asset cannot reference itself as a relation")
    end

    case Code.ensure_compiled(module) do
      {:module, _} -> validate_compiled_asset_module!(module, file, line)
      {:error, _reason} -> {:deferred, nil}
    end
  end

  defp validate_compiled_asset_module!(module, file, line) do
    if function_exported?(module, :__favn_single_asset__, 0) and module.__favn_single_asset__() do
      case Compiler.compile_module_assets(module) do
        {:ok, [%{produces: %RelationRef{} = produces}]} ->
          {:resolved, produces}

        {:ok, [%{produces: nil}]} ->
          compile_error!(
            file,
            line,
            "SQL asset reference #{inspect(module)} does not resolve to a produced relation"
          )

        {:ok, [_asset | _rest]} ->
          compile_error!(
            file,
            line,
            "invalid SQL asset reference #{inspect(module)}; expected a compiled single-asset module"
          )

        {:error, _reason} ->
          compile_error!(
            file,
            line,
            "invalid SQL asset reference #{inspect(module)}; failed to compile asset definition"
          )
      end
    else
      compile_error!(
        file,
        line,
        "invalid SQL asset reference #{inspect(module)}; expected a compiled single-asset module"
      )
    end
  end

  defp gather_requirements(nodes) do
    {runtime_inputs, query_params} = gather_requirements(nodes, MapSet.new(), MapSet.new())
    %Requirements{runtime_inputs: runtime_inputs, query_params: query_params}
  end

  defp gather_requirements([], runtime_inputs, query_params), do: {runtime_inputs, query_params}

  defp gather_requirements([node | rest], runtime_inputs, query_params) do
    {runtime_inputs, query_params} =
      case node do
        %Placeholder{name: name, source: :runtime} ->
          {MapSet.put(runtime_inputs, name), query_params}

        %Placeholder{name: name, source: :query_param} ->
          {runtime_inputs, MapSet.put(query_params, name)}

        %Call{args: args} ->
          Enum.reduce(args, {runtime_inputs, query_params}, fn %Fragment{nodes: arg_nodes}, acc ->
            gather_requirements(arg_nodes, elem(acc, 0), elem(acc, 1))
          end)

        _other ->
          {runtime_inputs, query_params}
      end

    gather_requirements(rest, runtime_inputs, query_params)
  end

  defp text_node(text, start_pos, end_pos),
    do: %Text{sql: to_string(text), span: span(start_pos, end_pos)}

  defp consume_single_char(char, state) do
    text = <<char::utf8>>
    {advance_state(state, [char]), text}
  end

  defp consume_line_comment(rest, start_pos, prefix_chars) do
    do_consume_line_comment(
      rest,
      advance_position(start_pos, prefix_chars),
      Enum.reverse(prefix_chars)
    )
  end

  defp do_consume_line_comment([], pos, acc), do: {acc |> Enum.reverse() |> to_string(), [], pos}

  defp do_consume_line_comment([char | rest] = chars, pos, acc) do
    next_pos = advance_position(pos, [char])

    if char == ?\n do
      {[char | acc] |> Enum.reverse() |> to_string(), rest, next_pos}
    else
      do_consume_line_comment(chars |> tl(), next_pos, [char | acc])
    end
  end

  defp consume_block_comment(rest, start_pos, prefix_chars) do
    do_consume_block_comment(
      rest,
      advance_position(start_pos, prefix_chars),
      Enum.reverse(prefix_chars)
    )
  end

  defp do_consume_block_comment([], pos, acc), do: {acc |> Enum.reverse() |> to_string(), [], pos}

  defp do_consume_block_comment([?*, ?/ | rest], pos, acc) do
    next_pos = advance_position(pos, ~c"*/")
    {[?/, ?* | acc] |> Enum.reverse() |> to_string(), rest, next_pos}
  end

  defp do_consume_block_comment([char | rest], pos, acc) do
    do_consume_block_comment(rest, advance_position(pos, [char]), [char | acc])
  end

  defp consume_quoted(rest, start_pos, quote_char) when is_integer(quote_char),
    do: consume_quoted(rest, start_pos, [quote_char])

  defp consume_quoted(rest, start_pos, [quote_char]) do
    do_consume_quoted(rest, advance_position(start_pos, [quote_char]), [quote_char], quote_char)
  end

  defp do_consume_quoted([], pos, acc, _quote_char),
    do: {acc |> Enum.reverse() |> to_string(), [], pos}

  defp do_consume_quoted([quote_char, quote_char | rest], pos, acc, quote_char) do
    next_pos = advance_position(pos, [quote_char, quote_char])
    do_consume_quoted(rest, next_pos, [quote_char, quote_char | acc], quote_char)
  end

  defp do_consume_quoted([quote_char | rest], pos, acc, quote_char) do
    next_pos = advance_position(pos, [quote_char])
    {[quote_char | acc] |> Enum.reverse() |> to_string(), rest, next_pos}
  end

  defp do_consume_quoted([char | rest], pos, acc, quote_char) do
    do_consume_quoted(rest, advance_position(pos, [char]), [char | acc], quote_char)
  end

  defp take_horizontal_space([char | rest], acc) when char in [32, 9, 10, 13],
    do: take_horizontal_space(rest, [char | acc])

  defp take_horizontal_space(rest, acc), do: {Enum.reverse(acc), rest}

  defp peek_nonspace_char([char | rest]) when char in [32, 9, 10, 13],
    do: peek_nonspace_char(rest)

  defp peek_nonspace_char([char | _rest]), do: char
  defp peek_nonspace_char([]), do: nil

  defp read_identifier(chars), do: do_read_identifier(chars, [])

  defp do_read_identifier([char | rest], acc)
       when (char >= ?A and char <= ?Z) or (char >= ?a and char <= ?z) or
              (char >= ?0 and char <= ?9) or char == ?_ do
    do_read_identifier(rest, [char | acc])
  end

  defp do_read_identifier(rest, acc), do: {acc |> Enum.reverse() |> to_string(), rest}

  defp read_alias_chain([?. | rest], segments) do
    case rest do
      [char | _] when (char >= ?A and char <= ?Z) or (char >= ?a and char <= ?z) or char == ?_ ->
        {segment, tail} = read_identifier(rest)
        read_alias_chain(tail, segments ++ [segment])

      _ ->
        {segments, [?. | rest]}
    end
  end

  defp read_alias_chain(rest, segments), do: {segments, rest}

  defp read_relation_chain([?. | rest], segments) do
    case rest do
      [char | _tail]
      when (char >= ?a and char <= ?z) or (char >= ?A and char <= ?Z) or char == ?_ ->
        {segment, tail_after_segment} = read_identifier(rest)
        read_relation_chain(tail_after_segment, segments ++ [segment])

      _other ->
        {segments, [?. | rest]}
    end
  end

  defp read_relation_chain(rest, segments), do: {segments, rest}

  defp uppercase_alias_segments?(segments),
    do: Enum.all?(segments, &String.match?(&1, ~r/^[A-Z][A-Za-z0-9_]*$/))

  defp update_relation_context(state, "from"),
    do: %{
      state
      | relation_entry?: true,
        join_prefix: nil,
        statement_kind: update_statement_kind(state.statement_kind, "from", state.paren_depth)
    }

  defp update_relation_context(state, "join"),
    do: %{
      state
      | relation_entry?: true,
        join_prefix: nil,
        statement_kind: update_statement_kind(state.statement_kind, "join", state.paren_depth)
    }

  defp update_relation_context(state, word) when word in @join_prefixes,
    do: %{
      state
      | relation_entry?: false,
        join_prefix: word,
        statement_kind: update_statement_kind(state.statement_kind, word, state.paren_depth)
    }

  defp update_relation_context(%{join_prefix: prefix} = state, "outer")
       when prefix in @outer_join_prefixes,
       do: %{
         state
         | relation_entry?: false,
           join_prefix: "#{prefix} outer",
           statement_kind: update_statement_kind(state.statement_kind, "outer", state.paren_depth)
       }

  defp update_relation_context(%{join_prefix: prefix} = state, "join")
       when prefix in @join_entry_prefixes,
       do: %{
         state
         | relation_entry?: true,
           join_prefix: nil,
           statement_kind: update_statement_kind(state.statement_kind, "join", state.paren_depth)
       }

  defp update_relation_context(state, word),
    do: %{
      state
      | relation_entry?: false,
        join_prefix: nil,
        statement_kind: update_statement_kind(state.statement_kind, word, state.paren_depth)
    }

  defp update_cte_state_for_identifier(state, word) do
    lower_word = String.downcase(word)

    state
    |> maybe_finalize_cte_after_query_identifier()
    |> transition_cte_identifier(lower_word)
  end

  defp maybe_finalize_cte_after_query_identifier(state) do
    if state.cte_ready_for_next? and state.paren_depth == 0 and not state.cte_expect_alias? do
      state
      |> Map.put(:cte_active?, false)
      |> Map.put(:cte_waiting_query?, false)
      |> Map.put(:cte_query_depth, nil)
      |> Map.put(:cte_ready_for_next?, false)
    else
      state
    end
  end

  defp transition_cte_identifier(%{paren_depth: 0} = state, "with") do
    state
    |> Map.put(:cte_names, MapSet.new())
    |> Map.put(:cte_active?, true)
    |> Map.put(:cte_expect_alias?, true)
    |> Map.put(:cte_waiting_query?, false)
    |> Map.put(:cte_query_depth, nil)
    |> Map.put(:cte_ready_for_next?, false)
  end

  defp transition_cte_identifier(
         %{cte_active?: true, cte_expect_alias?: true, paren_depth: 0} = state,
         "recursive"
       ),
       do: state

  defp transition_cte_identifier(
         %{cte_active?: true, cte_expect_alias?: true, paren_depth: 0} = state,
         alias_name
       ) do
    state
    |> Map.put(:cte_names, MapSet.put(state.cte_names, alias_name))
    |> Map.put(:cte_expect_alias?, false)
  end

  defp transition_cte_identifier(
         %{cte_active?: true, cte_expect_alias?: false, paren_depth: 0} = state,
         "as"
       ) do
    Map.put(state, :cte_waiting_query?, true)
  end

  defp transition_cte_identifier(state, _word), do: state

  defp maybe_enter_cte_query(state) do
    if state.cte_active? and state.cte_waiting_query? and state.paren_depth > 0 and
         is_nil(state.cte_query_depth) do
      state
      |> Map.put(:cte_query_depth, state.paren_depth)
      |> Map.put(:cte_waiting_query?, false)
      |> Map.put(:cte_ready_for_next?, false)
    else
      state
    end
  end

  defp maybe_exit_cte_query(state, previous_state) do
    if state.cte_active? and not is_nil(previous_state.cte_query_depth) and
         previous_state.paren_depth == previous_state.cte_query_depth do
      state
      |> Map.put(:cte_query_depth, nil)
      |> Map.put(:cte_ready_for_next?, true)
    else
      state
    end
  end

  defp maybe_continue_cte_sequence_after_comma(state) do
    if state.cte_active? and state.cte_ready_for_next? and state.paren_depth == 0 do
      state
      |> Map.put(:cte_expect_alias?, true)
      |> Map.put(:cte_ready_for_next?, false)
    else
      state
    end
  end

  defp maybe_close_cte_sequence(state, char) do
    if state.cte_active? and state.cte_ready_for_next? and state.paren_depth == 0 and
         char not in [32, 9, 10, 13] do
      state
      |> Map.put(:cte_active?, false)
      |> Map.put(:cte_waiting_query?, false)
      |> Map.put(:cte_query_depth, nil)
      |> Map.put(:cte_ready_for_next?, false)
    else
      state
    end
  end

  defp top_level_statement_kind(:query), do: :query
  defp top_level_statement_kind(:expression), do: :expression

  defp update_statement_kind(kind, word, depth) do
    if depth > 0 do
      kind
    else
      cond do
        kind in [:update, :merge] -> kind
        word == "update" -> :update
        word == "merge" -> :merge
        true -> kind
      end
    end
  end

  defp local_arg_index(args) do
    args
    |> Enum.with_index()
    |> Map.new(fn {name, index} -> {name, index} end)
  end

  defp span(%Position{} = start_pos, %Position{} = end_pos) do
    %Span{
      start_offset: start_pos.offset,
      end_offset: end_pos.offset,
      start_line: start_pos.line,
      start_column: start_pos.column,
      end_line: end_pos.line,
      end_column: end_pos.column
    }
  end

  defp advance_state(state, chars),
    do: %{state | position: advance_position(state.position, chars)}

  defp advance_position(%Position{} = pos, chars) when is_list(chars) do
    Enum.reduce(chars, pos, fn char, acc ->
      if char == ?\n do
        %Position{offset: acc.offset + 1, line: acc.line + 1, column: 1}
      else
        %Position{offset: acc.offset + 1, line: acc.line, column: acc.column + 1}
      end
    end)
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end
end
