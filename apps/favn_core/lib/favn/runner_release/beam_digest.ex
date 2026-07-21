defmodule Favn.RunnerRelease.BeamDigest do
  @moduledoc """
  Produces path-independent executable fingerprints from compiled BEAM files.

  Only chunks retained in a stripped OTP release and required for executable
  code, literals, imports, exports, and closures are hashed. Compiler/debug
  metadata, docs, line tables, timestamps, local symbol tables, and module
  attributes are deliberately excluded.

  Compiled absolute-path literals are rejected rather than rewritten: core
  cannot safely distinguish `__DIR__` from a business-significant hard-coded
  path after release stripping. Deployment paths must enter through runtime
  configuration. This keeps accepted fingerprints relocatable without hiding a
  behavioral change.
  """

  @executable_chunks MapSet.new([
                       "Atom",
                       "AtU8",
                       "Code",
                       "StrT",
                       "ImpT",
                       "ExpT",
                       "FunT"
                     ])

  @type metadata :: %{
          module: String.t(),
          digest: String.t(),
          imports: [String.t()],
          protocol_implementation: nil | %{protocol: String.t(), for: String.t()}
        }

  @type error ::
          {:invalid_beam,
           :malformed
           | :missing_executable_chunks
           | :invalid_literal_table
           | {:absolute_path_literal, non_neg_integer()}}

  @doc "Returns a canonical lowercase SHA-256 executable digest."
  @spec digest(binary()) :: {:ok, String.t()} | {:error, error()}
  def digest(beam) when is_binary(beam) do
    with {:ok, canonical} <- canonical_binary(beam) do
      {:ok, sha256(canonical)}
    end
  end

  def digest(_beam), do: {:error, {:invalid_beam, :malformed}}

  @doc "Returns the module name, executable digest, and sorted imported modules."
  @spec metadata(binary()) :: {:ok, metadata()} | {:error, error()}
  def metadata(beam) when is_binary(beam) do
    with {:ok, module, chunks} <- all_chunks(beam),
         {:ok, literals} <- canonical_literals(chunks),
         {:ok, canonical} <- canonical_from_chunks(chunks, literals),
         {:ok, attributes} <- optional_attributes(beam),
         {:ok, imports} <- imports(beam) do
      {:ok,
       %{
         module: Atom.to_string(module),
         digest: sha256(canonical),
         imports: imports,
         protocol_implementation: protocol_implementation(attributes)
       }}
    end
  end

  def metadata(_beam), do: {:error, {:invalid_beam, :malformed}}

  @doc "Returns the sorted unique module names imported by a BEAM file."
  @spec imports(binary()) :: {:ok, [String.t()]} | {:error, error()}
  def imports(beam) when is_binary(beam) do
    case :beam_lib.chunks(beam, [:imports]) do
      {:ok, {_module, [imports: imports]}} when is_list(imports) ->
        names =
          imports
          |> Enum.map(fn {module, _function, _arity} -> Atom.to_string(module) end)
          |> Enum.uniq()
          |> Enum.sort()

        {:ok, names}

      _other ->
        {:error, {:invalid_beam, :malformed}}
    end
  catch
    :exit, _reason -> {:error, {:invalid_beam, :malformed}}
  end

  def imports(_beam), do: {:error, {:invalid_beam, :malformed}}

  @doc "Returns the canonical executable chunk representation used for hashing."
  @spec canonical_binary(binary()) :: {:ok, binary()} | {:error, error()}
  def canonical_binary(beam) when is_binary(beam) do
    with {:ok, _module, chunks} <- all_chunks(beam),
         {:ok, literals} <- canonical_literals(chunks) do
      canonical_from_chunks(chunks, literals)
    end
  end

  def canonical_binary(_beam), do: {:error, {:invalid_beam, :malformed}}

  defp all_chunks(beam) do
    case :beam_lib.all_chunks(beam) do
      {:ok, module, chunks} when is_atom(module) and is_list(chunks) ->
        {:ok, module, chunks}

      _other ->
        {:error, {:invalid_beam, :malformed}}
    end
  catch
    :exit, _reason -> {:error, {:invalid_beam, :malformed}}
  end

  defp canonical_from_chunks(chunks, literals) do
    executable =
      chunks
      |> Enum.map(fn {id, contents} -> {List.to_string(id), contents} end)
      |> Enum.filter(fn {id, _contents} -> MapSet.member?(@executable_chunks, id) end)
      |> Enum.sort_by(&elem(&1, 0))

    if executable == [] do
      {:error, {:invalid_beam, :missing_executable_chunks}}
    else
      executable =
        executable ++
          [
            {"FavnLiterals", literals}
          ]

      encoded =
        executable
        |> Enum.map(fn {id, contents} ->
          [<<byte_size(id)::unsigned-16>>, id, <<byte_size(contents)::unsigned-64>>, contents]
        end)
        |> then(&["FAVN_EXECUTABLE_BEAM_V1", &1])
        |> IO.iodata_to_binary()

      {:ok, encoded}
    end
  end

  defp optional_attributes(beam) do
    case :beam_lib.chunks(beam, [:attributes]) do
      {:ok, {_module, [attributes: attributes]}} when is_list(attributes) ->
        {:ok, attributes}

      {:error, :beam_lib, {:missing_chunk, _beam, _chunk}} ->
        {:ok, []}

      _other ->
        {:error, {:invalid_beam, :malformed}}
    end
  catch
    :exit, _reason -> {:error, {:invalid_beam, :malformed}}
  end

  defp canonical_literals(chunks) do
    case Enum.find(chunks, fn {id, _contents} -> List.to_string(id) == "LitT" end) do
      nil ->
        {:ok, :erlang.term_to_binary([], [:deterministic])}

      {_id, contents} ->
        with {:ok, literals} <- decode_literal_table(contents),
             :ok <- reject_absolute_path_literals(literals) do
          {:ok, :erlang.term_to_binary(literals, [:deterministic])}
        end
    end
  end

  defp decode_literal_table(<<uncompressed_size::unsigned-32, payload::binary>>) do
    with {:ok, table} <- literal_table_payload(uncompressed_size, payload),
         <<count::unsigned-32, entries::binary>> <- table,
         {:ok, literals, <<>>} <- decode_literal_entries(count, entries, []) do
      {:ok, Enum.reverse(literals)}
    else
      _other -> {:error, {:invalid_beam, :invalid_literal_table}}
    end
  end

  defp decode_literal_table(_contents),
    do: {:error, {:invalid_beam, :invalid_literal_table}}

  defp decode_literal_entries(0, rest, acc), do: {:ok, acc, rest}

  defp decode_literal_entries(
         count,
         <<size::unsigned-32, encoded::binary-size(size), rest::binary>>,
         acc
       )
       when count > 0 do
    literal = :erlang.binary_to_term(encoded, [:safe])
    decode_literal_entries(count - 1, rest, [literal | acc])
  rescue
    ArgumentError -> {:error, {:invalid_beam, :invalid_literal_table}}
  end

  defp decode_literal_entries(_count, _rest, _acc),
    do: {:error, {:invalid_beam, :invalid_literal_table}}

  defp uncompress(compressed) do
    {:ok, :zlib.uncompress(compressed)}
  rescue
    ErlangError -> {:error, {:invalid_beam, :invalid_literal_table}}
  end

  defp literal_table_payload(0, payload), do: {:ok, payload}

  defp literal_table_payload(expected_size, payload) do
    with {:ok, table} <- uncompress(payload),
         true <- byte_size(table) == expected_size do
      {:ok, table}
    else
      _other -> {:error, {:invalid_beam, :invalid_literal_table}}
    end
  end

  defp reject_absolute_path_literals(literals) do
    case Enum.find_index(literals, &contains_absolute_path?/1) do
      nil -> :ok
      index -> {:error, {:invalid_beam, {:absolute_path_literal, index}}}
    end
  end

  defp contains_absolute_path?(value) when is_binary(value) do
    String.valid?(value) and Path.type(value) == :absolute
  end

  defp contains_absolute_path?(value) when is_list(value) do
    case charlist_to_string(value) do
      {:ok, string} -> Path.type(string) == :absolute
      :error -> contains_absolute_path_in_cons?(value)
    end
  end

  defp contains_absolute_path?(value) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.any?(&contains_absolute_path?/1)
  end

  defp contains_absolute_path?(value) when is_map(value) do
    Enum.any?(value, fn {key, child} ->
      contains_absolute_path?(key) or contains_absolute_path?(child)
    end)
  end

  defp contains_absolute_path?(_value), do: false

  defp charlist_to_string([]), do: :error

  defp charlist_to_string(value) do
    with {:ok, codepoints} <- collect_charlist(value, []) do
      {:ok, codepoints |> Enum.reverse() |> List.to_string()}
    end
  rescue
    ArgumentError -> :error
  end

  defp collect_charlist([], acc), do: {:ok, acc}

  defp collect_charlist([head | tail], acc) when is_integer(head),
    do: collect_charlist(tail, [head | acc])

  defp collect_charlist(_value, _acc), do: :error

  defp contains_absolute_path_in_cons?([]), do: false

  defp contains_absolute_path_in_cons?([head | tail]) do
    contains_absolute_path?(head) or contains_absolute_path?(tail)
  end

  defp protocol_implementation(attributes) do
    case Keyword.get(attributes, :__impl__) do
      values when is_list(values) ->
        case {Keyword.get(values, :protocol), Keyword.get(values, :for)} do
          {protocol, implementation_for} when is_atom(protocol) and is_atom(implementation_for) ->
            %{protocol: Atom.to_string(protocol), for: Atom.to_string(implementation_for)}

          _other ->
            nil
        end

      _other ->
        nil
    end
  end

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
