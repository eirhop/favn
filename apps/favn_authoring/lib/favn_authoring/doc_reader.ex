defmodule FavnAuthoring.DocReader do
  @moduledoc """
  Reads compiled module and function documentation from BEAM docs chunks.

  This module is intentionally narrow:

  - reads only local compiled docs via `Code.fetch_docs/1`
  - does not fetch external documentation
  - does not parse source files
  """

  @typedoc "Normalized docs entry for one public function/macro arity"
  @type function_entry :: %{
          kind: :function | :macro,
          name: atom(),
          arity: non_neg_integer(),
          signatures: [String.t()],
          doc: :none | map()
        }

  @typedoc "Result for module documentation reads"
  @type module_result :: %{
          module: module(),
          moduledoc: :none | :hidden | map(),
          format: String.t()
        }

  @typedoc "Result for function documentation reads"
  @type function_result :: %{
          module: module(),
          function: String.t(),
          format: String.t(),
          entries: [function_entry()]
        }

  @typedoc "Error reasons for docs reads"
  @type error_reason ::
          {:fetch_failed, term()}
          | :function_not_found
          | :invalid_docs_chunk

  @spec read_module(module()) :: {:ok, module_result()} | {:error, error_reason()}
  def read_module(module) when is_atom(module) do
    with {:ok, docs_v1} <- fetch_docs(module) do
      {:ok,
       %{
         module: module,
         moduledoc: docs_v1.moduledoc,
         format: docs_v1.format
       }}
    end
  end

  @spec read_function(module(), String.t()) :: {:ok, function_result()} | {:error, error_reason()}
  def read_function(module, function_name) when is_atom(module) and is_binary(function_name) do
    with {:ok, docs_v1} <- fetch_docs(module) do
      entries =
        docs_v1.docs
        |> Enum.flat_map(&normalize_function_entry(&1, function_name))
        |> Enum.sort_by(&{&1.arity, &1.kind})

      case entries do
        [] ->
          {:error, :function_not_found}

        _ ->
          {:ok,
           %{
             module: module,
             function: function_name,
             format: docs_v1.format,
             entries: entries
           }}
      end
    end
  end

  defp fetch_docs(module) do
    case Code.fetch_docs(module) do
      {:docs_v1, _anno, _language, format, moduledoc, _metadata, docs}
      when is_binary(format) and is_list(docs) ->
        {:ok, %{format: format, moduledoc: moduledoc, docs: docs}}

      {:error, reason} ->
        {:error, {:fetch_failed, reason}}

      _other ->
        {:error, :invalid_docs_chunk}
    end
  end

  defp normalize_function_entry(
         {{kind, name, arity}, _anno, signatures, doc, metadata},
         function_name
       )
       when kind in [:function, :macro] and is_atom(name) and is_integer(arity) and arity >= 0 do
    if Atom.to_string(name) == function_name and public_entry?(doc, metadata) do
      [
        %{
          kind: kind,
          name: name,
          arity: arity,
          signatures: normalize_signatures(signatures),
          doc: normalize_doc(doc)
        }
      ]
    else
      []
    end
  end

  defp normalize_function_entry(_entry, _function_name), do: []

  defp public_entry?(:hidden, _metadata), do: false

  defp public_entry?(_doc, metadata) when is_map(metadata) do
    Map.get(metadata, :exported, true)
  end

  defp public_entry?(_doc, _metadata), do: true

  defp normalize_signatures(signatures) when is_list(signatures) do
    Enum.map(signatures, &to_string/1)
  end

  defp normalize_signatures(_other), do: []

  defp normalize_doc(:none), do: :none
  defp normalize_doc(%{} = doc), do: doc
  defp normalize_doc(_other), do: :none
end
