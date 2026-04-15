defmodule Favn.DSL.Compiler do
  @moduledoc false

  @spec compile_error!(String.t() | charlist(), pos_integer() | nil, String.t()) :: no_return()
  def compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end

  @spec normalize_file(Path.t()) :: String.t()
  def normalize_file(file) do
    file
    |> to_string()
    |> Path.relative_to_cwd()
  end

  @spec normalize_doc(term()) :: String.t() | nil
  def normalize_doc({_line, false}), do: nil
  def normalize_doc({_line, doc}) when is_binary(doc), do: doc
  def normalize_doc(false), do: nil
  def normalize_doc(doc) when is_binary(doc), do: doc
  def normalize_doc(_), do: nil

  @spec module_atom?(atom()) :: boolean()
  def module_atom?(module) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end

  @spec fetch_accum_attribute(module(), atom()) :: list()
  def fetch_accum_attribute(module, attribute) when is_atom(module) and is_atom(attribute) do
    module
    |> Module.get_attribute(attribute)
    |> List.wrap()
  end

  @spec valid_relation_attr_value?(term()) :: boolean()
  def valid_relation_attr_value?(relation) do
    relation == true or (is_list(relation) and Keyword.keyword?(relation)) or is_map(relation)
  end
end
