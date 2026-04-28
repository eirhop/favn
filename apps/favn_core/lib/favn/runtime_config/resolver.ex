defmodule Favn.RuntimeConfig.Resolver do
  @moduledoc """
  Resolves manifest-declared runtime configuration references.

  This is runtime infrastructure used by the runner and connection loader. It
  turns `Favn.RuntimeConfig.Ref` values into ordinary Elixir values immediately
  before execution or adapter connection.

  Missing required environment variables are returned as structured
  `Favn.RuntimeConfig.Error` values instead of raising raw `System.fetch_env!/1`
  exceptions.
  """

  alias Favn.RuntimeConfig.Error
  alias Favn.RuntimeConfig.Ref
  alias Favn.RuntimeConfig.Requirements

  @type resolved :: %{atom() => %{atom() => term()}}

  @spec resolve_asset(Requirements.declarations()) :: {:ok, resolved()} | {:error, Error.t()}
  @doc false
  def resolve_asset(declarations) when is_map(declarations) do
    declarations
    |> Enum.reduce_while({:ok, %{}}, fn {scope, fields}, {:ok, acc} ->
      case resolve_fields(scope, fields) do
        {:ok, resolved_fields} -> {:cont, {:ok, Map.put(acc, scope, resolved_fields)}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  @spec resolve_value(term(), keyword()) :: {:ok, term()} | {:error, Error.t()}
  @doc false
  def resolve_value(value, opts \\ [])

  def resolve_value(%Ref{} = ref, opts) do
    scope = Keyword.get(opts, :scope)
    field = Keyword.get(opts, :field)

    case ref.provider do
      :env ->
        case System.fetch_env(ref.key) do
          {:ok, value} -> {:ok, value}
          :error when ref.required? -> {:error, missing_env(ref, scope, field)}
          :error -> {:ok, nil}
        end

      provider ->
        {:error,
         %Error{
           type: :invalid_ref,
           provider: provider,
           key: ref.key,
           scope: scope,
           field: field,
           secret?: ref.secret?,
           message: "invalid runtime config provider #{inspect(provider)}"
         }}
    end
  end

  def resolve_value(%_{} = value, _opts), do: {:ok, value}

  def resolve_value(value, opts) when is_map(value) do
    value
    |> Enum.reduce_while({:ok, %{}}, fn {key, child}, {:ok, acc} ->
      case resolve_value(child, opts) do
        {:ok, resolved} -> {:cont, {:ok, Map.put(acc, key, resolved)}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  def resolve_value(value, opts) when is_list(value) do
    value
    |> Enum.reduce_while({:ok, []}, fn child, {:ok, acc} ->
      case resolve_value(child, opts) do
        {:ok, resolved} -> {:cont, {:ok, [resolved | acc]}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, resolved} -> {:ok, Enum.reverse(resolved)}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  def resolve_value(value, opts) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.reduce_while({:ok, []}, fn child, {:ok, acc} ->
      case resolve_value(child, opts) do
        {:ok, resolved} -> {:cont, {:ok, [resolved | acc]}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, resolved} -> {:ok, resolved |> Enum.reverse() |> List.to_tuple()}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  def resolve_value(value, _opts), do: {:ok, value}

  @spec present?(Ref.t()) :: boolean()
  @doc false
  def present?(%Ref{provider: :env, key: key}), do: match?({:ok, _}, System.fetch_env(key))

  defp resolve_fields(scope, fields) when is_map(fields) do
    Enum.reduce_while(fields, {:ok, %{}}, fn {field, ref}, {:ok, acc} ->
      case resolve_value(ref, scope: scope, field: field) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, field, value)}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp missing_env(%Ref{} = ref, scope, field) do
    %Error{
      type: :missing_env,
      provider: :env,
      key: ref.key,
      scope: scope,
      field: field,
      secret?: ref.secret?,
      message: "missing_env #{ref.key}"
    }
  end
end
