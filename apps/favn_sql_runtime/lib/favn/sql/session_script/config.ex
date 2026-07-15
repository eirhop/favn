defmodule Favn.SQL.SessionScript.Config do
  @moduledoc """
  Normalized runtime configuration for SQL physical-session scripts.

  This is runtime configuration, not manifest data. Manifests carry stable
  resource names through `Favn.SQL.SessionRequirements`; adapters resolve those
  names against this configuration when preparing a physical session.
  """

  alias Favn.SQL.SessionRequirements

  defmodule Script do
    @moduledoc "One configured SQL script file and its runtime value parameters."

    @enforce_keys [:name, :file]
    defstruct [:name, :file, params: %{}, secret_params: MapSet.new()]

    @type file_locator :: {:priv, atom(), String.t()} | String.t()
    @type t :: %__MODULE__{
            name: String.t(),
            file: file_locator(),
            params: %{optional(String.t()) => term()},
            secret_params: MapSet.t(String.t())
          }
  end

  defmodule Catalog do
    @moduledoc "Favn-owned metadata for one catalog prepared by native SQL."

    @enforce_keys [:name, :write_concurrency]
    defstruct [:name, :resource, :write_scope, :write_concurrency]

    @type t :: %__MODULE__{
            name: String.t(),
            resource: String.t() | nil,
            write_scope: String.t() | nil,
            write_concurrency: pos_integer() | :unlimited
          }
  end

  @enforce_keys [:resources, :catalogs]
  defstruct [:startup, resources: %{}, catalogs: %{}]

  @type t :: %__MODULE__{
          startup: Script.t() | nil,
          resources: %{optional(String.t()) => Script.t()},
          catalogs: %{optional(String.t()) => Catalog.t()}
        }

  @doc """
  Normalizes and validates session-script configuration.

  Accepted top-level keys are `:startup`, `:resources`, and `:catalogs`.
  """
  @spec normalize(term(), keyword()) :: {:ok, t()} | {:error, term()}
  def normalize(value, opts \\ []) do
    secret_paths = normalize_secret_paths(Keyword.get(opts, :secret_paths, []))

    with {:ok, config} <- keyword_or_map(value, :duckdb),
         :ok <- reject_unknown_keys(config, [:startup, :resources, :catalogs], :duckdb),
         {:ok, startup} <- normalize_startup(fetch(config, :startup), secret_paths),
         {:ok, resources} <- normalize_resources(fetch(config, :resources, []), secret_paths),
         {:ok, catalogs} <- normalize_catalogs(fetch(config, :catalogs, []), resources) do
      {:ok, %__MODULE__{startup: startup, resources: resources, catalogs: catalogs}}
    end
  end

  @doc false
  @spec validate(term()) :: :ok | {:error, term()}
  def validate(value) do
    case normalize(value) do
      {:ok, _config} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_startup(nil, _secret_paths), do: {:ok, nil}
  defp normalize_startup([], _secret_paths), do: {:ok, nil}

  defp normalize_startup(value, secret_paths),
    do: normalize_script("startup", value, [:startup], secret_paths)

  defp normalize_resources(value, secret_paths) do
    with {:ok, entries} <- named_entries(value, :resources) do
      Enum.reduce_while(entries, {:ok, %{}}, fn {raw_name, script_config}, {:ok, acc} ->
        with {:ok, name} <- normalize_name(raw_name, :resource),
             false <- Map.has_key?(acc, name),
             {:ok, script} <-
               normalize_script(name, script_config, [:resources, name], secret_paths) do
          {:cont, {:ok, Map.put(acc, name, script)}}
        else
          true -> {:halt, {:error, {:duplicate_resource, raw_name}}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp normalize_catalogs(value, resources) do
    with {:ok, entries} <- named_entries(value, :catalogs) do
      Enum.reduce_while(entries, {:ok, %{}}, fn {raw_name, catalog_config}, {:ok, acc} ->
        with {:ok, name} <- normalize_name(raw_name, :catalog),
             false <- Map.has_key?(acc, name),
             {:ok, catalog} <- normalize_catalog(name, catalog_config, resources) do
          {:cont, {:ok, Map.put(acc, name, catalog)}}
        else
          true -> {:halt, {:error, {:duplicate_catalog, raw_name}}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp normalize_script(name, value, path, secret_paths) do
    with {:ok, config} <- keyword_or_map(value, {:script, name}),
         :ok <- reject_unknown_keys(config, [:file, :params], {:script, name}),
         {:ok, file} <- normalize_file(fetch(config, :file)),
         {:ok, params} <- normalize_params(fetch(config, :params, [])),
         secret_params <- secret_params(params, path, secret_paths) do
      {:ok, %Script{name: name, file: file, params: params, secret_params: secret_params}}
    end
  end

  defp normalize_catalog(name, value, resources) do
    with {:ok, config} <- keyword_or_map(value, {:catalog, name}),
         :ok <-
           reject_unknown_keys(
             config,
             [:resource, :write_concurrency, :write_scope],
             {:catalog, name}
           ),
         {:ok, resource} <- normalize_optional_resource(fetch(config, :resource), resources),
         {:ok, write_concurrency} <-
           normalize_write_concurrency(fetch(config, :write_concurrency, 1)),
         {:ok, write_scope} <- normalize_write_scope(fetch(config, :write_scope)) do
      {:ok,
       %Catalog{
         name: name,
         resource: resource,
         write_concurrency: write_concurrency,
         write_scope: write_scope
       }}
    end
  end

  defp normalize_file({:priv, app, relative})
       when is_atom(app) and not is_nil(app) and is_binary(relative) and relative != "" do
    cond do
      Path.type(relative) != :relative ->
        {:error, {:invalid_script_file, :priv_path_must_be_relative}}

      Enum.any?(Path.split(relative), &(&1 == "..")) ->
        {:error, {:invalid_script_file, :priv_path_cannot_escape}}

      true ->
        {:ok, {:priv, app, relative}}
    end
  end

  defp normalize_file(path) when is_binary(path) and path != "" do
    if Path.type(path) == :absolute do
      {:ok, Path.expand(path)}
    else
      {:error, {:invalid_script_file, :absolute_path_required}}
    end
  end

  defp normalize_file(nil), do: {:error, {:missing_script_field, :file}}
  defp normalize_file(_value), do: {:error, {:invalid_script_file, :invalid_value}}

  defp normalize_params(value) do
    with {:ok, entries} <- named_entries(value, :params) do
      Enum.reduce_while(entries, {:ok, %{}}, fn {raw_name, param_value}, {:ok, acc} ->
        with {:ok, name} <- normalize_name(raw_name, :parameter),
             false <- Map.has_key?(acc, name) do
          {:cont, {:ok, Map.put(acc, name, param_value)}}
        else
          true -> {:halt, {:error, {:duplicate_script_parameter, raw_name}}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp normalize_optional_resource(nil, _resources), do: {:ok, nil}

  defp normalize_optional_resource(value, resources) do
    with {:ok, name} <- normalize_name(value, :resource),
         true <- Map.has_key?(resources, name) do
      {:ok, name}
    else
      false -> {:error, {:unknown_catalog_resource, :not_configured}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_write_concurrency(:unlimited), do: {:ok, :unlimited}
  defp normalize_write_concurrency(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_write_concurrency(_value),
    do: {:error, {:invalid_catalog_write_concurrency, :expected_positive_integer_or_unlimited}}

  defp normalize_write_scope(nil), do: {:ok, nil}

  defp normalize_write_scope(value) when is_atom(value) and not is_nil(value),
    do: {:ok, Atom.to_string(value)}

  defp normalize_write_scope(value) when is_binary(value) and value != "", do: {:ok, value}

  defp normalize_write_scope(_value),
    do: {:error, {:invalid_catalog_write_scope, :expected_non_empty_atom_or_string}}

  defp normalize_name(value, kind) do
    {:ok, SessionRequirements.normalize_resources!([value]) |> hd()}
  rescue
    error in ArgumentError -> {:error, {:invalid_session_name, kind, error.message}}
  end

  defp secret_params(params, path, secret_paths) do
    params
    |> Map.keys()
    |> Enum.filter(fn name ->
      MapSet.member?(secret_paths, normalize_path(path ++ [:params, name]))
    end)
    |> MapSet.new()
  end

  defp normalize_secret_paths(paths) when is_list(paths) do
    paths
    |> Enum.map(fn
      [:duckdb | rest] -> normalize_path(rest)
      ["duckdb" | rest] -> normalize_path(rest)
      path -> normalize_path(path)
    end)
    |> MapSet.new()
  end

  defp normalize_secret_paths(_paths), do: MapSet.new()
  defp normalize_path(path) when is_list(path), do: Enum.map(path, &to_string/1)
  defp normalize_path(_path), do: []

  defp named_entries(nil, _context), do: {:ok, []}
  defp named_entries([], _context), do: {:ok, []}
  defp named_entries(value, _context) when is_map(value), do: {:ok, Map.to_list(value)}

  defp named_entries(value, context) when is_list(value) do
    if Keyword.keyword?(value) do
      {:ok, value}
    else
      {:error, {:expected_named_entries, context}}
    end
  end

  defp named_entries(_value, context), do: {:error, {:expected_named_entries, context}}

  defp keyword_or_map(nil, _context), do: {:ok, %{}}
  defp keyword_or_map([], _context), do: {:ok, %{}}

  defp keyword_or_map(value, context) when is_map(value) do
    case duplicate_map_keys(value) do
      [] -> {:ok, value}
      duplicates -> {:error, {:duplicate_config_keys, context, duplicates}}
    end
  end

  defp keyword_or_map(value, context) when is_list(value) do
    if Keyword.keyword?(value) do
      duplicates = duplicate_keys(value)

      if duplicates == [] do
        {:ok, Map.new(value)}
      else
        {:error, {:duplicate_config_keys, context, duplicates}}
      end
    else
      {:error, {:expected_keyword_or_map, context}}
    end
  end

  defp keyword_or_map(_value, context), do: {:error, {:expected_keyword_or_map, context}}

  defp reject_unknown_keys(config, allowed, context) do
    unknown =
      config
      |> Map.keys()
      |> Enum.reject(&(&1 in allowed or normalize_existing_key(&1, allowed) in allowed))

    if unknown == [], do: :ok, else: {:error, {:unknown_config_keys, context, unknown}}
  end

  defp fetch(config, key, default \\ nil) do
    case Map.fetch(config, key) do
      {:ok, value} -> value
      :error -> Map.get(config, Atom.to_string(key), default)
    end
  end

  defp normalize_existing_key(key, allowed) when is_binary(key) do
    Enum.find(allowed, &(Atom.to_string(&1) == key))
  end

  defp normalize_existing_key(_key, _allowed), do: nil

  defp duplicate_keys(keyword) do
    keyword
    |> Keyword.keys()
    |> Enum.frequencies()
    |> Enum.filter(fn {_key, count} -> count > 1 end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort()
  end

  defp duplicate_map_keys(map) do
    map
    |> Map.keys()
    |> Enum.filter(&(is_atom(&1) or is_binary(&1)))
    |> Enum.group_by(&to_string/1)
    |> Enum.filter(fn {_canonical, keys} -> length(keys) > 1 end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort()
  end
end
