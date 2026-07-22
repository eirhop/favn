defmodule FavnOrchestrator.RunManager.SubmissionOptions do
  @moduledoc false

  alias Favn.Retry.Policy
  alias Favn.Window.Anchor
  alias Favn.Window.Selection
  alias FavnOrchestrator.RunState

  @enforce_keys [
    :run_id,
    :params,
    :trigger,
    :metadata,
    :retry_policy_override,
    :timeout_ms,
    :dependencies,
    :exact_windows,
    :required_generation,
    :lineage_depth
  ]
  defstruct @enforce_keys ++
              [
                :anchor_window,
                :window_selection,
                :parent_run_id,
                :root_run_id,
                :workspace_id,
                :deployment_id
              ]

  @type t :: %__MODULE__{
          run_id: String.t(),
          params: map(),
          trigger: map(),
          metadata: map(),
          retry_policy_override: Policy.t() | nil,
          timeout_ms: pos_integer(),
          dependencies: :all | :none,
          anchor_window: Anchor.t() | nil,
          window_selection: Selection.t() | nil,
          exact_windows: map(),
          required_generation: map() | nil,
          parent_run_id: String.t() | nil,
          root_run_id: String.t() | nil,
          lineage_depth: non_neg_integer(),
          workspace_id: String.t() | nil,
          deployment_id: String.t() | nil
        }

  @spec new(keyword(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts, defaults \\ []) when is_list(opts) and is_list(defaults) do
    if Keyword.keyword?(opts) and Keyword.keyword?(defaults) do
      build(opts, defaults)
    else
      {:error, :invalid_options}
    end
  end

  defp build(opts, defaults) do
    with {:ok, retry_policy_override} <- retry_policy_override(opts, defaults),
         {:ok, window_selection} <-
           window_selection(option(opts, defaults, :window_selection, nil)) do
      values = %{
        run_id: option(opts, defaults, :run_id, new_run_id()),
        params: option(opts, defaults, :params, %{}),
        trigger: option(opts, defaults, :trigger, %{}),
        metadata: option(opts, defaults, :metadata, %{}),
        retry_policy_override: retry_policy_override,
        timeout_ms: option(opts, defaults, :timeout_ms, RunState.default_timeout_ms()),
        dependencies: option(opts, defaults, :dependencies, :all),
        anchor_window: option(opts, defaults, :anchor_window, nil),
        window_selection: window_selection,
        exact_windows: option(opts, defaults, :exact_windows, %{}),
        required_generation: option(opts, defaults, :required_generation, nil),
        parent_run_id: option(opts, defaults, :parent_run_id, nil),
        root_run_id: option(opts, defaults, :root_run_id, nil),
        lineage_depth: option(opts, defaults, :lineage_depth, 0),
        workspace_id: Keyword.get(opts, :_workspace_id),
        deployment_id: Keyword.get(opts, :_deployment_id)
      }

      with :ok <- non_empty_string(values.run_id, :invalid_run_id),
           :ok <- map(values.params, :invalid_run_params),
           :ok <- map(values.trigger, :invalid_pipeline_trigger),
           :ok <- map(values.metadata, :invalid_run_metadata),
           :ok <- positive_integer(values.timeout_ms, :invalid_timeout_ms),
           :ok <- dependencies(values.dependencies),
           :ok <- anchor(values.anchor_window),
           :ok <- window_input(values.anchor_window, values.window_selection),
           :ok <- map(values.exact_windows, :invalid_exact_windows),
           :ok <- required_generation(values.required_generation),
           :ok <- optional_string(values.parent_run_id, :invalid_parent_run_id),
           :ok <- optional_string(values.root_run_id, :invalid_root_run_id),
           :ok <- optional_string(values.workspace_id, :invalid_workspace_id),
           :ok <- optional_string(values.deployment_id, :invalid_deployment_id),
           :ok <- non_negative_integer(values.lineage_depth, :invalid_lineage_depth) do
        {:ok, struct!(__MODULE__, values)}
      end
    end
  end

  defp retry_policy_override(opts, defaults) do
    cond do
      Keyword.has_key?(opts, :retry_policy) ->
        Policy.new(Keyword.fetch!(opts, :retry_policy))

      Keyword.has_key?(defaults, :retry_policy) ->
        Policy.new(Keyword.fetch!(defaults, :retry_policy))

      true ->
        {:ok, nil}
    end
  end

  defp option(opts, defaults, key, fallback),
    do: Keyword.get(opts, key, Keyword.get(defaults, key, fallback))

  defp non_empty_string(value, _error) when is_binary(value) and value != "", do: :ok
  defp non_empty_string(_value, error), do: {:error, error}

  defp optional_string(nil, _error), do: :ok
  defp optional_string(value, error), do: non_empty_string(value, error)

  defp map(value, _error) when is_map(value), do: :ok
  defp map(_value, error), do: {:error, error}

  defp required_generation(nil), do: :ok

  defp required_generation(%{
         target_id: target_id,
         evidence_generation_id: evidence_generation_id,
         target_generation_id: target_generation_id
       }) do
    if non_empty_binary?(target_id) and non_empty_binary?(evidence_generation_id) and
         (is_nil(target_generation_id) or target_generation_id == evidence_generation_id),
       do: :ok,
       else: {:error, :invalid_required_generation}
  end

  defp required_generation(_value), do: {:error, :invalid_required_generation}

  defp non_empty_binary?(value), do: is_binary(value) and value != ""

  defp positive_integer(value, _error) when is_integer(value) and value > 0, do: :ok
  defp positive_integer(_value, error), do: {:error, error}

  defp non_negative_integer(value, _error) when is_integer(value) and value >= 0, do: :ok
  defp non_negative_integer(_value, error), do: {:error, error}

  defp dependencies(value) when value in [:all, :none], do: :ok
  defp dependencies(_value), do: {:error, :invalid_dependencies}

  defp anchor(nil), do: :ok
  defp anchor(%Anchor{} = anchor), do: Anchor.validate(anchor)
  defp anchor(_value), do: {:error, :invalid_anchor_window}

  defp window_selection(value), do: Selection.from_value(value)

  defp window_input(nil, _selection), do: :ok
  defp window_input(_anchor, nil), do: :ok
  defp window_input(_anchor, _selection), do: {:error, :ambiguous_window_selection}

  defp new_run_id do
    "run_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
