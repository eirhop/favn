defmodule Favn.Catalog.Schema do
  @moduledoc false
  alias Favn.Catalog.Value
  alias Favn.Connection.CircuitPolicySet
  alias Favn.Coverage.Effective
  alias Favn.ExecutionPool.PolicySet
  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest.Environment
  alias Favn.ResourceRecovery.Policy, as: RecoveryPolicy
  alias Favn.Retry.Policy, as: RetryPolicy
  alias Favn.RunnerPool
  alias Favn.SQL.PartitionSpec
  alias Favn.Window.{Policy, Spec}
  alias Favn.Semantic.Schema, as: SemanticSchema

  @asset_extra ~w(description category tags policies checks runtime_requirements)
  @asset_policy ~w(window coverage freshness retry_policy materialization partition_spec execution_pool runner_pool settings)
  @pipeline_policy ~w(window retry_policy max_concurrency execution_pool runner_pool resource_recovery source outputs settings)

  def validate(value) do
    valid!(
      keys?(
        value,
        ~w(schema_version manifest_version manifest_hash catalog_version assets pipelines schedules policies)
      )
    )

    valid!(value["schema_version"] == 1 and text?(value["manifest_version"]))
    valid!(digest?(value["manifest_hash"], "") and digest?(value["catalog_version"], "mc_"))
    valid!(list?(value["assets"], 10_000, &asset?/1))
    valid!(list?(value["pipelines"], 10_000, &pipeline?/1))
    valid!(list?(value["schedules"], 10_000, &schedule?/1))

    valid!(
      policy?(
        value["policies"],
        ~w(environment execution_pools connection_circuits runner_releases)
      )
    )

    snapshot = Enum.map(value["assets"], &Map.drop(&1, @asset_extra))
    valid!(SemanticSchema.validate_snapshot(snapshot) == :ok)

    Enum.each(["pipelines", "schedules"], fn key ->
      refs = Enum.map(value[key], & &1["ref"])
      valid!(refs == Enum.uniq(refs))
    end)

    asset_refs = MapSet.new(snapshot, & &1["ref"])

    Enum.each(value["pipelines"], fn pipeline ->
      Enum.each(pipeline["selectors"], fn
        ["asset", ref] -> valid!(MapSet.member?(asset_refs, ref))
        _ -> :ok
      end)
    end)

    schedules = MapSet.new(value["schedules"], & &1["ref"])

    Enum.each(value["pipelines"], fn pipeline ->
      case pipeline["schedule"] do
        %{"ref" => ref} -> valid!(MapSet.member?(schedules, ref))
        _ -> :ok
      end
    end)

    :ok
  catch
    :invalid_catalog_artifact -> {:error, :invalid_catalog_artifact}
  end

  defp asset?(value) do
    keys?(value, ~w(ref kind fingerprint relation dependencies contract) ++ @asset_extra) and
      list?(value["runtime_requirements"], 10_000, &requirement?/1) and
      list?(value["checks"], 132, &check?/1) and optional_text?(value["description"]) and
      optional_text?(value["category"]) and
      list?(value["tags"], 1000, &text?/1) and policy?(value["policies"], @asset_policy)
  end

  defp requirement?(value) do
    keys?(value, ~w(scope field provider key required)) and
      Enum.all?(~w(scope field key), &text?(value[&1])) and
      value["provider"] == "env" and is_boolean(value["required"])
  end

  defp check?(value) do
    keys?(value, ~w(name at on_violation when message origin claim_id)) and text?(value["name"]) and
      value["at"] in ~w(before_materialize after_materialize) and
      value["on_violation"] in ~w(fail warn skip_materialization) and
      value["when"] in [nil, "target_exists"] and optional_text?(value["message"]) and
      value["origin"] in ~w(authored contract) and optional_text?(value["claim_id"])
  end

  defp pipeline?(value) do
    keys?(value, ~w(ref selectors deps schedule policies category tags)) and
      text?(value["ref"]) and value["deps"] in ~w(all none) and
      list?(value["selectors"], 10_000, &selector?/1) and schedule_ref?(value["schedule"]) and
      optional_text?(value["category"]) and list?(value["tags"], 1000, &text?/1) and
      policy?(value["policies"], @pipeline_policy)
  end

  defp selector?("all"), do: true

  defp selector?([kind, value]) when kind in ~w(tag category module asset),
    do: text?(value)

  defp selector?(_), do: false
  defp schedule_ref?(nil), do: true
  defp schedule_ref?(%{"ref" => ref} = value), do: map_size(value) == 1 and text?(ref)
  defp schedule_ref?(%{"inline" => item} = value), do: map_size(value) == 1 and schedule?(item)
  defp schedule_ref?(_), do: false

  defp schedule?(value) do
    keys?(value, ~w(ref kind cron timezone timezone_source missed overlap origin)) and
      text?(value["ref"]) and value["kind"] in ~w(cron) and
      optional_text?(value["cron"]) and optional_text?(value["timezone"]) and
      value["timezone_source"] in ~w(local application_default utc_fallback) and
      value["missed"] in ~w(skip one all) and value["overlap"] in ~w(forbid allow queue_one) and
      value["origin"] in ~w(inline named)
  end

  # Policy envelopes are closed; their JSON payloads are descriptive declarations,
  # never decoded back into executable structs or used as runtime configuration.
  defp policy?(value, keys),
    do:
      keys?(value, keys) and
        Enum.all?(value, fn {key, v} -> policy_value?(key, v) and json?(v, 0) end)

  defp policy_value?(key, value) when key in ~w(execution_pool runner_pool source),
    do: optional_text?(value)

  defp policy_value?("outputs", value), do: list?(value, 10_000, &text?/1)

  defp policy_value?("max_concurrency", value),
    do: is_nil(value) or (is_integer(value) and value > 0)

  defp policy_value?("materialization", value), do: materialization?(value)

  defp policy_value?("settings", value), do: is_map(value)
  defp policy_value?("environment", value), do: canonical?(value, &Environment.from_manifest/1)
  defp policy_value?("execution_pools", value), do: canonical?(value, &PolicySet.new/1)
  defp policy_value?("connection_circuits", value), do: canonical?(value, &CircuitPolicySet.new/1)

  defp policy_value?("runner_releases", value),
    do: is_map(value) and RunnerPool.validate_releases(value) == :ok

  defp policy_value?(_, nil), do: true
  defp policy_value?("freshness", value), do: canonical?(value, &FreshnessPolicy.from_value/1)

  defp policy_value?("window", value),
    do: canonical?(value, &Spec.from_value/1) or canonical?(value, &Policy.from_value/1)

  defp policy_value?("coverage", value), do: canonical?(value, &Effective.from_value/1)
  defp policy_value?("retry_policy", value), do: canonical?(value, &RetryPolicy.new/1)

  defp policy_value?("partition_spec", value),
    do: canonical?(value, fn v -> {:ok, PartitionSpec.from_value!(v)} end)

  defp policy_value?("resource_recovery", value),
    do: canonical?(value, &RecoveryPolicy.from_value/1)

  # Reuse typed domain validators; exact re-encoding also rejects extra/missing
  # nested fields and noncanonical values without creating atoms from input.
  defp canonical?(value, decode) do
    case decode.(value) do
      {:ok, normalized} -> Value.encode(normalized) == value
      _ -> false
    end
  rescue
    _ in [ArgumentError, FunctionClauseError, KeyError] -> false
  end

  defp materialization?(value) when value in [nil, "view", "table"], do: true

  defp materialization?(["incremental", options]) when is_list(options) do
    valid_options =
      list?(options, 4, fn
        ["strategy", value] ->
          value in ~w(append replace delete_insert merge replace_groups)

        ["unique_key", value] ->
          list?(value, 10_000, &text?/1)

        ["window_column", value] ->
          text?(value)

        ["replacement_key", value] ->
          list?(value, 10_000, &text?/1) and value != [] and value == Enum.uniq(value)

        _ ->
          false
      end)

    if valid_options do
      map = Map.new(options, fn [key, value] -> {key, value} end)

      map_size(map) == length(options) and Map.has_key?(map, "strategy") and
        if map["strategy"] == "replace_groups" do
          Map.has_key?(map, "replacement_key") and not Map.has_key?(map, "unique_key") and
            not Map.has_key?(map, "window_column")
        else
          not Map.has_key?(map, "replacement_key")
        end
    else
      false
    end
  end

  defp materialization?(_), do: false

  defp json?(_, depth) when depth > 16, do: false
  defp json?(value, _) when is_boolean(value) or is_nil(value) or is_number(value), do: true
  defp json?(value, _) when is_binary(value), do: byte_size(value) <= 65_536
  defp json?(value, depth) when is_list(value), do: list?(value, 10_000, &json?(&1, depth + 1))

  defp json?(value, depth) when is_map(value),
    do:
      map_size(value) <= 10_000 and
        Enum.all?(value, fn {k, v} -> text?(k) and json?(v, depth + 1) end)

  defp json?(_, _), do: false
  defp keys?(value, keys), do: is_map(value) and Enum.sort(Map.keys(value)) == Enum.sort(keys)

  defp list?(value, limit, fun),
    do: is_list(value) and length(value) <= limit and Enum.all?(value, fun)

  defp text?(value),
    do:
      is_binary(value) and byte_size(value) in 1..65_536 and String.valid?(value) and
        not String.contains?(value, <<0>>)

  defp optional_text?(nil), do: true
  defp optional_text?(value), do: text?(value)

  defp digest?(value, prefix),
    do:
      is_binary(value) and
        Regex.match?(Regex.compile!("\\A" <> prefix <> "[0-9a-f]{64}\\z"), value)

  defp valid!(true), do: :ok
  defp valid!(false), do: throw(:invalid_catalog_artifact)
end
