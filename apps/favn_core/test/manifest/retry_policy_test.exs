defmodule Favn.Manifest.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias Favn.Retry.Policy
  alias Favn.ResourceRecovery.Policy, as: ResourceRecoveryPolicy

  test "manifest serialization and rehydration preserve typed asset and pipeline policies" do
    ref = {MyApp.Assets.RetryRoundtrip, :asset}
    asset_policy = Policy.new!(max_attempts: 4, backoff: 250)

    pipeline_policy =
      Policy.new!(
        max_attempts: 3,
        backoff: {:exponential, initial: 100, max: 5_000, jitter: 0.25}
      )

    resource_recovery = ResourceRecoveryPolicy.new!(:retry_remaining, max_age_ms: 3_600_000)

    manifest =
      %Manifest{
        assets: [
          %Asset{
            ref: ref,
            module: elem(ref, 0),
            name: :asset,
            type: :elixir,
            execution: %{entrypoint: :asset, arity: 1},
            retry_policy: asset_policy
          }
        ],
        pipelines: [
          %Pipeline{
            module: MyApp.Pipelines.RetryRoundtrip,
            name: :retry_roundtrip,
            selectors: [{:asset, ref}],
            retry_policy: pipeline_policy,
            resource_recovery: resource_recovery
          }
        ]
      }
      |> Map.from_struct()
      |> FavnTestSupport.with_manifest_contract()
      |> then(&struct!(Manifest, &1))
      |> FavnTestSupport.with_manifest_graph()

    assert {:ok, original} = Version.new(manifest, manifest_version_id: "mv_retry_roundtrip")
    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)
    assert {:ok, roundtrip} = Version.new(decoded, manifest_version_id: "mv_retry_roundtrip")

    assert [%Asset{retry_policy: ^asset_policy}] = roundtrip.manifest.assets

    assert [
             %Pipeline{
               retry_policy: ^pipeline_policy,
               resource_recovery: ^resource_recovery
             }
           ] = roundtrip.manifest.pipelines

    assert roundtrip.content_hash == original.content_hash
  end
end
