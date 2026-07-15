defmodule Favn.Manifest.FreshnessTest do
  use ExUnit.Case, async: true

  alias Favn.Freshness.Policy
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version

  describe "Asset.from_asset/1" do
    test "preserves missing freshness as nil" do
      assert %Asset{freshness: nil} = Asset.from_asset(%{})
    end

    test "normalizes authored freshness values" do
      assert %Asset{
               freshness: %Policy{mode: :calendar_period, kind: :day, timezone: "Europe/Oslo"}
             } =
               Asset.from_asset(%{freshness: {:daily, timezone: "Europe/Oslo"}})

      assert %Asset{freshness: %Policy{mode: :max_age, amount: 24, unit: :hour}} =
               Asset.from_asset(%{freshness: [max_age: {:hours, 24}]})

      assert %Asset{freshness: %Policy{mode: :window_success}} =
               Asset.from_asset(%{freshness: [window_success: true]})

      assert %Asset{freshness: %Policy{mode: :always}} =
               Asset.from_asset(%{freshness: :always})
    end
  end

  test "serializer emits stable JSON for freshness policy structs" do
    assert {:ok, encoded} =
             Serializer.encode_manifest(%{
               freshness: %Policy{mode: :calendar_period, kind: :day, timezone: "Europe/Oslo"}
             })

    assert encoded ==
             ~s|{"freshness":{"amount":null,"kind":"day","mode":"calendar_period","timezone":"Europe/Oslo","unit":null}}|
  end

  test "manifest serialization and rehydration round-trips freshness policies" do
    manifest =
      FavnTestSupport.with_manifest_graph(%Manifest{
        schema_version: 4,
        runner_contract_version: 4,
        assets: [
          asset(:missing, nil),
          asset(:daily_oslo, %Policy{
            mode: :calendar_period,
            kind: :day,
            timezone: "Europe/Oslo"
          }),
          asset(:max_age, %Policy{mode: :max_age, amount: 24, unit: :hour}),
          asset(:window_success, %Policy{mode: :window_success}),
          asset(:always, %Policy{mode: :always})
        ],
        pipelines: [],
        schedules: [],
        metadata: %{}
      })

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)
    assert {:ok, version} = Version.new(decoded, manifest_version_id: "mv_freshness_roundtrip")

    assert [missing, daily_oslo, max_age, window_success, always] = version.manifest.assets
    assert missing.freshness == nil

    assert daily_oslo.freshness == %Policy{
             mode: :calendar_period,
             kind: :day,
             timezone: "Europe/Oslo"
           }

    assert max_age.freshness == %Policy{mode: :max_age, amount: 24, unit: :hour}
    assert window_success.freshness == %Policy{mode: :window_success}
    assert always.freshness == %Policy{mode: :always}
  end

  defp asset(name, freshness) do
    module = Module.concat([MyApp, Assets, Macro.camelize(to_string(name))])

    %Asset{
      ref: {module, :asset},
      module: module,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1},
      freshness: freshness
    }
  end
end
