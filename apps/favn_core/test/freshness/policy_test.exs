defmodule Favn.Freshness.PolicyTest do
  use ExUnit.Case, async: true

  alias Favn.Freshness.Policy

  describe "from_value/1" do
    test "normalizes daily V1 forms" do
      assert {:ok, %Policy{mode: :calendar_period, kind: :day, timezone: "Etc/UTC"}} =
               Policy.from_value(:daily)

      assert {:ok, %Policy{mode: :calendar_period, kind: :day, timezone: "Europe/Oslo"}} =
               Policy.from_value({:daily, timezone: "Europe/Oslo"})
    end

    test "normalizes canonical day aliases" do
      assert {:ok, %Policy{mode: :calendar_period, kind: :day, timezone: "Etc/UTC"}} =
               Policy.from_value(:day)

      assert {:ok, %Policy{mode: :calendar_period, kind: :day, timezone: "Europe/Oslo"}} =
               Policy.from_value({:day, timezone: "Europe/Oslo"})
    end

    test "normalizes max age V1 forms" do
      assert {:ok, %Policy{mode: :max_age, amount: 24, unit: :hour}} =
               Policy.from_value(max_age: {:hours, 24})

      assert {:ok, %Policy{mode: :max_age, amount: 30, unit: :minute}} =
               Policy.from_value(max_age: {:minutes, 30})
    end

    test "normalizes window success and always forms" do
      assert {:ok, %Policy{mode: :window_success}} = Policy.from_value(window_success: true)
      assert {:ok, %Policy{mode: :always}} = Policy.from_value(:always)
    end

    test "normalizes nil as missing policy" do
      assert {:ok, nil} = Policy.from_value(nil)
    end

    test "normalizes canonical map forms" do
      assert {:ok, %Policy{mode: :calendar_period, kind: :day, timezone: "Etc/UTC"}} =
               Policy.from_value(%{mode: :calendar_period, kind: :day, timezone: "Etc/UTC"})

      assert {:ok, %Policy{mode: :calendar_period, kind: :day, timezone: "Etc/UTC"}} =
               Policy.from_value(%{
                 "mode" => "calendar_period",
                 "kind" => "day",
                 "timezone" => "Etc/UTC"
               })

      assert {:ok, %Policy{mode: :max_age, amount: 1, unit: :day}} =
               Policy.from_value(%{mode: :max_age, amount: 1, unit: :day})

      assert {:ok, %Policy{mode: :max_age, amount: 1, unit: :day}} =
               Policy.from_value(%{"mode" => "max_age", "amount" => 1, "unit" => "days"})

      assert {:ok, %Policy{mode: :window_success}} =
               Policy.from_value(%{mode: :window_success})

      assert {:ok, %Policy{mode: :always}} = Policy.from_value(%{mode: :always})
    end

    test "rejects invalid timezone" do
      assert {:error, {:invalid_timezone, "Definitely/NotAZone"}} =
               Policy.from_value({:daily, timezone: "Definitely/NotAZone"})
    end

    test "rejects invalid max age units and counts" do
      assert {:error, {:invalid_freshness_max_age_unit, :weeks}} =
               Policy.from_value(max_age: {:weeks, 1})

      assert {:error, {:invalid_freshness_max_age_amount, 0}} =
               Policy.from_value(max_age: {:hours, 0})

      assert {:error, {:invalid_freshness_max_age_amount, -1}} =
               Policy.from_value(max_age: {:hours, -1})
    end

    test "rejects unsupported forms" do
      assert {:error, {:invalid_freshness_policy, :weekly}} = Policy.from_value(:weekly)

      assert {:error, {:invalid_freshness_policy, [window_success: false]}} =
               Policy.from_value(window_success: false)
    end
  end

  describe "from_value!/1" do
    test "raises on invalid policy" do
      assert_raise ArgumentError, ~r/invalid freshness policy/, fn ->
        Policy.from_value!(max_age: {:hours, 0})
      end
    end
  end

  describe "validate/1" do
    test "validates canonical maps and clears irrelevant fields" do
      assert {:ok, %Policy{mode: :max_age, kind: nil, timezone: nil, amount: 2, unit: :day}} =
               Policy.validate(%Policy{
                 mode: :max_age,
                 kind: :day,
                 timezone: "Etc/UTC",
                 amount: 2,
                 unit: :day
               })
    end

    test "rejects plural units in already-normalized structs" do
      assert {:error, {:invalid_freshness_max_age_unit, :hours}} =
               Policy.validate(%Policy{mode: :max_age, amount: 24, unit: :hours})
    end
  end
end
