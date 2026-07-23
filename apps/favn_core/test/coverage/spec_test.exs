defmodule Favn.Coverage.SpecTest do
  use ExUnit.Case, async: true

  alias Favn.Coverage.Spec

  test "normalizes supported duration units" do
    for {unit, expected} <- [
          second: 1,
          seconds: 1,
          minute: 60,
          minutes: 60,
          hour: 3_600,
          hours: 3_600,
          day: 86_400,
          days: 86_400
        ] do
      assert {:ok, %Spec{availability_delay_seconds: ^expected}} =
               Spec.new(from: ~D[2020-01-01], availability_delay: {unit, 1})
    end
  end

  test "requires from and rejects unknown, duplicate, and invalid options" do
    assert {:error, :coverage_from_required} = Spec.new([])
    assert {:error, {:unknown_opt, :timezone}} = Spec.new(from: ~D[2020-01-01], timezone: "UTC")

    assert {:error, {:duplicate_coverage_options, [:from]}} =
             Spec.new(from: ~D[2020-01-01], from: ~D[2021-01-01])

    assert {:error, {:invalid_coverage_delay_unit, :weeks}} =
             Spec.new(from: ~D[2020-01-01], availability_delay: {:weeks, 1})

    assert {:error, {:invalid_coverage_delay, {:hours, -1}}} =
             Spec.new(from: ~D[2020-01-01], availability_delay: {:hours, -1})

    assert {:error, {:unknown_coverage_spec_fields, ["timezone"]}} =
             Spec.from_value(%{"from" => "2020-01-01", "timezone" => "Etc/UTC"})
  end

  test "availability delay is valid only for latest closed coverage" do
    assert {:error, {:coverage_delay_requires_latest_closed, :current}} =
             Spec.new(
               from: ~D[2020-01-01],
               through: :current,
               availability_delay: {:hours, 0}
             )
  end

  test "fixed boundaries are inclusive policies and must be ordered" do
    assert {:ok, %Spec{through: ~D[2020-02-01]}} =
             Spec.new(from: ~D[2020-01-01], through: ~D[2020-02-01])

    assert {:error, {:coverage_through_before_from, ~D[2020-02-01], ~D[2020-01-01]}} =
             Spec.new(from: ~D[2020-02-01], through: ~D[2020-01-01])
  end
end
