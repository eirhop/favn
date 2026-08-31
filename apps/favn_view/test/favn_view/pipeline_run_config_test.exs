defmodule FavnView.PipelineRunConfigTest do
  use ExUnit.Case, async: true

  alias FavnView.PipelineRunConfig

  doctest FavnView.PipelineRunConfig

  describe "default/1" do
    test "opens on what the pipeline declares" do
      pipeline = %{
        window: %{"kind" => "day", "timezone" => "Europe/Oslo", "combine_windows" => true}
      }

      assert PipelineRunConfig.default(pipeline) == %{
               kind: "day",
               from: "",
               to: "",
               refresh: "auto",
               combine_windows: true,
               timezone: "Europe/Oslo"
             }
    end

    test "an unwindowed pipeline still gets values its form fields can hold" do
      config = PipelineRunConfig.default(%{window: nil})

      assert config.from == ""
      assert config.to == ""
      assert config.timezone == "Etc/UTC"
    end

    test "a legacy kind spelling normalizes to the one the form offers" do
      assert PipelineRunConfig.default(%{window: %{kind: :monthly}}).kind == "month"
    end
  end

  describe "from_params/2" do
    test "a field the form did not send keeps its current value" do
      current = %{PipelineRunConfig.default(nil) | from: "2026-03", combine_windows: true}

      config =
        PipelineRunConfig.from_params(%{"run_config" => %{"refresh" => "missing"}}, current)

      assert config.from == "2026-03"
      assert config.combine_windows
      assert config.refresh == "missing"
    end

    test "period bounds are trimmed, so a whitespace-only end is not a range" do
      config =
        PipelineRunConfig.from_params(
          %{"run_config" => %{"from" => " 2026-03 ", "to" => "   "}},
          PipelineRunConfig.default(nil)
        )

      assert config.from == "2026-03"
      refute PipelineRunConfig.range_requested?(config)
    end

    test "the unchecked checkbox reads as false" do
      current = %{PipelineRunConfig.default(nil) | combine_windows: true}

      config =
        PipelineRunConfig.from_params(%{"run_config" => %{"combine_windows" => "false"}}, current)

      refute config.combine_windows
    end
  end

  describe "validate/2" do
    test "the declared configuration submits" do
      assert PipelineRunConfig.validate(PipelineRunConfig.default(nil), true) == nil
    end

    test "an unwindowed pipeline cannot be asked for a period" do
      config = %{PipelineRunConfig.default(nil) | from: "2026-03"}

      assert PipelineRunConfig.validate(config, false) ==
               "This pipeline has no window, so it cannot run a period."
    end

    test "an unwindowed pipeline with no period submits" do
      assert PipelineRunConfig.validate(PipelineRunConfig.default(nil), false) == nil
    end

    test "a refresh choice the dialog does not offer is refused" do
      config = %{PipelineRunConfig.default(nil) | refresh: "force_selected"}

      assert PipelineRunConfig.validate(config, true) == "Refresh choice is invalid."
    end

    test "an invalid timezone is refused only when a period is asked for" do
      config = %{PipelineRunConfig.default(nil) | timezone: "Europe/Oslo; drop"}

      assert PipelineRunConfig.validate(config, true) == nil

      assert PipelineRunConfig.validate(%{config | from: "2026-03"}, true) ==
               "Timezone is invalid."
    end
  end

  describe "changed_fields/2" do
    test "the declared configuration changes nothing" do
      default = PipelineRunConfig.default(nil)

      assert PipelineRunConfig.changed_fields(default, default) == []
    end

    test "the declared window kind survives a payload that names another" do
      current = PipelineRunConfig.default(%{window: %{"kind" => "month"}})

      config = PipelineRunConfig.from_params(%{"run_config" => %{"kind" => "day"}}, current)

      assert config.kind == "month"
    end

    test "any period is a change, because the default is to run the latest complete one" do
      default = PipelineRunConfig.default(nil)

      assert PipelineRunConfig.changed_fields(%{default | from: "2026-03"}, default) == [:period]
    end

    test "combine windows only counts as changed once a range asks for it" do
      default = PipelineRunConfig.default(nil)
      combined = %{default | combine_windows: true}

      refute :combine_windows in PipelineRunConfig.changed_fields(combined, default)

      assert :combine_windows in PipelineRunConfig.changed_fields(
               %{combined | from: "2026-01", to: "2026-08"},
               default
             )
    end
  end
end
