defmodule Favn.RunnerReleaseTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease

  @digest_a String.duplicate("a", 64)
  @digest_b String.duplicate("b", 64)
  @digest_c String.duplicate("c", 64)
  @digest_d String.duplicate("d", 64)

  test "canonical identity and release ID are independent of entry order and build metadata" do
    assert {:ok, first} = RunnerRelease.new(base_attrs())

    reordered =
      base_attrs()
      |> Map.update!(:runtime_modules, &Enum.reverse/1)
      |> Map.update!(:runtime_applications, &Enum.reverse/1)
      |> Map.update!(:plugins, &Enum.reverse/1)
      |> Map.put(:build_metadata, %{"built_at" => "2030-01-01T00:00:00Z"})

    assert {:ok, second} = RunnerRelease.new(reordered)

    assert first.runner_release_id == second.runner_release_id
    assert first.runtime_code_digest == second.runtime_code_digest
    assert first.runtime_dependency_digest == second.runtime_dependency_digest
    assert RunnerRelease.identity_json(first) == RunnerRelease.identity_json(second)
    assert String.match?(first.runner_release_id, ~r/\Arr_[0-9a-f]{64}\z/)

    assert Enum.map(first.runtime_modules, & &1.module) == [
             "Elixir.Acme.Helper",
             "Elixir.Acme.Run"
           ]

    assert Enum.map(first.runtime_applications, & &1.application) == ["acme", "favn_runner"]

    assert Enum.map(first.plugins, & &1.plugin) == [
             "Elixir.Acme.Plugin",
             "Elixir.Favn.Adapter"
           ]
  end

  test "canonical JSON round trips and preserves non-identity build metadata" do
    assert {:ok, descriptor} = RunnerRelease.new(base_attrs())
    assert {:ok, json} = RunnerRelease.encode(descriptor)
    assert {:ok, decoded} = RunnerRelease.decode(json)

    assert decoded == descriptor
    assert {:ok, ^json} = RunnerRelease.encode(decoded)
    assert json =~ ~s("build_metadata":{"built_at":"2026-07-21T12:00:00Z")
  end

  test "verification requires every serialized descriptor field" do
    assert {:ok, descriptor} = RunnerRelease.new(base_attrs())
    value = Map.from_struct(descriptor)

    for field <- [
          :schema_version,
          :favn_version,
          :runner_contract_version,
          :elixir_version,
          :otp_release,
          :target,
          :runtime_code_digest,
          :runtime_dependency_digest,
          :runtime_modules,
          :runtime_applications,
          :plugins,
          :build_profile,
          :runner_release_id,
          :build_metadata
        ] do
      assert {:error, {:missing_runner_release_field, ^field}} =
               value |> Map.delete(field) |> RunnerRelease.verify()
    end
  end

  test "rejects unsupported schema, protocol, target, profile, and Favn series" do
    current_contract = Compatibility.current_runner_contract_version()
    future_contract = current_contract + 1

    assert {:error, {:unsupported_runner_release_schema, 2, 1}} =
             base_attrs() |> Map.put(:schema_version, 2) |> RunnerRelease.new()

    assert {:error, {:unsupported_runner_contract, ^future_contract, ^current_contract}} =
             base_attrs()
             |> Map.put(:runner_contract_version, future_contract)
             |> RunnerRelease.new()

    assert {:error, {:unsupported_favn_version, "1.0.0", "0.5.x"}} =
             base_attrs() |> Map.put(:favn_version, "1.0.0") |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :target, :unsupported_value}} =
             base_attrs() |> Map.put(:target, "linux/arm64") |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :build_profile, :unsupported_value}} =
             base_attrs() |> Map.put(:build_profile, "dev") |> RunnerRelease.new()
  end

  test "rejects malformed language, OTP, nested digest, and metadata fields" do
    assert {:error, {:invalid_runner_release_field, :elixir_version, :invalid_version}} =
             base_attrs() |> Map.put(:elixir_version, "latest") |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :otp_release, :invalid_version}} =
             base_attrs() |> Map.put(:otp_release, "OTP-28") |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :digest, :invalid_sha256}} =
             base_attrs()
             |> Map.put(:runtime_modules, [%{module: Acme.Run, digest: "bad"}])
             |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :lock_fingerprint, :invalid_sha256}} =
             base_attrs()
             |> Map.put(:runtime_applications, [
               %{application: :acme, version: "1.0.0", lock_fingerprint: "bad"}
             ])
             |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :build_metadata, :invalid_json_value}} =
             base_attrs() |> Map.put(:build_metadata, %{pid: self()}) |> RunnerRelease.new()
  end

  test "rejects duplicate modules, applications, plugins, and plugin declarations" do
    duplicate_module = %{module: Acme.Run, digest: @digest_a}

    assert {:error, {:duplicate_runner_release_entry, :module, "Elixir.Acme.Run"}} =
             base_attrs()
             |> Map.put(:runtime_modules, [duplicate_module, duplicate_module])
             |> RunnerRelease.new()

    duplicate_application = %{
      application: :acme,
      version: "1.0.0",
      lock_fingerprint: @digest_c
    }

    assert {:error, {:duplicate_runner_release_entry, :application, "acme"}} =
             base_attrs()
             |> Map.put(:runtime_applications, [duplicate_application, duplicate_application])
             |> RunnerRelease.new()

    duplicate_plugin = %{
      plugin: Acme.Plugin,
      version: "1.0.0",
      modules: [],
      capabilities: []
    }

    assert {:error, {:duplicate_runner_release_entry, :plugin, "Elixir.Acme.Plugin"}} =
             base_attrs()
             |> Map.put(:plugins, [duplicate_plugin, duplicate_plugin])
             |> RunnerRelease.new()

    assert {:error, {:duplicate_runner_release_entry, :modules, "Elixir.Acme.Plugin"}} =
             base_attrs()
             |> Map.put(:plugins, [
               %{
                 plugin: Acme.Plugin,
                 version: "1.0.0",
                 modules: [Acme.Plugin, Acme.Plugin],
                 capabilities: []
               }
             ])
             |> RunnerRelease.new()
  end

  test "rejects missing canonical nested plugin fields" do
    for field <- [:plugin, :version, :modules, :capabilities] do
      plugin =
        %{
          plugin: Acme.Plugin,
          version: "1.0.0",
          modules: [],
          capabilities: []
        }
        |> Map.delete(field)

      assert {:error, {:missing_runner_release_field, ^field}} =
               base_attrs() |> Map.put(:plugins, [plugin]) |> RunnerRelease.new()
    end
  end

  test "rejects missing canonical nested module and application fields" do
    for field <- [:module, :digest] do
      module_fingerprint = %{module: Acme.Run, digest: @digest_a} |> Map.delete(field)

      assert {:error, {:missing_runner_release_field, ^field}} =
               base_attrs()
               |> Map.put(:runtime_modules, [module_fingerprint])
               |> RunnerRelease.new()
    end

    for field <- [:application, :version, :lock_fingerprint] do
      application_fingerprint =
        %{application: :acme, version: "1.0.0", lock_fingerprint: @digest_c}
        |> Map.delete(field)

      assert {:error, {:missing_runner_release_field, ^field}} =
               base_attrs()
               |> Map.put(:runtime_applications, [application_fingerprint])
               |> RunnerRelease.new()
    end
  end

  test "rejects supplied aggregate digests and release IDs that do not match" do
    assert {:ok, descriptor} = RunnerRelease.new(base_attrs())
    expected_code_digest = descriptor.runtime_code_digest
    expected_dependency_digest = descriptor.runtime_dependency_digest
    expected_release_id = descriptor.runner_release_id

    assert {:error, {:runtime_code_digest_mismatch, ^expected_code_digest, @digest_d}} =
             base_attrs() |> Map.put(:runtime_code_digest, @digest_d) |> RunnerRelease.new()

    assert {:error, {:runtime_dependency_digest_mismatch, ^expected_dependency_digest, @digest_d}} =
             base_attrs()
             |> Map.put(:runtime_dependency_digest, @digest_d)
             |> RunnerRelease.new()

    forged = "rr_" <> @digest_d

    assert {:error, {:runner_release_id_mismatch, ^expected_release_id, ^forged}} =
             base_attrs() |> Map.put(:runner_release_id, forged) |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}} =
             descriptor
             |> Map.from_struct()
             |> Map.put(:runner_release_id, "bad")
             |> RunnerRelease.verify()
  end

  test "rejects malformed JSON documents without creating an artifact" do
    assert {:error, {:invalid_runner_release_json, :invalid_root}} = RunnerRelease.decode("[]")
    assert {:error, {:invalid_runner_release_json, _reason}} = RunnerRelease.decode("{")
  end

  defp base_attrs do
    %{
      schema_version: RunnerRelease.current_schema_version(),
      favn_version: RunnerRelease.current_favn_version(),
      runner_contract_version: Compatibility.current_runner_contract_version(),
      elixir_version: System.version(),
      otp_release: :erlang.system_info(:otp_release) |> to_string(),
      target: RunnerRelease.current_target(),
      runtime_modules: [
        %{module: Acme.Run, digest: @digest_a},
        %{module: Acme.Helper, digest: @digest_b}
      ],
      runtime_applications: [
        %{application: :favn_runner, version: "0.5.0-dev", lock_fingerprint: @digest_d},
        %{application: :acme, version: "1.0.0", lock_fingerprint: @digest_c}
      ],
      plugins: [
        %{
          plugin: Favn.Adapter,
          version: "2.0.0",
          modules: [Favn.Adapter],
          capabilities: ["sql.query"]
        },
        %{
          plugin: Acme.Plugin,
          version: "1.0.0",
          modules: [Acme.Plugin],
          capabilities: ["runtime.inputs"]
        }
      ],
      build_profile: "prod",
      build_metadata: %{"built_at" => "2026-07-21T12:00:00Z", "git_sha" => "abc123"}
    }
  end
end
