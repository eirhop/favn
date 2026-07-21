defmodule FavnRunner.ReleaseVerifierTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease
  alias Favn.RunnerRelease.BeamDigest
  alias FavnRunner.ReleaseVerifier

  @digest String.duplicate("a", 64)
  @runtime_module FavnRunner.ReleaseVerifier

  defmodule OmittedPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(opts) do
      if test_pid = Keyword.get(opts, :test_pid), do: send(test_pid, :omitted_plugin_called)
      {:ok, []}
    end
  end

  defmodule OmittedChild do
    def child_spec(opts) do
      send(Keyword.fetch!(opts, :test_pid), :omitted_child_spec_called)

      %{
        id: __MODULE__,
        start: {Agent, :start_link, [fn -> opts end]}
      }
    end
  end

  setup do
    on_exit(fn ->
      install_fixture_release!()
    end)

    :ok
  end

  @tag :tmp_dir
  test "verifies packaged module and application fingerprints", %{tmp_dir: tmp_dir} do
    application = install_application_fixture!(tmp_dir, @digest)
    descriptor = descriptor(runtime_applications: [application])
    path = write_descriptor!(tmp_dir, descriptor)

    assert {:ok, ^descriptor} = ReleaseVerifier.verify_file(path)
  end

  @tag :tmp_dir
  test "installs only bounded operational release identity", %{tmp_dir: tmp_dir} do
    descriptor = descriptor()
    path = write_descriptor!(tmp_dir, descriptor)

    assert :ok = ReleaseVerifier.verify_test_startup(mode: :required, path: path)
    assert {:ok, info} = FavnRunner.release_info()

    assert info == %{
             runner_release_id: descriptor.runner_release_id,
             favn_version: descriptor.favn_version,
             runner_contract_version: descriptor.runner_contract_version,
             elixir_version: descriptor.elixir_version,
             otp_release: descriptor.otp_release,
             target: descriptor.target,
             build_profile: descriptor.build_profile
           }

    refute Map.has_key?(info, :runtime_modules)
    refute Map.has_key?(info, :runtime_applications)
    refute Map.has_key?(info, :build_metadata)
  end

  @tag :tmp_dir
  test "rejects a missing descriptor and a forged self hash", %{tmp_dir: tmp_dir} do
    missing = Path.join(tmp_dir, "missing.json")

    assert {:error, :runner_release_descriptor_missing} =
             ReleaseVerifier.verify_file(missing)

    descriptor = descriptor()
    {:ok, json} = RunnerRelease.encode(descriptor)

    forged =
      json
      |> Jason.decode!()
      |> Map.put("runner_release_id", "rr_" <> String.duplicate("f", 64))
      |> Jason.encode!()

    path = Path.join(tmp_dir, "forged.json")
    File.write!(path, forged)

    assert {:error, {:runner_release_descriptor_invalid, :self_hash_mismatch}} =
             ReleaseVerifier.verify_file(path)
  end

  @tag :tmp_dir
  test "required startup fails closed while optional Mix startup may omit the file", %{
    tmp_dir: tmp_dir
  } do
    missing = Path.join(tmp_dir, "missing.json")

    assert {:error, :runner_release_descriptor_missing} =
             ReleaseVerifier.verify_test_startup(mode: :required, path: missing)

    assert :ok = ReleaseVerifier.verify_test_startup(mode: :optional, path: missing)
    assert {:error, :runner_release_not_verified} = FavnRunner.release_info()
  end

  @tag :tmp_dir
  test "rejects a stamped packaged application omitted from the descriptor", %{tmp_dir: tmp_dir} do
    application = install_application_fixture!(tmp_dir, @digest)
    path = write_descriptor!(tmp_dir, descriptor(runtime_applications: []))

    assert {:error, {:runner_release_dependency_mismatch, name}} =
             ReleaseVerifier.verify_file(path)

    assert name == application.application
  end

  @tag :tmp_dir
  test "rejects a configured plugin omitted from the descriptor", %{tmp_dir: tmp_dir} do
    previous = Application.get_env(:favn, :runner_plugins)
    Application.put_env(:favn, :runner_plugins, [{OmittedPlugin, []}])

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous)
      end
    end)

    path = write_descriptor!(tmp_dir, descriptor())

    assert {:error, {:runner_release_plugin_mismatch, plugin}} =
             ReleaseVerifier.verify_test_startup(mode: :required, path: path)

    assert plugin == Atom.to_string(OmittedPlugin)
  end

  @tag :tmp_dir
  test "rejects a plugin whose entrypoint is omitted from its module fingerprints", %{
    tmp_dir: tmp_dir
  } do
    previous = Application.get_env(:favn, :runner_plugins)
    Application.put_env(:favn, :runner_plugins, [{OmittedPlugin, test_pid: self()}])

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous)
      end
    end)

    plugin_module = Atom.to_string(OmittedPlugin)
    unrelated_module = Atom.to_string(@runtime_module)

    descriptor =
      descriptor(
        plugins: [
          %{
            plugin: plugin_module,
            version: "1.0.0",
            modules: [unrelated_module],
            capabilities: []
          }
        ]
      )

    path = write_descriptor!(tmp_dir, descriptor)

    assert {:error, {:runner_release_module_missing, ^plugin_module}} =
             ReleaseVerifier.verify_test_startup(mode: :required, path: path)

    refute_received :omitted_plugin_called
  end

  @tag :tmp_dir
  test "rejects an options-selected supervised child omitted from the descriptor", %{
    tmp_dir: tmp_dir
  } do
    previous = Application.get_env(:favn, :runner_plugins)

    Application.put_env(:favn, :runner_plugins, [
      {Favn.Runner.SupervisedChildren, children: [{OmittedChild, test_pid: self()}]}
    ])

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous)
      end
    end)

    plugin_module = Atom.to_string(Favn.Runner.SupervisedChildren)

    descriptor =
      descriptor(
        runtime_modules: [
          module_fingerprint(@runtime_module),
          module_fingerprint(Favn.Runner.SupervisedChildren)
        ],
        plugins: [
          %{
            plugin: plugin_module,
            version: "1.0.0",
            modules: [plugin_module],
            capabilities: []
          }
        ]
      )

    path = write_descriptor!(tmp_dir, descriptor)

    assert {:error,
            {:unverified_plugin_child_module, Favn.Runner.SupervisedChildren, OmittedChild}} =
             ReleaseVerifier.verify_test_startup(mode: :required, path: path)

    refute_received :omitted_child_spec_called
  end

  @tag :tmp_dir
  test "rejects missing and changed packaged modules", %{tmp_dir: tmp_dir} do
    missing = descriptor(runtime_modules: [%{module: "Elixir.NotPackaged", digest: @digest}])

    assert {:error, {:runner_release_module_missing, "Elixir.NotPackaged"}} =
             missing
             |> write_descriptor!(tmp_dir)
             |> ReleaseVerifier.verify_file()

    changed =
      descriptor(runtime_modules: [%{module: Atom.to_string(@runtime_module), digest: @digest}])

    assert {:error, {:runner_release_module_mismatch, module_name}} =
             changed
             |> write_descriptor!(tmp_dir)
             |> ReleaseVerifier.verify_file()

    assert module_name == Atom.to_string(@runtime_module)
  end

  @tag :tmp_dir
  test "rejects missing, changed, and runtime-incompatible applications", %{tmp_dir: tmp_dir} do
    missing =
      descriptor(
        runtime_applications: [
          %{application: "not_packaged", version: "1.0.0", lock_fingerprint: @digest}
        ]
      )

    assert {:error, {:runner_release_application_missing, "not_packaged"}} =
             missing
             |> write_descriptor!(tmp_dir)
             |> ReleaseVerifier.verify_file()

    changed_application = install_application_fixture!(tmp_dir, @digest)

    changed =
      descriptor(
        runtime_applications: [
          %{changed_application | version: "9.9.9"}
        ]
      )

    assert {:error, {:runner_release_dependency_mismatch, "favn_verifier_fixture"}} =
             changed
             |> write_descriptor!(tmp_dir)
             |> ReleaseVerifier.verify_file()

    wrong_lock =
      descriptor(
        runtime_applications: [
          %{changed_application | lock_fingerprint: String.duplicate("b", 64)}
        ]
      )

    assert {:error, {:runner_release_dependency_mismatch, "favn_verifier_fixture"}} =
             wrong_lock
             |> write_descriptor!(tmp_dir)
             |> ReleaseVerifier.verify_file()

    incompatible = descriptor(elixir_version: incompatible_elixir_version())

    assert {:error, {:runner_release_runtime_mismatch, :elixir_version}} =
             incompatible
             |> write_descriptor!(tmp_dir)
             |> ReleaseVerifier.verify_file()
  end

  @tag :tmp_dir
  test "rejects a descriptor built for a different runtime target", %{tmp_dir: tmp_dir} do
    path = write_descriptor!(tmp_dir, descriptor())

    assert {:error, {:runner_release_runtime_mismatch, :target}} =
             ReleaseVerifier.verify_test_file(path, target: "linux/arm64")
  end

  defp descriptor(overrides \\ []) do
    attrs = %{
      schema_version: RunnerRelease.current_schema_version(),
      favn_version: RunnerRelease.current_favn_version(),
      runner_contract_version: Compatibility.current_runner_contract_version(),
      elixir_version: System.version(),
      otp_release: to_string(:erlang.system_info(:otp_release)),
      target: RunnerRelease.current_target(),
      runtime_modules: [module_fingerprint(@runtime_module)],
      runtime_applications: [],
      plugins: [],
      build_profile: "prod",
      build_metadata: %{"test" => true}
    }

    {:ok, descriptor} = RunnerRelease.new(Map.merge(attrs, Map.new(overrides)))
    descriptor
  end

  defp module_fingerprint(module) do
    {:module, ^module} = Code.ensure_loaded(module)
    beam = module |> :code.which() |> List.to_string() |> File.read!()
    {:ok, digest} = BeamDigest.digest(beam)
    %{module: Atom.to_string(module), digest: digest}
  end

  defp write_descriptor!(descriptor, directory) when is_struct(descriptor, RunnerRelease),
    do: write_descriptor!(directory, descriptor)

  defp write_descriptor!(directory, descriptor) do
    path = Path.join(directory, "runner-release-#{System.unique_integer([:positive])}.json")
    {:ok, json} = RunnerRelease.encode(descriptor)
    File.write!(path, json)
    path
  end

  defp incompatible_elixir_version do
    {:ok, version} = Version.parse(System.version())
    "#{version.major}.#{version.minor}.#{version.patch + 1}"
  end

  defp install_application_fixture!(tmp_dir, lock_fingerprint) do
    application = :favn_verifier_fixture
    application_name = Atom.to_string(application)
    code_path = Path.join(tmp_dir, "fixture_#{System.unique_integer([:positive])}")
    File.mkdir_p!(code_path)

    contents =
      :io_lib.format(
        "~tp.~n",
        [
          {:application, application,
           [
             vsn: ~c"1.2.3",
             modules: [],
             favn_runner_lock_fingerprint: lock_fingerprint
           ]}
        ]
      )
      |> IO.iodata_to_binary()

    File.write!(Path.join(code_path, application_name <> ".app"), contents)
    true = Code.prepend_path(code_path)
    on_exit(fn -> Code.delete_path(code_path) end)

    %{
      application: application_name,
      version: "1.2.3",
      lock_fingerprint: lock_fingerprint
    }
  end

  defp install_fixture_release! do
    directory = System.tmp_dir!()
    path = write_descriptor!(directory, FavnTestSupport.runner_release())

    try do
      :ok = ReleaseVerifier.verify_test_startup(mode: :required, path: path)
    after
      File.rm(path)
    end
  end
end
