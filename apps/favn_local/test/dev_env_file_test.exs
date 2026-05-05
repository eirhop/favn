defmodule Favn.Dev.EnvFileTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.EnvFile

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_env_file_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "parse/2 handles dotenv assignments, comments, export, and quotes" do
    contents = """
    # ignored
    PLAIN=value
    export EXPORTED=enabled
    SPACED=hello world # comment
    DOUBLE="hello\\nworld"
    SINGLE='literal # value'
    EMPTY=
    """

    assert {:ok,
            %{
              "PLAIN" => "value",
              "EXPORTED" => "enabled",
              "SPACED" => "hello world",
              "DOUBLE" => "hello\nworld",
              "SINGLE" => "literal # value",
              "EMPTY" => ""
            }} = EnvFile.parse(contents)
  end

  test "parse/2 rejects invalid lines" do
    assert {:error, {:invalid_env_line, "test.env", 1}} = EnvFile.parse("not valid", "test.env")
    assert {:error, {:invalid_env_line, "test.env", 1}} = EnvFile.parse("1BAD=value", "test.env")
    assert {:error, {:invalid_env_line, "test.env", 1}} = EnvFile.parse("BAD=\"open", "test.env")
  end

  test "load/1 reads root .env by default and keeps shell env values", %{root_dir: root_dir} do
    restore_favn_env_file = unset_env("FAVN_ENV_FILE")
    restore_shell = set_env("FAVN_ENV_FILE_TEST_SHELL", "shell")
    restore_loaded = unset_env("FAVN_ENV_FILE_TEST_LOADED")

    File.write!(
      Path.join(root_dir, ".env"),
      "FAVN_ENV_FILE_TEST_SHELL=file\nFAVN_ENV_FILE_TEST_LOADED=file\n"
    )

    on_exit(fn ->
      restore_favn_env_file.()
      restore_shell.()
      restore_loaded.()
    end)

    assert {:ok, result} = EnvFile.load(root_dir: root_dir)

    assert result.values["FAVN_ENV_FILE_TEST_SHELL"] == "file"
    refute Map.has_key?(result.loaded, "FAVN_ENV_FILE_TEST_SHELL")
    assert result.loaded["FAVN_ENV_FILE_TEST_LOADED"] == "file"
    assert System.get_env("FAVN_ENV_FILE_TEST_SHELL") == "shell"
    assert System.get_env("FAVN_ENV_FILE_TEST_LOADED") == "file"
  end

  test "load/1 supports FAVN_ENV_FILE override relative to root", %{root_dir: root_dir} do
    restore_favn_env_file = set_env("FAVN_ENV_FILE", "local.env")
    restore_override = unset_env("FAVN_ENV_FILE_TEST_OVERRIDE")

    File.write!(Path.join(root_dir, ".env"), "FAVN_ENV_FILE_TEST_OVERRIDE=default\n")
    File.write!(Path.join(root_dir, "local.env"), "FAVN_ENV_FILE_TEST_OVERRIDE=override\n")

    on_exit(fn ->
      restore_favn_env_file.()
      restore_override.()
    end)

    assert {:ok, result} = EnvFile.load(root_dir: root_dir)

    assert result.path == Path.join(root_dir, "local.env")
    assert System.get_env("FAVN_ENV_FILE_TEST_OVERRIDE") == "override"
  end

  test "load/1 fails when explicit FAVN_ENV_FILE is missing", %{root_dir: root_dir} do
    restore_favn_env_file = set_env("FAVN_ENV_FILE", "missing.env")

    on_exit(fn ->
      restore_favn_env_file.()
    end)

    path = Path.join(root_dir, "missing.env")
    assert {:error, {:env_file_not_found, ^path}} = EnvFile.load(root_dir: root_dir)
  end

  defp set_env(key, value) do
    previous = System.get_env(key)
    System.put_env(key, value)
    fn -> restore_env(key, previous) end
  end

  defp unset_env(key) do
    previous = System.get_env(key)
    System.delete_env(key)
    fn -> restore_env(key, previous) end
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)
end
