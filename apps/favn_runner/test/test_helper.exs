ExUnit.start()

descriptor = FavnTestSupport.runner_release()
{:ok, json} = Favn.RunnerRelease.encode(descriptor)

path =
  Path.join(
    System.tmp_dir!(),
    "favn-runner-test-release-#{System.unique_integer([:positive])}.json"
  )

File.write!(path, json)

try do
  :ok = FavnRunner.ReleaseVerifier.verify_test_startup(mode: :required, path: path)
after
  File.rm(path)
end
