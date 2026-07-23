ExUnit.start()

:ok =
  FavnRunner.ReleaseVerifier.verify_test_startup(%{
    "FAVN_RUNNER_RELEASE_ID" => FavnTestSupport.runner_release_id()
  })
