# Favn's runner is started by `mix favn.dev` in normal use. Tests that
# materialize a SQL asset need its connection registry, so start it here.
{:ok, _apps} = Application.ensure_all_started(:favn_runner)

ExUnit.start()
