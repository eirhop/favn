defmodule FavnOrchestrator.RunSubmission.IntentTest do
  use ExUnit.Case, async: true

  alias Favn.Retry.Policy
  alias Favn.Window.Anchor
  alias Favn.Window.Selection
  alias FavnOrchestrator.RunSubmission.Intent

  test "round-trips only allowlisted planning options without losing term identity" do
    anchor =
      Anchor.new!(
        :day,
        ~U[2026-07-01 00:00:00Z],
        ~U[2026-07-02 00:00:00Z]
      )

    {:ok, selection} = Selection.manual(anchor, "Etc/UTC")
    retry_policy = Policy.new!(max_attempts: 3, backoff: 100)

    opts = [
      dependencies: :none,
      metadata: %{reason: :manual},
      retry_policy: retry_policy,
      window_selection: selection,
      replay_node_keys: [{{__MODULE__, :asset}, nil}]
    ]

    assert {:ok, intent} = Intent.new(:rerun, "source-run", opts)
    assert {:ok, {:rerun, "source-run", ^opts}} = Intent.decode(intent)
  end

  test "rejects unknown options and tampered payloads" do
    assert {:error, :invalid_run_submission_intent} =
             Intent.new(:asset, "asset-id", secret_callback: fn -> :unsafe end)

    assert {:error, :invalid_run_submission_intent} =
             Intent.decode(%{
               "format" => "favn.run_submission.intent.v1",
               "payload" =>
                 ~s({"format":"json-v1","value":{"__type__":"atom","value":"new_atom"}})
             })
  end

  test "rejects oversized intent before it reaches persistence" do
    assert {:error, :invalid_run_submission_intent} =
             Intent.new(:asset, "asset-id", metadata: %{value: String.duplicate("x", 240_000)})
  end
end
