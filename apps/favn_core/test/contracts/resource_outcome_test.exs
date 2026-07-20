defmodule Favn.Contracts.ResourceOutcomeTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.ResourceOutcome
  alias Favn.Resource.Ref

  test "normalizes redaction-safe resource outcomes" do
    assert {:ok,
            %ResourceOutcome{
              resource: %Ref{kind: :connection, name: "warehouse"},
              status: :failure,
              category: "transport",
              safe_to_repeat?: true
            }} =
             ResourceOutcome.new(%{
               "resource" => %{"kind" => "connection", "name" => "warehouse"},
               "status" => "failure",
               "category" => "transport",
               "safe_to_repeat?" => true
             })
  end

  test "rejects unknown kinds and statuses" do
    assert {:error, {:invalid_resource_kind, :database}} =
             ResourceOutcome.new(
               resource: %{kind: :database, name: :warehouse},
               status: :failure
             )

    assert {:error, {:invalid_resource_outcome_status, :unknown}} =
             ResourceOutcome.new(
               resource: %{kind: :connection, name: :warehouse},
               status: :unknown
             )
  end
end
