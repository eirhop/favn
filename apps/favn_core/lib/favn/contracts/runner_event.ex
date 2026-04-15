defmodule Favn.Contracts.RunnerEvent do
  @moduledoc """
  Runner event contract emitted during manifest-pinned execution.
  """

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          event_type: atom() | String.t(),
          occurred_at: DateTime.t() | nil,
          payload: map()
        }

  defstruct run_id: nil,
            manifest_version_id: nil,
            manifest_content_hash: nil,
            event_type: nil,
            occurred_at: nil,
            payload: %{}
end
