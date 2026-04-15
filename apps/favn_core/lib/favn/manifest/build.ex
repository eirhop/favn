defmodule Favn.Manifest.Build do
  @moduledoc """
  Compile/build output wrapper around a canonical runtime manifest.

  `%Favn.Manifest.Build{}` keeps build-only data (timestamps, diagnostics, and
  other compiler metadata) separate from the canonical runtime payload used for
  manifest hashing and pinning.
  """

  @type t :: %__MODULE__{
          manifest: map() | struct(),
          diagnostics: [term()],
          generated_at: DateTime.t() | nil,
          compiler_version: String.t() | nil,
          build_metadata: map()
        }

  defstruct manifest: %{},
            diagnostics: [],
            generated_at: nil,
            compiler_version: nil,
            build_metadata: %{}

  @type opt ::
          {:diagnostics, [term()]}
          | {:generated_at, DateTime.t()}
          | {:compiler_version, String.t()}
          | {:build_metadata, map()}

  @spec new(map() | struct(), [opt()]) :: t()
  def new(manifest, opts \\ []) when is_list(opts) do
    %__MODULE__{
      manifest: manifest,
      diagnostics: Keyword.get(opts, :diagnostics, []),
      generated_at: Keyword.get(opts, :generated_at, DateTime.utc_now()),
      compiler_version: Keyword.get(opts, :compiler_version),
      build_metadata: Keyword.get(opts, :build_metadata, %{})
    }
  end
end
