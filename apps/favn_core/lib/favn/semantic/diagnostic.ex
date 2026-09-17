defmodule Favn.Semantic.Diagnostic do
  @moduledoc "A bounded, source-attributed semantic compilation diagnostic."

  @enforce_keys [:code, :message]
  defstruct [:code, :message, :model, :metric, :file, :line]

  @type t :: %__MODULE__{
          code: atom(),
          message: String.t(),
          model: String.t() | nil,
          metric: String.t() | nil,
          file: String.t() | nil,
          line: pos_integer() | nil
        }

  @doc "Builds a diagnostic without retaining arbitrary native exception data."
  @spec new(atom(), String.t(), map()) :: t()
  def new(code, message, source \\ %{}) do
    %__MODULE__{
      code: code,
      message: String.slice(message, 0, 256),
      model: source[:model],
      metric: source[:metric],
      file: source[:file],
      line: source[:line]
    }
  end
end
