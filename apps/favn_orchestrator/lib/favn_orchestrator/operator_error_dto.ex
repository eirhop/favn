defmodule FavnOrchestrator.OperatorErrorDTO do
  @moduledoc """
  Browser-safe operator error DTOs exposed by the orchestrator boundary.

  Raw storage, runner, adapter, and plugin reasons must stay server-side. This
  DTO gives thin UI clients stable labels plus optional correlation IDs without
  requiring them to inspect backend terms.
  """

  @type t :: %__MODULE__{
          code: atom(),
          title: String.t(),
          detail: String.t(),
          retryable?: boolean() | nil,
          correlation_id: String.t() | nil
        }

  @enforce_keys [:code, :title, :detail]
  defstruct [:code, :title, :detail, :retryable?, :correlation_id]

  @doc "Builds a safe DTO for catalogue/detail load failures."
  @spec load(term()) :: t()
  def load(:not_found), do: not_found("Not found")
  def load(:schedule_not_found), do: not_found("Schedule was not found.")

  def load(:active_manifest_not_set) do
    %__MODULE__{
      code: :active_manifest_not_set,
      title: "Active manifest not set",
      detail: "Active manifest not set"
    }
  end

  def load(reason), do: backend_unavailable(reason)

  @doc "Builds a safe DTO for schedule occurrence preview failures."
  @spec schedule_occurrences(term()) :: t()
  def schedule_occurrences(reason) do
    %__MODULE__{
      code: reason_code(reason, :schedule_occurrences_unavailable),
      title: "Could not load schedule occurrences.",
      detail: "Could not load schedule occurrences.",
      retryable?: true,
      correlation_id: correlation_id(reason)
    }
  end

  @doc "Builds a safe DTO for schedule activation failures."
  @spec schedule_activation(term()) :: t()
  def schedule_activation(:forbidden),
    do: forbidden("Operator role required to change schedules.")

  def schedule_activation(:unauthenticated),
    do: unauthenticated("Sign in again to change schedules.")

  def schedule_activation(:schedule_not_found), do: not_found("Schedule was not found.")
  def schedule_activation(:not_found), do: not_found("Schedule was not found.")

  def schedule_activation(reason) when reason in [:runtime_starting, :runtime_draining] do
    %__MODULE__{
      code: reason,
      title: "Control plane is not accepting changes.",
      detail: "Control plane is starting or draining. Try again after it becomes ready.",
      retryable?: true
    }
  end

  def schedule_activation(reason) do
    %__MODULE__{
      code: reason_code(reason, :schedule_update_failed),
      title: "Could not update schedule.",
      detail: "Could not update schedule. Try again later.",
      retryable?: true,
      correlation_id: correlation_id(reason)
    }
  end

  @doc "Builds a safe DTO for run cancellation failures."
  @spec run_cancel(term()) :: t()
  def run_cancel(:forbidden), do: forbidden("Operator role required to cancel runs.")
  def run_cancel(:unauthenticated), do: unauthenticated("Sign in again to cancel runs.")
  def run_cancel(:not_found), do: not_found("Run was not found.")
  def run_cancel(:run_already_terminal), do: conflict("Run is already finished.")

  def run_cancel(:backfill_parent_cancel_not_supported) do
    %__MODULE__{
      code: :backfill_parent_cancel_not_supported,
      title: "Backfill parent cancellation is not supported yet.",
      detail:
        "Backfill parent cancellation is not supported yet. Cancel active window runs individually.",
      retryable?: false
    }
  end

  def run_cancel(reason) do
    %__MODULE__{
      code: reason_code(reason, :run_cancel_failed),
      title: "Run cancellation failed.",
      detail: "Run cancellation failed. Try again later.",
      retryable?: retryable?(reason),
      correlation_id: correlation_id(reason)
    }
  end

  @doc "Builds a safe DTO for arbitrary run failure details."
  @spec run_failure_detail(term()) :: t()
  def run_failure_detail(reason) do
    %__MODULE__{
      code: reason_code(reason, :run_failed),
      title: "Run failed.",
      detail: "Run failed. Check server logs for details.",
      retryable?: retryable?(reason),
      correlation_id: correlation_id(reason)
    }
  end

  defp backend_unavailable(reason) do
    %__MODULE__{
      code: reason_code(reason, :backend_unavailable),
      title: "Backend unavailable",
      detail: "Backend unavailable. Try again later.",
      retryable?: true,
      correlation_id: correlation_id(reason)
    }
  end

  defp not_found(detail),
    do: %__MODULE__{code: :not_found, title: detail, detail: detail, retryable?: false}

  defp forbidden(detail),
    do: %__MODULE__{code: :forbidden, title: detail, detail: detail, retryable?: false}

  defp unauthenticated(detail),
    do: %__MODULE__{code: :unauthenticated, title: detail, detail: detail, retryable?: true}

  defp conflict(detail),
    do: %__MODULE__{code: :conflict, title: detail, detail: detail, retryable?: false}

  defp reason_code(%{code: code}, _default) when is_atom(code), do: code
  defp reason_code(%{type: type}, _default) when is_atom(type), do: type
  defp reason_code(%{"code" => code}, _default) when is_atom(code), do: code
  defp reason_code(%{"type" => type}, _default) when is_atom(type), do: type
  defp reason_code(_reason, default), do: default

  defp retryable?(%{retryable?: retryable}) when is_boolean(retryable), do: retryable
  defp retryable?(%{"retryable?" => retryable}) when is_boolean(retryable), do: retryable
  defp retryable?(_reason), do: nil

  defp correlation_id(%{correlation_id: id}) when is_binary(id), do: id
  defp correlation_id(%{"correlation_id" => id}) when is_binary(id), do: id
  defp correlation_id(_reason), do: nil
end
