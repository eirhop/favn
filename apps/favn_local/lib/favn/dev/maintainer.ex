defmodule Favn.Dev.Maintainer do
  @moduledoc """
  Prepares and runs the explicit non-production maintainer development mode.

  The consuming project must load Favn from the checkout selected by
  `FAVN_CHECKOUT`. The checkout builds an unpublished local control plane while
  the consumer project continues to own its runner, manifest, Compose file,
  PostgreSQL data, and additional services.
  """

  alias Favn.Dev.Build.ControlPlane
  alias Favn.Dev.ComposeLifecycle
  alias Favn.Dev.Maintainer.{Candidate, Source}

  @doc "Builds or reuses the selected checkout and applies it to local development."
  @spec run(keyword()) :: :ok | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with :ok <- require_development_environment(),
         {:ok, source} <- Source.resolve(opts),
         :ok <- progress(opts, "Preparing local Favn checkout #{source.checkout}"),
         {:ok, build} <- build_candidate(source, opts),
         {:ok, candidate} <- Candidate.from_build(build, source),
         :ok <- progress(opts, candidate_message(candidate, build.image_status)) do
      run_lifecycle(candidate, opts)
    end
  end

  defp require_development_environment do
    if Mix.env() in [:dev, :test],
      do: :ok,
      else: {:error, {:maintainer_environment_forbidden, Mix.env()}}
  end

  defp build_candidate(source, opts) do
    case Keyword.get(opts, :maintainer_build_fun) do
      fun when is_function(fun, 2) ->
        if Mix.env() == :test,
          do: fun.(source, opts),
          else: {:error, :maintainer_build_injection_not_allowed}

      _other ->
        ControlPlane.run_from_checkout(source.checkout, load: true)
    end
  end

  defp candidate_message(candidate, image_status) do
    dirty = if candidate.checkout_dirty, do: " dirty", else: " clean"

    "Maintainer control plane #{image_status}: #{candidate.control_plane_build_id}; " <>
      "checkout #{candidate.checkout_revision}#{dirty}"
  end

  defp run_lifecycle(candidate, opts) do
    case Keyword.get(opts, :maintainer_lifecycle_fun) do
      fun when is_function(fun, 2) ->
        if Mix.env() == :test,
          do: fun.(candidate, opts),
          else: {:error, :maintainer_lifecycle_injection_not_allowed}

      _other ->
        ComposeLifecycle.maintainer_dev(candidate, opts)
    end
  end

  defp progress(opts, message), do: Keyword.get(opts, :progress_fun, fn _ -> :ok end).(message)
end
