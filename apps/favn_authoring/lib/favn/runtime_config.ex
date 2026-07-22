defmodule Favn.RuntimeConfig do
  @moduledoc """
  Defines reusable, unresolved runtime configuration bundles for assets.

  Bundle modules belong in ordinary authoring code. Calling a generated bundle
  function returns references and provenance only; environment values are
  resolved later by the runner for the selected asset manifest.

  ## Example

      defmodule MyApp.RuntimeConfigs do
        use Favn.RuntimeConfig

        bundle :github,
          url: env!("GITHUB_URL"),
          username: env!("GITHUB_USERNAME"),
          api_key: secret_env!("GITHUB_API_KEY")
      end
  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      import Favn.RuntimeConfig,
        only: [bundle: 2, env!: 1, env!: 2, secret_env!: 1, secret_env!: 2]
    end
  end

  @doc """
  Defines a named runtime configuration bundle function.

  The bundle name is also the scope under `ctx.runtime_config`.
  """
  defmacro bundle(name, fields) do
    caller = __CALLER__
    file = Favn.DSL.Compiler.normalize_file(caller.file)
    doc = "Returns the unresolved #{name} runtime configuration bundle."

    unless is_atom(name) do
      raise ArgumentError,
            "runtime config bundle name must be an atom, got: #{Macro.to_string(name)}"
    end

    quote do
      @doc unquote(doc)
      @spec unquote(name)() :: Favn.RuntimeConfig.Bundle.t()
      def unquote(name)() do
        Favn.RuntimeConfig.Bundle.new!(unquote(name), unquote(fields),
          module: __MODULE__,
          file: unquote(file),
          line: unquote(caller.line)
        )
      end
    end
  end

  @doc "Builds an environment-variable reference for a bundle field."
  defmacro env!(key, opts \\ []) do
    quote do
      Favn.RuntimeConfig.Ref.env!(unquote(key), unquote(opts))
    end
  end

  @doc "Builds a secret environment-variable reference for a bundle field."
  defmacro secret_env!(key, opts \\ []) do
    quote do
      Favn.RuntimeConfig.Ref.secret_env!(unquote(key), unquote(opts))
    end
  end
end
