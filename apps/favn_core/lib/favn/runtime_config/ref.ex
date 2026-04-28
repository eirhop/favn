defmodule Favn.RuntimeConfig.Ref do
  @moduledoc """
  Manifest-safe reference to a runtime-supplied configuration value.

  The reference describes where a value must be resolved from at runtime. It
  never contains the resolved value itself.

  Use this module when runtime values are part of the Favn contract and should
  be visible as requirements in manifests, diagnostics, and the local UI without
  embedding the actual value.

  ## Asset Example

      defmodule MyApp.SourceOrders do
        use Favn.Asset

        source_config :source_system,
          segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
          token: secret_env!("SOURCE_SYSTEM_TOKEN")

        def asset(ctx) do
          segment_id = ctx.config.source_system.segment_id
          token = ctx.config.source_system.token

          MyApp.Client.fetch_orders(segment_id, token)
          :ok
        end
      end

  `source_config/2`, `env!/1`, and `secret_env!/1` are imported by
  `use Favn.Asset`. The manifest stores the environment variable names and
  secret flags, not the resolved values.

  ## Connection Example

      config :favn,
        connections: [
          warehouse: [
            database: Favn.RuntimeConfig.Ref.env!("WAREHOUSE_DB_PATH"),
            password: Favn.RuntimeConfig.Ref.secret_env!("WAREHOUSE_PASSWORD")
          ]
        ]

  The connection loader resolves these references before adapter connection. A
  missing required environment variable produces a structured Favn error such as
  `missing_env WAREHOUSE_PASSWORD`.

  ## Secret Handling

  Secret refs are available to runner code after resolution, but diagnostics and
  UI-facing payloads should only show presence/redaction metadata. Do not return
  secret values from asset metadata.
  """

  @enforce_keys [:provider, :key]
  defstruct [:provider, :key, secret?: false, required?: true]

  @type provider :: :env

  @type t :: %__MODULE__{
          provider: provider(),
          key: String.t(),
          secret?: boolean(),
          required?: boolean()
        }

  @spec env!(String.t(), keyword()) :: t()
  @doc """
  Builds a required environment-variable reference.

  Pass `secret?: true` when the value must be redacted anywhere it is displayed.
  """
  def env!(key, opts \\ []) when is_binary(key) and key != "" do
    %__MODULE__{
      provider: :env,
      key: key,
      secret?: Keyword.get(opts, :secret?, false),
      required?: Keyword.get(opts, :required?, true)
    }
  end

  @spec secret_env!(String.t()) :: t()
  @doc """
  Builds a required secret environment-variable reference.
  """
  def secret_env!(key) when is_binary(key) and key != "", do: env!(key, secret?: true)

  @spec validate!(t()) :: t()
  @doc false
  def validate!(
        %__MODULE__{provider: :env, key: key, secret?: secret?, required?: required?} = ref
      )
      when is_binary(key) and key != "" and is_boolean(secret?) and is_boolean(required?) do
    ref
  end

  def validate!(%__MODULE__{} = ref) do
    raise ArgumentError, "invalid runtime config ref: #{inspect(ref)}"
  end
end
