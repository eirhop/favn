defmodule FavnReferenceWorkload.LandingFiles do
  @moduledoc "Fixed local landing-path writer for the temporary CRM example."

  @landing_dir ".data/generic_crm/landing"

  @spec landing_dir() :: String.t()
  def landing_dir, do: Path.expand(@landing_dir)

  @spec write_seed!(map()) :: :ok
  def write_seed!(seed) do
    write_json!("crm_seed.json", seed)
  end

  @spec write_entity!(atom(), [map()]) :: :ok
  def write_entity!(entity, rows) when is_atom(entity) and is_list(rows) do
    write_json!("#{entity}.json", rows)
  end

  @spec seed_path() :: String.t()
  def seed_path, do: Path.join(landing_dir(), "crm_seed.json")

  @spec entity_path(atom()) :: String.t()
  def entity_path(entity), do: Path.join(landing_dir(), "#{entity}.json")

  defp write_json!(filename, value) do
    File.mkdir_p!(landing_dir())
    path = Path.join(landing_dir(), filename)
    File.write!(path, Jason.encode!(value))
    :ok
  end
end
