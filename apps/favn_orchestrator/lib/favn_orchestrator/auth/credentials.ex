defmodule FavnOrchestrator.Auth.Credentials do
  @moduledoc false

  @min_password_bytes 15
  @max_password_bytes 1_024
  @max_username_bytes 128
  @max_display_name_bytes 256
  @roles [:viewer, :operator, :admin]
  @username_pattern ~r/\A[A-Za-z0-9][A-Za-z0-9_.@-]*\z/

  @spec normalize_actor(String.t(), String.t(), [atom() | String.t()]) ::
          {:ok, map()} | {:error, term()}
  def normalize_actor(username, display_name, roles)
      when is_binary(username) and is_binary(display_name) and is_list(roles) do
    username = String.trim(username)
    display_name = String.trim(display_name)

    with :ok <- validate_username(username),
         :ok <- validate_display_name(display_name),
         {:ok, roles} <- normalize_roles(roles) do
      {:ok, %{username: username, display_name: display_name, roles: roles}}
    end
  end

  def normalize_actor(_username, _display_name, _roles), do: {:error, :invalid_actor}

  @spec normalize_roles([atom() | String.t()]) :: {:ok, [atom()]} | {:error, :invalid_roles}
  def normalize_roles(roles) when is_list(roles) and roles != [] do
    normalized = Enum.map(roles, &normalize_role/1)

    if Enum.all?(normalized, &(&1 in @roles)) do
      {:ok, Enum.uniq(normalized)}
    else
      {:error, :invalid_roles}
    end
  end

  def normalize_roles(_roles), do: {:error, :invalid_roles}

  @spec validate_password(String.t()) :: :ok | {:error, term()}
  def validate_password(password) when is_binary(password) do
    cond do
      byte_size(password) > @max_password_bytes -> {:error, :password_too_long}
      String.trim(password) == "" -> {:error, :password_blank}
      byte_size(password) < @min_password_bytes -> {:error, :password_too_short}
      true -> :ok
    end
  end

  def validate_password(_password), do: {:error, :invalid_password}

  @spec valid_login_input?(String.t(), String.t()) :: boolean()
  def valid_login_input?(username, password) when is_binary(username) and is_binary(password) do
    byte_size(username) in 1..@max_username_bytes and byte_size(password) <= @max_password_bytes
  end

  def valid_login_input?(_username, _password), do: false

  @spec hash_password(String.t()) :: map()
  def hash_password(password), do: %{password_hash: Argon2.hash_pwd_salt(password)}

  @spec verify_password(String.t(), term()) :: :ok | {:error, :invalid_credentials}
  def verify_password(password, %{password_hash: password_hash})
      when is_binary(password) and is_binary(password_hash) do
    if byte_size(password) <= @max_password_bytes and
         String.starts_with?(password_hash, "$argon2") and
         Argon2.verify_pass(password, password_hash) do
      :ok
    else
      {:error, :invalid_credentials}
    end
  rescue
    _exception -> {:error, :invalid_credentials}
  end

  def verify_password(_password, _credential), do: {:error, :invalid_credentials}

  @spec dummy_verify() :: {:error, :invalid_credentials}
  def dummy_verify do
    Argon2.no_user_verify()
    {:error, :invalid_credentials}
  end

  defp validate_username(username) do
    cond do
      username == "" -> {:error, :invalid_username}
      byte_size(username) > @max_username_bytes -> {:error, :username_too_long}
      not Regex.match?(@username_pattern, username) -> {:error, :invalid_username}
      true -> :ok
    end
  end

  defp validate_display_name(display_name) do
    cond do
      display_name == "" -> {:error, :invalid_display_name}
      byte_size(display_name) > @max_display_name_bytes -> {:error, :display_name_too_long}
      true -> :ok
    end
  end

  defp normalize_role(role) when role in @roles, do: role
  defp normalize_role("viewer"), do: :viewer
  defp normalize_role("operator"), do: :operator
  defp normalize_role("admin"), do: :admin
  defp normalize_role(_role), do: nil
end
