defmodule FavnStoragePostgres.Bootstrap.RolePolicy do
  @moduledoc false

  alias FavnStoragePostgres.Bootstrap.Scram

  @type status :: %{
          role: String.t(),
          exists?: boolean(),
          safe?: boolean(),
          login?: boolean(),
          superuser?: boolean(),
          create_database?: boolean(),
          create_role?: boolean(),
          inherit?: boolean(),
          replication?: boolean(),
          bypass_rls?: boolean(),
          memberships: [String.t()]
        }

  @spec status(pid(), String.t()) :: {:ok, status()} | {:error, atom()}
  def status(connection, role) when is_pid(connection) and is_binary(role) do
    with {:ok, quoted_role} <- quote_identifier(role),
         {:ok, %{rows: rows}} <-
           Postgrex.query(
             connection,
             """
             SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit,
                    rolreplication, rolbypassrls
             FROM pg_catalog.pg_roles
             WHERE rolname = $1
             """,
             [role]
           ) do
      case rows do
        [] ->
          {:ok, missing_status(role)}

        [
          [
            login?,
            superuser?,
            create_database?,
            create_role?,
            inherit?,
            replication?,
            bypass_rls?
          ]
        ] ->
          with {:ok, memberships} <- memberships(connection, role) do
            safe? =
              login? and not superuser? and not create_database? and not create_role? and
                not inherit? and not replication? and not bypass_rls? and memberships == []

            {:ok,
             %{
               role: role,
               quoted_role: quoted_role,
               exists?: true,
               safe?: safe?,
               login?: login?,
               superuser?: superuser?,
               create_database?: create_database?,
               create_role?: create_role?,
               inherit?: inherit?,
               replication?: replication?,
               bypass_rls?: bypass_rls?,
               memberships: memberships
             }}
          end
      end
    else
      {:error, _reason} -> {:error, :role_inspection_failed}
    end
  end

  @spec ensure_password_role(pid(), String.t(), String.t()) ::
          {:ok, :exact | :created | :hardened} | {:error, atom()}
  def ensure_password_role(connection, role, password)
      when is_pid(connection) and is_binary(role) and is_binary(password) do
    with {:ok, status} <- status(connection, role),
         {:ok, created?} <- maybe_create_password_role(connection, status, role, password),
         {:ok, convergence} <- ensure_hardened(connection, role) do
      {:ok, if(created?, do: :created, else: convergence)}
    end
  end

  @spec ensure_hardened(pid(), String.t()) ::
          {:ok, :exact | :hardened} | {:error, atom()}
  def ensure_hardened(connection, role) when is_pid(connection) and is_binary(role) do
    with {:ok, status} <- status(connection, role),
         :ok <- require_existing_safe_superuser_state(status),
         {:ok, changed?} <- converge_attributes(connection, status, true),
         {:ok, membership_changed?} <- revoke_memberships(connection, status) do
      verify_hardened(connection, role, changed? or membership_changed?)
    else
      {:error, code} -> {:error, code}
    end
  end

  @spec ensure_safe_before_identity_mapping(pid(), String.t()) ::
          {:ok, :exact | :hardened} | {:error, atom()}
  def ensure_safe_before_identity_mapping(connection, role) do
    with {:ok, status} <- status(connection, role),
         :ok <- require_existing_safe_superuser_state(status),
         {:ok, changed?} <- converge_attributes(connection, status, false),
         {:ok, membership_changed?} <- revoke_memberships(connection, status) do
      verify_mapping_safe(connection, role, changed? or membership_changed?)
    else
      {:error, code} -> {:error, code}
    end
  end

  @spec current_role(pid()) :: {:ok, String.t()} | {:error, atom()}
  def current_role(connection) do
    case Postgrex.query(connection, "SELECT current_user", []) do
      {:ok, %{rows: [[role]]}} -> {:ok, role}
      {:error, _reason} -> {:error, :role_inspection_failed}
    end
  end

  @spec quote_identifier(String.t()) :: {:ok, String.t()} | {:error, :invalid_role}
  def quote_identifier(value) when is_binary(value) do
    if Regex.match?(~r/\A[a-z_][a-z0-9_]{0,62}\z/, value),
      do: {:ok, ~s("#{value}")},
      else: {:error, :invalid_role}
  end

  defp maybe_create_password_role(_connection, %{exists?: true}, _role, _password),
    do: {:ok, false}

  defp maybe_create_password_role(connection, %{exists?: false}, role, password) do
    with {:ok, iterations} <- scram_iterations(connection) do
      create_password_role(connection, role, Scram.verifier(password, iterations))
    end
  end

  defp create_password_role(connection, role, verifier) do
    result =
      Postgrex.transaction(connection, fn transaction ->
        with {:ok, _result} <-
               Postgrex.query(
                 transaction,
                 "SELECT pg_catalog.set_config('favn.bootstrap_role', $1, true)",
                 [role]
               ),
             {:ok, _result} <-
               Postgrex.query(
                 transaction,
                 "SELECT pg_catalog.set_config('favn.bootstrap_verifier', $1, true)",
                 [verifier]
               ),
             {:ok, _result} <-
               Postgrex.query(
                 transaction,
                 """
                 DO $favn$
                 BEGIN
                   EXECUTE pg_catalog.format(
                     'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L',
                     pg_catalog.current_setting('favn.bootstrap_role'),
                     pg_catalog.current_setting('favn.bootstrap_verifier')
                   );
                 END
                 $favn$
                 """,
                 []
               ) do
          :ok
        else
          {:error, reason} -> Postgrex.rollback(transaction, reason)
        end
      end)

    case result do
      {:ok, :ok} ->
        {:ok, true}

      {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} ->
        {:error, :role_creation_not_authorized}

      {:error, _reason} ->
        {:error, :unknown_outcome}
    end
  end

  defp scram_iterations(connection) do
    case Postgrex.query(
           connection,
           "SELECT setting::bigint, min_val::bigint, max_val::bigint FROM pg_catalog.pg_settings WHERE name = 'scram_iterations'",
           []
         ) do
      {:ok, %{rows: [[iterations, minimum, maximum]]}}
      when is_integer(iterations) and is_integer(minimum) and is_integer(maximum) and
             iterations >= minimum and iterations <= maximum and iterations > 0 ->
        {:ok, iterations}

      {:ok, _result} ->
        {:error, :scram_policy_unsupported}

      {:error, _reason} ->
        {:error, :scram_policy_inspection_failed}
    end
  end

  defp require_existing_safe_superuser_state(%{exists?: false}), do: {:error, :role_missing}
  defp require_existing_safe_superuser_state(%{superuser?: true}), do: {:error, :unsafe_authority}
  defp require_existing_safe_superuser_state(_status), do: :ok

  defp converge_attributes(connection, status, include_login?) do
    changes = [
      {include_login? and not status.login?, "LOGIN"},
      {status.create_database?, "NOCREATEDB"},
      {status.create_role?, "NOCREATEROLE"},
      {status.inherit?, "NOINHERIT"},
      {status.replication?, "NOREPLICATION"},
      {status.bypass_rls?, "NOBYPASSRLS"}
    ]

    Enum.reduce_while(changes, {:ok, false}, fn
      {false, _attribute}, result ->
        {:cont, result}

      {true, attribute}, {:ok, _changed?} ->
        case Postgrex.query(
               connection,
               "ALTER ROLE #{status.quoted_role} #{attribute}",
               []
             ) do
          {:ok, _result} ->
            {:cont, {:ok, true}}

          {:error, reason} ->
            {:halt, mutation_error(reason, :role_hardening_unsupported)}
        end
    end)
  end

  defp revoke_memberships(_connection, %{memberships: []}), do: {:ok, false}

  defp revoke_memberships(connection, status) do
    Enum.reduce_while(status.memberships, {:ok, false}, fn parent, {:ok, _changed?} ->
      with {:ok, quoted_parent} <- quote_identifier(parent),
           {:ok, _result} <-
             Postgrex.query(
               connection,
               "REVOKE #{quoted_parent} FROM #{status.quoted_role}",
               []
             ) do
        {:cont, {:ok, true}}
      else
        {:error, :invalid_role} ->
          {:halt, {:error, :role_membership_hardening_unsupported}}

        {:error, reason} ->
          {:halt, mutation_error(reason, :role_membership_hardening_unsupported)}
      end
    end)
  end

  defp verify_hardened(connection, role, changed?) do
    case status(connection, role) do
      {:ok, %{safe?: true}} ->
        if changed?, do: {:ok, :hardened}, else: {:ok, :exact}

      {:ok, _unsafe} ->
        {:error, :role_hardening_not_converged}

      {:error, _code} when changed? ->
        {:error, :unknown_outcome}

      {:error, code} ->
        {:error, code}
    end
  end

  defp verify_mapping_safe(connection, role, changed?) do
    case status(connection, role) do
      {:ok, status} ->
        if mapping_safe?(status) do
          if changed?, do: {:ok, :hardened}, else: {:ok, :exact}
        else
          {:error, :role_hardening_not_converged}
        end

      {:error, _code} when changed? ->
        {:error, :unknown_outcome}

      {:error, code} ->
        {:error, code}
    end
  end

  defp mapping_safe?(status) do
    status.exists? and not status.superuser? and not status.create_database? and
      not status.create_role? and not status.inherit? and not status.replication? and
      not status.bypass_rls? and status.memberships == []
  end

  defp mutation_error(%DBConnection.ConnectionError{}, _deterministic_code),
    do: {:error, :unknown_outcome}

  defp mutation_error(
         %Postgrex.Error{postgres: %{code: :insufficient_privilege}},
         :role_membership_hardening_unsupported
       ),
       do: {:error, :role_membership_hardening_not_authorized}

  defp mutation_error(%Postgrex.Error{}, deterministic_code),
    do: {:error, deterministic_code}

  defp mutation_error(_reason, _deterministic_code), do: {:error, :unknown_outcome}

  defp memberships(connection, role) do
    case Postgrex.query(
           connection,
           """
           SELECT parent.rolname
           FROM pg_catalog.pg_auth_members membership
           JOIN pg_catalog.pg_roles member ON member.oid = membership.member
           JOIN pg_catalog.pg_roles parent ON parent.oid = membership.roleid
           WHERE member.rolname = $1
           ORDER BY parent.rolname
           """,
           [role]
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [name] -> name end)}
      {:error, _reason} -> {:error, :role_inspection_failed}
    end
  end

  defp missing_status(role) do
    %{
      role: role,
      exists?: false,
      safe?: false,
      login?: false,
      superuser?: false,
      create_database?: false,
      create_role?: false,
      inherit?: false,
      replication?: false,
      bypass_rls?: false,
      memberships: []
    }
  end
end
