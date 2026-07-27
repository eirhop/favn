defmodule Favn.DistributionTLSTest do
  use ExUnit.Case, async: false

  test "requires mutual peer verification and readable credentials on both sides" do
    credential = temporary_file("credential.pem", "test")
    options_file = temporary_options(credential)

    assert {:ok, %{transport: :tls, mutual_tls?: true}} =
             Favn.DistributionTLS.validate(%{
               "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE" => options_file
             })

    insecure =
      tls_options(credential)
      |> Keyword.update!(:server, &Keyword.put(&1, :fail_if_no_peer_cert, false))

    File.write!(
      options_file,
      IO.iodata_to_binary(:io_lib.format("~p.~n", [insecure]))
    )

    assert {:error,
            {:invalid_env, "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE", "fail_if_no_peer_cert=true"}} =
             Favn.DistributionTLS.validate(%{
               "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE" => options_file
             })
  end

  test "the running VM must actually use TLS distribution" do
    credential = temporary_file("credential.pem", "test")
    options_file = temporary_options(credential)

    assert {:error, {:invalid_env, "ERL_FLAGS", "-proto_dist inet_tls"}} =
             Favn.DistributionTLS.validate_running_transport(%{
               "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE" => options_file
             })
  end

  @tag timeout: 60_000
  test "mutual TLS peers connect while a same-cookie plaintext peer is rejected" do
    directory = temporary_directory()
    host = ~c"127.0.0.1"
    control_name = "favn_tls_control@" <> List.to_string(host)
    {control_certificate, runner_certificate} = certificate_configurations(host)

    assert :public_key.pkix_verify_hostname(control_certificate[:cert], [
             {:ip, {127, 0, 0, 1}}
           ])

    control_options = tls_peer_options(directory, "control", control_certificate)
    runner_options = tls_peer_options(directory, "runner", runner_certificate)
    cookie = String.to_atom("favn_tls_test_cookie_#{System.unique_integer([:positive])}")

    control = start_peer("favn_tls_control", host, cookie, control_options)
    runner = start_peer("undefined", host, cookie, runner_options)

    on_exit(fn ->
      stop_peer(runner)
      stop_peer(control)
    end)

    control_node = :peer.call(control, :erlang, :node, [], 10_000)
    assert Atom.to_string(control_node) == control_name
    assert :peer.call(control, :erlang, :get_cookie, [], 10_000) == cookie
    assert :peer.call(runner, :erlang, :get_cookie, [], 10_000) == cookie

    assert {:ok, [[~c"inet_tls"]]} =
             :peer.call(control, :init, :get_argument, [:proto_dist], 10_000)

    assert {:ok, [[~c"inet_tls"]]} =
             :peer.call(runner, :init, :get_argument, [:proto_dist], 10_000)

    assert true = :peer.call(runner, :net_kernel, :connect_node, [control_node], 10_000)

    dynamic_runner_node = :peer.call(runner, :erlang, :node, [], 10_000)

    refute Atom.to_string(dynamic_runner_node) in [
             "undefined",
             "undefined@" <> List.to_string(host)
           ]

    assert dynamic_runner_node in :peer.call(control, :erlang, :nodes, [[:hidden]], 10_000)

    plaintext = start_peer("undefined", host, cookie, nil)
    on_exit(fn -> stop_peer(plaintext) end)

    refute :peer.call(plaintext, :net_kernel, :connect_node, [control_node], 10_000)
  end

  defp temporary_options(credential) do
    path = temporary_file("ssl_dist.config", "")
    File.write!(path, IO.iodata_to_binary(:io_lib.format("~p.~n", [tls_options(credential)])))
    path
  end

  defp tls_options(credential) do
    common = [
      certfile: String.to_charlist(credential),
      keyfile: String.to_charlist(credential),
      cacertfile: String.to_charlist(credential),
      verify: :verify_peer
    ]

    [server: Keyword.put(common, :fail_if_no_peer_cert, true), client: common]
  end

  defp temporary_file(name, bytes) do
    directory = temporary_directory()
    path = Path.join(directory, name)
    File.write!(path, bytes)
    path
  end

  defp temporary_directory do
    directory =
      Path.join(
        System.tmp_dir!(),
        "favn-distribution-tls-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf(directory) end)
    directory
  end

  defp certificate_configurations(host) do
    root =
      :public_key.pkix_test_root_cert(~c"Favn BEAM Distribution Test CA",
        digest: :sha256,
        key: {:rsa, 2_048, 65_537}
      )

    peer_options = [
      digest: :sha256,
      key: {:rsa, 2_048, 65_537},
      extensions: [
        {:Extension, {2, 5, 29, 17}, false, [{:dNSName, host}, {:iPAddress, <<127, 0, 0, 1>>}]}
      ]
    ]

    {
      :public_key.pkix_test_data(%{root: root, peer: peer_options}),
      :public_key.pkix_test_data(%{root: root, peer: peer_options})
    }
  end

  defp tls_peer_options(directory, name, certificate_config) do
    path = Path.join(directory, "#{name}.ssl_dist.config")

    certificate_path = Path.join(directory, "#{name}.pem")
    key_path = Path.join(directory, "#{name}.key")
    ca_path = Path.join(directory, "#{name}.ca.pem")

    File.write!(
      certificate_path,
      :public_key.pem_encode([{:Certificate, certificate_config[:cert], :not_encrypted}])
    )

    {key_type, key_der} = certificate_config[:key]
    File.write!(key_path, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))

    File.write!(
      ca_path,
      certificate_config[:cacerts]
      |> Enum.map(&{:Certificate, &1, :not_encrypted})
      |> :public_key.pem_encode()
    )

    common = [
      certfile: String.to_charlist(certificate_path),
      keyfile: String.to_charlist(key_path),
      cacertfile: String.to_charlist(ca_path),
      verify: :verify_peer
    ]

    options = [
      server: Keyword.put(common, :fail_if_no_peer_cert, true),
      client: common
    ]

    File.write!(path, IO.iodata_to_binary(:io_lib.format("~p.~n", [options])))
    path
  end

  defp start_peer(name, host, cookie, tls_options) do
    args =
      [
        ~c"-setcookie",
        cookie |> Atom.to_string() |> String.to_charlist()
      ] ++
        if tls_options do
          [
            ~c"-proto_dist",
            ~c"inet_tls",
            ~c"-ssl_dist_optfile",
            String.to_charlist(tls_options)
          ]
        else
          []
        end

    assert {:ok, peer, _node} =
             :peer.start_link(%{
               name: String.to_charlist(name),
               host: host,
               longnames: true,
               connection: :standard_io,
               args: args,
               wait_boot: 15_000
             })

    peer
  end

  defp stop_peer(peer) do
    if is_pid(peer) and Process.alive?(peer), do: :peer.stop(peer)
  catch
    :exit, _reason -> :ok
  end
end
