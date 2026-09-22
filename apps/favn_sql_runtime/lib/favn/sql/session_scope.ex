defmodule Favn.SQL.SessionScope do
  @moduledoc false
  alias Favn.SQL.{Client, Deadline, Error, SessionPool}

  @spec run(atom(), keyword(), (term() -> term())) :: term()
  def run(connection, opts, fun) do
    parent = self()
    ref = make_ref()
    {guard, monitor} = spawn_monitor(fn -> supervise(parent, ref, connection, opts, fun) end)

    receive do
      {^ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^guard, reason} ->
        {:error, failure(:admitted, reason)}
    end
  end

  defp supervise(parent, ref, connection, opts, fun) do
    parent_monitor = Process.monitor(parent)
    guard = self()
    deadline = Keyword.fetch!(opts, :deadline)
    {worker, monitor} = spawn_monitor(fn -> execute(guard, ref, connection, opts, fun) end)
    await(parent, parent_monitor, ref, worker, monitor, deadline, :acquiring, nil, [])
  end

  defp execute(guard, ref, connection, opts, fun) do
    Process.put(__MODULE__, {guard, ref})
    prepare = Keyword.get(opts, :prepare, &{:ok, &1})

    case Client.connect(connection, Keyword.delete(opts, :prepare)) do
      {:ok, session} ->
        {:links, links} = Process.info(self(), :links)
        send(guard, {ref, :resources, Enum.filter(links, &is_pid/1)})

        if session.pool_checkout,
          do: SessionPool.mark_discard(session.pool_checkout.token, :session_scope)

        try do
          result =
            with {:ok, context} <- prepare.(session) do
              send(guard, {ref, :admit})

              receive do
                {^ref, :admitted} -> fun.(context)
              end
            end

          send(guard, {ref, :completed, result})
        after
          Client.disconnect(session)
        end

      error ->
        send(guard, {ref, :completed, error})
    end
  end

  defp await(parent, parent_monitor, ref, worker, monitor, deadline, phase, completed, resources) do
    receive do
      {^ref, :resources, pids} ->
        resources = Enum.map(pids, &{&1, Process.monitor(&1)})
        await(parent, parent_monitor, ref, worker, monitor, deadline, phase, completed, resources)

      {^ref, :operation_timeout, error} ->
        stop(worker, monitor, resources)

        send(
          parent,
          {ref,
           completed || {:error, failure(phase, {{Client, :nested_operation_timeout}, error})}}
        )

      {^ref, :admit} ->
        if Deadline.expired?(deadline) do
          finish(parent, ref, worker, monitor, phase, completed, resources)
        else
          send(worker, {ref, :admitted})

          await(
            parent,
            parent_monitor,
            ref,
            worker,
            monitor,
            deadline,
            :admitted,
            completed,
            resources
          )
        end

      {^ref, :completed, result} ->
        result = phase_result(result, phase)

        await(
          parent,
          parent_monitor,
          ref,
          worker,
          monitor,
          deadline,
          :completed,
          result,
          resources
        )

      {:DOWN, ^monitor, :process, ^worker, reason} ->
        reap(resources)
        send(parent, {ref, completed || {:error, failure(phase, reason)}})

      {:DOWN, ^parent_monitor, :process, ^parent, _} ->
        stop(worker, monitor, resources)
    after
      Deadline.remaining_ms(deadline) ->
        finish(parent, ref, worker, monitor, phase, completed, resources)
    end
  end

  defp finish(parent, ref, worker, monitor, phase, completed, resources) do
    stop(worker, monitor, resources)
    send(parent, {ref, completed || {:error, failure(phase, :timeout)}})
  end

  defp stop(worker, monitor, resources) do
    linked =
      case Process.info(worker, :links) do
        {:links, links} -> for pid <- links, is_pid(pid), do: {pid, Process.monitor(pid)}
        nil -> []
      end

    resources = resources ++ linked
    Process.exit(worker, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^worker, _} -> :ok
    end

    reap(resources)
  end

  defp reap(resources) do
    Enum.each(resources, fn {pid, _} -> Process.exit(pid, :kill) end)

    Enum.each(resources, fn {pid, ref} ->
      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      end
    end)
  end

  defp phase_result({support, {:error, %Error{} = error}}, phase)
       when support in [:supported, :unsupported],
       do: {support, phase_result({:error, error}, phase)}

  defp phase_result({:error, %Error{} = error}, phase),
    do: {:error, %{error | details: Map.put(error.details, :session_phase, phase)}}

  defp phase_result(result, _phase), do: result

  defp failure(phase, {{Client, :nested_operation_timeout}, %Error{} = error}) do
    %{
      error
      | details:
          Map.merge(error.details, %{
            session_phase: phase,
            unknown_outcome?: phase != :acquiring,
            transaction_outcome: if(phase == :acquiring, do: :not_started, else: :unknown)
          })
    }
  end

  defp failure(phase, reason) do
    %Error{
      type: if(reason == :timeout, do: :operation_timeout, else: :execution_error),
      operation: if(phase == :acquiring, do: :connect, else: :transaction),
      message: "SQL session scope interrupted",
      retryable?: false,
      details: %{
        session_phase: phase,
        unknown_outcome?: phase != :acquiring,
        transaction_outcome: if(phase == :acquiring, do: :not_started, else: :unknown),
        reason: if(reason == :timeout, do: "deadline", else: "owner_exit")
      }
    }
  end
end
