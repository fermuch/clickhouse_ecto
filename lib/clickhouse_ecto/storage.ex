defmodule ClickhouseEcto.Storage do
  @behaviour Ecto.Adapter.Storage

  def storage_up(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    opts = Keyword.put(opts, :database, nil)

    cluster = Keyword.get(opts, :cluster)

    opts = Keyword.put(opts, :database, nil)

    command =
      if is_nil(cluster) do
        ~s[CREATE DATABASE IF NOT EXISTS "#{database}"]
      else
        ~s[CREATE DATABASE IF NOT EXISTS "#{database}" ON CLUSTER '#{cluster}']
      end

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, %{code: :database_already_exists}} ->
        {:error, :already_up}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  @doc false
  def storage_down(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    cluster = Keyword.get(opts, :cluster)

    command =
      if is_nil(cluster) do
        ~s[DROP DATABASE "#{database}"]
      else
        ~s[DROP DATABASE "#{database}" ON CLUSTER '#{cluster}']
      end

    opts = Keyword.put(opts, :database, nil)

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, %{code: :database_does_not_exists}} ->
        {:error, :already_down}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  def storage_status(opts) do
    command = ~s[SELECT 1]

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, %{code: :database_does_not_exists}} ->
        {:error, :already_down}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  defp run_query(sql, opts) do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:clickhousex)

    opts =
      opts
      |> Keyword.drop([:name, :log, :pool, :pool_size])
      |> Keyword.put(:backoff_type, :stop)
      |> Keyword.put(:max_restarts, 0)

    {:ok, pid} = Task.Supervisor.start_link()

    task =
      Task.Supervisor.async_nolink(pid, fn ->
        # {:ok, conn} = DBConnection.start_link(Clickhousex.Protocol, opts)
        {:ok, conn} = Clickhousex.start_link(opts)
        value = ClickhouseEcto.Connection.execute(conn, sql, [], opts)
        GenServer.stop(conn)
        value
      end)

    timeout = Keyword.get(opts, :timeout, 15_000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, {:ok, result}} ->
        {:ok, result}

      {:ok, {:error, error}} ->
        {:error, error}

      {:exit, {%{__struct__: struct} = error, _}}
      when struct in [DBConnection.Error] ->
        {:error, error}

      {:exit, reason} ->
        {:error, RuntimeError.exception(Exception.format_exit(reason))}

      nil ->
        {:error, RuntimeError.exception("command timed out")}
    end
  end
end
