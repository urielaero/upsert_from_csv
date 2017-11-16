defmodule Mix.Tasks.Upsert do
  use Mix.Task

  def run(["update_m_l"]) do
    IO.puts UpsertFromCsv.process_postgres_m_l(UpsertFromCsv.read("assets/escuelas_2016_corregido_utf.csv"))
  end

  def run(["update_supervisores", n]) do
    #IO.puts "ALTER TABLE supervisores ADD COLUMN correoelectronico varchar(250);"
    #IO.puts "ALTER TABLE supervisores ADD COLUMN subnivel integer;"
    #IO.puts "ALTER TABLE supervisores ADD COLUMN subcontrol integer;"
    #IO.puts "ALTER TABLE supervisores ALTER COLUMN telefono TYPE varchar(250);"
    IO.puts "BEGIN;"
    IO.puts UpsertFromCsv.process_postgres_supervisores(UpsertFromCsv.read("files/supervisores/sup_#{n}.csv"))
    IO.puts "COMMIT;"
  end

  def run(["update_school_simple"]) do
    # without relationships...
    # like assets/act_escuelas_2016.csv;
    # only update without upsert
    IO.puts "BEGIN;"
    IO.puts "LOCK TABLE escuelas IN SHARE ROW EXCLUSIVE MODE;"
    IO.puts UpsertFromCsv.process_postgres_only_update(UpsertFromCsv.read("assets/act_escuelas_2016.csv"))
    IO.puts "COMMIT;"
  end

  def run(_args) do
    #update general data.
    # update with upsert/insert
    IO.puts "ALTER TABLE escuelas ALTER COLUMN correoelectronico TYPE varchar(250);"
    IO.puts "UPDATE escuelas set status = 2;"
    IO.puts "UPDATE escuelas set status = 1 where cct ~ '^..BB';"
    IO.puts "BEGIN;"
    IO.puts "LOCK TABLE escuelas IN SHARE ROW EXCLUSIVE MODE;"
    #IO.puts UpsertFromCsv.process_postgres(UpsertFromCsv.read("assets/escuelas_2016_corregido_utf.csv"))
    IO.puts "COMMIT;"
  end
end
