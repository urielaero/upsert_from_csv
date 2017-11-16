defmodule UpsertFromCsv do

  @ignoring ~w(id igno1 igno2 igno3 igno4)
  @integers ~w(localidad municipio entidad turno tipo nivel subnivel servicio modalidad control subcontrol sostenimiento status total_alumnos total_grupos total_personal)
  # sostenimie -> sostenimiento
  # correoele -> correoelectronico
  # ALTER TABLE escuelas ADD COLUMN callepost varchar(250);
  # ALTER TABLE escuelas ADD COLUMN numint varchar(250);
  # ALTER TABLE escuelas ADD COLUMN numext varchar(250);
  # ALTER TABLE escuelas ADD COLUMN cct_zona varchar(250);
  # ALTER TABLE escuelas ADD COLUMN total_alumnos integer;
  # ALTER TABLE escuelas ADD COLUMN total_grupos integer;
  # ALTER TABLE escuelas ADD COLUMN total_personal integer;
  # ALTER TABLE escuelas ADD COLUMN director varchar(250);
  # ALTER TABLE escuelas ADD COLUMN zonaescola varchar(250);















  def read(file) do
    File.stream!(file) |> CSV.decode(headers: true)
  end

  def process_postgres_only_update(stream) do
    #upsert = false
    Enum.reduce(stream, "", &process_row_only_update/2)
  end

  def process_postgres(stream) do
    Enum.reduce(stream, "", &process_row/2)
  end

  def process_postgres_m_l(stream) do
    # run function for update municipio and localidad
    Enum.reduce(stream, "", &process_row_m_l/2)
  end

  def process_postgres_supervisores(stream) do
    Enum.reduce(stream, "", &process_row_supervisores/2)
  end

  def process_row_m_l(row, acc) do
    acc_n = if acc == "", do: acc, else: "#{acc}\r\n"
    municipio = if row["municipio"] != "NA", do: row["municipio"], else: 0
    entidad = if row["entidad"] != "NA", do: row["entidad"], else: 0
    localidad = if row["localidad"] != "NA", do: row["localidad"], else: 0
    "#{acc_n} SELECT update_m_l('#{row["cct"]}', #{municipio}, #{entidad}, #{localidad});"
  end

  defp process_row_only_update(row, acc) do
    acc_n = if acc == "", do: acc, else: "#{acc}\r\n"
    update = "UPDATE escuelas SET "
    values = Enum.reduce(row, "", &format_update_sql/2)
    up = "#{update} #{values} where cct='#{row["cct"]}';"
    "#{acc_n} #{up}"
  end

  defp process_row(row, acc) do
    #IO.inspect row
    acc_n = if acc == "", do: acc, else: "#{acc}\r\n"
    update = "UPDATE escuelas SET "
    values = Enum.reduce(row, "", &format_update_sql/2)
    upsert = "#{update} #{values} where cct='#{row["cct"]}' RETURNING *"
    insert = format_insert_sql(row)
    "#{acc_n} WITH upsert as (#{upsert}) #{insert}"
  end

  def process_row_supervisores(row, acc) do
    acc_n = if acc == "", do: acc, else: "#{acc}\r\n"
    update = "UPDATE supervisores SET "
    values = Enum.reduce(row, "", &format_update_sql/2)
    upsert = "#{update} #{values} where cct='#{row["cct"]}' RETURNING *"
    insert = format_insert_sql(row, "supervisores")
    "#{acc_n} WITH upsert as (#{upsert}) #{insert}"
  end

  def format_update_sql({name, _value}, acc) when name in @ignoring do
    acc
  end

  def format_update_sql({_name, "NA"}, acc) do
    acc
  end

  def format_update_sql({_name, "0NA"}, acc) do
    acc
  end

  def format_update_sql({_name, ""}, acc) do
    acc
  end

  def format_update_sql({name, value}, acc) do
    val = "#{name}=#{unquoted_postgres(name, value)}"
    if acc != "", do: "#{acc},#{val}", else: val
  end

  def format_name_insert_sql({name, _value}, acc) when name in @ignoring do
    acc
  end

  def format_name_insert_sql({_name, ""}, acc) do
    acc
  end

  def format_name_insert_sql({_name, "NA"}, acc) do
    acc
  end

  def format_name_insert_sql({_name, "0NA"}, acc) do
    acc
  end

  def format_name_insert_sql({name, _value}, acc) do
    if acc != "", do: "#{acc}, #{name}", else: name
  end

  def format_values_insert_sql({name, _value}, acc) when name in @ignoring do
    acc
  end

  def format_values_insert_sql({_name, ""}, acc) do
    acc
  end

  def format_values_insert_sql({_name, "NA"}, acc) do
    acc
  end

  def format_values_insert_sql({_name, "0NA"}, acc) do
    acc
  end

  def format_values_insert_sql({name, value}, acc) do
    v = "#{unquoted_postgres(name, value)}"
    if acc != "", do: "#{acc}, #{v}", else: v
  end

  def format_insert_sql(row, table \\ "escuelas") do
    names = Enum.reduce(row, "", &format_name_insert_sql/2)
    values = Enum.reduce(row, "", &format_values_insert_sql/2)
    "INSERT INTO #{table} (#{names}) SELECT #{values} WHERE NOT EXISTS (SELECT * FROM upsert);"
  end

  def unquoted_postgres(name, text) when name in @integers do
   {int, _} = :string.to_integer(to_char_list(text))
   case int do
    :error -> "NULL"
    x -> "'#{x}'"
   end
  end

  def unquoted_postgres(_name, text) do
    "'#{String.replace(text, "'", "''")}'"
  end
end
