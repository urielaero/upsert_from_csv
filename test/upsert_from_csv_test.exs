defmodule UpsertFromCsvTest do
  use ExUnit.Case
  doctest UpsertFromCsv

  test "the truth" do
    assert 1 + 1 == 2
  end

  test "read and decode file" do
    assert UpsertFromCsv.read("assets/escuelas_2016.csv")
  end

  test "process row for postgres" do
    assert UpsertFromCsv.process_postgres(UpsertFromCsv.read("assets/escuelas_2016_corregido_extract.csv"))
  end

  test "process for postgres update_m_l" do
    assert UpsertFromCsv.process_postgres_m_l(UpsertFromCsv.read("assets/escuelas_2016_corregido_extract.csv")) =~ " SELECT update_m_l('01DPR0698R', 1, 1, 1);"
  end

  test "process row for postgres update_m_l " do
    data = %{"entidad" => "1", "localidad" => "2", "municipio" => "3", "cct"=>"41d"}
    assert UpsertFromCsv.process_row_m_l(data, "") == " SELECT update_m_l('41d', 3, 1, 2);"
  end


  test "process row for postgres update_m_l set 0 if NA" do
    data = %{"entidad" => "NA", "localidad" => "2", "municipio" => "3", "cct"=>"41d"}
    assert UpsertFromCsv.process_row_m_l(data, "") == " SELECT update_m_l('41d', 3, 0, 2);"
  end

  test "process row for supervisores" do
    data = %{"entidad" => "NA", "cct"=>"41d", "nivel" => "3"}
    assert UpsertFromCsv.process_row_supervisores(data, "") == " WITH upsert as (UPDATE supervisores SET  cct='41d',nivel='3' where cct='41d' RETURNING *) INSERT INTO supervisores (cct, nivel) SELECT '41d', '3' WHERE NOT EXISTS (SELECT * FROM upsert);"
  end

  test "process row" do
    data = %{"entidad" => "NA", "cct"=>"41d", "nivel" => "3", "nombre" => "lol"}
    assert UpsertFromCsv.process_row(data, "") == " WITH upsert as (UPDATE escuelas SET  cct='41d',nivel='3',nombre='lol' where cct='41d' RETURNING *) INSERT INTO escuelas (cct, nivel, nombre) SELECT '41d', '3', 'lol' WHERE NOT EXISTS (SELECT * FROM upsert);"
  end


  test "process row ignoring if dont have required fields for insert" do
    data = %{"entidad" => "NA", "cct"=>"", "nivel" => "3", "nombre" => ""}
    assert UpsertFromCsv.process_row(data, "") == ""
  end


  test "format update sql" do
    data = %{"cct" => "a", "dos" => "b", "id" => "c"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_update_sql/2)
    assert res == "cct='a',dos='b'" 
  end

  test "format names insert sql" do
    data = %{"cct" => "a", "dos" => "b", "id" => "c"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_name_insert_sql/2)
    assert res == "cct, dos" 
  end


  test "format values insert sql" do
    data = %{"cct" => "a", "dos" => "b"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_values_insert_sql/2)
    assert res == "'a', 'b'" 
  end

  test "format insert sql" do
    data = %{"cct" => "a", "dos" => "b", "id" => "c"}
    res = UpsertFromCsv.format_insert_sql(data)
    assert res == "INSERT INTO escuelas (cct, dos) SELECT 'a', 'b' WHERE NOT EXISTS (SELECT * FROM upsert);"
  end

  test "quoted for postgres when dont in list" do
    text = ~s(baby's on fire)
    assert UpsertFromCsv.unquoted_postgres("some", text) == ~s('baby''s on fire')
  end


  test "quoted for postgres when in list of integers" do
    assert UpsertFromCsv.unquoted_postgres("localidad", 1) == "'1'"
    assert UpsertFromCsv.unquoted_postgres("localidad", "1") == "'1'"
    assert UpsertFromCsv.unquoted_postgres("localidad", "na") == "NULL"
    assert UpsertFromCsv.unquoted_postgres("localidad", "") == "NULL"
  end

  test "quoted for postgres when in list of floats" do
    assert UpsertFromCsv.unquoted_postgres("longitud", "1.5") == "'1.5'"
    assert UpsertFromCsv.unquoted_postgres("latitud", "1") == "'1.0'"
    assert UpsertFromCsv.unquoted_postgres("latitud", "-1") == "'-1.0'"
    assert UpsertFromCsv.unquoted_postgres("latitud", "-1.5092") == "'-1.5092'"
    assert UpsertFromCsv.unquoted_postgres("latitud", "na") == "NULL"
    assert UpsertFromCsv.unquoted_postgres("latitud", "") == "NULL"
  end

  test "quoted for max in varchar(length) per column" do
    ls_string_260 = Enum.concat([?a..?z, ?a..?z, ?a..?z, ?a..?z, ?a..?z, ?a..?z, ?a..?z, ?a..?z, ?a..?z, ?a..?z])
                  |> Enum.to_list
    string_260 = ls_string_260 |> List.to_string

    expect = ls_string_260 |> Enum.take(20) |> List.to_string # 20
    assert UpsertFromCsv.unquoted_postgres("telextension", string_260) == "'#{expect}'"

    expect = ls_string_260 |> Enum.take(30) |> List.to_string
    assert UpsertFromCsv.unquoted_postgres("sector", string_260) == "'#{expect}'"

    expect = ls_string_260 |> Enum.take(5) |> List.to_string
    assert UpsertFromCsv.unquoted_postgres("codigopostal", string_260) == "'#{expect}'"

    expect = ls_string_260 |> Enum.take(200) |> List.to_string
    assert UpsertFromCsv.unquoted_postgres("nombre", string_260) == "'#{expect}'"

    expect = ls_string_260 |> Enum.take(250) |> List.to_string
    assert UpsertFromCsv.unquoted_postgres("zonaescola", string_260) == "'#{expect}'"

    assert UpsertFromCsv.unquoted_postgres("dontinlist", string_260) == "'#{string_260}'"
  end

  test "ignore if cast invalid, format update" do
    data = %{"cct" => "a", "municipio" => "b", "entidad" => "1", "localidad" => 2}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_update_sql/2)
    assert res == "cct='a',entidad='1',localidad='2',municipio=NULL"  
  end

  test "ignore if cast invalid, format insert" do
    data = %{"cct" => "a", "municipio" => "b", "entidad" => "1", "localidad" => 2}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_values_insert_sql/2)
    assert res == "'a', '1', '2', NULL"
  end

  test "transform to static in update if name in @static" do
    data = %{"cct" => "a", "municipio" => "b", "subcontrol" => "FEDERAL"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_update_sql/2)
    assert res == "cct='a',municipio=NULL,subcontrol='1'"

    data = %{"cct" => "a", "municipio" => "b", "subcontrol" => "OTHER"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_update_sql/2)
    assert res == "cct='a',municipio=NULL,subcontrol=NULL"

    data = %{"cct" => "a", "municipio" => "b", "subcontrol" => "1"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_update_sql/2)
    assert res == "cct='a',municipio=NULL,subcontrol='1'"
  end

  test "transform to static in insert if name in @static" do
    data = %{"cct" => "a", "municipio" => "b", "entidad" => "1", "localidad" => 2, "subcontrol" => "FEDERAL"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_values_insert_sql/2)
    assert res == "'a', '1', '2', NULL, '1'"

    data = %{"cct" => "a", "municipio" => "b", "entidad" => "1", "localidad" => 2, "subcontrol" => "OTHER"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_values_insert_sql/2)
    assert res == "'a', '1', '2', NULL, NULL"

    data = %{"cct" => "a", "municipio" => "b", "entidad" => "1", "localidad" => 2, "subcontrol" => "1"}
    res = Enum.reduce(data, "", &UpsertFromCsv.format_values_insert_sql/2)
    assert res == "'a', '1', '2', NULL, '1'"
  end
end
