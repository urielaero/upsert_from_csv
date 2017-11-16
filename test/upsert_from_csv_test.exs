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

  test "quoted for postgres" do
    text = ~s(baby's on fire)
    assert UpsertFromCsv.unquoted_postgres("some", text) == ~s('baby''s on fire')
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
end
