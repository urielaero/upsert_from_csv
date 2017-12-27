defmodule UpsertFromCsv.Static do
  # only 20<rows
  @subcontrol %{"FEDERAL" => 1, "ESTATAL" => 2, "AUTONOMO" => 3, "PARTICULAR" => 6, "SUBSIDIADO" => 7}
  def data("subcontrol", value) do
    {int, _} = :string.to_integer(to_char_list(value))
   case int do
    :error -> @subcontrol[value]
    x -> x
   end
  end


  @string_5 ~w(codigopostal)
  @string_20 ~w(telextension)
  @string_30 ~w(sector cct_sector)
  @string_40 ~w(cct telefono fax faxextension)
  @string_200 ~w(nombre domicilio entrecalle ycalle paginaweb)
  @string_250 ~w(zonaescola director cct_zona numext numint callepost correoelectronico )
  @strings %{5 => @string_5, 20 => @string_20, 30 => @string_30, 40 => @string_40, 200 => @string_200, 250 => @string_250}
  def string_to_length(name, value) do
    exist = Enum.filter(@strings, fn({_k, list}) ->
      name in list
    end)
    do_string_to_length(exist, name, value)
  end


  defp do_string_to_length([], _n, v), do: v
  defp do_string_to_length([{len, _ls}], _n, v), do: String.slice(v, 0..(len-1))
end

