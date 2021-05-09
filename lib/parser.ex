defmodule FreelasParallelism.Parser do
  @moduledoc """
  Turns the data of a file into a list.
  """

  @spec parse_file(String) :: %Stream{
          :done => nil,
          :funs => nonempty_maybe_improper_list
        }
  @doc """
  Turns the data of a file into a list.

  ## Examples

        iex> FreelasParallelism.Parser.parse_file("report_test_1.csv") |> Enum.map(& &1)
        [
          [:Daniele, 7, 29, 4, :"2018"],
          [:Mayk, 4, 9, 12, :"2019"],
          [:Daniele, 5, 27, 12, :"2016"],
          [:Mayk, 1, 2, 12, :"2017"],
          [:Giuliano, 3, 13, 2, :"2017"],
          [:Cleiton, 1, 22, 6, :"2020"],
          [:Giuliano, 6, 18, 2, :"2019"],
          [:Jakeliny, 8, 18, 7, :"2017"],
          [:Joseph, 3, 17, 3, :"2017"],
          [:Jakeliny, 6, 23, 3, :"2019"]
        ]

  """
  def parse_file(file_name) do
    "reports/#{file_name}"
    |> File.stream!()
    |> Stream.map(&parse_line/1)
  end

  defp parse_line(line) do
    # "Daniele,7,29,4,2018\n"
    line
    |> String.trim()
    # "Daniele,7,29,4,2018"
    |> String.split(",")
    # ["Daniele", "7", "29", "4", "2018"]
    |> List.update_at(0, &String.to_atom/1)
    |> List.update_at(1, &String.to_integer/1)
    |> List.update_at(2, &String.to_integer/1)
    |> List.update_at(3, &String.to_integer/1)
    |> List.update_at(4, &String.to_atom/1)

    # ["Daniele", 7, 29, 4, "2018"]
  end
end
