defmodule FreelasParallelism do
  alias FreelasParallelism.Parser

  @people [
    :Daniele,
    :Mayk,
    :Giuliano,
    :Cleiton,
    :Jakeliny,
    :Joseph,
    :Diego,
    :Rafael,
    :Danilo,
    :Vinicius
  ]

  @months [
    :janeiro,
    :fevereiro,
    :março,
    :abril,
    :maio,
    :junho,
    :julho,
    :agosto,
    :setembro,
    :outubro,
    :novembro,
    :dezembro
  ]

  @years [
    :"2016",
    :"2017",
    :"2018",
    :"2019",
    :"2020"
  ]

  def build_report(file_name) do
    file_name
    |> Parser.parse_file()
    |> calculates_hours()
  end

  def build_report_from_many(file_name_list) do
    acc_initial_value = report_layout()

    result =
      file_name_list
      |> Task.async_stream(&build_report/1)
      |> Enum.reduce(acc_initial_value, fn {:ok, result}, report_acc ->
        sum_reports(report_acc, result)
      end)

    {:ok, result}
  end

  defp sum_reports(
         %{
           all_hours: people_with_all_hours_acc,
           hours_per_month: people_with_months_acc,
           hours_per_year: people_with_years_acc
         },
         %{
           all_hours: people_with_all_hours_result,
           hours_per_month: people_with_months_result,
           hours_per_year: people_with_years_result
         }
       ) do
    people_with_all_hours = merge_maps(people_with_all_hours_acc, people_with_all_hours_result)

    people_with_months =
      merge_maps_people_with_months(people_with_months_acc, people_with_months_result)

    people_with_years =
      merge_maps_people_with_years(people_with_years_acc, people_with_years_result)

    %{
      all_hours: people_with_all_hours,
      hours_per_month: people_with_months,
      hours_per_year: people_with_years
    }
  end

  defp merge_maps(map1, map2) do
    Map.merge(map1, map2, fn _key, value1, value2 -> value1 + value2 end)
  end

  defp merge_maps_people_with_months(people_with_months_acc, people_with_months_result) do
    Map.merge(
      people_with_months_acc,
      people_with_months_result,
      fn _person_key, months_acc, months_result ->
        merge_maps_months(months_acc, months_result)
      end
    )
  end

  defp merge_maps_people_with_years(people_with_years_acc, people_with_years_result) do
    Map.merge(
      people_with_years_acc,
      people_with_years_result,
      fn _person_key, years_acc, years_result ->
        merge_maps_years(years_acc, years_result)
      end
    )
  end

  defp merge_maps_months(months_acc, months_result) do
    Map.merge(months_acc, months_result, fn _month_key, month_acc_value, month_result_value ->
      month_acc_value + month_result_value
    end)
  end

  defp merge_maps_years(years_acc, years_result) do
    Map.merge(years_acc, years_result, fn _year_key, year_acc_value, year_result_value ->
      year_acc_value + year_result_value
    end)
  end

  defp calculates_hours(list_with_all_data_from_a_file) do
    acc_initial_value = report_layout()

    list_with_all_data_from_a_file
    |> Enum.reduce(acc_initial_value, fn line_list, report_acc ->
      sum_hours(report_acc, line_list)
    end)
  end

  defp sum_hours(report_acc, [person_name, hours_day, _day, month, year]) do
    people_with_all_hours = sum_all_hours(report_acc, person_name, hours_day)

    people_with_months = sum_hours_per_month(report_acc, person_name, hours_day, month)

    people_with_years = sum_hours_per_year(report_acc, person_name, hours_day, year)

    %{
      all_hours: people_with_all_hours,
      hours_per_month: people_with_months,
      hours_per_year: people_with_years
    }
  end

  defp sum_hours_per_year(%{hours_per_year: people_with_years}, person_name, hours_day, year) do
    years = people_with_years[person_name]

    updated_years = Map.put(years, year, years[year] + hours_day)

    person_with_years = Map.put_new(%{}, person_name, updated_years)

    Map.merge(people_with_years, person_with_years)
  end

  defp sum_hours_per_month(%{hours_per_month: people_with_months}, person_name, hours_day, month) do
    months = people_with_months[person_name]

    month =
      case month do
        1 -> :janeiro
        2 -> :fevereiro
        3 -> :março
        4 -> :abril
        5 -> :maio
        6 -> :junho
        7 -> :julho
        8 -> :agosto
        9 -> :setembro
        10 -> :outubro
        11 -> :novembro
        12 -> :dezembro
      end

    updated_months = Map.put(months, month, months[month] + hours_day)

    person_with_months = Map.put_new(%{}, person_name, updated_months)

    Map.merge(people_with_months, person_with_months)
  end

  defp sum_all_hours(%{all_hours: people}, person_name, hours_day) do
    Map.put(people, person_name, hours_day + people[person_name])
  end

  defp report_layout do
    people = Enum.into(@people, %{}, fn person_name -> {person_name, 0} end)

    months = Enum.into(@months, %{}, fn month -> {month, 0} end)
    people_with_months = Enum.into(@people, %{}, fn person_name -> {person_name, months} end)

    years = Enum.into(@years, %{}, fn year -> {year, 0} end)
    people_with_years = Enum.into(@people, %{}, fn person_name -> {person_name, years} end)

    %{all_hours: people, hours_per_month: people_with_months, hours_per_year: people_with_years}
  end
end
