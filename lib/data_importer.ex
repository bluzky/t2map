defmodule T2map.DataImporter do
  @default_opts [
    strategy: :horizontal_table,
    start_row_idx: 0,
    start_column_idx: 0,
    max_entry: 0,
    column_parser: [],
    shared_rows: [0, 0],
    shared_keys: [],
    chunk_size: 0,
    group_idx: 0,
    group_key: nil
  ]

  require Logger

  # strategy: horizontal_table | vertical_table
  @doc """
  opts = [
  group_idx: 0,
  start_row_idx: 2,
  start_column_idx: 2,
  strategy: :vertical_chunked_table,
  group_key: "project_code",
  chunk_size: 8,
  share_rows: [1, 1],
  share_keys: ["month"]
  ]

  keys = ["revenue", "fixed_costs", "variable_costs", "total_costs", "profit_before_tax",
  "goodwill", "pbt_after_gw", "profit_after_tax"]
  parse_excel("./data/finance.xlsx", keys, opts)

  opts =
  [
  group_idx: 1,
  start_row_idx: 5,
  start_column_idx: 3,
  strategy: :vertical_chunked_table,
  group_key: "group",
  chunk_size: 4,
  share_rows: [3, 2],
  share_keys: ["rank", "level"]
  ]


  keys =  ["fn", "ln", "email", "phone"]
  """
  def parse_excel(file_path, column_keys, opts \\ []) do
    # try do
    stream = Xlsxir.stream_list(file_path, 0)

    items = parse_data(stream, column_keys, opts)
    {:ok, items}
    # rescue
    #   err ->
    #     IO.inspect(err)
    #     Logger.error(Exception.format_stacktrace())
    #     {:error, "Cannot open file"}
    # end
  end

  def parse_data(data_stream, column_keys, opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)
    process_data(opts[:strategy], data_stream, column_keys, opts)
  end

  @doc """
  opts:
  start_row_idx: 0
  start_column_idx: 0
  max_process_row: 0
  group_key: nil
  group_idx: 0
  """

  defp process_data(:horizontal_table, data_stream, keys, opts) do
    template_map = build_template_map(data_stream, keys, opts)

    Stream.drop(data_stream, opts[:start_row_idx])
    |> Stream.transform(0, fn i, acc ->
      if opts[:max_entry] == 0 or acc < opts[:max_entry] do
        item =
          wrap_item(i, keys, opts)
          |> Map.merge(template_map)

        if Enum.any?(item, fn {_, v} -> not is_nil(v) end) do
          {[item], acc + 1}
        else
          {[], acc + 1}
        end
      else
        {:halt, acc}
      end
    end)
    |> Enum.to_list()
  end

  defp build_template_map(data_stream, keys, opts) do
    header_rows = Stream.take(data_stream, opts[:start_row_idx]) |> Enum.to_list()

    if opts[:group_key] do
      value =
        header_rows
        |> Enum.at(opts[:group_idx])
        |> Enum.slice(opts[:start_column_idx], length(keys))
        |> Enum.filter(&(not is_nil(&1)))
        |> List.first()

      %{opts[:group_key] => value}
    else
      %{}
    end
  end

  defp process_data(:vertical_table, data_stream, keys, opts) do
    data =
      Stream.drop(data_stream, opts[:start_row_idx])
      |> Stream.take(length(keys))
      |> Enum.to_list()
      |> transpose

    opts = Keyword.merge(opts, start_row_idx: opts[:start_column_idx], start_column_idx: 0)
    process_data(:horizontal_table, data, keys, opts)
  end

  defp process_data(:vertical_chunked_table, data_stream, keys, opts) do
    [start_idx, count] = opts[:shared_rows]

    shared_rows =
      Stream.take(data_stream, opts[:start_row_idx])
      |> Enum.to_list()
      |> Enum.slice(start_idx, count)

    chunk_size = if opts[:chunk_size] > 0, do: opts[:chunk_size], else: length(keys)

    Stream.drop(data_stream, opts[:start_row_idx])
    |> Stream.chunk_every(chunk_size)
    |> Stream.map(fn chunk ->
      data =
        (chunk ++ shared_rows)
        |> transpose

      opts = Keyword.merge(opts, start_row_idx: opts[:start_column_idx], start_column_idx: 0)
      process_data(:horizontal_table, data, keys ++ opts[:shared_keys], opts)
    end)
    |> Stream.concat()
    |> Enum.to_list()
  end

  defp wrap_item(cells, keys, opts \\ []) do
    cell_count = length(keys)
    meaningful_data = Enum.slice(cells, opts[:start_column_idx], cell_count)

    Enum.zip(keys, meaningful_data)
    |> Enum.into(%{})
  end

  defp stringify_map_value(map_data) do
    Enum.map(map_data, fn {k, v} ->
      if is_map(v) do
        {k, stringify_map_value(v)}
      else
        {k, to_string(v)}
      end
    end)
    |> Enum.into(Map.new())
  end

  def transpose([]), do: []
  def transpose([[] | _]), do: []

  def transpose(a) do
    try do
      [Enum.map(a, &hd/1) | transpose(Enum.map(a, &tl/1))]
    rescue
      _ -> []
    end
  end
end
