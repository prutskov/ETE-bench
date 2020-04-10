#include "arrow/csv/api.h"
#include "arrow/api.h"
#include <arrow/io/file.h>
#include <cstdio>
#include <memory>

int main() {
  arrow::Status st;
  auto memory_pool = arrow::default_memory_pool();

  std::shared_ptr<arrow::io::ReadableFile> input;
  auto file_result = arrow::io::ReadableFile::Open("dsf");
  st = file_result.status();
  input = file_result.ValueOrDie();

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  auto table_reader_result =
      arrow::csv::TableReader::Make(memory_pool, input, read_options,
                                    parse_options, convert_options);
  st = table_reader_result.status();
  auto table_reader = table_reader_result.ValueOrDie();

  std::shared_ptr<arrow::Table> arrowTable;
  auto arrow_table_result = table_reader->Read();
  st = arrow_table_result.status();
  arrowTable = arrow_table_result.ValueOrDie();

  return 0;
}