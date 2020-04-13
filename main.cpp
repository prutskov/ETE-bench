#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/util/timestamp_converter.h"
#include <arrow/io/file.h>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>

const std::vector<std::string> formats = {
    "1917-10-17", "2018-09-13 22", "1941-06-22 04:00", "1945-05-09 09:45:38"};
int64_t read_csv(const std::string &path_csv, bool isVirtual);
bool generate_data(const std::string &path_csv, size_t cols, size_t rows);
std::string random_date();

int main(int argc, char **argv) {
  ::srand(777);

  std::string file = "data.csv";
  if (argc > 1) {
    printf("Generating data...\n");
    const size_t rows = std::atoi(argv[1]);
    const size_t cols = std::atoi(argv[2]);
    generate_data(file, cols, rows);
  }
  printf("Measuring...\n");

  const size_t n_iterations = 10;

  int64_t elapsed_time = 0;
  int64_t elapsed_time_virtual = 0;

  for (size_t i = 0; i < n_iterations; ++i) {
    elapsed_time_virtual += read_csv("data.csv", true);
  }
  printf("Time (virtual) read_csv: %.2f ms\n",
         static_cast<float>(elapsed_time_virtual) / n_iterations);

  for (size_t i = 0; i < n_iterations; ++i) {
    elapsed_time += read_csv("data.csv", false);
  }
  printf("Time (classic) read_csv: %.2f ms\n",
         static_cast<float>(elapsed_time) / n_iterations);
  return 0;
}

int64_t read_csv(const std::string &path_csv, bool isVirtual) {
  arrow::Status st;
  auto memory_pool = arrow::default_memory_pool();

  std::shared_ptr<arrow::io::ReadableFile> input;
  auto file_result = arrow::io::ReadableFile::Open(path_csv.c_str());
  st = file_result.status();
  input = file_result.ValueOrDie();

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  if (isVirtual) {
    convert_options.timestamp_converters.push_back(
        std::make_shared<arrow::ISO8601Parser>(
            timestamp(arrow::TimeUnit::SECOND)));
  }

  auto table_reader_result = arrow::csv::TableReader::Make(
      memory_pool, input, read_options, parse_options, convert_options);
  st = table_reader_result.status();
  auto table_reader = table_reader_result.ValueOrDie();

  std::shared_ptr<arrow::Table> arrowTable;

  // Measuring
  auto start = std::chrono::system_clock::now();

  auto arrow_table_result = table_reader->Read();
  st = arrow_table_result.status();
  arrowTable = arrow_table_result.ValueOrDie();

  auto end = std::chrono::system_clock::now();

  int64_t duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  return duration;
}

std::string random_date() { return formats[rand() % formats.size()]; }

bool generate_data(const std::string &path_csv, size_t cols, size_t rows) {
  std::ofstream file;
  file.open(path_csv, std::ios::out);
  std::string str = random_date();

  for (size_t row = 0; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      file << random_date() << ",";
    }
    file << "\n";
  }
  file.close();
  return true;
}
