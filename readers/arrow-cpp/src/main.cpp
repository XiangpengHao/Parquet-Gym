#include <arrow/io/api.h>
#include <arrow/table.h>
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

arrow::Status read_parquet(std::string input_path, bool print)
{
	arrow::MemoryPool *pool = arrow::default_memory_pool();
	std::shared_ptr<arrow::io::RandomAccessFile> input;
	auto a = arrow::io::ReadableFile::Open(input_path);
	ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(input_path));

	std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
	ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

	std::shared_ptr<arrow::Table> table;
	ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

	if (print)
	{
		std::cout << "Table: " << std::endl;
		std::cout << table->ToString() << std::endl;
	}
	return arrow::Status::OK();
}

int main(int argc, char **argv)
{
	if (argc < 2)
	{
		std::cerr << "Usage: " << argv[0] << " <input_parquet_file> [-p|--print]" << std::endl;
		return -1;
	}

	std::string input_path = argv[1];
	bool print = false;

	// Parse command line for print flag
	std::vector<std::string> args(argv, argv + argc);
	if (std::find(args.begin(), args.end(), "-p") != args.end() ||
		std::find(args.begin(), args.end(), "--print") != args.end())
	{
		print = true;
	}

	auto status = read_parquet(input_path, print);
	if (status != arrow::Status::OK())
	{
		std::cerr << "Error reading parquet file: " << status.ToString() << std::endl;
		return -1;
	}
	return 0;
}
