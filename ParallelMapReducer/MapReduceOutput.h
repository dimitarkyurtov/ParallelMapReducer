#pragma once
#include <vector>
#include <string>
class MapReduceOutput
{
public:
	std::vector<std::string> fileNames;
	MapReduceOutput(){}
	MapReduceOutput(const std::vector<std::string>& fileNames)
	{
		this->fileNames = fileNames;
	}
};

