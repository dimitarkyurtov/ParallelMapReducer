#pragma once
#include <vector>
#include <string>

class MapReduceInput
{
public:
	std::vector<std::string> fileNames;
	MapReduceInput(){}
	MapReduceInput(const std::vector<std::string>& fileNames)
	{
		this->fileNames = fileNames;
	}
};

