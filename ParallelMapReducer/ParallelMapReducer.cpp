#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include "lib.h"
#include "Mapper.h"


using namespace std::chrono;


int main()
{
    
    std::vector<std::string> inputFileNames = {"data.txt"};
    std::vector<std::string> outputFileNames = { "out_data.txt" };
    std::vector<std::string> fileNames2 = { "data3-long.txt", "data4-long.txt"}; //25 000KB

    MapReduceInput input(inputFileNames);
    MapReduceOutput output(outputFileNames);
    MapReduceSpecification spec(100, 3, 3, input, output);

    MapReduce(spec);

    auto start = high_resolution_clock::now();
    auto stop = high_resolution_clock::now();
   
    auto duration = duration_cast<milliseconds>(stop - start);

    std::cout << "Time taken by function: "
        << duration.count() << " milliseconds" << std::endl;
    return 0;
}
