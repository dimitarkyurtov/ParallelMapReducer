#include <iostream>
#include <string>
#include <chrono>
#include <fstream>
#include "MapReducer.h"

using namespace std;
using namespace std::chrono;

class MapReducerImpl : public MapReducer {
    using MapReducer::MapReducer;
public:
    void mapFunction(std::string line, const int& threadIdx)
    {
        const int n = line.length();
        for (size_t i = 0; i < n; ++i)
        {
            while ((i < n) && line[i] == ' ')
                i++;

            int start = i;
            while ((i < n) && line[i] != ' ')
                i++;

            if (start < i)
                this->Emit(line.substr(start, i - start), "1", threadIdx);
        }
    }

    virtual void reduceFunction(std::pair<std::string, std::vector<std::string>> keyValues, const int& threadIdx) {
        int sum = 0;
        for (auto& value : keyValues.second)
        {
            sum += std::stoi(value);
        }
        this->Emit2(keyValues.first, std::to_string(sum), threadIdx);
    }

};

int main()
{
    
    std::vector<std::string> fileNames = { "data.txt", "data2.txt" };
    std::vector<std::string> fileNames2 = { "data3-long.txt", "data4-long.txt"}; //25 000KB
    int numThreads;
    cin >> numThreads;
    MapReducerImpl* mr = new MapReducerImpl(fileNames, 100, 5, numThreads);
    mr->readFromFile();

    auto start = high_resolution_clock::now();
    mr->parallelMapReduce();
    auto stop = high_resolution_clock::now();
   
/*
    for (size_t i = 0; i < mr->numThreads; i++)
    {
        for (auto val : mr->mappedKeyValuesFinalPerThread[i])
        {
            std::cout << "key: " + val.first << " value: " + val.second << std::endl;
        }
    }
    */
    auto duration = duration_cast<milliseconds>(stop - start);

    cout << "Time taken by function: "
        << duration.count() << " milliseconds" << endl;

    mr->printResult();
    /*
    std::string line;
    std::ifstream myfile ("data/data.txt");
    if (myfile.is_open())
    {
        while ( getline (myfile,line) )
        {
            cout << line.length() << endl;
        }
        myfile.close();
    }*/
    return 0;
}
