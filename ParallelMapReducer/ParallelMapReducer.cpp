#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include <fstream>
#include "MapReducer.h"


class WordCounter : public MapReducer {
public:
    using MapReducer::MapReducer;

    virtual void mapFunction(const std::string& line, const int& threadIdx) {
        const int n = line.size();
        for (int i = 0; i < n; ) {
            // Skip past leading whitespace
            while ((i < n) && isspace(line[i]))
                i++;
            // Find word end
            int start = i;
            while ((i < n) && !isspace(line[i]))
                i++;
            if (start < i)
                Emit(line.substr(start, i - start), "1", threadIdx);
        }
    }

    virtual void reduceFunction(const KeyMultipleValuePair& kvm, const int& threadIdx) {
        // Iterate over all entries with the
        // same key and add the values
        int sum = 0;
        for (const std::string& value : kvm.values)
        {
            sum += std::stoi(value);
        }
        // Emit sum for input->key()
        Emit2(kvm.key, std::to_string(sum), threadIdx);
    }

};

using namespace std::chrono;

#include <stdlib.h>
#include <time.h> 


int main()
{
    std::vector<std::string> kb_file = {"data.txt", "data2.txt"}; // 1 KB
    std::vector<std::string> outputFileNames = { "out_data.txt" };
    std::vector<std::string> mb_file = { "data3-long.txt", "data4-long.txt"}; //25 MB
    std::vector<std::string> gb_file = { "GB-file.txt" }; //600 MB
    std::vector<std::string> dict = { "word", "random", "lmao", "xd", "tree", "nigga", "tezt" };
  

    /*
    std::ofstream file("data/GB-file.txt");
    srand(time(NULL));

    for (size_t i = 0; i < 112'958'080; i++)
    {
        file << dict[rand() % 7] << " ";
        if (i % 10 == 0)
        {
            file << '\n';
        }
    }
    */
    
    
    WordCounter wc(mb_file, 1'000'000, 30, 1);

    auto start = high_resolution_clock::now();
    wc.parallelMapReduce();
    //wc.printResult();
    auto stop = high_resolution_clock::now();
   
    auto duration = duration_cast<milliseconds>(stop - start);

    //std::cout << "Time taken by function: "
    //    << duration.count() << " milliseconds" << std::endl;
    
    return 0;
}

