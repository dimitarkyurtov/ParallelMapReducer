#ifndef MAPREDUCER_H
#define MAPREDUCER_H

#include<string>
#include<vector>
#include<unordered_map>
#include<utility>
#include<mutex>

typedef void (*ReduceFunction)(std::string, std::vector<std::string>);

const unsigned int l1_cache_size = 256;
const int numMutexes = 1000;

enum Status { DEAD, PENDING, COMPLETED };

class MapReducer
{
public:
    MapReducer();
    MapReducer(const std::vector<std::string>&, const unsigned int&, const unsigned int&, const int &);
    MapReducer(const MapReducer& mr);
    MapReducer& operator=(const MapReducer& mr);
    void parallelMapReduce();
    void readFromFile();
    void Emit(const std::string& key, const std::string& value, const int &threadIdx);
    void Emit2(const std::string& key, const std::string& value, const int& threadIdx);
    void printResult() const;
    virtual void mapFunction(std::string line, const int &threadIdx) = 0;
    virtual void reduceFunction(std::pair<std::string, std::vector<std::string>>, const int& threadIdx) = 0;
    ~MapReducer();

//protected:

//private:
    std::vector<std::string> fileNames;
    //virtual void reduceFunction(std::string line);
    unsigned int sizePerBlock; // size of map chunks in B
    unsigned int M; // number of map blocks
    unsigned int R; // number of reduce blocks
    int currentTask;
    int numThreads;
    std::hash<std::string> hash_str;
    std::hash<std::vector<std::pair<std::string, std::string>>*> hash_mutex_addr;
    std::mutex taskMutex;
    std::mutex reduceMutex;
    std::mutex* mappedKeyValuesMutexes;
    std::vector<std::pair<std::vector<std::string>, Status>> mapTasks;
    std::vector<std::pair<std::string, std::string>>* mappedKeyValues;
    std::vector<std::pair<std::string, std::string>>** mappedKeyValuesPerThread;
    std::pair<std::string, std::string>* mappedKeyValuesFinal;
    std::vector<std::pair<std::string, std::string>>* mappedKeyValuesFinalPerThread;
    std::unordered_map<std::string, std::string> reducedKeyValues;
    void mapThread(int);
    void reduceThread(int);
    int nextTask(const int &limit);
    int nextTask2(const int& limit);
};

#endif // MAPREDUCER_H
