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

struct KeyValuePair{
    std::string key;
    std::string value;
    KeyValuePair(std::string k, std::string v) : key(k), value(v){}

    bool operator < (const KeyValuePair& str) const
    {
        return (key < str.key);
    }
};

struct KeyMultipleValuePair {
    std::string key;
    std::vector<std::string> values;
    KeyMultipleValuePair(std::string k, std::vector<std::string> v) : key(k), values(v) {}
};

class MapReducer
{
public:
    MapReducer(const std::vector<std::string>&, const unsigned int&, const unsigned int&, const int &);

    void parallelMapReduce();
    void readFromFile();
    void Emit(const std::string& key, const std::string& value, const int &threadIdx);
    void Emit2(const std::string& key, const std::string& value, const int& threadIdx);
    void printResult() const;
    virtual void mapFunction(const std::string& line, const int &threadIdx) = 0;
    virtual void reduceFunction(const KeyMultipleValuePair&, const int& threadIdx) = 0;

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
    std::mutex taskMutex;
    std::mutex reduceMutex;
    std::vector<std::vector<std::string>> mapTasks;
    std::vector<std::vector<std::vector<KeyValuePair>>> mappedKeyValuesPerThread;
    std::vector<std::vector<KeyValuePair>> mappedKeyValuesFinalPerThread;
    void mapThread(int);
    void reduceThread(int);
    int nextTask(const int &limit);
};

#endif // MAPREDUCER_H
