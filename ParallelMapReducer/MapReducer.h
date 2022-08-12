#ifndef MAPREDUCER_H
#define MAPREDUCER_H

#include<string>
#include<vector>
#include<unordered_map>
#include<utility>
#include<mutex>
#include<queue>
#include<cstring>

typedef void (*ReduceFunction)(std::string, std::vector<std::string>);

const unsigned int l1_cache_size = 256;
const int numMutexes = 1000;

struct KeyValuePair{
    std::string key;
    std::string value;
    KeyValuePair(const std::string& k, const std::string& v) : key(k), value(v)
    {
        
    }

    bool operator < (const KeyValuePair& str) const
    {
        return (key < str.key);
    }
};

struct KeyValuePair2 {
    char* key;
    char* value;

    KeyValuePair2(){}

    KeyValuePair2(const std::string& k, const std::string& v)
    {
        int kLength = k.length();
        int vLength = v.length();
        key = new char[kLength+1];
        value = new char[vLength+1];
        for (size_t i = 0; i < kLength; i++)
        {
            key[i] = k[i];
        }
        for (size_t i = 0; i < vLength; i++)
        {
            value[i] = v[i];
        }

        key[kLength] = '\0';
        value[vLength] = '\0';
    }

    KeyValuePair2(const KeyValuePair2& kvp)
    {
        int kLength = strlen(kvp.key);
        int vLength = strlen(kvp.value);


        key = new char[kLength + 1];
        value = new char[vLength + 1];
        for (size_t i = 0; i < kLength; i++)
        {
            key[i] = kvp.key[i];
        }
        for (size_t i = 0; i < vLength; i++)
        {
            value[i] = kvp.value[i];
        }

        key[kLength] = '\0';
        value[vLength] = '\0';
    }

    KeyValuePair2& operator=(const KeyValuePair2& other)
    {
        if (this == &other) return *this;
        else {
            delete[]key;
            delete[]value;

            int kLength = strlen(other.key);
            int vLength = strlen(other.value);


            key = new char[kLength + 1];
            value = new char[vLength + 1];
            for (size_t i = 0; i < kLength; i++)
            {
                key[i] = other.key[i];
            }
            for (size_t i = 0; i < vLength; i++)
            {
                value[i] = other.value[i];
            }

            key[kLength] = '\0';
            value[vLength] = '\0';

        }
        return *this;
    }

    ~KeyValuePair2()
    {
        delete[]key;
        delete[]value;
    }

    bool operator < (const KeyValuePair2& str) const
    {
        int kLength = strlen(this->key);
        for (size_t i = 0; i < kLength; i++)
        {
            if (key[i] < str.key[i])
            {
                return true;
            }
        }
        return false;
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
    bool hasMapWork;
    std::hash<std::string> hash_str;
    std::mutex taskMutex;
    std::mutex reduceMutex;
    std::mutex mapQueueMutex;
    std::condition_variable cv;
    std::vector<std::vector<std::string>> mapTasks;
    std::vector<std::vector<std::vector<KeyValuePair>>> mappedKeyValuesPerThread;
    std::vector<std::vector<KeyValuePair>> mappedKeyValuesFinalPerThread;
    void mapThread(int, std::queue<std::vector<std::string>>&);
    void reduceThread(int);
    int nextTask(const int &limit);
    void read();
};

#endif // MAPREDUCER_H
