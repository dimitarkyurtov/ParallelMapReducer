#include "MapReducer.h"
#include <fstream>
#include <iostream>
#include <thread>
#include <algorithm>

MapReducer::MapReducer(const std::vector<std::string>& fileNames, const unsigned int& blockSize = l1_cache_size, const unsigned int& reduceTasks = 0, const int &nT = 1)
{
    this->fileNames = fileNames;
    this->sizePerBlock = blockSize;
    this->R = reduceTasks;
    this->numThreads = nT;
    this->mappedKeyValuesFinalPerThread = std::vector<std::vector<KeyValuePair>>(this->numThreads);
    this->mappedKeyValuesPerThread = std::vector<std::vector<std::vector<KeyValuePair>>>(this->numThreads);
    for (size_t i = 0; i < this->numThreads; i++)
    {
        this->mappedKeyValuesPerThread[i] = std::vector<std::vector<KeyValuePair>>(this->R);
    }
    //this->mappedKeyValuesMutexes.reserve(numMutexes);
}

void MapReducer::readFromFile()
{
    unsigned int currentTaskSize = 0;
    std::vector<std::string> currentTask;
    std::string line;
    for (const std::string& fileName : this->fileNames)
    {
        std::ifstream myfile("data/" + fileName);
        if (myfile.is_open())
        {
            while (getline(myfile, line))
            {
                if (currentTaskSize + line.length() > this->sizePerBlock)
                {
                    this->mapTasks.push_back(currentTask);
                    currentTask.clear();
                    currentTaskSize = 0;
                }
                currentTask.push_back(line);
                currentTaskSize += line.length();
            }
            myfile.close();
        }
    }
    if (currentTask.size() > 0)
    {
        this->mapTasks.push_back(currentTask);
    }

    this->M = this->mapTasks.size();
    this->currentTask = 0;

    /*
    for(auto task : this->mapTasks)
    {
        std::cout << "Task has started" << std::endl;
        std::cout << task.first.size() << std::endl;
        std::cout << "Task has ended" << std::endl;
    }
    */
}

void MapReducer::Emit(const std::string& key, const std::string& value, const int& threadIdx)
{
    int reducerIdx = this->hash_str(key) % this->R;
    {
        //const std::lock_guard<std::mutex> lock(this->mappedKeyValuesMutexes[this->hash_mutex_addr(&this->mappedKeyValues[reducerIdx]) % numMutexes]);
        this->mappedKeyValuesPerThread[threadIdx][reducerIdx].push_back(KeyValuePair(key, value));
    }
}

void MapReducer::Emit2(const std::string& key, const std::string& value, const int &threadIdx)
{
    {
        //const std::lock_guard<std::mutex> lock(this->reduceMutex);
        this->mappedKeyValuesFinalPerThread[threadIdx].push_back( KeyValuePair(key, value));
    }
}

void MapReducer::mapThread(int threadIdx)
{
    int currentTaskIdx = this->nextTask(this->M);
    std::vector<std::string>& currentTask = this->mapTasks[currentTaskIdx];
    while (currentTaskIdx != -1)
    {
        currentTask = this->mapTasks[currentTaskIdx];
        for (auto& line : currentTask)
        {
            //std::cout << "Map function started" << std::endl;
            //std::cout << line << std::endl;
            this->mapFunction(line, threadIdx);
            //std::cout << "Map function ended" << std::endl;
        }
        currentTaskIdx = this->nextTask(this->M);
        //std::cout << currentTaskIdx << std::endl;
    }
}

void MapReducer::reduceThread(int threadIdx)
{
    int currentTaskIdx = this->nextTask(this->R);           
    std::vector<KeyValuePair> currentTask;
    std::vector<KeyMultipleValuePair> modifiedTask;
    while (currentTaskIdx != -1)
    {
        for (size_t i = 0; i < this->numThreads; i++)
        {
            currentTask.insert(currentTask.end(), 
                this->mappedKeyValuesPerThread[i][currentTaskIdx].begin(), 
                this->mappedKeyValuesPerThread[i][currentTaskIdx].end());
        }
        /*
        for (auto task : currentTask)
        {
            std::cout << "key: " + task.first << " value: " + task.second << std::endl;
        }
        */
        std::sort(currentTask.begin(), currentTask.end());

       
        std::string lastKey;
        std::vector<std::string> currentValues;
        if (currentTask.size() > 0)
        {
            lastKey = currentTask[0].key;
        }
        for (auto& keyValuePair : currentTask)
        {
            if (keyValuePair.key == lastKey)
            {
                currentValues.push_back(keyValuePair.value);
            }
            else
            {
                modifiedTask.push_back(KeyMultipleValuePair(lastKey, currentValues));
                currentValues.clear();
                lastKey = keyValuePair.key;
                currentValues.push_back(keyValuePair.value);
            }
        }
        if (currentValues.size() > 0)
        {
            modifiedTask.push_back(KeyMultipleValuePair(lastKey, currentValues));
            currentValues.clear();
        }

        
        for (auto& keyValues : modifiedTask)
        {
            //std::cout << "Map function started" << std::endl;
            //std::cout << line << std::endl;
            this->reduceFunction(keyValues, threadIdx);
            //std::cout << "Map function ended" << std::endl;
        }
        currentTaskIdx = this->nextTask(this->R);
        currentValues.clear();
        currentTask.clear();

        modifiedTask.clear();
        //std::cout << currentTaskIdx << std::endl;
    }
}



void MapReducer::parallelMapReduce() 
{
    std::vector<std::thread> mapWorkers;
    for (int c = 0; c < this->numThreads; c++) {
        mapWorkers.push_back(std::thread([this, c] {this->mapThread(c); }));
    }

    
    for (int c = 0; c < this->numThreads; c++) {
        mapWorkers[c].join();
    }
    
    

    this->currentTask = 0;

    std::vector<std::thread> reduceWorkers;
    for (int c = 0; c < this->numThreads; c++) {
        reduceWorkers.push_back(std::thread([this, c] {this->reduceThread(c); }));
    }


    for (int c = 0; c < this->numThreads; c++) {
        reduceWorkers[c].join();
    }
    
}


int MapReducer::nextTask(const int &limit)
{
    int returnTask;
    {
        const std::lock_guard<std::mutex> lock(this->taskMutex);
        returnTask = this->currentTask++;
    }
    if (returnTask >= limit)
    {
        return -1;
    }
    return returnTask;
}

void MapReducer::printResult() const
{
    for (size_t i = 0; i < this->numThreads; i++)
    {
        for (auto val : this->mappedKeyValuesFinalPerThread[i])
        {
            std::cout << "key: " + val.key << " value: " + val.value << std::endl;
        }
    }
}

