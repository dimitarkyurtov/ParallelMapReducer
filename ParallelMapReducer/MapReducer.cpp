#include "MapReducer.h"
#include <fstream>
#include <iostream>
#include <thread>
#include <algorithm>


MapReducer::MapReducer(const MapReducer& mr)
{
    this->fileNames = mr.fileNames;
    this->sizePerBlock = mr.sizePerBlock;
    this->R = mr.R;
    this->mappedKeyValuesFinal = new std::pair<std::string, std::string>[this->R];
    this->numThreads = mr.numThreads;
    this->mappedKeyValuesFinalPerThread = new std::vector<std::pair<std::string, std::string>>[this->numThreads];
    this->mappedKeyValuesPerThread = new std::vector<std::pair<std::string, std::string>>*[this->numThreads];
    for (size_t i = 0; i < this->numThreads; i++)
    {
        this->mappedKeyValuesPerThread[i] = new std::vector<std::pair<std::string, std::string>>[this->R];
        for (size_t j = 0; j < this->R; ++j)
        {
            this->mappedKeyValuesPerThread[i][j] = mr.mappedKeyValuesPerThread[i][j];
        }
    }
    this->mappedKeyValues = new std::vector<std::pair<std::string, std::string>>[mr.R];
    this->mappedKeyValuesMutexes = new std::mutex[numMutexes];
    for (size_t i = 0; i < this->R; ++i)
    {
        this->mappedKeyValues[i] = mr.mappedKeyValues[i];
    }
}

MapReducer& MapReducer::operator=(const MapReducer& mr)
{
    if(this == &mr)
    {
        return *this;
    }
    else
    {
        delete[] mappedKeyValues;
        delete[] mappedKeyValuesMutexes;
        delete[] mappedKeyValuesFinal;
        delete[] mappedKeyValuesFinalPerThread;
        for (size_t i = 0; i < this->numThreads; i++)
        {
            delete[] this->mappedKeyValuesPerThread[i];
            
        }
        delete[] this->mappedKeyValuesPerThread;
        this->fileNames = mr.fileNames;
        this->sizePerBlock = mr.sizePerBlock;
        this->R = mr.R;
        this->mappedKeyValuesFinal = new std::pair<std::string, std::string>[this->R];
        this->numThreads = mr.numThreads;
        this->mappedKeyValuesFinalPerThread = new std::vector<std::pair<std::string, std::string>>[this->numThreads];
        this->mappedKeyValuesPerThread = new std::vector<std::pair<std::string, std::string>>*[this->numThreads];
        for (size_t i = 0; i < this->numThreads; i++)
        {
            this->mappedKeyValuesPerThread[i] = new std::vector<std::pair<std::string, std::string>>[this->R];
            for (size_t j = 0; j < this->R; ++j)
            {
                this->mappedKeyValuesPerThread[i][j] = mr.mappedKeyValuesPerThread[i][j];
            }
        }
        this->mappedKeyValues = new std::vector<std::pair<std::string, std::string>>[mr.R];
        this->mappedKeyValuesMutexes = new std::mutex[numMutexes];
        for (size_t i = 0; i < this->R; ++i)
        {
            this->mappedKeyValues[i] = mr.mappedKeyValues[i];
        }

        return *this;
    }
}


MapReducer::MapReducer(const std::vector<std::string>& fileNames, const unsigned int& blockSize = l1_cache_size, const unsigned int& reduceTasks = 0, const int &nT = 1)
{
    this->fileNames = fileNames;
    this->sizePerBlock = blockSize;
    this->R = reduceTasks;
    this->numThreads = nT;
    this->mappedKeyValuesFinalPerThread = new std::vector<std::pair<std::string, std::string>>[this->numThreads];
    this->mappedKeyValuesPerThread = new std::vector<std::pair<std::string, std::string>>*[this->numThreads];
    for (size_t i = 0; i < this->numThreads; i++)
    {
        this->mappedKeyValuesPerThread[i] = new std::vector<std::pair<std::string, std::string>>[this->R];
    }
    this->mappedKeyValues = new std::vector<std::pair<std::string, std::string>>[this->R];
    this->mappedKeyValuesFinal = new std::pair<std::string, std::string>[this->R];
    this->mappedKeyValuesMutexes = new std::mutex[numMutexes];
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
                    this->mapTasks.push_back(std::make_pair(currentTask, DEAD));
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
        this->mapTasks.push_back(std::make_pair(currentTask, DEAD));
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
        this->mappedKeyValuesPerThread[threadIdx][reducerIdx].push_back(std::make_pair(key, value));
    }
}

void MapReducer::Emit2(const std::string& key, const std::string& value, const int &threadIdx)
{
    {
        //const std::lock_guard<std::mutex> lock(this->reduceMutex);
        this->mappedKeyValuesFinalPerThread[threadIdx].push_back( std::make_pair(key, value));
    }
}

void MapReducer::mapThread(int threadIdx)
{
    int currentTaskIdx = this->nextTask(this->M);
    std::vector<std::string> currentTask;
    while (currentTaskIdx != -1)
    {
        currentTask = this->mapTasks[currentTaskIdx].first;
        this->mapTasks[currentTaskIdx].second = PENDING;
        for (auto& line : currentTask)
        {
            //std::cout << "Map function started" << std::endl;
            //std::cout << line << std::endl;
            this->mapFunction(line, threadIdx);
            //std::cout << "Map function ended" << std::endl;
        }
        this->mapTasks[currentTaskIdx].second = COMPLETED;
        currentTaskIdx = this->nextTask(this->M);
        //std::cout << currentTaskIdx << std::endl;
    }
}

void MapReducer::reduceThread(int threadIdx)
{
    int currentTaskIdx = this->nextTask(this->R);           
    std::vector<std::pair<std::string, std::string>> currentTask;
    std::vector<std::pair<std::string, std::vector<std::string>>> modifiedTask;
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
            lastKey = currentTask[0].first;
        }
        for (auto& keyValuePair : currentTask)
        {
            if (keyValuePair.first == lastKey)
            {
                currentValues.push_back(keyValuePair.second);
            }
            else
            {
                modifiedTask.push_back(std::make_pair(lastKey, currentValues));
                currentValues.clear();
                lastKey = keyValuePair.first;
                currentValues.push_back(keyValuePair.second);
            }
        }
        if (currentValues.size() > 0)
        {
            modifiedTask.push_back(std::make_pair(lastKey, currentValues));
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
            std::cout << "key: " + val.first << " value: " + val.second << std::endl;
        }
    }
}

MapReducer::~MapReducer()
{
    delete[] this->mappedKeyValues;
    delete[] this->mappedKeyValuesMutexes;
    delete[] mappedKeyValuesFinal;
    delete[] mappedKeyValuesFinalPerThread;
    for (size_t i = 0; i < this->numThreads; i++)
    {
        delete[] this->mappedKeyValuesPerThread[i];

    }
    delete[] this->mappedKeyValuesPerThread;
}
