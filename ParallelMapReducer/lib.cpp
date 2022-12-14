#include "lib.h"
#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <string>
#include <queue>
#include <shared_mutex>
#include <condition_variable>
#include "Mapper.h"

class WordCounter : public Mapper {
    using Mapper::Mapper;
public:
    virtual void Map(const std::string& line) {
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
                Emit(line.substr(start, i - start), "1");
        }
    }
};

void libF()
{
    std::cout << "Hello from lib";
}

std::queue<std::vector<std::string>> mapTaskQueue;
std::mutex mapQueueMutex;
std::mutex streamMutex;
bool hasMapWork = true;
std::condition_variable_any cv;

void mapWorker(int threadIdx, const unsigned& R, std::ostream& stream)
{
    WordCounter m(R);
    std::vector<std::string> currentTask;
    while (true)
    {
        {
            std::unique_lock<std::mutex> lk(mapQueueMutex);
            cv.wait(
                lk,
                [] {
                    return !mapTaskQueue.empty() || (mapTaskQueue.empty() && !hasMapWork);
                }
            );

            if (mapTaskQueue.empty())
            {
                break;
            }

            currentTask = mapTaskQueue.front();
            mapTaskQueue.pop();
        }

        for (auto& line : currentTask)
        {
            m.Map(line);
        }
    }

    {
        std::lock_guard<std::mutex> writerLock(streamMutex);
        m.print(stream);
    }
}


/*
void reduceWorker(int threadIdx)
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
        for (auto task : currentTask)
        {
            std::cout << "key: " + task.first << " value: " + task.second << std::endl;
        }
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
*/

void MapReduce(MapReduceSpecification& spec)
{
    std::ofstream outputFile("data/" + spec.outputs.fileNames[0]);
    std::vector<std::thread> mapWorkers;
    for (int c = 0; c < spec.numThreads; c++) {
        mapWorkers.push_back(std::thread([c, &spec, &outputFile] {mapWorker(c, spec.R, outputFile);}));
    }

    unsigned int currentTaskSize = 0;
    std::vector<std::string> currentTask;
    std::string line;
    for (const std::string& fileName: spec.inputs.fileNames)
    {
        std::ifstream myfile("data/" + fileName);
        if (myfile.is_open())
        {
            while (getline(myfile, line))
            {
                if (currentTaskSize + line.length() > spec.sizePerBlock)
                {
                    {
                        std::lock_guard<std::mutex> writerLock(mapQueueMutex);
                        mapTaskQueue.push(currentTask);
                    }
                    cv.notify_one();
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
        {
            std::lock_guard<std::mutex> writerLock(mapQueueMutex);
            mapTaskQueue.push(currentTask);
        }
    }

    {
        std::lock_guard<std::mutex> writerLock(mapQueueMutex);
        hasMapWork = false;
    }
    


    for (int c = 0; c < spec.numThreads; c++) {
        mapWorkers[c].join();
    }

    std::vector<std::thread> reducerWorker;
    for (int c = 0; c < spec.numThreads; c++) {
        mapWorkers.push_back(std::thread([c, &spec, &outputFile] {mapWorker(c, spec.R, outputFile); }));
    }
}
