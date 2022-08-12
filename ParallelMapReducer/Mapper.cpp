#include "Mapper.h"

Mapper::Mapper(const unsigned& numThrread, const unsigned& R)
{
    this->R = R;
    this->numThread = numThread;
	this->mappedKeyValues = std::vector<std::vector<std::vector<KeyValuePair>>>(this->numThread);
    for (size_t i = 0; i < numThread; i++)
    {
        mappedKeyValues[i] = std::vector<std::vector<KeyValuePair>>(this->R);
    }
}

void Mapper::Emit(const std::string& key, const std::string& value, const unsigned& threadIdx)
{
    int reducerIdx = this->hash_str(key) % this->R;
    {
        this->mappedKeyValues[threadIdx][reducerIdx].push_back(KeyValuePair(key, value));
    }
}

void Mapper::print(std::ostream& out = std::cout, const unsigned& threadIdx = 0) const
{
    for (const auto& region : this->mappedKeyValues[threadIdx])
    {
        for (const auto& pair : region)
        {
            out << pair.key << ": " << pair.value << "\n";
        }
    }
}
