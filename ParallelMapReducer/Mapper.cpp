#include "Mapper.h"

Mapper::Mapper(const unsigned& R)
{
    this->R = R;
	this->mappedKeyValues = std::vector<std::vector<KeyValuePair>>(R);
}

void Mapper::Emit(const std::string& key, const std::string& value)
{
    int reducerIdx = this->hash_str(key) % this->R;
    {
        this->mappedKeyValues[reducerIdx].push_back(KeyValuePair(key, value));
    }
}

void Mapper::print(std::ostream& out = std::cout) const
{
    for (const auto& region : this->mappedKeyValues)
    {
        for (const auto& pair : region)
        {
            out << pair.key << ": " << pair.value << "\n";
        }
    }
}
