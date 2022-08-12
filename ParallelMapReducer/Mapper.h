#pragma once
#include <string>
#include <vector>
#include <iostream>

struct KeyValuePair {
    std::string key;
    std::string value;
    KeyValuePair(std::string k, std::string v) : key(k), value(v) {}

    bool operator < (const KeyValuePair& str) const
    {
        return (key < str.key);
    }
};

class Mapper
{
public:
    Mapper(const unsigned&, const unsigned&);
    unsigned int R;
    unsigned int numThread;
    std::vector<std::vector<std::vector<KeyValuePair>>> mappedKeyValues;
    std::hash<std::string> hash_str;
	virtual void Map(const std::string& line) = 0;
    virtual void Reduce(const std::string& line) = 0;
    void Emit(const std::string&, const std::string&, const unsigned&);
    void print(std::ostream&, const unsigned&) const;
};

