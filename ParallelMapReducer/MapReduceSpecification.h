#pragma once
#include "MapReduceInput.h"
#include "MapReduceOutput.h"

class MapReduceSpecification
{
public:
    unsigned int sizePerBlock;
    unsigned int numThreads;
    unsigned int R;
    MapReduceInput inputs;
    MapReduceOutput outputs;

    MapReduceSpecification(){}
    MapReduceSpecification(const unsigned& sizePerBlock, const unsigned& numThreads, const unsigned& R, const MapReduceInput& inputs, const MapReduceOutput& outputs)
    {
        this->sizePerBlock = sizePerBlock;
        this->numThreads = numThreads;
        this->R = R;
        this->inputs = inputs;
        this->outputs = outputs;
    }
};

