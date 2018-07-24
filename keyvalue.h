//
// Created by timmytonga on 7/24/18.
//

#ifndef MAPREDUCECPP_KEYVALUE_H
#define MAPREDUCECPP_KEYVALUE_H

#include <map>
#include <vector>

namespace MAPREDUCE_NAMESPACE {

template<class Key, class Value>
class KeyValue {
public:
    std::map<Key, Value> *kvmap;

    KeyValue();

    ~KeyValue();
};
}

#endif //MAPREDUCECPP_KEYVALUE_H
