//
// Created by timmytonga on 7/24/18.
//

#include "keyvalue.h"

using namespace MAPREDUCE_NAMESPACE;

template <class Key, class Value>
KeyValue::KeyValue(){
    kvmap = new std::map<Key,Value>;

}

KeyValue::~KeyValue() {
    delete kvmap;

}