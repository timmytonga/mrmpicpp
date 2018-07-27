//
// Created by timmytonga on 7/24/18.
//

#include "keyvalue.h"

using namespace MAPREDUCE_NAMESPACE;

template <class Key, class Value>
KeyValue<Key, Value>::KeyValue(){
    kvmap = new std::map<Key,valueVector>;
    finalMap = new std::map<Key, Value>;
}



template <class Key, class Value>
void KeyValue<Key, Value>::add_kv(const Key &k, const Value &v) {
    /* should be called by emit in mapreduce */
    (*kvmap)[k].push_back(v);
}

template <class Key, class Value>
void KeyValue<Key, Value>::add_kv_final(const Key &k, const Value &v) {
    /* should be called by emit in mapreduce */
    (*finalMap)[k] = v;
}


template <class Key, class Value>
KeyValue<Key, Value>::~KeyValue() {
    delete kvmap;
    delete finalMap;
}
