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
    typedef std::vector<Value> valueVector;
    KeyValue();
    ~KeyValue();
    void add_kv(const Key &k, const Value &v);
    void add_kv_final(const Key &k, const Value&v);
    typename std::map<Key, valueVector>::iterator begin() { return kvmap->begin();};
    typename std::map<Key, valueVector>::iterator end() { return kvmap->end();};
    std::map<Key,Value> get_result() const { return *finalMap;};
private:
    std::map<Key, valueVector>  *kvmap;
    std::map<Key,Value>         *finalMap;   // this map is for outputting
};



} // namespace

#endif //MAPREDUCECPP_KEYVALUE_H
