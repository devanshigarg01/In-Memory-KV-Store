#include "RedisStore.h"

#include <chrono>
#include <set>
#include <stdexcept>
#include <variant>

using namespace std;
using namespace std::chrono;

uint64_t RedisStore::now() const {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
        .count();
}

bool RedisStore::isExpired(const string& key) const {
    auto it = expiry_.find(key);
    if (it == expiry_.end()) return false;
    return static_cast<long long>(it->second) < static_cast<long long>(now());
}

void RedisStore::set(string key, string val, optional<uint64_t> expiry_ms) {
    data_[key] = val;
    if (expiry_ms.has_value()) {
        expiry_[key] = now() + expiry_ms.value();
    } else {
        expiry_.erase(key);
    }
}

optional<string> RedisStore::get(const string& key) {
    auto it = data_.find(key);
    if (it == data_.end()) return nullopt;
    if (isExpired(key)) {
        data_.erase(key);
        expiry_.erase(key);
        return nullopt;
    }
    return it->second;
}

bool RedisStore::del(const string& key) {
    if (data_.find(key) == data_.end()) return false;
    data_.erase(key);
    expiry_.erase(key);
    return true;
}

bool RedisStore::incr(const string& key, long long& out) {
    auto it = data_.find(key);
    if (it == data_.end()) {
        data_[key] = "1";
        out = 1;
        return true;
    }
    try {
        size_t pos;
        long long val = stoll(it->second, &pos);
        if (pos != it->second.size()) return false;
        val++;
        it->second = to_string(val);
        out = val;
        return true;
    } catch (...) {
        return false;
    }
}

vector<string> RedisStore::keys() const {
    vector<string> result;
    for (auto& [k, _] : data_) {
        if (!isExpired(k)) result.push_back(k);
    }
    return result;
}

void RedisStore::addWatcher(const string& key, int fd) {
    watchers_[key].insert(fd);
}

void RedisStore::removeWatcher(int fd) {
    for (auto& [key, fds] : watchers_) fds.erase(fd);
}

set<int> RedisStore::getWatchers(const string& key) const {
    auto it = watchers_.find(key);
    if (it == watchers_.end()) return {};
    return it->second;
}

void RedisStore::loadFromRDB(vector<string> loaded_keys,
                              unordered_map<string, string> data,
                              unordered_map<string, uint64_t> expiry) {
    keys_ = move(loaded_keys);
    data_ = move(data);
    expiry_ = move(expiry);
}
