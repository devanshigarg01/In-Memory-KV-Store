#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

class RedisStore {
public:
    void set(std::string key, std::string val,
             std::optional<uint64_t> expiry_ms = std::nullopt);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);
    bool incr(const std::string& key, long long& out);
    std::vector<std::string> keys() const;

    void loadFromRDB(std::vector<std::string> loaded_keys,
                     std::unordered_map<std::string, std::string> data,
                     std::unordered_map<std::string, uint64_t> expiry);

private:
    std::unordered_map<std::string, std::string> data_;
    std::unordered_map<std::string, uint64_t> expiry_;
    std::vector<std::string> keys_;

    uint64_t now() const;
    bool isExpired(const std::string& key) const;
};
