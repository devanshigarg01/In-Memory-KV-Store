#pragma once

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "ClientState.h"

extern std::unordered_map<std::string, std::string> config;
extern ReplicationState replicationState;
extern std::vector<int> slavePortList;
extern std::vector<int> replicaFdList;
extern std::unordered_map<std::string, std::set<int>> pubsub_channels;
extern std::unordered_map<std::string, std::set<int>> pubsub_patterns;
extern std::set<int> masterFds;
extern std::set<std::string> write_commands;
extern const std::string empty_rdb;
