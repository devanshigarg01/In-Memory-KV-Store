#pragma once

#include <queue>
#include <set>
#include <string>
#include <vector>

struct ClientState {
    int client_fd;
    bool clientQueueFlag = false;
    bool pubsubMode = false;
    std::queue<std::vector<std::string>> clientMultiQueue;
    std::set<std::string> channelSet;
    std::set<std::string> patternSet;
};

struct ReplicationState {
    std::string role;
    std::string master_replid;
    size_t master_repl_offset = 0;
};
