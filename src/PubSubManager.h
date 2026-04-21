#pragma once

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "ClientState.h"

class PubSubManager {
public:
    // Returns the full RESP subscribe confirmation messages to send back
    std::string subscribe(int fd, const std::vector<std::string>& channels,
                          ClientState& client);
    std::string unsubscribe(int fd, const std::vector<std::string>& channels,
                            ClientState& client);
    std::string psubscribe(int fd, const std::vector<std::string>& patterns,
                           ClientState& client);
    std::string punsubscribe(int fd, const std::vector<std::string>& patterns,
                             ClientState& client);

    // Returns ":N\r\n" — count of clients that received the message
    std::string publish(const std::string& channel, const std::string& message);

    // Returns RESP array of channel:count pairs for PUBSUB NUMSUB
    std::string numsub(const std::vector<std::string>& channels) const;

    // Cleans up all subscriptions for a disconnecting client
    void removeClient(int fd, ClientState& client);

private:
    std::unordered_map<std::string, std::set<int>> channels_;
    std::unordered_map<std::string, std::set<int>> patterns_;
};
