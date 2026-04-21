#pragma once

#include <set>
#include <string>
#include <vector>

#include "ClientState.h"

class ReplicationManager {
public:
    explicit ReplicationManager(const std::string& role);

    // Command handlers (called from CommandHandler)
    std::string handleReplconf(std::vector<std::string>& args, ClientState& c);
    std::string handlePsync(std::vector<std::string>& args, ClientState& c);
    std::string handleWait(std::vector<std::string>& args, ClientState& c);
    std::string infoReplication() const;

    // Propagate a write command to all connected replicas
    void propagate(const std::vector<std::string>& args);

    // Track bytes received from master (slave mode only)
    void trackOffset(size_t bytes);

    // Replica handshake — connects to master, returns socket fd (-1 on failure)
    int connectToMaster(int master_port, int slave_port);

    // fd classification used by the event loop
    bool isMasterFd(int fd) const;
    void registerMasterFd(int fd);
    void unregisterMasterFd(int fd);

    bool isSlave() const;

private:
    std::string role_;
    std::string replid_;
    size_t offset_ = 0;

    std::vector<int> replicaFds_;
    std::vector<int> slavePorts_;
    std::set<int> masterFds_;

    static const std::string kEmptyRdb;
    static const std::set<std::string> kWriteCommands;

    static void sendHandshakeCommand(int sock, const std::string& cmd);
};
