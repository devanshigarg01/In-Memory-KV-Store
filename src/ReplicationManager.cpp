#include "ReplicationManager.h"

#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include "RespProtocol.h"

using namespace std;
using namespace std::chrono;

// ---- static data ----

const string ReplicationManager::kEmptyRdb =
    "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76"
    "\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69"
    "\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08"
    "\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66"
    "\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";

const set<string> ReplicationManager::kWriteCommands = {"set", "del"};

// ---- constructor ----

ReplicationManager::ReplicationManager(const string& role)
    : role_(role),
      replid_("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
      offset_(0) {}

// ---- command handlers ----

string ReplicationManager::handleReplconf(vector<string>& args, ClientState& c) {
    string sub = args[1];
    transform(sub.begin(), sub.end(), sub.begin(), ::tolower);
    if (sub == "listening-port") {
        slavePorts_.push_back(stoi(args[2]));
        return "+OK\r\n";
    }
    if (sub == "getack") {
        return RespProtocol::writeBulkString({"REPLCONF", "ACK", to_string(offset_)});
    }
    return "+OK\r\n";
}

string ReplicationManager::handlePsync(vector<string>& args, ClientState& c) {
    string out = "+FULLRESYNC " + replid_ + " " + to_string(offset_) + "\r\n";
    out += "$" + to_string(kEmptyRdb.length()) + "\r\n" + kEmptyRdb;
    replicaFds_.push_back(c.client_fd);
    return out;
}

string ReplicationManager::handleWait(vector<string>& args, ClientState& c) {
    int numReplicas = stoi(args[1]);
    long long timeout = stoll(args[2]);

    if (replicaFds_.empty() || offset_ == 0)
        return ":" + to_string(replicaFds_.size()) + "\r\n";

    string getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    for (auto rfd : replicaFds_)
        send(rfd, getack.c_str(), getack.length(), 0);

    int ackCount = 0;
    auto start = steady_clock::now();
    while (ackCount < numReplicas) {
        auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start).count();
        if (elapsed >= timeout) break;
        long long remaining = timeout - elapsed;

        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = 0;
        for (auto rfd : replicaFds_) { FD_SET(rfd, &rfds); if (rfd > maxfd) maxfd = rfd; }

        struct timeval tv{remaining / 1000, (remaining % 1000) * 1000};
        if (select(maxfd + 1, &rfds, nullptr, nullptr, &tv) <= 0) break;

        for (auto rfd : replicaFds_) {
            if (FD_ISSET(rfd, &rfds)) {
                char buf[256];
                int n = recv(rfd, buf, sizeof(buf) - 1, 0);
                if (n > 0) {
                    string resp(buf, n);
                    if (resp.find("REPLCONF") != string::npos ||
                        resp.find("ACK") != string::npos)
                        ackCount++;
                }
            }
        }
    }
    return ":" + to_string(ackCount) + "\r\n";
}

string ReplicationManager::infoReplication() const {
    string body = "role:" + role_ + "\r\n" +
                  "master_repl_offset:" + to_string(offset_) + "\r\n" +
                  "master_replid:" + replid_ + "\r\n";
    return "$" + to_string(body.length()) + "\r\n" + body + "\r\n";
}

// ---- propagation ----

void ReplicationManager::propagate(const vector<string>& args) {
    if (role_ == "slave") return;
    string cmd = args[0];
    transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);
    if (kWriteCommands.find(cmd) == kWriteCommands.end()) return;

    string encoded = RespProtocol::writeBulkString(args);
    for (auto rfd : replicaFds_)
        send(rfd, encoded.c_str(), encoded.length(), 0);
    offset_ += encoded.length();
}

void ReplicationManager::trackOffset(size_t bytes) {
    if (role_ == "slave") offset_ += bytes;
}

// ---- master fd tracking ----

bool ReplicationManager::isMasterFd(int fd) const {
    return masterFds_.count(fd) > 0;
}

void ReplicationManager::registerMasterFd(int fd) {
    masterFds_.insert(fd);
}

void ReplicationManager::unregisterMasterFd(int fd) {
    masterFds_.erase(fd);
}

bool ReplicationManager::isSlave() const {
    return role_ == "slave";
}

// ---- handshake ----

void ReplicationManager::sendHandshakeCommand(int sock, const string& cmd) {
    send(sock, cmd.c_str(), cmd.length(), 0);
    char buf[1024] = {0};
    ssize_t n = recv(sock, buf, sizeof(buf) - 1, 0);
    if (n > 0) cout << "Received: " << buf << "\n";
}

int ReplicationManager::connectToMaster(int master_port, int slave_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("Socket creation failed"); return -1; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(master_port);
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) <= 0 ||
        connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("Connection to master failed");
        close(fd);
        return -1;
    }

    sendHandshakeCommand(fd, "*1\r\n$4\r\nPING\r\n");

    string port_str = to_string(slave_port);
    sendHandshakeCommand(fd, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                                 to_string(port_str.length()) + "\r\n" + port_str + "\r\n");
    sendHandshakeCommand(fd, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");

    string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    send(fd, psync.c_str(), psync.length(), 0);

    string fullresync = RespProtocol::recvLine(fd);
    cout << "Received: " << fullresync << "\n";
    string rdb_header = RespProtocol::recvLine(fd);
    if (!rdb_header.empty() && rdb_header[0] == '$') {
        int rdb_len = stoi(rdb_header.substr(1));
        vector<char> rdb_data(rdb_len);
        RespProtocol::recvExact(fd, rdb_data.data(), rdb_len);
    }

    return fd;
}
