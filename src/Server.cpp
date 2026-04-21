#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "ClientState.h"
#include "CommandHandler.h"
#include "Globals.h"
#include "RedisStore.h"
#include "RespProtocol.h"

using namespace std::chrono;
using namespace std;

// --- Global definitions (declared extern in Globals.h) ---
unordered_map<string, string> config;
ReplicationState replicationState;
vector<int> slavePortList;
vector<int> replicaFdList;
unordered_map<string, set<int>> pubsub_channels;
unordered_map<string, set<int>> pubsub_patterns;
set<int> masterFds;
set<string> write_commands = {"set", "del"};
const string empty_rdb =
    "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76"
    "\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69"
    "\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08"
    "\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66"
    "\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";

unordered_map<int, ClientState> client_map;

// --- Helpers ---

static ClientState makeClientState(int fd) {
    ClientState cs;
    cs.client_fd = fd;
    return cs;
}

static void initializeReplicationState(const string& role) {
    replicationState.role = role;
    replicationState.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    replicationState.master_repl_offset = 0;
}

static void deleteClientPubsub(int fd) {
    ClientState& cs = client_map[fd];
    for (auto& ch : cs.channelSet) pubsub_channels[ch].erase(fd);
    for (auto& pat : cs.patternSet) pubsub_patterns[pat].erase(fd);
    client_map.erase(fd);
}

// --- RDB parsing ---

static int readLength(ifstream& file) {
    unsigned char firstByte;
    file.read(reinterpret_cast<char*>(&firstByte), 1);
    if ((firstByte & 0xC0) == 0x00) return firstByte & 0x3F;
    if ((firstByte & 0xC0) == 0x40) {
        unsigned char secondByte;
        file.read(reinterpret_cast<char*>(&secondByte), 1);
        return ((firstByte & 0x3F) << 8) | secondByte;
    }
    if ((firstByte & 0xC0) == 0x80) {
        uint32_t length;
        file.read(reinterpret_cast<char*>(&length), 4);
        return length;
    }
    int specialType = firstByte & 0x3F;
    switch (specialType) {
        case 0: return 1;
        case 1: return 2;
        case 2: return 4;
        case 3: { int c = readLength(file); readLength(file); return c; }
        default: throw runtime_error("Unknown special encoding");
    }
}

static string readString(ifstream& file, int length) {
    vector<char> buf(length);
    file.read(buf.data(), length);
    return string(buf.begin(), buf.end());
}

static void parseRDB(const string& filename, RedisStore& store) {
    ifstream file(filename, ios::binary);
    if (!file) return;

    char header[9] = {0};
    file.read(header, 9);
    if (string(header, 5) != "REDIS") { cerr << "Invalid RDB file header.\n"; return; }

    unsigned char type;
    file.read(reinterpret_cast<char*>(&type), 1);

    while (type == 0xFA) {
        int nl = readLength(file); readString(file, nl);
        int vl = readLength(file); readString(file, vl);
        file.read(reinterpret_cast<char*>(&type), 1);
    }

    if (type != 0xFE) return;
    readLength(file);  // db index

    vector<string> keys;
    unordered_map<string, string> data;
    unordered_map<string, uint64_t> expiry;

    while (true) {
        file.read(reinterpret_cast<char*>(&type), 1);
        if (file.eof() || type == 0xFF) break;
        if (type == 0xFB) { readLength(file); readLength(file); continue; }

        bool flag_expiry = false;
        uint64_t expiry_ms = 0;
        if (type == 0xFC || type == 0xFD) {
            flag_expiry = true;
            if (type == 0xFD) {
                uint32_t s; file.read(reinterpret_cast<char*>(&s), 4);
                expiry_ms = static_cast<uint64_t>(s) * 1000;
            } else {
                file.read(reinterpret_cast<char*>(&expiry_ms), 8);
            }
            file.read(reinterpret_cast<char*>(&type), 1);
        }

        if (type == 0x00) {
            string key = readString(file, readLength(file));
            string val = readString(file, readLength(file));
            data[key] = val;
            keys.push_back(key);
            if (flag_expiry) expiry[key] = expiry_ms;
            continue;
        }
        break;
    }

    store.loadFromRDB(move(keys), move(data), move(expiry));
}

// --- Replication handshake ---

static void sendCommand(int sock, const string& cmd) {
    send(sock, cmd.c_str(), cmd.length(), 0);
    char buf[1024] = {0};
    ssize_t n = recv(sock, buf, sizeof(buf) - 1, 0);
    if (n > 0) cout << "Received: " << buf << "\n";
}

static int ReplicaServer(int master_port, int slave_port) {
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

    sendCommand(fd, "*1\r\n$4\r\nPING\r\n");

    string port_str = to_string(slave_port);
    sendCommand(fd, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                        to_string(port_str.length()) + "\r\n" + port_str + "\r\n");
    sendCommand(fd, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");

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

// --- Thin RespParser: parse RESP → args → CommandHandler::dispatch ---

static pair<string, bool> RespParser(istream& input, ClientState& client,
                                      CommandHandler& handler) {
    string word;
    getline(input, word);

    if (word[0] == ':') {
        RespProtocol::parseInteger(input);
        return {"", false};
    }
    if (word.empty() || word[0] != '*')
        throw runtime_error("Invalid RESP format");

    int numArgs = stoi(word.substr(1));
    vector<string> args;
    size_t totalBytes = 0;
    for (int i = 0; i < numArgs; ++i) {
        string arg = RespProtocol::parseBulkString(input);
        totalBytes += 1 + to_string(arg.size()).length() + 2 + arg.size() + 2;
        args.push_back(arg);
    }
    totalBytes += 1 + to_string(numArgs).length() + 2;

    auto [output, response_flag] = handler.dispatch(args, client);

    if (replicationState.role == "slave")
        replicationState.master_repl_offset += totalBytes;

    return {output, response_flag};
}

// --- main ---

int main(int argc, char** argv) {
    cout << unitbuf;
    cerr << unitbuf;

    config["dir"] = "/tmp";
    config["dbfilename"] = "dump.rdb";
    bool has_rdb = false;
    int port_number = 6379;
    string server_role = "master";
    string master_host;
    int master_port = 0;

    for (int i = 1; i < argc - 1; i += 2) {
        string flag = argv[i], value = argv[i + 1];
        if (flag == "--dir")          config["dir"] = value;
        else if (flag == "--dbfilename") { config["dbfilename"] = value; has_rdb = true; }
        else if (flag == "--port")    port_number = stoi(value);
        else if (flag == "--replicaof") {
            server_role = "slave";
            istringstream ss(value);
            ss >> master_host >> master_port;
            i++;
        }
    }

    RedisStore store;
    if (has_rdb)
        parseRDB(config["dir"] + "/" + config["dbfilename"], store);

    initializeReplicationState(server_role);

    CommandHandler handler(store);

    int master_fd = -1;
    if (server_role == "slave")
        master_fd = ReplicaServer(master_port, port_number);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { cerr << "Failed to create server socket\n"; return 1; }

    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_number);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        cerr << "Failed to bind to port " << port_number << "\n"; return 1;
    }
    if (listen(server_fd, 5) != 0) { cerr << "listen failed\n"; return 1; }

    fd_set active_fds, ready_fds;
    FD_ZERO(&active_fds);
    FD_SET(server_fd, &active_fds);
    int max_fd = server_fd;

    if (master_fd >= 0) {
        FD_SET(master_fd, &active_fds);
        if (master_fd > max_fd) max_fd = master_fd;
        masterFds.insert(master_fd);
        client_map[master_fd] = makeClientState(master_fd);
    }

    while (true) {
        ready_fds = active_fds;
        if (select(max_fd + 1, &ready_fds, nullptr, nullptr, nullptr) < 0) {
            cerr << "Select error\n"; break;
        }

        for (int fd = 0; fd <= max_fd; fd++) {
            if (!FD_ISSET(fd, &ready_fds)) continue;

            if (fd == server_fd) {
                int new_fd = accept(server_fd, nullptr, nullptr);
                if (new_fd < 0) { cerr << "Accept failed\n"; continue; }
                FD_SET(new_fd, &active_fds);
                if (new_fd > max_fd) max_fd = new_fd;
                client_map[new_fd] = makeClientState(new_fd);
            } else {
                char buffer[1024];
                int bytes_rec = recv(fd, buffer, sizeof(buffer) - 1, 0);
                if (bytes_rec <= 0) {
                    deleteClientPubsub(fd);
                    masterFds.erase(fd);
                    close(fd);
                    FD_CLR(fd, &active_fds);
                    continue;
                }
                stringstream input(string(buffer, bytes_rec));
                ClientState& curr_client = client_map[fd];
                bool is_master = masterFds.find(fd) != masterFds.end();
                while (input.peek() == '*') {
                    try {
                        auto [response, response_flag] =
                            RespParser(input, curr_client, handler);
                        bool should_reply = response_flag &&
                            (!is_master || (!response.empty() && response[0] == '*'));
                        if (should_reply)
                            send(fd, response.c_str(), response.size(), 0);
                    } catch (...) { break; }
                }
            }
        }
    }

    close(server_fd);
    return 0;
}
