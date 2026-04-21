#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "ClientState.h"
#include "CommandHandler.h"
#include "PubSubManager.h"
#include "RDBParser.h"
#include "ReplicationManager.h"
#include "RespProtocol.h"

using namespace std;

// --- Global definitions ---
unordered_map<string, string> config;
unordered_map<int, ClientState> client_map;

// --- Client helpers ---

static ClientState makeClientState(int fd) {
    ClientState cs;
    cs.client_fd = fd;
    return cs;
}

static void removeClient(int fd, PubSubManager& pubsub, ReplicationManager& repl) {
    ClientState& cs = client_map[fd];
    pubsub.removeClient(fd, cs);
    repl.unregisterMasterFd(fd);
    client_map.erase(fd);
}


// --- Thin RespParser ---

static pair<string, bool> RespParser(istream& input, ClientState& client,
                                      CommandHandler& handler,
                                      ReplicationManager& repl) {
    string word;
    getline(input, word);

    if (!word.empty() && word[0] == ':') {
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
    repl.trackOffset(totalBytes);
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
        if (flag == "--dir")             config["dir"] = value;
        else if (flag == "--dbfilename") { config["dbfilename"] = value; has_rdb = true; }
        else if (flag == "--port")       port_number = stoi(value);
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

    PubSubManager pubsub;
    ReplicationManager repl(server_role);
    CommandHandler handler(store, pubsub, repl, config);

    int master_fd = -1;
    if (server_role == "slave") {
        master_fd = repl.connectToMaster(master_port, port_number);
        if (master_fd < 0) { cerr << "Failed to connect to master\n"; return 1; }
    }

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
        repl.registerMasterFd(master_fd);
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
                    removeClient(fd, pubsub, repl);
                    close(fd);
                    FD_CLR(fd, &active_fds);
                    continue;
                }
                stringstream input(string(buffer, bytes_rec));
                ClientState& curr_client = client_map[fd];
                bool is_master = repl.isMasterFd(fd);
                while (input.peek() == '*') {
                    try {
                        auto [response, response_flag] =
                            RespParser(input, curr_client, handler, repl);
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
