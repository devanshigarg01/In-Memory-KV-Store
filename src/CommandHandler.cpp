#include "CommandHandler.h"

#include <arpa/inet.h>
#include <fnmatch.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "Globals.h"
#include "RespProtocol.h"

using namespace std;
using namespace std::chrono;

CommandHandler::CommandHandler(RedisStore& store) : store_(store) {}

// ---------- dispatch ----------

pair<string, bool> CommandHandler::dispatch(vector<string>& args,
                                             ClientState& client) {
    bool response_flag = true;
    string command = args[0];
    transform(command.begin(), command.end(), command.begin(), ::tolower);

    // MULTI queue: buffer all commands except EXEC/DISCARD/MULTI
    if (command != "exec" && command != "discard" && command != "multi" &&
        client.clientQueueFlag) {
        client.clientMultiQueue.push(args);
        return {"+QUEUED\r\n", true};
    }

    // Pub/Sub mode: only allow specific commands
    static const set<string> pubsub_allowed = {
        "subscribe", "unsubscribe", "psubscribe", "punsubscribe",
        "ping", "quit", "reset"};
    if (client.pubsubMode &&
        pubsub_allowed.find(command) == pubsub_allowed.end()) {
        return {"-ERR Can't execute '" + args[0] +
                    "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / "
                    "RESET are allowed in this context\r\n",
                true};
    }

    string output;
    if (command == "ping")         output = handlePing(args, client);
    else if (command == "echo")    output = handleEcho(args, client);
    else if (command == "quit")    output = handleQuit(args, client, response_flag);
    else if (command == "set")     output = handleSet(args, client);
    else if (command == "get")     output = handleGet(args, client);
    else if (command == "incr")    output = handleIncr(args, client);
    else if (command == "keys")    output = handleKeys(args, client);
    else if (command == "config")  output = handleConfig(args, client);
    else if (command == "info")    output = handleInfo(args, client);
    else if (command == "replconf") output = handleReplconf(args, client);
    else if (command == "psync")   output = handlePsync(args, client);
    else if (command == "wait")    output = handleWait(args, client);
    else if (command == "multi")   output = handleMulti(args, client);
    else if (command == "exec")    output = handleExec(args, client);
    else if (command == "discard") output = handleDiscard(args, client);
    else if (command == "subscribe")   output = handleSubscribe(args, client);
    else if (command == "unsubscribe") output = handleUnsubscribe(args, client);
    else if (command == "publish")     output = handlePublish(args, client);
    else if (command == "psubscribe")  output = handlePsubscribe(args, client);
    else if (command == "punsubscribe") output = handlePunsubscribe(args, client);
    else if (command == "pubsub")  output = handlePubsub(args, client);

    return {output, response_flag};
}

// ---------- handlers ----------

string CommandHandler::handlePing(vector<string>& args, ClientState& c) {
    if (c.pubsubMode) return "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
    return "+PONG\r\n";
}

string CommandHandler::handleEcho(vector<string>& args, ClientState& c) {
    if (args.size() > 2) {
        string out = "*" + to_string(args.size() - 1) + "\r\n";
        for (size_t i = 1; i < args.size(); i++)
            out += "$" + to_string(args[i].length()) + "\r\n" + args[i] + "\r\n";
        return out;
    }
    return "$" + to_string(args[1].length()) + "\r\n" + args[1] + "\r\n";
}

string CommandHandler::handleQuit(vector<string>& args, ClientState& c,
                                   bool& response_flag) {
    c.pubsubMode = false;
    response_flag = false;
    return "+OK\r\n";
}

string CommandHandler::handleSet(vector<string>& args, ClientState& c) {
    string key = args[1];
    string value = args[2];
    optional<uint64_t> expiry_ms;

    if (args.size() > 3) {
        string opt = args[3];
        transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
        if (opt == "px" && args.size() > 4) {
            expiry_ms = static_cast<uint64_t>(stoll(args[4]));
        }
    }

    store_.set(key, value, expiry_ms);

    // propagate to replicas
    string cmd_str = RespProtocol::writeBulkString(args);
    if (replicationState.role == "master") {
        for (auto rfd : replicaFdList)
            send(rfd, cmd_str.c_str(), cmd_str.length(), 0);
        replicationState.master_repl_offset += cmd_str.length();
    }

    return "+OK\r\n";
}

string CommandHandler::handleGet(vector<string>& args, ClientState& c) {
    auto val = store_.get(args[1]);
    if (!val.has_value()) return "$-1\r\n";
    return "$" + to_string(val->length()) + "\r\n" + *val + "\r\n";
}

string CommandHandler::handleIncr(vector<string>& args, ClientState& c) {
    long long result;
    if (!store_.incr(args[1], result))
        return "-ERR value is not an integer or out of range\r\n";
    return ":" + to_string(result) + "\r\n";
}

string CommandHandler::handleKeys(vector<string>& args, ClientState& c) {
    auto all_keys = store_.keys();
    string out = "*" + to_string(all_keys.size()) + "\r\n";
    for (auto& k : all_keys)
        out += "$" + to_string(k.size()) + "\r\n" + k + "\r\n";
    return out;
}

string CommandHandler::handleConfig(vector<string>& args, ClientState& c) {
    string sub = args[1];
    transform(sub.begin(), sub.end(), sub.begin(), ::tolower);
    if (sub == "get" && args.size() > 2) {
        string param = args[2];
        transform(param.begin(), param.end(), param.begin(), ::tolower);
        if (config.find(param) != config.end()) {
            string val = config[param];
            return "*2\r\n$" + to_string(param.length()) + "\r\n" + param +
                   "\r\n$" + to_string(val.length()) + "\r\n" + val + "\r\n";
        }
        return "*0\r\n";
    }
    return "-ERR unknown subcommand\r\n";
}

string CommandHandler::handleInfo(vector<string>& args, ClientState& c) {
    string sub = args[1];
    transform(sub.begin(), sub.end(), sub.begin(), ::tolower);
    if (sub == "replication") {
        string body = "role:" + replicationState.role + "\r\n" +
                      "master_repl_offset:" +
                      to_string(replicationState.master_repl_offset) + "\r\n" +
                      "master_replid:" + replicationState.master_replid + "\r\n";
        return "$" + to_string(body.length()) + "\r\n" + body + "\r\n";
    }
    return "$-1\r\n";
}

string CommandHandler::handleReplconf(vector<string>& args, ClientState& c) {
    string sub = args[1];
    transform(sub.begin(), sub.end(), sub.begin(), ::tolower);
    if (sub == "listening-port") {
        slavePortList.push_back(stoi(args[2]));
        return "+OK\r\n";
    }
    if (sub == "getack") {
        string offset = to_string(replicationState.master_repl_offset);
        return RespProtocol::writeBulkString({"REPLCONF", "ACK", offset});
    }
    return "+OK\r\n";
}

string CommandHandler::handlePsync(vector<string>& args, ClientState& c) {
    string out = "+FULLRESYNC " + replicationState.master_replid + " " +
                 to_string(replicationState.master_repl_offset) + "\r\n";
    out += "$" + to_string(empty_rdb.length()) + "\r\n" + empty_rdb;
    replicaFdList.push_back(c.client_fd);
    return out;
}

string CommandHandler::handleWait(vector<string>& args, ClientState& c) {
    int numReplicas = stoi(args[1]);
    long long timeout = stoll(args[2]);

    if (replicaFdList.empty() || replicationState.master_repl_offset == 0)
        return ":" + to_string(replicaFdList.size()) + "\r\n";

    string getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    for (auto rfd : replicaFdList)
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
        for (auto rfd : replicaFdList) {
            FD_SET(rfd, &rfds);
            if (rfd > maxfd) maxfd = rfd;
        }
        struct timeval tv{remaining / 1000, (remaining % 1000) * 1000};
        if (select(maxfd + 1, &rfds, nullptr, nullptr, &tv) <= 0) break;

        for (auto rfd : replicaFdList) {
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

string CommandHandler::handleMulti(vector<string>& args, ClientState& c) {
    c.clientQueueFlag = true;
    return "+OK\r\n";
}

string CommandHandler::handleExec(vector<string>& args, ClientState& c) {
    if (!c.clientQueueFlag) return "-ERR EXEC without MULTI\r\n";
    c.clientQueueFlag = false;
    if (c.clientMultiQueue.empty()) return "*0\r\n";

    int count = c.clientMultiQueue.size();
    string out = "*" + to_string(count) + "\r\n";
    while (!c.clientMultiQueue.empty()) {
        auto queued = c.clientMultiQueue.front();
        c.clientMultiQueue.pop();
        auto [resp, flag] = dispatch(queued, c);
        out += resp;
    }
    return out;
}

string CommandHandler::handleDiscard(vector<string>& args, ClientState& c) {
    if (!c.clientQueueFlag) return "-ERR DISCARD without MULTI\r\n";
    c.clientQueueFlag = false;
    while (!c.clientMultiQueue.empty()) c.clientMultiQueue.pop();
    return "+OK\r\n";
}

string CommandHandler::handleSubscribe(vector<string>& args, ClientState& c) {
    c.pubsubMode = true;
    string out;
    for (size_t i = 1; i < args.size(); i++) {
        string ch = args[i];
        transform(ch.begin(), ch.end(), ch.begin(), ::tolower);
        pubsub_channels[ch].insert(c.client_fd);
        c.channelSet.insert(ch);
        int count = c.channelSet.size();
        out += "*3\r\n$9\r\nsubscribe\r\n$" + to_string(ch.size()) + "\r\n" +
               ch + "\r\n:" + to_string(count) + "\r\n";
    }
    return out;
}

string CommandHandler::handleUnsubscribe(vector<string>& args, ClientState& c) {
    string out;
    if (args.size() == 1) {
        for (auto it = c.channelSet.begin(); it != c.channelSet.end();) {
            auto ch = *it;
            pubsub_channels[ch].erase(c.client_fd);
            it = c.channelSet.erase(it);
            int remaining = c.channelSet.size() + c.patternSet.size();
            out += "*3\r\n$11\r\nunsubscribe\r\n$" + to_string(ch.size()) +
                   "\r\n" + ch + "\r\n:" + to_string(remaining) + "\r\n";
        }
    } else {
        for (size_t i = 1; i < args.size(); i++) {
            string ch = args[i];
            transform(ch.begin(), ch.end(), ch.begin(), ::tolower);
            pubsub_channels[ch].erase(c.client_fd);
            c.channelSet.erase(ch);
            int remaining = c.channelSet.size() + c.patternSet.size();
            out += "*3\r\n$11\r\nunsubscribe\r\n$" + to_string(ch.size()) +
                   "\r\n" + ch + "\r\n:" + to_string(remaining) + "\r\n";
        }
    }
    return out;
}

string CommandHandler::handlePublish(vector<string>& args, ClientState& c) {
    string channel = args[1];
    string message = args[2];
    transform(channel.begin(), channel.end(), channel.begin(), ::tolower);

    set<int> subscribers;
    if (pubsub_channels.find(channel) != pubsub_channels.end())
        subscribers = pubsub_channels[channel];

    for (auto& [pattern, fds] : pubsub_patterns) {
        if (fnmatch(pattern.c_str(), channel.c_str(), 0) == 0)
            for (int fd : fds) subscribers.insert(fd);
    }

    string msg = "*3\r\n$7\r\nmessage\r\n$" + to_string(channel.size()) +
                 "\r\n" + channel + "\r\n$" + to_string(message.size()) +
                 "\r\n" + message + "\r\n";
    for (int fd : subscribers)
        send(fd, msg.c_str(), msg.size(), 0);

    return ":" + to_string(subscribers.size()) + "\r\n";
}

string CommandHandler::handlePsubscribe(vector<string>& args, ClientState& c) {
    c.pubsubMode = true;
    vector<string> out_parts;
    for (size_t i = 1; i < args.size(); i++) {
        string pat = args[i];
        transform(pat.begin(), pat.end(), pat.begin(), ::tolower);
        pubsub_patterns[pat].insert(c.client_fd);
        c.patternSet.insert(pat);
        out_parts.push_back("psubscribe");
        out_parts.push_back(pat);
    }
    return RespProtocol::writeBulkString(out_parts);
}

string CommandHandler::handlePunsubscribe(vector<string>& args, ClientState& c) {
    vector<string> out_parts;
    if (args.size() == 1) {
        for (auto it = c.patternSet.begin(); it != c.patternSet.end();) {
            auto pat = *it;
            pubsub_patterns[pat].erase(c.client_fd);
            it = c.patternSet.erase(it);
            out_parts.push_back("punsubscribe");
            out_parts.push_back(pat);
        }
    } else {
        for (size_t i = 1; i < args.size(); i++) {
            string pat = args[i];
            transform(pat.begin(), pat.end(), pat.begin(), ::tolower);
            pubsub_patterns[pat].erase(c.client_fd);
            c.patternSet.erase(pat);
            out_parts.push_back("punsubscribe");
            out_parts.push_back(pat);
        }
    }
    return RespProtocol::writeBulkString(out_parts);
}

string CommandHandler::handlePubsub(vector<string>& args, ClientState& c) {
    string sub = args[1];
    transform(sub.begin(), sub.end(), sub.begin(), ::tolower);
    if (sub == "numsub") {
        string out;
        for (size_t i = 2; i < args.size(); i++) {
            string ch = args[i];
            transform(ch.begin(), ch.end(), ch.begin(), ::tolower);
            out += ":" + to_string(pubsub_channels[ch].size()) + "\r\n";
        }
        return out;
    }
    return "-ERR unknown subcommand\r\n";
}
