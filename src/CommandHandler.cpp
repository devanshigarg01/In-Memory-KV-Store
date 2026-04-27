#include "CommandHandler.h"

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "PubSubManager.h"
#include "ReplicationManager.h"
#include "RespProtocol.h"

using namespace std;

CommandHandler::CommandHandler(RedisStore& store, PubSubManager& pubsub,
                               ReplicationManager& repl,
                               const unordered_map<string, string>& config)
    : store_(store), pubsub_(pubsub), repl_(repl), config_(config) {}

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
        "ping",      "quit",        "reset"};
    if (client.pubsubMode &&
        pubsub_allowed.find(command) == pubsub_allowed.end()) {
        return {"-ERR Can't execute '" + args[0] +
                    "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / "
                    "RESET are allowed in this context\r\n",
                true};
    }

    string output;
    if (command == "ping")
        output = handlePing(args, client);
    else if (command == "echo")
        output = handleEcho(args, client);
    else if (command == "quit")
        output = handleQuit(args, client, response_flag);
    else if (command == "set")
        output = handleSet(args, client);
    else if (command == "get")
        output = handleGet(args, client);
    else if (command == "incr")
        output = handleIncr(args, client);
    else if (command == "keys")
        output = handleKeys(args, client);
    else if (command == "config")
        output = handleConfig(args, client);
    else if (command == "info")
        output = handleInfo(args, client);
    else if (command == "replconf")
        output = handleReplconf(args, client);
    else if (command == "psync")
        output = handlePsync(args, client);
    else if (command == "wait")
        output = handleWait(args, client);
    else if (command == "multi")
        output = handleMulti(args, client);
    else if (command == "exec")
        output = handleExec(args, client);
    else if (command == "discard")
        output = handleDiscard(args, client);
    else if (command == "subscribe")
        output = handleSubscribe(args, client);
    else if (command == "unsubscribe")
        output = handleUnsubscribe(args, client);
    else if (command == "publish")
        output = handlePublish(args, client);
    else if (command == "psubscribe")
        output = handlePsubscribe(args, client);
    else if (command == "punsubscribe")
        output = handlePunsubscribe(args, client);
    else if (command == "pubsub")
        output = handlePubsub(args, client);
    else if (command == "watch")
        output = handleWatch(args, client);

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
            out +=
                "$" + to_string(args[i].length()) + "\r\n" + args[i] + "\r\n";
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
        if (opt == "px" && args.size() > 4)
            expiry_ms = static_cast<uint64_t>(stoll(args[4]));
    }

    store_.set(key, value, expiry_ms);
    repl_.propagate(args);
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
        auto it = config_.find(param);
        if (it != config_.end()) {
            string val = it->second;
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
    if (sub == "replication") return repl_.infoReplication();
    return "$-1\r\n";
}

string CommandHandler::handleReplconf(vector<string>& args, ClientState& c) {
    return repl_.handleReplconf(args, c);
}

string CommandHandler::handlePsync(vector<string>& args, ClientState& c) {
    return repl_.handlePsync(args, c);
}

string CommandHandler::handleWait(vector<string>& args, ClientState& c) {
    return repl_.handleWait(args, c);
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
    return pubsub_.subscribe(c.client_fd, {args.begin() + 1, args.end()}, c);
}

string CommandHandler::handleUnsubscribe(vector<string>& args, ClientState& c) {
    return pubsub_.unsubscribe(c.client_fd, {args.begin() + 1, args.end()}, c);
}

string CommandHandler::handlePublish(vector<string>& args, ClientState& c) {
    return pubsub_.publish(args[1], args[2]);
}

string CommandHandler::handlePsubscribe(vector<string>& args, ClientState& c) {
    c.pubsubMode = true;
    return pubsub_.psubscribe(c.client_fd, {args.begin() + 1, args.end()}, c);
}

string CommandHandler::handlePunsubscribe(vector<string>& args,
                                          ClientState& c) {
    return pubsub_.punsubscribe(c.client_fd, {args.begin() + 1, args.end()}, c);
}

string CommandHandler::handlePubsub(vector<string>& args, ClientState& c) {
    string sub = args[1];
    transform(sub.begin(), sub.end(), sub.begin(), ::tolower);
    if (sub == "numsub") return pubsub_.numsub({args.begin() + 2, args.end()});
    return "-ERR unknown subcommand\r\n";
}

string CommandHandler::handleWatch(vector<string>& args, ClientState& c) {
    if (!c.clientQueueFlag) return "-ERR WATCH inside MULTI is not allowed\r\n";
    return "+OK\r\n";
}
