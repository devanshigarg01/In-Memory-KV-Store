#pragma once

#include <string>
#include <utility>
#include <vector>

#include "ClientState.h"
#include "RedisStore.h"

class CommandHandler {
public:
    explicit CommandHandler(RedisStore& store);
    std::pair<std::string, bool> dispatch(std::vector<std::string>& args,
                                          ClientState& client);

private:
    RedisStore& store_;

    std::string handlePing(std::vector<std::string>& args, ClientState& c);
    std::string handleEcho(std::vector<std::string>& args, ClientState& c);
    std::string handleQuit(std::vector<std::string>& args, ClientState& c, bool& response_flag);
    std::string handleSet(std::vector<std::string>& args, ClientState& c);
    std::string handleGet(std::vector<std::string>& args, ClientState& c);
    std::string handleIncr(std::vector<std::string>& args, ClientState& c);
    std::string handleKeys(std::vector<std::string>& args, ClientState& c);
    std::string handleConfig(std::vector<std::string>& args, ClientState& c);
    std::string handleInfo(std::vector<std::string>& args, ClientState& c);
    std::string handleReplconf(std::vector<std::string>& args, ClientState& c);
    std::string handlePsync(std::vector<std::string>& args, ClientState& c);
    std::string handleWait(std::vector<std::string>& args, ClientState& c);
    std::string handleMulti(std::vector<std::string>& args, ClientState& c);
    std::string handleExec(std::vector<std::string>& args, ClientState& c);
    std::string handleDiscard(std::vector<std::string>& args, ClientState& c);
    std::string handleSubscribe(std::vector<std::string>& args, ClientState& c);
    std::string handleUnsubscribe(std::vector<std::string>& args, ClientState& c);
    std::string handlePublish(std::vector<std::string>& args, ClientState& c);
    std::string handlePsubscribe(std::vector<std::string>& args, ClientState& c);
    std::string handlePunsubscribe(std::vector<std::string>& args, ClientState& c);
    std::string handlePubsub(std::vector<std::string>& args, ClientState& c);
};
