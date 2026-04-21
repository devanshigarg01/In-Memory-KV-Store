#include "PubSubManager.h"

#include <fnmatch.h>
#include <sys/socket.h>

#include <algorithm>
#include <string>

using namespace std;

string PubSubManager::subscribe(int fd, const vector<string>& channels,
                                 ClientState& client) {
    string out;
    for (auto ch : channels) {
        transform(ch.begin(), ch.end(), ch.begin(), ::tolower);
        channels_[ch].insert(fd);
        client.channelSet.insert(ch);
        int count = client.channelSet.size();
        out += "*3\r\n$9\r\nsubscribe\r\n$" + to_string(ch.size()) + "\r\n" +
               ch + "\r\n:" + to_string(count) + "\r\n";
    }
    return out;
}

string PubSubManager::unsubscribe(int fd, const vector<string>& channels,
                                   ClientState& client) {
    string out;
    // Empty list means unsubscribe from all
    vector<string> targets = channels.empty()
        ? vector<string>(client.channelSet.begin(), client.channelSet.end())
        : channels;

    for (auto ch : targets) {
        transform(ch.begin(), ch.end(), ch.begin(), ::tolower);
        channels_[ch].erase(fd);
        client.channelSet.erase(ch);
        int remaining = client.channelSet.size() + client.patternSet.size();
        out += "*3\r\n$11\r\nunsubscribe\r\n$" + to_string(ch.size()) + "\r\n" +
               ch + "\r\n:" + to_string(remaining) + "\r\n";
    }
    return out;
}

string PubSubManager::psubscribe(int fd, const vector<string>& patterns,
                                  ClientState& client) {
    string out;
    for (auto pat : patterns) {
        transform(pat.begin(), pat.end(), pat.begin(), ::tolower);
        patterns_[pat].insert(fd);
        client.patternSet.insert(pat);
        int count = client.patternSet.size();
        out += "*3\r\n$10\r\npsubscribe\r\n$" + to_string(pat.size()) + "\r\n" +
               pat + "\r\n:" + to_string(count) + "\r\n";
    }
    return out;
}

string PubSubManager::punsubscribe(int fd, const vector<string>& patterns,
                                    ClientState& client) {
    string out;
    vector<string> targets = patterns.empty()
        ? vector<string>(client.patternSet.begin(), client.patternSet.end())
        : patterns;

    for (auto pat : targets) {
        transform(pat.begin(), pat.end(), pat.begin(), ::tolower);
        patterns_[pat].erase(fd);
        client.patternSet.erase(pat);
        int remaining = client.channelSet.size() + client.patternSet.size();
        out += "*3\r\n$12\r\npunsubscribe\r\n$" + to_string(pat.size()) + "\r\n" +
               pat + "\r\n:" + to_string(remaining) + "\r\n";
    }
    return out;
}

string PubSubManager::publish(const string& channel, const string& message) {
    string ch = channel;
    transform(ch.begin(), ch.end(), ch.begin(), ::tolower);

    set<int> recipients;
    auto it = channels_.find(ch);
    if (it != channels_.end()) recipients = it->second;

    for (auto& [pat, fds] : patterns_) {
        if (fnmatch(pat.c_str(), ch.c_str(), 0) == 0)
            for (int fd : fds) recipients.insert(fd);
    }

    string msg = "*3\r\n$7\r\nmessage\r\n$" + to_string(ch.size()) + "\r\n" +
                 ch + "\r\n$" + to_string(message.size()) + "\r\n" + message + "\r\n";
    for (int fd : recipients)
        send(fd, msg.c_str(), msg.size(), 0);

    return ":" + to_string(recipients.size()) + "\r\n";
}

string PubSubManager::numsub(const vector<string>& channels) const {
    string out;
    for (auto ch : channels) {
        transform(ch.begin(), ch.end(), ch.begin(), ::tolower);
        auto it = channels_.find(ch);
        int count = (it != channels_.end()) ? it->second.size() : 0;
        out += ":" + to_string(count) + "\r\n";
    }
    return out;
}

void PubSubManager::removeClient(int fd, ClientState& client) {
    for (auto& ch : client.channelSet) channels_[ch].erase(fd);
    for (auto& pat : client.patternSet) patterns_[pat].erase(fd);
}
