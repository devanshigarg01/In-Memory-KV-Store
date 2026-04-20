#include <arpa/inet.h>
#include <fnmatch.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
// #include "parser.h"
using namespace std::chrono;
using namespace std;

unordered_map<string, string> config;
unordered_map<string, string> mp;
unordered_map<string, uint64_t> expiry;
vector<string> keys;
const string empty_rdb =
    "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76"
    "\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69"
    "\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08"
    "\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66"
    "\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
set<string> write_commands = {"set", "del"};
vector<int> slavePortList;
vector<int> replicaFdList;
unordered_map<string, set<int>> pubsub_channels;
unordered_map<string, set<int>> pubsub_patterns;

struct ClientState {
    int client_fd;
    bool clientQueueFlag;
    queue<vector<string>> clientMultiQueue;
    set<string> channelSet;
    set<string> patternSet;
};
unordered_map<int, ClientState> client_map;

struct ReplicationState {
    string role;  // master or slave
    string master_replid;
    size_t master_repl_offset;
};

ClientState initializeClientState(int fd, ClientState& curr_client) {
    curr_client.client_fd = fd;
    curr_client.clientMultiQueue = queue<vector<string>>();
    curr_client.clientQueueFlag = false;
    curr_client.channelSet = set<string>();
    curr_client.patternSet = set<string>();

    return curr_client;
}

ReplicationState replicationState;

void initializeReplicationState(string server_role) {
    replicationState.role = server_role;
    replicationState.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    replicationState.master_repl_offset = 0;
}

uint64_t getCurrentTime() {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
        .count();
}

int parseInteger(istream& input) {
    string num;
    getline(input, num);
    return stoi(num);
}

string parseBulkString(istream& input) {
    string len;
    getline(input, len);
    // cout << "len" << len << endl;

    if (len.empty() || len[0] != '$') {
        throw runtime_error("Invalid bulk string format");
    }

    int length = stoi(len.substr(1));

    if (length < 0) {
        return "";
    }

    string buffer(length, '\0');
    input.read(&buffer[0], length);
    // cout << "buffer" << buffer << endl;

    input.ignore(2);
    return buffer;
}

string writeBulkString(vector<string> data) {
    string output = "*" + to_string(data.size()) + "\r\n";

    for (auto word : data) {
        string add = "$" + to_string(word.length()) + "\r\n" + word + "\r\n";
        output += add;
    }

    return output;
}

stringstream writeBulkStringStream(vector<string> data) {
    stringstream result;
    result << "*" + to_string(data.size()) << "\r\n";

    for (auto word : data) {
        result << "$" + to_string(word.length()) << "\r\n" << word << "\r\n";
    }

    return result;
}

void propogate(vector<string> data, string server_role) {
    if (server_role == "slave") return;

    string command = data[0];
    transform(command.begin(), command.end(), command.begin(), ::tolower);

    if (write_commands.find(command) != write_commands.end()) {
        string command_string = writeBulkString(data);
        for (auto replica_fd : replicaFdList) {
            send(replica_fd, command_string.c_str(), command_string.length(), 0);
        }
    }

    return;
}

using NumberType = variant<int, long long, double, string>;

NumberType convertValue(const string& value) {
    try {
        size_t pos;

        // Try parsing as int
        int intValue = stoi(value, &pos);
        if (pos == value.length()) return intValue;

        // Try parsing as long long
        long long longValue = stoll(value, &pos);
        if (pos == value.length()) return longValue;

        // Try parsing as double
        double doubleValue = stod(value, &pos);
        if (pos == value.length()) return doubleValue;

    } catch (...) {
        // Ignore exceptions, fall through to invalid case
    }

    return value;
}

pair<bool, NumberType> isNumber(const string& input) {
    NumberType result = convertValue(input);

    if (holds_alternative<string>(result)) {
        return {false, result};
    }

    return {true, result};
}

void sendPublishMessage(string publish_channel, string publish_message,
                        int client_fd) {
    string response = "*2\r\n$" + to_string(publish_channel.size()) + "\r\n" +
                      publish_channel + "\r\n$" +
                      to_string(publish_message.size()) + "\r\n" +
                      publish_message + "\r\n";

    ssize_t bytes_sent = send(client_fd, response.c_str(), response.size(), 0);
    if (bytes_sent < 0) {
        cerr << "Failed to send message to client FD " << client_fd << "\n";
    }
    cout << "Message sent to client FD " << client_fd << "\n";
}

pair<string, bool> RespParser(istream& input, ClientState& curr_client) {
    bool response_flag = true;
    string server_role = replicationState.role;
    string word;
    getline(input, word);
    // cout << "word_0" << word[0] << endl;

    if (word[0] == ':') {
        int numRecv = parseInteger(input);
    } else if (word.empty() || word[0] != '*') {
        throw runtime_error("Invalid RESP format");
    }

    cout << "here" << endl;

    int numArgs = stoi(word.substr(1));
    vector<string> data;

    size_t totalBytes = 0;
    for (int i = 0; i < numArgs; ++i) {
        string arg = parseBulkString(input);
        cout << arg << endl;
        data.push_back(arg);
        // Calculate bytes for this argument
        totalBytes += arg.size();  // Length of the argument
        totalBytes += 6;           // CRLF after the argument
    }

    // Add bytes for RESP argument count line
    totalBytes += 4;  // Length of "*<numArgs>\r\n"

    if (server_role == "slave")
        replicationState.master_repl_offset += totalBytes;
    string command = data[0];
    transform(command.begin(), command.end(), command.begin(), ::tolower);
    cout << command << " " << totalBytes << endl;
    string output;

    if (command != "exec" && command != "discard" && command != "multi" &&
        curr_client.clientQueueFlag == 1) {
        curr_client.clientMultiQueue.push(data);
        output = "+QUEUED\r\n";
        return {output, response_flag};
    }
    if (command == "echo") {
        if (data.size() > 2) {
            output = "*" + to_string(data.size()) + "\r\n";
            for (int i = 1; i < data.size(); i++) {
                string add = "$" + to_string(data[i].length()) + "\r\n" +
                             data[i] + "\r\n";
                output += add;
            }
        } else {
            output =
                "$" + to_string(data[1].length()) + "\r\n" + data[1] + "\r\n";
        }
    } else if (command == "ping") {
        cout << "pong" << endl;
        output = "+PONG\r\n";
    } else if (command == "set") {
        string key = data[1];
        string value = data[2];
        cout << "set " << value << value.length() << endl;
        mp[key] = value;

        if (data.size() > 3) {
            string command_expiry = data[3];
            transform(command_expiry.begin(), command_expiry.end(),
                      command_expiry.begin(), ::tolower);
            if (command_expiry == "px") {
                string et = data[4];
                uint64_t expiry_time = getCurrentTime() + stoll(et);
                expiry[key] = expiry_time;
            }
        }

        output = "+OK\r\n";
    } else if (command == "get") {
        string key = data[1];
        if (mp.find(key) != mp.end()) {
            string value = mp[key];
            output = "$" + to_string(value.length()) + "\r\n" + value + "\r\n";
            if (expiry.find(key) != expiry.end()) {
                long long expiry_time = expiry[key];
                if (expiry_time < getCurrentTime()) {
                    mp.erase(key);
                    expiry.erase(key);
                    output = "$-1\r\n";
                }
            }
        } else {
            output = "$-1\r\n";
        }
    } else if (command == "config") {
        string config_command = data[1];
        transform(config_command.begin(), config_command.end(),
                  config_command.begin(), ::tolower);
        if (data.size() > 2 && config_command == "get") {
            string param = data[2];
            transform(param.begin(), param.end(), param.begin(), ::tolower);

            if (config.find(param) != config.end()) {
                output = "*2\r\n";
                output +=
                    "$" + to_string(param.length()) + "\r\n" + param + "\r\n";
                string value = config[param];
                output +=
                    "$" + to_string(value.length()) + "\r\n" + value + "\r\n";
            } else {
                output = "*0\r\n";
            }
        }
    } else if (command == "keys") {
        string command_keys = data[1];
        transform(command_keys.begin(), command_keys.end(),
                  command_keys.begin(), ::tolower);

        if (command_keys == "*") {
            output = "*" + to_string(keys.size()) + "\r\n";
            for (const auto& key : keys) {
                string add =
                    "$" + to_string(key.size()) + "\r\n" + key + "\r\n";
                output += add;
            }
        }
    } else if (command == "info") {
        string command_info = data[1];
        transform(command_info.begin(), command_info.end(),
                  command_info.begin(), ::tolower);

        if (command_info == "replication") {
            string add =
                "role:" + replicationState.role + "\r\n" +
                "master_repl_offset:" +
                to_string(replicationState.master_repl_offset) + "\r\n" +
                "master_replid:" + replicationState.master_replid + "\r\n";
            output = "$" + to_string(add.length()) + "\r\n" + add + "\r\n";
            cout << "debug statement" << endl;
            cout << output << endl;
        }
    } else if (command == "replconf") {
        string command_info = data[1];
        transform(command_info.begin(), command_info.end(),
                  command_info.begin(), ::tolower);
        output = "+OK\r\n";
        if (command_info == "listening-port") {
            slavePortList.push_back(stoi(data[2]));
        } else if (command_info == "getack") {
            string offset = to_string(replicationState.master_repl_offset);
            vector<string> output_data = {"REPLCONF", "ACK", offset};
            output = writeBulkString(output_data);
            response_flag = true;
        }

    } else if (command == "psync") {
        output = "+FULLRESYNC " + replicationState.master_replid + " " +
                 to_string(replicationState.master_repl_offset) + "\r\n";
        output += "$" + to_string(empty_rdb.length()) + "\r\n" + empty_rdb;
        replicaFdList.push_back(curr_client.client_fd);
    } else if (command == "wait") {
        int numReplicaforWait = stoi(data[1]);
        long long waitTimeout = stoll(data[2]);

        output = ":" + to_string(slavePortList.size()) + "\r\n";
    } else if (command == "incr") {
        string incr_key = data[1];

        if (mp.find(incr_key) != mp.end()) {
            string incr_val = mp[incr_key];

            auto [num_flag, num_val] = isNumber(incr_val);
            cout << incr_key << " : " << incr_val << " " << num_flag << endl;
            if (num_flag) {
                if (holds_alternative<int>(num_val)) {
                    int new_val = get<int>(num_val) + 1;
                    mp[incr_key] = to_string(new_val);
                    output = ":" + to_string(new_val) + "\r\n";
                } else if (holds_alternative<long long>(num_val)) {
                    long long new_val = get<long long>(num_val) + 1;
                    mp[incr_key] = to_string(new_val);
                    output = ":" + to_string(new_val) + "\r\n";
                } else if (holds_alternative<double>(num_val)) {
                    double new_val = get<double>(num_val) + 1.0;
                    mp[incr_key] = to_string(new_val);
                    output = ":" + to_string(new_val) + "\r\n";
                }
            } else {
                output = "-ERR value is not an integer or out of range\r\n";
            }
        } else {
            mp[incr_key] = "1";
            output = ":1\r\n";
        }

    } else if (command == "multi") {
        curr_client.clientQueueFlag = true;
        output = "+OK\r\n";
    } else if (command == "exec") {
        if (curr_client.clientQueueFlag == 0)
            output = "-ERR EXEC without MULTI\r\n";
        else {
            curr_client.clientQueueFlag = false;
            if (curr_client.clientMultiQueue.size() == 0)
                output = "*0\r\n";
            else {
                string exec_output = "";
                while (!curr_client.clientMultiQueue.empty()) {
                    cout << "here" << endl;
                    vector<string> queued_input =
                        curr_client.clientMultiQueue.front();
                    stringstream queued_input_str =
                        writeBulkStringStream(queued_input);
                    curr_client.clientMultiQueue.pop();
                    cout << "Executing" << endl;
                    auto [queued_output, queued_response_flag] =
                        RespParser(queued_input_str, curr_client);
                    exec_output += queued_output;
                }

                output = exec_output;
            }
        }
    } else if (command == "discard") {
        if (curr_client.clientQueueFlag == 0) {
            output = "-ERR DISCARD without MULTI\r\n";
        } else {
            curr_client.clientQueueFlag = false;
            while (!curr_client.clientMultiQueue.empty())
                curr_client.clientMultiQueue.pop();
            output = "+OK\r\n";
        }
    } else if (command == "subscribe") {
        vector<string> subscribeOutput;
        for (int i = 1; i < data.size(); i++) {
            string channel_name = data[i];
            transform(channel_name.begin(), channel_name.end(),
                      channel_name.begin(), ::tolower);
            cout << "channel " << channel_name << endl;
            pubsub_channels[channel_name].insert(curr_client.client_fd);
            curr_client.channelSet.insert(channel_name);
            subscribeOutput.push_back("subscribe");
            subscribeOutput.push_back(channel_name);
            cout << "pubsub " << channel_name
                 << pubsub_channels[channel_name].size() << endl;
        }

        output = writeBulkString(subscribeOutput);
    } else if (command == "unsubscribe") {
        vector<string> unsubscribeOutput;
        cout << "unsubscribe" << data.size() << endl;
        if (data.size() == 1) {
            for (auto it = curr_client.channelSet.begin();
                 it != curr_client.channelSet.end();) {
                auto channel = *it;  // Capture the channel before erasing
                pubsub_channels[channel].erase(curr_client.client_fd);
                it = curr_client.channelSet.erase(
                    it);  // Erase safely and move the iterator forward
                unsubscribeOutput.push_back("unsubscribe");
                unsubscribeOutput.push_back(channel);
            }

        } else {
            for (int i = 1; i < data.size(); i++) {
                string unsubscribe_channel = data[i];
                transform(unsubscribe_channel.begin(),
                          unsubscribe_channel.end(),
                          unsubscribe_channel.begin(), ::tolower);
                pubsub_channels[unsubscribe_channel].erase(
                    curr_client.client_fd);
                curr_client.channelSet.erase(unsubscribe_channel);
                unsubscribeOutput.push_back("unsubscribe");
                unsubscribeOutput.push_back(unsubscribe_channel);
            }
        }

        output = writeBulkString(unsubscribeOutput);
    } else if (command == "publish") {
        string publish_channel = data[1];
        string publish_message = data[2];
        transform(publish_channel.begin(), publish_channel.end(),
                  publish_channel.begin(), ::tolower);
        set<int> subscribed_clients;
        if (pubsub_channels.find(publish_channel) != pubsub_channels.end()) {
            subscribed_clients = pubsub_channels[publish_channel];
        } else {
            cout << "No subscribers for channel: " << publish_channel << "\n";
        }

        for (auto& [pattern, fds] : pubsub_patterns) {
            if (fnmatch(pattern.c_str(), publish_channel.c_str(), 0) == 0) {
                // Check if channel matches the pattern
                for (int fd : fds) {
                    subscribed_clients.insert(fd);
                }
            }
        }

        for (auto& fd : subscribed_clients) {
            sendPublishMessage(publish_channel, publish_message, fd);
        }

        output += ":" + to_string(subscribed_clients.size()) + "\r\n";

    } else if (command == "pubsub") {
        string pubsub_command = data[1];
        transform(pubsub_command.begin(), pubsub_command.end(),
                  pubsub_command.begin(), ::tolower);

        if (pubsub_command == "numsub") {
            string outputNumsub = "";
            for (int i = 2; i < data.size(); i++) {
                string channel_name = data[i];
                transform(channel_name.begin(), channel_name.end(),
                          channel_name.begin(), ::tolower);
                outputNumsub +=
                    ":" + to_string(pubsub_channels[channel_name].size()) +
                    "\r\n";
            }

            output = outputNumsub;
        }
    } else if (command == "psubscribe") {
        vector<string> outputPsubscribe;
        for (int i = 1; i < data.size(); i++) {
            string pattern = data[i];
            transform(pattern.begin(), pattern.end(), pattern.begin(),
                      ::tolower);
            pubsub_patterns[pattern].insert(curr_client.client_fd);
            curr_client.patternSet.insert(pattern);
            outputPsubscribe.push_back("psubscribe");
            outputPsubscribe.push_back(pattern);
        }
        output = writeBulkString(outputPsubscribe);
    } else if (command == "punsubscribe") {
        vector<string> punsubscribeOutput;
        cout << "punsubscribe" << data.size() << endl;
        if (data.size() == 1) {
            for (auto it = curr_client.patternSet.begin();
                 it != curr_client.patternSet.end();) {
                auto pattern = *it;  // Capture the pattern before erasing
                pubsub_patterns[pattern].erase(curr_client.client_fd);
                it = curr_client.patternSet.erase(
                    it);  // Erase safely and move the iterator forward
                punsubscribeOutput.push_back("punsubscribe");
                punsubscribeOutput.push_back(pattern);
            }

        } else {
            for (int i = 1; i < data.size(); i++) {
                string unsubscribe_pattern = data[i];
                transform(unsubscribe_pattern.begin(),
                          unsubscribe_pattern.end(),
                          unsubscribe_pattern.begin(), ::tolower);
                pubsub_patterns[unsubscribe_pattern].erase(
                    curr_client.client_fd);
                curr_client.patternSet.erase(unsubscribe_pattern);
                punsubscribeOutput.push_back("punsubscribe");
                punsubscribeOutput.push_back(unsubscribe_pattern);
            }
        }

        output = writeBulkString(punsubscribeOutput);
    }

    propogate(data, server_role);
    return {output, response_flag};
}

void deleteClientPubsub(int fd) {
    ClientState curr_client = client_map[fd];

    for (auto channel : curr_client.channelSet) {
        pubsub_channels[channel].erase(fd);
    }

    for (auto pattern : curr_client.patternSet) {
        pubsub_patterns[pattern].erase(fd);
    }

    client_map.erase(fd);
}

int readLength(ifstream& file) {
    unsigned char firstByte;
    file.read(reinterpret_cast<char*>(&firstByte), 1);
    if ((firstByte & 0xC0) == 0x00) {  // 6-bit encoding
        return firstByte & 0x3F;
    } else if ((firstByte & 0xC0) == 0x40) {  // 14-bit encoding
        unsigned char secondByte;
        file.read(reinterpret_cast<char*>(&secondByte), 1);
        return ((firstByte & 0x3F) << 8) | secondByte;
    } else if ((firstByte & 0xC0) == 0x80) {  // 32-bit encoding
        uint32_t length;
        file.read(reinterpret_cast<char*>(&length), 4);
        return length;
    } else {
        int specialType = firstByte & 0x3F;
        cout << "data type" << specialType << endl;

        switch (specialType) {
            case 0:
                return 1;  // 8-bit integer length
            case 1:
                return 2;  // 16-bit integer length
            case 2:
                return 4;  // 32-bit integer length
            case 3: {      // LZF compressed string
                int compressedLength = readLength(file);
                int uncompressedLength = readLength(file);
                return compressedLength;
            }
            default:
                throw std::runtime_error("Unknown special encoding");
        }
    }
}

string readString(std::ifstream& file, int length) {
    vector<char> buffer(length);
    file.read(buffer.data(), length);
    return string(buffer.begin(), buffer.end());
}

vector<string> parseRDB(const string& filename) {
    vector<string> result;
    ifstream file(filename, ios::binary);

    if (!file) {
        return result;
    }
    char header[9] = {0};
    file.read(header, 9);
    if (string(header, 5) != "REDIS") {
        cerr << "Invalid RDB file header.\n";
        return result;
    }

    unsigned char type;
    file.read(reinterpret_cast<char*>(&type), 1);
    cout << static_cast<int>(type) << endl;

    while (type == 0xFA) {
        int metadataNameLength = readLength(file);
        cout << "Metadata Name Length: " << metadataNameLength << endl;
        string metadataName = readString(file, metadataNameLength);
        cout << "Metadata Name: " << metadataName << endl;

        int metadataValueLength = readLength(file);
        cout << "Metadata Value Length: " << metadataValueLength << endl;
        string metadataValue = readString(file, metadataValueLength);
        cout << "Metadata Value: " << metadataValue << endl;

        file.read(reinterpret_cast<char*>(&type), 1);
    }

    if (type == 0xFE) {  // Start of a database subsection
        int dbIndex = readLength(file);
        cout << "Processing database index: " << dbIndex << endl;

        while (true) {
            file.read(reinterpret_cast<char*>(&type), 1);

            if (file.eof()) {
                cerr << "Unexpected end of file.\n";
                break;
            }

            if (type == 0xFF) {  // End of RDB file
                cout << "End of RDB file.\n";
                break;
            }

            if (type == 0xFB) {  // Hash table size information
                int keyValueHashSize = readLength(file);
                int keyExpiryHashSize = readLength(file);
                cout << "Hash Table Sizes - Key/Value: " << keyValueHashSize
                     << ", Expiry: " << keyExpiryHashSize << endl;
                continue;
            }

            int flag_expiry = false;
            uint64_t expiry_time_milliseconds;
            cout << static_cast<int>(type) << endl;
            if (type == 0xFC || type == 0xFD) {  // Expiry timestamps
                int expireLength = (type == 0xFC) ? 8 : 4;
                flag_expiry = true;

                if (expireLength == 4) {
                    uint32_t expiry_time_seconds;
                    file.read(reinterpret_cast<char*>(&expiry_time_seconds),
                              sizeof(expiry_time_seconds));

                    expiry_time_milliseconds =
                        static_cast<uint64_t>(expiry_time_seconds) * 1000;
                    std::cout
                        << "Expiry time (ms): " << expiry_time_milliseconds
                        << std::endl;
                } else if (expireLength == 8) {
                    file.read(
                        reinterpret_cast<char*>(&expiry_time_milliseconds),
                        sizeof(expiry_time_milliseconds));
                    std::cout
                        << "Expiry time (ms): " << expiry_time_milliseconds
                        << std::endl;
                }

                file.read(reinterpret_cast<char*>(&type), 1);
                cout << static_cast<int>(type) << endl;
            }

            if (type == 0x00) {  // String key-value pair
                int keyLength = readLength(file);
                string key = readString(file, keyLength);

                int valueLength = readLength(file);
                string value = readString(file, valueLength);

                mp[key] = value;
                cout << "Key: " << key << ", Value: " << value << endl;
                result.push_back(key);

                if (flag_expiry) expiry[key] = expiry_time_milliseconds;
                continue;
            }

            cerr << "Unknown type encountered: " << static_cast<int>(type)
                 << "\n";
            break;
        }
    }

    file.close();
    return result;
}

void sendCommand(int socket, string command) {
    send(socket, command.c_str(), command.length(), 0);
    std::cout << "Sent: " << command << std::endl;

    char buffer[1024] = {0};
    ssize_t bytesRead = recv(socket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead > 0) {
        std::cout << "Received: " << buffer << std::endl;
    } else {
        std::cerr << "Failed to receive response\n";
    }
}

void sendReplicaACK() {
    for (auto slavePort : slavePortList) {
        int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd < 0) {
            perror("Socket creation failed");
            return;
        }

        // Configure server address
        sockaddr_in serverAddress{};
        serverAddress.sin_family = AF_INET;
        serverAddress.sin_port = htons(slavePort);

        if (inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr) <= 0) {
            perror("Invalid address");
            close(socket_fd);
            return;
        }

        // Connect to the master server
        if (connect(socket_fd, (struct sockaddr*)&serverAddress,
                    sizeof(serverAddress)) < 0) {
            perror("Connection failed");
            close(socket_fd);
            return;
        }

        std::cout << "Connected to the slave server at " << slavePort << endl;

        sendCommand(socket_fd,
                    "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
        close(socket_fd);
    }
}

void handShake(int replicaSocket, int slave_port) {
    sendCommand(replicaSocket, "*1\r\n$4\r\nPING\r\n");
    string port_command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                          to_string(to_string(slave_port).length()) + "\r\n" +
                          to_string(slave_port) + "\r\n";
    sendCommand(replicaSocket, port_command);
    sendCommand(replicaSocket,
                "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
    sendCommand(replicaSocket, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
}

int ReplicaServer(int master_port, int slave_port) {
    // Create socket
    int replicaSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (replicaSocket < 0) {
        perror("Socket creation failed");
        return 1;
    }

    // Configure server address
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(master_port);

    if (inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr) <= 0) {
        perror("Invalid address");
        close(replicaSocket);
        return 1;
    }

    // Connect to the master server
    if (connect(replicaSocket, (struct sockaddr*)&serverAddress,
                sizeof(serverAddress)) < 0) {
        perror("Connection failed");
        close(replicaSocket);
        return 1;
    }

    std::cout << "Connected to the master server\n";

    // Send commands and receive responses

    handShake(replicaSocket, slave_port);

    // Close the socket
    close(replicaSocket);

    return 0;
}

int main(int argc, char** argv) {
    // Flush after every cout / cerr
    cout << unitbuf;
    cerr << unitbuf;

    cout << "hey";
    config["dir"] = "/tmp";
    config["dbfilename"] = "dump.rdb";
    bool has_rdb = false;
    int port_number = 6379;
    string server_role = "master";
    string master_host = "";
    int master_port = 0;
    // Process command line args
    for (int i = 1; i < argc - 1; i += 2) {
        string flag = argv[i];
        string value = argv[i + 1];

        if (flag == "--dir") {
            config["dir"] = value;
        } else if (flag == "--dbfilename") {
            config["dbfilename"] = value;
            has_rdb = true;
        } else if (flag == "--port") {
            port_number = stoi(value);
        } else if (flag == "--replicaof") {
            server_role = "slave";
            istringstream stream(value);
            stream >> master_host >> master_port;
            i++;
        }
    }

    if (has_rdb) {
        string rdbFile = config["dir"] + "/" + config["dbfilename"];
        cout << "here" << endl;
        keys = parseRDB(rdbFile);
    }

    initializeReplicationState(server_role);

    if (server_role == "slave") {
        int replica_flag = ReplicaServer(master_port, port_number);
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        cerr << "Failed to create server socket\n";
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
        cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_number);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) !=
        0) {
        cerr << "Failed to bind to port" << port_number << endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        cerr << "listen failed\n";
        return 1;
    }

    fd_set active_fds, ready_fds;
    FD_ZERO(&active_fds);
    FD_SET(server_fd, &active_fds);
    int max_fd = server_fd;

    // Parser parser;

    while (true) {
        ready_fds = active_fds;  // Copy active_fds because select modifies it

        // Wait for an event (e.g., new connection, incoming data)
        int activity =
            select(max_fd + 1, &ready_fds, nullptr, nullptr, nullptr);
        if (activity < 0) {
            cerr << "Select error\n";
            break;
        }

        for (int fd = 0; fd <= max_fd; fd++) {
            if (FD_ISSET(fd, &ready_fds)) {  // Check if this fd is ready
                if (fd == server_fd) {
                    // Handle new connection
                    int new_client_fd = accept(server_fd, nullptr, nullptr);
                    if (new_client_fd < 0) {
                        cerr << "Accept failed\n";
                        continue;
                    }
                    FD_SET(new_client_fd,
                           &active_fds);  // Add new client to set
                    if (new_client_fd > max_fd) {
                        max_fd = new_client_fd;  // Update max_fd
                    }
                    cout << "New client connected: " << new_client_fd << "\n";
                    ClientState curr_client;
                    client_map[new_client_fd] =
                        initializeClientState(new_client_fd, curr_client);
                    // cout << "initiliazed " << new_client_fd << "size " <<
                    // client_map.size() << endl;
                } else {
                    char buffer[1024];
                    int bytes_rec = recv(fd, buffer, sizeof(buffer) - 1, 0);

                    if (bytes_rec <= 0) {
                        cout << "closing client at " << fd << endl;
                        deleteClientPubsub(fd);
                        close(fd);
                        FD_CLR(fd, &active_fds);
                        continue;
                    }

                    stringstream input;
                    input << buffer;
                    cout << "recieved" << endl;
                    ClientState& curr_client = client_map[fd];
                    // cout << "command from " << fd << endl;
                    auto [response, response_flag] =
                        RespParser(input, curr_client);
                    cout << response << endl;
                    if (response_flag)
                        send(fd, response.c_str(), response.size(), 0);
                }
            }
        }
    }

    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    cout << "Logs from your program will appear here!\n";

    // Uncomment this block to pass the first stage
    //

    close(server_fd);

    return 0;
}
