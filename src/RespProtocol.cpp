#include "RespProtocol.h"

#include <sys/socket.h>

#include <stdexcept>
#include <string>
#include <vector>

using namespace std;

int RespProtocol::parseInteger(istream& input) {
    string num;
    getline(input, num);
    return stoi(num);
}

string RespProtocol::parseBulkString(istream& input) {
    string len;
    getline(input, len);

    if (len.empty() || len[0] != '$') {
        throw runtime_error("Invalid bulk string format");
    }

    int length = stoi(len.substr(1));

    if (length < 0) {
        return "";
    }

    string buffer(length, '\0');
    input.read(&buffer[0], length);

    input.ignore(2);
    return buffer;
}

string RespProtocol::writeBulkString(vector<string> data) {
    string output = "*" + to_string(data.size()) + "\r\n";

    for (auto word : data) {
        output += "$" + to_string(word.length()) + "\r\n" + word + "\r\n";
    }

    return output;
}

stringstream RespProtocol::writeBulkStringStream(vector<string> data) {
    stringstream result;
    result << "*" + to_string(data.size()) << "\r\n";

    for (auto word : data) {
        result << "$" + to_string(word.length()) << "\r\n" << word << "\r\n";
    }

    return result;
}

bool RespProtocol::recvExact(int sock, char* buf, size_t n) {
    size_t received = 0;
    while (received < n) {
        ssize_t r = recv(sock, buf + received, n - received, 0);
        if (r <= 0) return false;
        received += r;
    }
    return true;
}

string RespProtocol::recvLine(int sock) {
    string line;
    char c;
    while (true) {
        ssize_t r = recv(sock, &c, 1, 0);
        if (r <= 0) break;
        if (c == '\n' && !line.empty() && line.back() == '\r') {
            line.pop_back();
            break;
        }
        line += c;
    }
    return line;
}
