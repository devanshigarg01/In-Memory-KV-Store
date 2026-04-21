#pragma once

#include <sstream>
#include <string>
#include <vector>

class RespProtocol {
public:
    static int parseInteger(std::istream& input);
    static std::string parseBulkString(std::istream& input);
    static std::string writeBulkString(std::vector<std::string> data);
    static std::stringstream writeBulkStringStream(std::vector<std::string> data);
    static bool recvExact(int sock, char* buf, size_t n);
    static std::string recvLine(int sock);
};
