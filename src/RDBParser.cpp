#include "RDBParser.h"

#include <cstdint>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

static int readLength(ifstream& file) {
    unsigned char b;
    file.read(reinterpret_cast<char*>(&b), 1);
    if ((b & 0xC0) == 0x00) return b & 0x3F;
    if ((b & 0xC0) == 0x40) {
        unsigned char b2;
        file.read(reinterpret_cast<char*>(&b2), 1);
        return ((b & 0x3F) << 8) | b2;
    }
    if ((b & 0xC0) == 0x80) {
        uint32_t len;
        file.read(reinterpret_cast<char*>(&len), 4);
        return len;
    }
    switch (b & 0x3F) {
        case 0: return 1;
        case 1: return 2;
        case 2: return 4;
        case 3: { int c = readLength(file); readLength(file); return c; }
        default: throw runtime_error("Unknown RDB special encoding");
    }
}

static string readString(ifstream& file, int length) {
    vector<char> buf(length);
    file.read(buf.data(), length);
    return string(buf.begin(), buf.end());
}

void parseRDB(const string& filename, RedisStore& store) {
    ifstream file(filename, ios::binary);
    if (!file) return;

    char header[9] = {0};
    file.read(header, 9);
    if (string(header, 5) != "REDIS") { cerr << "Invalid RDB file header.\n"; return; }

    unsigned char type;
    file.read(reinterpret_cast<char*>(&type), 1);

    while (type == 0xFA) {
        readString(file, readLength(file));
        readString(file, readLength(file));
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

        bool has_expiry = false;
        uint64_t expiry_ms = 0;
        if (type == 0xFC || type == 0xFD) {
            has_expiry = true;
            if (type == 0xFD) {
                uint32_t s; file.read(reinterpret_cast<char*>(&s), 4);
                expiry_ms = static_cast<uint64_t>(s) * 1000;
            } else {
                file.read(reinterpret_cast<char*>(&expiry_ms), 8);
            }
            file.read(reinterpret_cast<char*>(&type), 1);
        }

        if (type != 0x00) break;
        string key = readString(file, readLength(file));
        string val = readString(file, readLength(file));
        data[key] = val;
        keys.push_back(key);
        if (has_expiry) expiry[key] = expiry_ms;
    }

    store.loadFromRDB(move(keys), move(data), move(expiry));
}
