#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sstream>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <ctime>
#include <chrono>
#include <fstream>
#include <cstdint>
using namespace std::chrono;
using namespace std;

unordered_map<string, string> config;
unordered_map<string,string> mp;
unordered_map<string,uint64_t> expiry;
vector<string> keys;

struct ReplicationState {
    string role; // master or slave
    string master_repl_offset;
    string master_repl_offset;
};

ReplicationState replicationState;

void initializeReplicationState(string server_role)
{
  replicationState.role = server_role;
  replicationState.master_repl_offset = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  replicationState.master_repl_offset = "0";

}

uint64_t getCurrentTime() {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

string parseBulkString(istream &input) {
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

string RespParser(istream &input) {
    string word;
    getline(input, word);

    if (word.empty() || word[0] != '*') {
        throw runtime_error("Invalid RESP format");
    }

    int numArgs = stoi(word.substr(1));

    vector<string> data;
        
    for (int i =0; i <numArgs;++i)
        {
        data.push_back(parseBulkString(input));
    }
    string command = data[0];
    transform(command.begin(), command.end(), command.begin(), ::tolower);
    string output;
    if (command == "echo")
    {
        
        if (data.size() > 2)
        {
            output = "*" + to_string(data.size()) +"\r\n";
            for(int i =1; i < data.size();i++)
            {
                string add = "$"+ to_string(data[i].length())+"\r\n"+data[i]+"\r\n";
                output += add;
            }
        }
        else
        {
            output = "$"+to_string(data[1].length()) +"\r\n" + data[1]+"\r\n";
        }
    }
     else if (command == "ping")
     {
      output = "+PONG\r\n";
     }
     else if (command == "set")
     {
        string key = data[1];
        string value = data[2];
        mp[key] = value;

        if (data.size() > 3)
        {
            string command_expiry = data[3];
            transform(command_expiry.begin(), command_expiry.end(), command_expiry.begin(), ::tolower);
            if (command_expiry == "px")
            {
               string et = data[4];
               uint64_t expiry_time = getCurrentTime() + stoll(et);
               expiry[key] = expiry_time; 
            }
        }
        
        output = "+OK\r\n";
     }
     else if(command == "get")
     {
        string key = data[1];
        if(mp.find(key) != mp.end())
        {
            string value = mp[key];
            output = "$" + to_string(value.length()) + "\r\n" + value + "\r\n";
            if (expiry.find(key) != expiry.end())
            {  
                long long expiry_time = expiry[key];
                if (expiry_time < getCurrentTime())
                {
                    mp.erase(key);
                    expiry.erase(key);
                    output = "$-1\r\n"; 
                }
                
            }
        }
        else
        {
            output = "$-1\r\n";
        }
     }
     else if(command == "config")
     {
         string config_command = data[1];
         transform(config_command.begin(), config_command.end(), config_command.begin(), ::tolower);
         if(data.size()>2 && config_command == "get")
         {
            string param = data[2];
            transform(param.begin(), param.end(), param.begin(), ::tolower);
            
            if (config.find(param) != config.end()) {
                output = "*2\r\n";
                output += "$" + to_string(param.length()) + "\r\n" + param + "\r\n";
                string value = config[param];
                output += "$" + to_string(value.length()) + "\r\n" + value + "\r\n";
            } else {
                output = "*0\r\n";
            }
         }
     }
     else if(command == "keys")
     {
       string command_keys = data[1];
       transform(command_keys.begin(), command_keys.end(), command_keys.begin(), ::tolower);

       if(command_keys == "*")
       {
        output = "*" + to_string(keys.size()) + "\r\n";
        for (const auto &key : keys) {
          string add = "$" + to_string(key.size()) + "\r\n" + key + "\r\n";
          output += add;
        }
       }
     }
     else if(command == "info")
     {
        string command_info = data[1];
        transform(command_info.begin(), command_info.end(), command_info.begin(), ::tolower);

        if(command_info == "replication")
        {
          output = "$" + to_string(5+replicationState.role.length()) + "\r\n" + "role:" + replicationState.role + "\r\n";
          output += "master_repl_offset:" + replicationState.master_repl_offset + "\r\n";
          output += "master_replid:" + replicationState.master_replid + "\r\n";
        }
     }
    
    return output;
    
}

int readLength(ifstream &file)
{
  unsigned char firstByte;
  file.read(reinterpret_cast<char *>(&firstByte), 1);
  if ((firstByte & 0xC0) == 0x00) {  // 6-bit encoding
        return firstByte & 0x3F;
    } else if ((firstByte & 0xC0) == 0x40) {  // 14-bit encoding
        unsigned char secondByte;
        file.read(reinterpret_cast<char *>(&secondByte), 1);
        return ((firstByte & 0x3F) << 8) | secondByte;
    } else if ((firstByte & 0xC0) == 0x80) {  // 32-bit encoding
        uint32_t length;
        file.read(reinterpret_cast<char *>(&length), 4);
        return length;
    } else {
        int specialType = firstByte & 0x3F;
        cout << "data type" << specialType << endl;
        
        switch (specialType) {
        case 0: return 1;  // 8-bit integer length
        case 1: return 2;  // 16-bit integer length
        case 2: return 4;  // 32-bit integer length
        case 3: {  // LZF compressed string
            int compressedLength = readLength(file);
            int uncompressedLength = readLength(file);
            return compressedLength;
        }
        default:
            throw std::runtime_error("Unknown special encoding");

        }
    }
}

string readString(std::ifstream &file, int length) {
    vector<char> buffer(length);
    file.read(buffer.data(), length);
    return string(buffer.begin(), buffer.end());
}

vector<string>parseRDB(const string &filename)
{ 
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
  file.read(reinterpret_cast<char *>(&type), 1);
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
    
    file.read(reinterpret_cast<char *>(&type), 1);
  }


  if (type == 0xFE) {  // Start of a database subsection
    int dbIndex = readLength(file);  
    cout << "Processing database index: " << dbIndex << endl;

    while (true) { 
        file.read(reinterpret_cast<char *>(&type), 1);
        
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
              file.read(reinterpret_cast<char *>(&expiry_time_seconds), sizeof(expiry_time_seconds));
              
              expiry_time_milliseconds = static_cast<uint64_t>(expiry_time_seconds) * 1000;
              std::cout << "Expiry time (ms): " << expiry_time_milliseconds << std::endl;
            } 
            else if (expireLength == 8) {
              file.read(reinterpret_cast<char *>(&expiry_time_milliseconds), sizeof(expiry_time_milliseconds));
              std::cout << "Expiry time (ms): " << expiry_time_milliseconds << std::endl;
            }

            file.read(reinterpret_cast<char *>(&type), 1);
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



        cerr << "Unknown type encountered: " << static_cast<int>(type) << "\n";
        break;
    }
  }
  
  file.close();
  return result;
}

int main(int argc, char **argv) {
  // Flush after every cout / cerr
  cout << unitbuf;
  cerr << unitbuf;

  cout << "hey";
  config["dir"] = "/tmp";
  config["dbfilename"] = "dump.rdb";
  bool has_rdb = false;
  int port_number = 6379;
  string server_role = "master";
  // Process command line args
  for (int i = 1; i < argc - 1; i += 2) {
      string flag = argv[i];
      string value = argv[i + 1];
      
      if (flag == "--dir") {
          config["dir"] = value;
      }
      else if (flag == "--dbfilename") {
          config["dbfilename"] = value;
          has_rdb = true;
      }
      else if(flag == "--port") port_number = stoi(value);
      else if(flag == "--replicaof") server_role = "slave";
  }

  if(has_rdb)
  {
    string rdbFile = config["dir"] + "/" + config["dbfilename"];
    cout << "here" << endl;
    keys = parseRDB(rdbFile);
  }

  initializeReplicationState(server_role);
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port_number);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
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

  while(true)
  {
    ready_fds = active_fds;       // Copy active_fds because select modifies it

    // Wait for an event (e.g., new connection, incoming data)
    int activity = select(max_fd + 1, &ready_fds, nullptr, nullptr, nullptr);
    if (activity < 0) {
        cerr << "Select error\n";
        break;
    }

    for(int fd=0; fd <= max_fd;fd++)
    {
       if (FD_ISSET(fd, &ready_fds)) { // Check if this fd is ready
            if (fd == server_fd) {
                // Handle new connection
                int new_client_fd = accept(server_fd, nullptr, nullptr);
                if (new_client_fd < 0) {
                    cerr << "Accept failed\n";
                    continue;
                }
                FD_SET(new_client_fd, &active_fds); // Add new client to set
                if (new_client_fd > max_fd) {
                    max_fd = new_client_fd;         // Update max_fd
                }
                cout << "New client connected: " << new_client_fd << "\n";
            }
            else
            {
              char buffer[1024];
              int bytes_rec = recv(fd,buffer,sizeof(buffer)-1,0);

              if (bytes_rec <= 0)
              {
                close(fd);
                FD_CLR(fd, &active_fds);
                continue;
              }

              stringstream input;
              input << buffer;
              const string response = RespParser(input);
              send(fd, response.c_str(), response.size(), 0);
            }
          }
        }
     }
  

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  cout << "Logs from your program will appear here!\n";

  // Uncomment this block to pass the first stage
  // 

  close(server_fd);


  return 0;
}
