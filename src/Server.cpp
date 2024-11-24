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
using namespace std::chrono;
using namespace std;

unordered_map<string, string> config = {
    {"dir", "/tmp"},
    {"dbfilename", "dump.rdb"}
};

unordered_map<string,string> mp;
unordered_map<string,long long> expiry;

long long getCurrentTime() {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}


string parseBulkString(istream &input) {
    string len;
    getline(input, len);

    if (len.empty() || len[0] != '$') {
        throw std::runtime_error("Invalid bulk string format");
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
               long long expiry_time = getCurrentTime() + stoll(et);
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
                // Return array with two elements: parameter name and value
                output = "*2\r\n";
                // Parameter name
                output += "$" + to_string(param.length()) + "\r\n" + param + "\r\n";
                // Parameter value
                string value = config[param];
                output += "$" + to_string(value.length()) + "\r\n" + value + "\r\n";
            } else {
                // Return empty array if parameter not found
                output = "*0\r\n";
            }
         }
     }
    
    return output;
    
}

int main(int argc, char **argv) {
  // Flush after every cout / cerr
  cout << unitbuf;
  cerr << unitbuf;
  
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
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    cerr << "Failed to bind to port 6379\n";
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
