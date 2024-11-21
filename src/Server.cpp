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
using namespace std;

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
    string command = parseBulkString(input);
    transform(command.begin(), command.end(), command.begin(), ::tolower);
    string output;
    if (command == "echo")
    {
        vector<string> data;
        
        for (int i =0; i <numArgs-1;++i)
        {
            data.push_back(parseBulkString(input));
        }
        
        if (data.size() > 1)
        {
            output = "*" + to_string(data.size()) +"\r\n";
            for(int i =0; i < data.size();i++)
            {
                string add = "$"+ to_string(data[i].length())+"\r\n"+data[i]+"\rn";
                output += add;
            }
        }
        else
        {
            output = "$"+to_string(data[0].length()) +"\r\n" + data[0]+"\r\n";
        }
     }
     else if (command = "ping")
     {
      output = "+PONG\r\n";
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
