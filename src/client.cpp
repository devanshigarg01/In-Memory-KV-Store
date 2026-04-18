#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <poll.h>
#include <vector>

const int PORT = 6379;

void sendCommand(int clientSocket, const std::string& command) {
    send(clientSocket, command.c_str(), command.size(), 0);
    std::cout << "Sent: " << command << std::endl;
}

int main() {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket < 0) {
        perror("Socket creation failed");
        return 1;
    }

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(PORT);

    if (inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr) <= 0) {
        perror("Invalid address");
        close(clientSocket);
        return 1;
    }

    if (connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("Connection failed");
        close(clientSocket);
        return 1;
    }

    std::cout << "Connected to the server\n";
    //const char* command8 = "*3\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n$6\r\nsports\r\n";
    //const char* command8 = "*4\r\n$6\r\npubsub\r\n$6\r\nnumsub\r\n$4\r\nnews\r\n$6\r\nsports\r\n";
    const char* command8 = "*3\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n$8\r\nsports.*\r\n";
    sendCommand(clientSocket,command8);

    // Set up polling
    struct pollfd fds[2];
    fds[0].fd = STDIN_FILENO; // Standard input for user commands
    fds[0].events = POLLIN;  // Watch for input ready to read
    fds[1].fd = clientSocket; // Server socket
    fds[1].events = POLLIN;   // Watch for data from the server

    char buffer[1024] = {0};
    std::string input;

    while (true) {
        // Wait for an event on either input or server socket
        int pollCount = poll(fds, 2, -1); // Wait indefinitely for an event
        if (pollCount < 0) {
            perror("poll failed");
            break;
        }

        // Check if user entered a command
        if (fds[0].revents & POLLIN) {
            std::getline(std::cin, input);

            if (input == "*") {
                std::cout << "Exiting client...\n";
                break;
            }

            sendCommand(clientSocket, input);
        }

        // Check if server sent a message
        if (fds[1].revents & POLLIN) {
            ssize_t bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
            if (bytesRead > 0) {
                buffer[bytesRead] = '\0'; // Null-terminate the buffer
                std::cout << "\n[Server]: " << buffer << std::endl;
            } else if (bytesRead == 0) {
                std::cerr << "Connection closed by server.\n";
                break;
            } else {
                perror("Error receiving message");
            }
        }
    }

    close(clientSocket);
    return 0;
}
