#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>

const int PORT = 6379;

void sendCommand(int clientSocket, const char* command) {
    send(clientSocket, command, strlen(command), 0);
    std::cout << "Sent: " << command << std::endl;

    // Receive the response
    char buffer[1024] = {0};
    ssize_t bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead > 0) {
        std::cout << "Received: " << buffer << std::endl;
    } else {
        std::cerr << "Failed to receive response\n";
    }
}

int createClientSocket() {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket < 0) {
        perror("Socket creation failed");
        return -1;
    }

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(PORT);

    if (inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr) <= 0) {
        perror("Invalid address");
        close(clientSocket);
        return -1;
    }

    if (connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("Connection failed");
        close(clientSocket);
        return -1;
    }

    std::cout << "Connected to the server (Socket: " << clientSocket << ")\n";
    return clientSocket;
}

int main() {
    // Create two client sockets
    int clientSocket1 = createClientSocket();
    int clientSocket2 = createClientSocket();

    if (clientSocket1 < 0 || clientSocket2 < 0) {
        return 1; // Exit if socket creation fails
    }

    // Define commands for both clients
    const char* client1_command1 = "*1\r\n$5\r\nMULTI\r\n";
    const char* client1_command2 = "*3\r\n$3\r\nSET\r\n$3\r\nFOO\r\n$1\r\n3\r\n";
    const char* client1_command3 = "*2\r\n$4\r\nINCR\r\n$3\r\nFOO\r\n";
    const char* client1_command4 = "*1\r\n$4\r\nEXEC\r\n";

    const char* client2_command1 = "*1\r\n$5\r\nMULTI\r\n";
    const char* client2_command2 = "*2\r\n$4\r\nINCR\r\n$3\r\nFOO\r\n";
    const char* client2_command3 = "*1\r\n$4\r\nEXEC\r\n";

    // Simulate interleaved communication
    sendCommand(clientSocket1, client1_command1); // Client 1 sends MULTI
    sendCommand(clientSocket1, client1_command2); // Client 1 sends SET
    sendCommand(clientSocket1, client1_command3); // Client 1 sends INCR
    sendCommand(clientSocket2, client2_command1); // Client 2 sends MULTI
    sendCommand(clientSocket2, client2_command2); // Client 2 sends INCR
    sendCommand(clientSocket1, client1_command4); // Client 2 sends EXEC
    sendCommand(clientSocket2, client2_command3); // Client 2 sends EXEC

    // Close client sockets
    close(clientSocket1);
    close(clientSocket2);

    return 0;
}
