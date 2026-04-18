#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>

const int PORT = 6379;

void sendCommand(int clientSocket, const char* command)
{
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

    // Send RESP PING command
    //const char* command = "*3\r\n$3\r\nSET\r\n$3\r\nFOO\r\n$3\r\nBAR\r\n";
    const char* command = "*1\r\n$5\r\nMULTI\r\n";

    //const char* command = "*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n";
    //const char* command = "*3\r\n$4\r\nwait\r\n$1\r\n0\r\n$4\r\n6000\r\n";
    //const char* command2 = "*2\r\n$4\r\nINCR\r\n$3\r\nFOO\r\n";
    const char* command2 = "*3\r\n$3\r\nSET\r\n$3\r\nFOO\r\n$3\r\nxyz\r\n";
    const char* command3 = "*2\r\n$4\r\nINCR\r\n$3\r\nFOO\r\n";
    const char* command4 = "*2\r\n$4\r\nINCR\r\n$3\r\nBAR\r\n";
    const char* command5 = "*1\r\n$4\r\nEXEC\r\n";
    const char* command6 = "*2\r\n$3\r\nGET\r\n$3\r\nBAR\r\n";
    const char* command7 = "*1\r\n$7\r\nEXEC\r\n";
    //const char* command8 = "*3\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n$6\r\nSPORTS\r\n";
    const char* command8 = "*3\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n$7\r\nFASHION\r\n";
    //const char* command9 = "*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\nnews\r\n";
    //const char* command9 = "*1\r\n$11\r\nUNSUBSCRIBE\r\n";
    const char* command10 = "*3\r\n$7\r\nPUBLISH\r\n$7\r\nNEWS.US\r\n$5\r\nTRUMP\r\n";
    //const char* command7 = "*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n";
    //const char* command3 = "*2\r\n$4\r\nINCR\r\n$3\r\nBAR\r\n";
    //sendCommand(clientSocket,command);
    //sendCommand(clientSocket,command2);
    //sendCommand(clientSocket,command3);
    //sendCommand(clientSocket,command4);
    //sendCommand(clientSocket,command5);
    //sendCommand(clientSocket,command6);
    //sendCommand(clientSocket,command7);
    //sendCommand(clientSocket,command8);
    //sendCommand(clientSocket,command9);
    sendCommand(clientSocket,command10);
    std::string input;
    while (true) {
        std::cout << "Enter command (* to exit): ";
        std::getline(std::cin, input);

        if (input == "*") {
            std::cout << "Exiting client...\n";
            break;
        }

        // Send the command entered by the user
        sendCommand(clientSocket, input.c_str());
    }


    close(clientSocket);
    return 0;
}
