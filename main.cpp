#include <winsock2.h>

#include <ws2tcpip.h>

#include <thread>

#include <vector>

#include <atomic>

#include <iostream>

#include <chrono>
 
#pragma comment(lib, "Ws2_32.lib")
 
constexpr const char* SERVER_IP = "192.168.1.57";

constexpr int PORT = 5555;
 
constexpr int TCP_THREADS = 8;          // tune = CPU cores

constexpr int BATCH_RECORDS = 2000;     // records per send

constexpr int RECORD_SIZE = 96;         // fixed size payload
 
std::atomic<bool> running{ true };
 
// -----------------------------

// Fixed-size record (NO BSON)

// -----------------------------

#pragma pack(push, 1)

struct Record {

    int worker;

    int decimal;

    int processed;

    int64_t timestamp;

    int64_t sequence;

};

#pragma pack(pop)
 
// -----------------------------

// TCP sender thread

// -----------------------------

void tcp_sender(int id)

{

    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
 
    int flag = 1;

    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
 
    int sndbuf = 8 * 1024 * 1024;

    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char*)&sndbuf, sizeof(sndbuf));
 
    sockaddr_in server{};

    server.sin_family = AF_INET;

    server.sin_port = htons(PORT);

    inet_pton(AF_INET, SERVER_IP, &server.sin_addr);
 
    while (connect(sock, (sockaddr*)&server, sizeof(server)) != 0)

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
 
    std::vector<Record> batch(BATCH_RECORDS);

    uint64_t seq = 0;
 
    while (running)

    {

        auto now = std::chrono::high_resolution_clock::now();

        auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(

            now.time_since_epoch())
            .count();
 
        for (int i = 0; i < BATCH_RECORDS; i++)

        {

            batch[i] = {

                id,

                rand() % 100,

                rand() % 100,

                ts,

                static_cast<int64_t>(seq++)

            };

        }
 
        int bytes = BATCH_RECORDS * sizeof(Record);

        int sent = send(sock, reinterpret_cast<char*>(batch.data()), bytes, 0);

        if (sent <= 0) break;

    }
 
    closesocket(sock);

}
 
// -----------------------------

// MAIN

// -----------------------------

int main()

{

    WSADATA wsa;

    WSAStartup(MAKEWORD(2, 2), &wsa);
 
    std::cout << ">>> HIGH RATE C++ PRODUCER STARTED <<<\n";
 
    std::vector<std::thread> threads;

    for (int i = 0; i < TCP_THREADS; i++)

        threads.emplace_back(tcp_sender, i);
 
    for (auto& t : threads)

        t.join();
 
    WSACleanup();

}

 
