#if defined(USE_ESP_IDF)

#pragma once

#include "esphome/components/network/ip_address.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

class IPAddress {
public:
    uint8_t bytes[4];
    IPAddress() : bytes{0,0,0,0} {}
    IPAddress(uint8_t a, uint8_t b, uint8_t c, uint8_t d) : bytes{a,b,c,d} {}
    uint8_t& operator[](int i) { return bytes[i]; }
    const uint8_t& operator[](int i) const { return bytes[i]; }
    bool operator==(const IPAddress& other) const {
        return bytes[0] == other.bytes[0] && bytes[1] == other.bytes[1] && 
               bytes[2] == other.bytes[2] && bytes[3] == other.bytes[3];
    }
    bool operator!=(const IPAddress& other) const { return !(*this == other); }
};

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief device_groups_WiFiUDP class that provides ESPHome-compatible UDP functionality for ESP-IDF
 * 
 * This wrapper implements the ESPHome WiFiUDP interface using ESP-IDF's native
 * socket API, allowing components that depend on ESPHome's WiFiUdp.h to work
 * with ESP-IDF without modification.
 */
class device_groups_WiFiUDP {
private:
    int sock_fd;
    struct sockaddr_in remote_addr;
    bool is_connected;
    char* buffer;
    size_t buffer_size;
    size_t data_length;
    size_t read_position;

public:
    /**
     * @brief Default constructor
     */
    device_groups_WiFiUDP();
    
    /**
     * @brief Destructor
     */
    ~device_groups_WiFiUDP();
    
    /**
     * @brief Begin UDP communication on specified port
     * @param port The port number to bind to
     * @return true if successful, false otherwise
     */
    bool begin(uint16_t port);
    
    /**
     * @brief Begin UDP communication with multicast support
     * @param port The port number to bind to
     * @param multicast_ip The multicast IP address
     * @param interface_ip The interface IP address
     * @return true if successful, false otherwise
     */
    bool beginMulticast(uint16_t port, const char* multicast_ip, const char* interface_ip);
    
    /**
     * @brief Begin UDP communication with multicast support (IPAddress overload)
     * @param multicast_ip The multicast IP address as IPAddress
     * @param port The port number to bind to
     * @return true if successful, false otherwise
     */
    bool beginMulticast(const IPAddress& multicast_ip, uint16_t port);
    
    /**
     * @brief Stop UDP communication and close socket
     */
    void stop();
    
    /**
     * @brief Begin packet transmission to specified IP and port
     * @param ip The destination IP address
     * @param port The destination port
     * @return true if successful, false otherwise
     */
    bool beginPacket(const char* ip, uint16_t port);
    
    /**
     * @brief Begin packet transmission to specified IP and port
     * @param ip The destination IP address as uint32_t
     * @param port The destination port
     * @return true if successful, false otherwise
     */
    bool beginPacket(uint32_t ip, uint16_t port);
    
    /**
     * @brief Begin packet transmission to specified IP and port
     * @param ip The destination IP address as IPAddress
     * @param port The destination port
     * @return true if successful, false otherwise
     */
    bool beginPacket(const IPAddress& ip, uint16_t port);
    
    /**
     * @brief End packet transmission and send data
     * @return true if successful, false otherwise
     */
    bool endPacket();
    
    /**
     * @brief Write a single byte to the packet
     * @param byte The byte to write
     * @return Number of bytes written (1 if successful, 0 otherwise)
     */
    size_t write(uint8_t byte);
    
    /**
     * @brief Write data to the packet
     * @param buffer Pointer to the data buffer
     * @param size Size of the data to write
     * @return Number of bytes written
     */
    size_t write(const uint8_t* buffer, size_t size);
    
    /**
     * @brief Write a string to the packet
     * @param str The string to write
     * @return Number of bytes written
     */
    size_t write(const char* str);
    
    /**
     * @brief Parse incoming packet
     * @return Size of the received packet, 0 if no packet available
     */
    int parsePacket();
    
    /**
     * @brief Get the size of the received packet
     * @return Size of the packet in bytes
     */
    int available();
    
    /**
     * @brief Read a single byte from the received packet
     * @return The byte read, or -1 if no data available
     */
    int read();
    
    /**
     * @brief Read data from the received packet
     * @param buffer Pointer to the buffer to store the data
     * @param size Maximum number of bytes to read
     * @return Number of bytes read
     */
    int read(uint8_t* buffer, size_t size);
    
    /**
     * @brief Read data from the received packet
     * @param buffer Pointer to the buffer to store the data
     * @param size Maximum number of bytes to read
     * @return Number of bytes read
     */
    int read(char* buffer, size_t size);
    
    /**
     * @brief Peek at the next byte without removing it from the buffer
     * @return The next byte, or -1 if no data available
     */
    int peek();
    
    /**
     * @brief Flush the receive buffer
     */
    void flush();
    
    /**
     * @brief Get the remote IP address of the received packet
     * @return IP address as IPAddress object
     */
    IPAddress remoteIP();
    
    /**
     * @brief Get the remote port of the received packet
     * @return Port number
     */
    uint16_t remotePort();
    
    /**
     * @brief Check if the UDP connection is active
     * @return true if connected, false otherwise
     */
    bool connected();
    
    /**
     * @brief Set timeout for socket operations
     * @param timeout_ms Timeout in milliseconds
     */
    void setTimeout(int timeout_ms);
    
    /**
     * @brief Get the local port number
     * @return Local port number
     */
    uint16_t localPort();
    
    /**
     * @brief Get the local IP address
     * @return Local IP address as string
     */
    const char* localIP();

private:
    /**
     * @brief Initialize the UDP socket
     * @return true if successful, false otherwise
     */
    bool initSocket();
    
    /**
     * @brief Set socket options
     * @return true if successful, false otherwise
     */
    bool setSocketOptions();
    
    /**
     * @brief Convert IP address to string
     * @param ip IP address as uint32_t
     * @return IP address as string
     */
    static const char* ipToString(uint32_t ip);
};

#ifdef __cplusplus
}
#endif 

#endif  // USE_ESP_IDF 