#if defined(USE_ESP_IDF) || defined(ESP_IDF_VERSION) || defined(CONFIG_IDF_TARGET)

#include "device_groups_WiFiUdp.h"
#include <fcntl.h>
#include <netdb.h>

#include "esp_wifi.h"
#include "esp_netif.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esphome/core/log.h"

// Default buffer size for UDP packets
#define DEFAULT_BUFFER_SIZE 1024
#define MAX_RETRIES 5
#define RETRY_DELAY_MS 25
#define DEDUP_WINDOW_MS 200  // 200ms window for deduplication (increased from 100ms)

static const char *const TAG = "dgr";

device_groups_WiFiUDP::device_groups_WiFiUDP() : sock_fd(-1), is_connected(false), 
                     send_buffer(nullptr), recv_buffer(nullptr), 
                     send_buffer_size(0), recv_buffer_size(0), 
                     send_data_length(0), recv_data_length(0), recv_read_position(0),
                     last_packet_hash(0), last_packet_time(0), 
                     packets_sent(0), packets_received(0), send_failures(0), last_stats_time(0),
                     consecutive_retries(0), last_retry_detection_time(0) {
    memset(&remote_addr, 0, sizeof(remote_addr));
    memset(&sender_addr, 0, sizeof(sender_addr));
    remote_addr.sin_family = AF_INET;
    sender_addr.sin_family = AF_INET;
    ESP_LOGCONFIG(TAG, "ESP-IDF WiFiUDP implementation initialized");
}

device_groups_WiFiUDP::~device_groups_WiFiUDP() {
    // Always close socket first
    if (sock_fd >= 0) {
        close(sock_fd);
        sock_fd = -1;
    }
    
    // Call stop to clean up other resources
    stop();
    
    // Double-check buffer cleanup in case stop() failed
    if (send_buffer) {
        free(send_buffer);
        send_buffer = nullptr;
        send_buffer_size = 0;
    }
    if (recv_buffer) {
        free(recv_buffer);
        recv_buffer = nullptr;
        recv_buffer_size = 0;
    }
    
    ESP_LOGCONFIG(TAG, "ESP-IDF WiFiUDP implementation destroyed");
}

bool device_groups_WiFiUDP::initSocket() {
    if (sock_fd >= 0) {
        close(sock_fd);
        sock_fd = -1;  // Ensure it's reset
    }
    
    sock_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock_fd < 0) {
        ESP_LOGE(TAG, "Failed to create UDP socket: %s", strerror(errno));
        return false;
    }
    
    return setSocketOptions();
}

bool device_groups_WiFiUDP::setSocketOptions() {
    // Set socket to non-blocking mode
    int flags = fcntl(sock_fd, F_GETFL, 0);
    if (flags < 0) {
        ESP_LOGE(TAG, "Failed to get socket flags: %s", strerror(errno));
        return false;
    }
    
    if (fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        ESP_LOGE(TAG, "Failed to set socket non-blocking: %s", strerror(errno));
        return false;
    }
    
    // Allow address reuse
    int opt = 1;
    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        ESP_LOGE(TAG, "Failed to set SO_REUSEADDR: %s", strerror(errno));
        return false;
    }
    
    // Set receive timeout (shorter for better responsiveness)
    struct timeval tv;
    tv.tv_sec = 0;  // 0 seconds
    tv.tv_usec = 100000;  // 100ms timeout (reduced from 1 second)
    if (setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        ESP_LOGW(TAG, "Failed to set receive timeout: %s", strerror(errno));
    }
    
    // Set send timeout to prevent blocking
    tv.tv_sec = 0;
    tv.tv_usec = 100000;  // 100ms timeout
    if (setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        ESP_LOGW(TAG, "Failed to set send timeout: %s", strerror(errno));
    }
    
    // Set socket buffer sizes for better performance
    int buffer_size = 8192;  // 8KB buffer
    if (setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        ESP_LOGW(TAG, "Failed to set receive buffer size: %s", strerror(errno));
    }
    
    if (setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        ESP_LOGW(TAG, "Failed to set send buffer size: %s", strerror(errno));
    }
    
    return true;
}

bool device_groups_WiFiUDP::isNetworkReady() {
    esp_netif_t* netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
    if (netif == nullptr) {
        ESP_LOGW(TAG, "No WiFi STA interface found");
        return false;
    }
    esp_netif_ip_info_t ip_info;
    if (esp_netif_get_ip_info(netif, &ip_info) != ESP_OK) {
        ESP_LOGW(TAG, "Failed to get IP info");
        return false;
    }
    if (ip_info.ip.addr == 0) {
        ESP_LOGW(TAG, "No valid IP address");
        return false;
    }
    return true;
}

bool device_groups_WiFiUDP::beginMulticast(uint16_t port, const char* multicast_ip, const char* interface_ip) {
    if (!isNetworkReady()) {
        ESP_LOGE(TAG, "Network not ready for multicast begin");
        return false;
    }
    
    if (!initSocket()) {
        return false;
    }
    
    struct sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_port = htons(port);
    
    if (bind(sock_fd, (struct sockaddr*)&local_addr, sizeof(local_addr)) < 0) {
        ESP_LOGE(TAG, "Failed to bind UDP socket to port %d: %s", port, strerror(errno));
        close(sock_fd);
        sock_fd = -1;
        return false;
    }
    
    // Join multicast group
    struct ip_mreq mreq;
    mreq.imr_multiaddr.s_addr = inet_addr(multicast_ip);
    mreq.imr_interface.s_addr = inet_addr(interface_ip);
    
    if (setsockopt(sock_fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        ESP_LOGE(TAG, "Failed to join multicast group: %s", strerror(errno));
        close(sock_fd);
        sock_fd = -1;
        return false;
    }
    
    is_connected = true;
    ESP_LOGVV(TAG, "Joined multicast group %s on port %d", multicast_ip, port);
    return true;
}

bool device_groups_WiFiUDP::beginMulticast(const IPAddress& multicast_ip, uint16_t port) {
    if (!isNetworkReady()) {
        ESP_LOGE(TAG, "Network not ready for multicast begin");
        return false;
    }
    
    if (!initSocket()) {
        return false;
    }
    
    struct sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_port = htons(port);
    
    if (bind(sock_fd, (struct sockaddr*)&local_addr, sizeof(local_addr)) < 0) {
        ESP_LOGE(TAG, "Failed to bind UDP socket to port %d: %s", port, strerror(errno));
        close(sock_fd);
        sock_fd = -1;
        return false;
    }
    
    // Join multicast group
    struct ip_mreq mreq;
    mreq.imr_multiaddr.s_addr = htonl((multicast_ip[0] << 24) | (multicast_ip[1] << 16) | (multicast_ip[2] << 8) | multicast_ip[3]);
    mreq.imr_interface.s_addr = INADDR_ANY;  // Use default interface
    
    if (setsockopt(sock_fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        ESP_LOGE(TAG, "Failed to join multicast group: %s", strerror(errno));
        close(sock_fd);
        sock_fd = -1;
        return false;
    }
    
    is_connected = true;
    ESP_LOGVV(TAG, "Joined multicast group on port %d", port);
    return true;
}

void device_groups_WiFiUDP::stop() {
    if (sock_fd >= 0) {
        close(sock_fd);
        sock_fd = -1;
    }
    is_connected = false;
    
    if (send_buffer) {
        free(send_buffer);
        send_buffer = nullptr;
        send_buffer_size = 0;
    }
    if (recv_buffer) {
        free(recv_buffer);
        recv_buffer = nullptr;
        recv_buffer_size = 0;
    }
    send_data_length = 0;
    recv_data_length = 0;
    recv_read_position = 0;
    ESP_LOGVV(TAG, "UDP socket stopped");
}

bool device_groups_WiFiUDP::beginPacket(const IPAddress& ip, uint16_t port) {
    // Don't recreate socket unnecessarily - just validate existing one
    if (sock_fd < 0) {
        ESP_LOGE(TAG, "No socket available for packet to %u.%u.%u.%u:%d", 
                 ip[0], ip[1], ip[2], ip[3], port);
        return false;
    }
    
    remote_addr.sin_addr.s_addr = htonl((ip[0] << 24) | (ip[1] << 16) | (ip[2] << 8) | ip[3]);
    remote_addr.sin_port = htons(port);
    
    ESP_LOGVV(TAG, "Prepared packet for %u.%u.%u.%u:%d", ip[0], ip[1], ip[2], ip[3], port);
    return true;
}

bool device_groups_WiFiUDP::endPacket() {
    if (sock_fd < 0) {
        ESP_LOGE(TAG, "No valid socket for packet transmission");
        return false;
    }
    
    if (send_data_length == 0) {
        ESP_LOGW(TAG, "Attempting to send empty packet");
        return false;
    }
    
    // Detect potential retry loops (Arduino devices not sending ACKs)
    uint32_t current_time = esp_timer_get_time() / 1000; // Convert to milliseconds
    bool potential_retry_loop = false;
    
    // Check if we're sending to the same address repeatedly within a short time
    static struct sockaddr_in last_remote_addr = {0};
    static uint32_t last_send_time = 0;
    
    if (memcmp(&remote_addr, &last_remote_addr, sizeof(remote_addr)) == 0) {
        if (current_time - last_send_time < 2000) { // Same address within 2 seconds
            consecutive_retries++;
            if (consecutive_retries > 10) { // More than 10 retries to same address
                potential_retry_loop = true;
                ESP_LOGW(TAG, "Potential retry loop detected to %s:%d (attempt %u) - likely non-ACK device", 
                         inet_ntoa(remote_addr.sin_addr), ntohs(remote_addr.sin_port), consecutive_retries);
            }
        } else {
            consecutive_retries = 0; // Reset if enough time has passed
        }
    } else {
        consecutive_retries = 0; // Reset for new address
    }
    
    last_remote_addr = remote_addr;
    last_send_time = current_time;
    
    // Check if we're sending an ACK packet for diagnostic purposes
    bool sending_ack = false;
    if (send_data_length >= 20 && strncmp(send_buffer, "TASMOTA_DGR", 11) == 0) {
        if (send_data_length > 16) {
            uint16_t flags = (send_buffer[16] << 8) | send_buffer[15];
            sending_ack = (flags & 0x08) != 0;  // DGR_FLAG_ACK = 8
        }
    }
    
    ESP_LOGVV(TAG, "Attempting to send UDP packet: %d bytes to %s:%d (retry count: %u, ACK: %s)", 
             send_data_length, inet_ntoa(remote_addr.sin_addr), ntohs(remote_addr.sin_port), consecutive_retries,
             sending_ack ? "yes" : "no");
             
    // Log ACK packets at higher verbosity for debugging
    if (sending_ack) {
        ESP_LOGD(TAG, "Sending ACK packet to %s:%d", 
                 inet_ntoa(remote_addr.sin_addr), ntohs(remote_addr.sin_port));
    }
    
    // If we detect a retry loop, introduce progressive delays to reduce flooding
    if (potential_retry_loop && consecutive_retries > 20) {
        uint32_t delay_ms = (consecutive_retries - 20) * 100; // Progressive delay
        if (delay_ms > 2000) delay_ms = 2000; // Cap at 2 seconds
        ESP_LOGD(TAG, "Introducing delay of %ums to reduce retry flooding", delay_ms);
        vTaskDelay(pdMS_TO_TICKS(delay_ms));
    }
    
    int retries = MAX_RETRIES;
    while (retries-- > 0) {
        ssize_t sent = sendto(sock_fd, send_buffer, send_data_length, 0,
                              (struct sockaddr*)&remote_addr, sizeof(remote_addr));
        
        if (sent >= 0) {
            // Verify that the entire packet was sent
            if (sent == send_data_length) {
                // ESP_LOGD(TAG, "UDP packet sent successfully (%d bytes)", (int)sent);
                packets_sent++;
                // Clear the send buffer completely
                send_data_length = 0;
                if (send_buffer) {
                    memset(send_buffer, 0, send_buffer_size);
                }
                
                // Log stats periodically
                uint32_t current_time = esp_timer_get_time() / 1000000; // Convert to seconds
                if (current_time - last_stats_time > 60) { // Every 60 seconds
                    ESP_LOGI(TAG, "UDP Stats: Sent=%u, Received=%u, Failures=%u", 
                             packets_sent, packets_received, send_failures);
                    last_stats_time = current_time;
                }
                
                return true;
            } else {
                ESP_LOGW(TAG, "Partial packet send: %d of %d bytes sent", (int)sent, send_data_length);
                // Don't count this as a successful send, continue retrying
            }
        } else {
            int error = errno;
            if (error == EAGAIN || error == EWOULDBLOCK) {
                // Non-blocking socket would block, try again
                ESP_LOGVV(TAG, "Socket would block, retrying... (%d retries left)", retries);
                vTaskDelay(pdMS_TO_TICKS(RETRY_DELAY_MS));
                continue;
            } else if (error == ENOBUFS || error == ENOMEM) {
                // Network buffer full, wait longer before retrying
                ESP_LOGW(TAG, "Network buffer full, waiting longer... (%d retries left)", retries);
                vTaskDelay(pdMS_TO_TICKS(RETRY_DELAY_MS * 2));
                continue;
            } else {
                ESP_LOGE(TAG, "Failed to send UDP packet: %s (retries left: %d)", strerror(error), retries);
                break;
            }
        }
        
        if (retries > 0) {
            vTaskDelay(pdMS_TO_TICKS(RETRY_DELAY_MS));
        }
    }
    
    ESP_LOGE(TAG, "All retry attempts failed for UDP packet transmission");
    send_failures++;
    return false;
}

size_t device_groups_WiFiUDP::write(const uint8_t* data, size_t size) {
    if (!send_buffer) {
        send_buffer = (char*)malloc(DEFAULT_BUFFER_SIZE);
        send_buffer_size = DEFAULT_BUFFER_SIZE;
        if (!send_buffer) {
            ESP_LOGE(TAG, "Failed to allocate send buffer for UDP write");
            return 0;
        }
        memset(send_buffer, 0, send_buffer_size);
    }
    
    // Ensure buffer is large enough
    while (send_data_length + size > send_buffer_size) {
        size_t new_size = send_buffer_size * 2;
        char* new_buffer = (char*)realloc(send_buffer, new_size);
        if (!new_buffer) {
            ESP_LOGE(TAG, "Failed to resize send buffer from %d to %d bytes", send_buffer_size, new_size);
            // Free the old buffer to prevent memory leaks
            free(send_buffer);
            send_buffer = nullptr;
            send_buffer_size = 0;
            send_data_length = 0;
            return 0;
        }
        send_buffer = new_buffer;
        send_buffer_size = new_size;
        ESP_LOGVV(TAG, "Send buffer resized to %d bytes", new_size);
    }
    
    memcpy(send_buffer + send_data_length, data, size);
    send_data_length += size;
    // Reduced logging verbosity to prevent performance issues during packet storms
    // ESP_LOGD(TAG, "Wrote %d bytes to send buffer (total: %d)", size, send_data_length);
    return size;
}



int device_groups_WiFiUDP::parsePacket() {
    if (sock_fd < 0) {
        return 0;
    }
    
    // Allocate receive buffer if not already done
    if (!recv_buffer) {
        recv_buffer = (char*)malloc(DEFAULT_BUFFER_SIZE);
        recv_buffer_size = DEFAULT_BUFFER_SIZE;
        if (!recv_buffer) {
            ESP_LOGE(TAG, "Failed to allocate receive buffer for parsePacket");
            return 0;
        }
        memset(recv_buffer, 0, recv_buffer_size);
    }
    
    // Clear any previous packet data
    recv_data_length = 0;
    recv_read_position = 0;
    memset(recv_buffer, 0, recv_buffer_size);
    
    struct sockaddr_in temp_sender_addr;
    socklen_t sender_len = sizeof(temp_sender_addr);
    
    ssize_t received = recvfrom(sock_fd, recv_buffer, recv_buffer_size - 1, MSG_DONTWAIT,
                               (struct sockaddr*)&temp_sender_addr, &sender_len);
    
    if (received > 0) {
        // Smart packet deduplication - be more lenient with ACK packets
        uint32_t current_time = esp_timer_get_time() / 1000; // Convert to milliseconds
        uint32_t packet_hash = 0;
        
        // Check if this might be an ACK packet by looking at packet structure
        bool is_likely_ack = false;
        if (received >= 20 && strncmp(recv_buffer, "TASMOTA_DGR", 11) == 0) {
            // Look at the flags field (typically at offset 15-16 in device group messages)
            if (received > 16) {
                uint16_t flags = (recv_buffer[16] << 8) | recv_buffer[15];
                is_likely_ack = (flags & 0x08) != 0;  // DGR_FLAG_ACK = 8
            }
        }
        
        // Generate hash of packet content and sender for deduplication
        for (int i = 0; i < received && i < 32; i++) { // Hash first 32 bytes
            packet_hash = ((packet_hash << 5) + packet_hash) + recv_buffer[i];
        }
        packet_hash ^= temp_sender_addr.sin_addr.s_addr;
        packet_hash ^= temp_sender_addr.sin_port;
        packet_hash ^= (uint32_t)received;  // Include packet size in hash
        
        // Check if this is a duplicate packet within the deduplication window
        // Use shorter deduplication window for ACK packets (they're more time-sensitive)
        uint32_t dedup_window = is_likely_ack ? (DEDUP_WINDOW_MS / 4) : DEDUP_WINDOW_MS;
        if (packet_hash == last_packet_hash && 
            (current_time - last_packet_time) < dedup_window) {
            ESP_LOGVV(TAG, "Dropping duplicate packet (hash: 0x%08x, time: %d ms, ACK: %s)", 
                     packet_hash, current_time - last_packet_time, is_likely_ack ? "yes" : "no");
            return 0;
        }
        
        // Update deduplication tracking
        last_packet_hash = packet_hash;
        last_packet_time = current_time;
        
        recv_data_length = received;
        recv_read_position = 0;
        recv_buffer[recv_data_length] = '\0';  // Null-terminate for string operations
        
        // Store sender info separately - DON'T overwrite remote_addr!
        sender_addr = temp_sender_addr;
        
        // Check if this is an ACK packet for diagnostic purposes
        bool is_ack_response = is_likely_ack;
        ESP_LOGVV(TAG, "Received UDP packet: %d bytes from %s:%d (hash: 0x%08x, ACK: %s)", 
                 (int)received, inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port), packet_hash,
                 is_ack_response ? "yes" : "no");
        
        packets_received++;
        
        // Log ACK packets at higher verbosity for debugging
        if (is_ack_response) {
            ESP_LOGD(TAG, "ACK packet received from %s:%d", 
                     inet_ntoa(sender_addr.sin_addr), ntohs(sender_addr.sin_port));
        }
        
        return recv_data_length;
    } else if (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        ESP_LOGE(TAG, "Error receiving UDP packet: %s", strerror(errno));
    }
    
    return 0;
}

int device_groups_WiFiUDP::read(uint8_t* data, size_t size) {
    if (recv_read_position >= recv_data_length) {
        return -1;
    }
    
    size_t available = recv_data_length - recv_read_position;
    size_t to_read = (size < available) ? size : available;
    
    memcpy(data, recv_buffer + recv_read_position, to_read);
    recv_read_position += to_read;
    
    return to_read;
}

void device_groups_WiFiUDP::flush() {
    // Clear the receive buffer
    recv_data_length = 0;
    recv_read_position = 0;
    if (recv_buffer) {
        memset(recv_buffer, 0, recv_buffer_size);
    }
}

IPAddress device_groups_WiFiUDP::remoteIP() {
    // Use sender_addr for received packets, remote_addr for sent packets
    uint32_t ip = ntohl(sender_addr.sin_addr.s_addr);
    return IPAddress((ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
}

#endif  // USE_ESP_IDF || ESP_IDF_VERSION || CONFIG_IDF_TARGET 
