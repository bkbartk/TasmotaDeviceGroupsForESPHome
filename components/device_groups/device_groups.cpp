﻿#include "device_groups.h"
#include "esphome/core/helpers.h"
#include "esphome/core/log.h"
#include "esphome/components/network/ip_address.h"
#include "esphome/components/network/util.h"

namespace esphome {
namespace device_groups {

static const char *const TAG = "dgr";

char *IPAddressToString(const IPAddress &ip_address) {
  static char buffer[16];
  sprintf_P(buffer, PSTR("%u.%u.%u.%u"), ip_address[0], ip_address[1], ip_address[2], ip_address[3]);
  return buffer;
}

uint8_t *BeginDeviceGroupMessage(struct device_group *device_group, uint16_t flags, bool hold_sequence = false) {
  uint8_t *message_ptr = &device_group->message[device_group->message_header_length];
  if (!hold_sequence && !++device_group->outgoing_sequence)
    device_group->outgoing_sequence = 1;
  *message_ptr++ = device_group->outgoing_sequence & 0xff;
  *message_ptr++ = device_group->outgoing_sequence >> 8;
  *message_ptr++ = flags & 0xff;
  *message_ptr++ = flags >> 8;
  return message_ptr;
}

uint32_t DeviceGroupSharedMask(uint8_t item) {
  uint32_t mask = 0;
  if (item == DGR_ITEM_LIGHT_BRI || item == DGR_ITEM_BRI_POWER_ON)
    mask = DGR_SHARE_LIGHT_BRI;
  else if (item == DGR_ITEM_POWER)
    mask = DGR_SHARE_POWER;
  else if (item == DGR_ITEM_LIGHT_SCHEME)
    mask = DGR_SHARE_LIGHT_SCHEME;
  else if (item == DGR_ITEM_LIGHT_FIXED_COLOR || item == DGR_ITEM_LIGHT_CHANNELS)
    mask = DGR_SHARE_LIGHT_COLOR;
  else if (item == DGR_ITEM_LIGHT_FADE || item == DGR_ITEM_LIGHT_SPEED)
    mask = DGR_SHARE_LIGHT_FADE;
  else if (item == DGR_ITEM_BRI_PRESET_LOW || item == DGR_ITEM_BRI_PRESET_HIGH)
    mask = DGR_SHARE_DIMMER_SETTINGS;
  else if (item == DGR_ITEM_EVENT)
    mask = DGR_SHARE_EVENT;
  return mask;
}

void device_groups::setup() {
  ESP_LOGCONFIG(TAG, "Setting up Device Groups Component for group %s", this->device_group_name_.c_str());

  std::string registered_group_name = this->device_group_name_;
#if defined(ESP8266)
  registered_group_names.push_back(registered_group_name);
#endif

#ifdef USE_SWITCH
  for (switch_::Switch *obj : this->switches_) {
    obj->add_on_state_callback([this, obj](bool state) {
      ExecuteCommandPower(1, state, SRC_SWITCH);
    });
  }
#endif
#ifdef USE_LIGHT
  for (light::LightState *obj : this->lights_) {
    set_light_intial_values(obj);

    obj->add_new_remote_values_callback([this, obj]() {
      bool power_state;
      float brightness, color_brightness, red, green, blue, cold_white, warm_white;
      esphome::light::ColorMode color_mode;

      get_light_values(obj, power_state, brightness, color_brightness, red, green, blue, cold_white, warm_white, color_mode);

      if (power_state != previous_power_state) {
        ExecuteCommandPower(1, power_state, SRC_LIGHT);
      }

      if (power_state != previous_power_state
          ||red != previous_red
          || green != previous_green
          || blue != previous_blue
          || warm_white != previous_warm_white
          || cold_white != previous_cold_white
          || brightness != previous_brightness
          || color_brightness != previous_color_brightness
          || color_mode != previous_color_mode
      ) {
        uint8_t light_channels[6] = {
          (uint8_t)(red * 255),
          (uint8_t)(green * 255),
          (uint8_t)(blue * 255),
          (uint8_t)(cold_white * 255),
          (uint8_t)(warm_white * 255),
          0
        };

        SendDeviceGroupMessage(1, (DevGroupMessageType) (DGR_MSGTYP_UPDATE),
                              DGR_ITEM_LIGHT_CHANNELS, light_channels);
      }

      if (brightness != previous_brightness) {
        SendDeviceGroupMessage(1, (DevGroupMessageType) (DGR_MSGTYP_UPDATE + DGR_MSGTYPFLAG_WITH_LOCAL),
                              DGR_ITEM_LIGHT_BRI, (uint8_t)(brightness * 255));
      }

      previous_power_state = power_state;
      previous_brightness = brightness;
      previous_color_brightness = color_brightness;
      previous_red = red;
      previous_green = green;
      previous_blue = blue;
      previous_cold_white = cold_white;
      previous_warm_white = warm_white;
      previous_color_mode = color_mode;
    });
  }
#endif
}

#ifdef USE_LIGHT
void device_groups::get_light_values(light::LightState *obj, bool &power_state, float &brightness, float &color_brightness, float &red, float &green, float &blue, float &cold_white, float &warm_white, esphome::light::ColorMode &color_mode) {
  power_state = obj->remote_values.is_on();
  brightness = obj->remote_values.get_brightness();
  color_brightness = obj->remote_values.get_color_brightness();

  if (obj->get_traits().supports_color_capability(light::ColorCapability::COLOR_TEMPERATURE)) {
    float min_mireds = 0.0f, max_mireds = 0.0f, color_temperature = 0.0f;
    color_temperature = obj->remote_values.get_color_temperature();
    min_mireds = obj->get_traits().get_min_mireds();
    max_mireds = obj->get_traits().get_max_mireds();
    warm_white = (color_temperature - min_mireds) / (max_mireds - min_mireds);
    cold_white = 1.0f - warm_white;
  } else if (obj->get_traits().supports_color_capability(light::ColorCapability::COLD_WARM_WHITE)) {
    cold_white = obj->remote_values.get_cold_white();
    warm_white = obj->remote_values.get_warm_white();
  } else if (obj->get_traits().supports_color_capability(light::ColorCapability::WHITE)) {
    warm_white = cold_white = obj->remote_values.get_white();
  }

  if (obj->get_traits().supports_color_capability(light::ColorCapability::RGB) && color_brightness > 0) {
    red = obj->remote_values.get_red();
    green = obj->remote_values.get_green();
    blue = obj->remote_values.get_blue();
  }

  color_mode = obj->remote_values.get_color_mode();

  bool has_rgb_mode = color_mode & light::ColorCapability::RGB;
  bool has_white_mode = color_mode & light::ColorCapability::WHITE
      || color_mode & light::ColorCapability::COLOR_TEMPERATURE
      || color_mode & light::ColorCapability::COLD_WARM_WHITE;
  bool has_rgb_values = red > 0 || green > 0 || blue > 0;
  bool has_white_values = warm_white > 0 || cold_white > 0;

  if ((!has_rgb_mode && has_rgb_values) || has_rgb_mode && !has_rgb_values) {
    red = green = blue = 0;
    color_brightness - 0;
    color_mode = (light::ColorMode)(static_cast<uint8_t>(color_mode) ^ static_cast<uint8_t>(light::ColorCapability::RGB));
    has_rgb_mode = has_rgb_values = false;
  }

  if ((!has_white_mode && has_white_values) || (has_white_mode && !has_white_values)) {
    warm_white = cold_white = 0;
    color_mode = (light::ColorMode)(static_cast<uint8_t>(color_mode) ^ static_cast<uint8_t>(light::ColorCapability::WHITE));
    color_mode = (light::ColorMode)(static_cast<uint8_t>(color_mode) ^ static_cast<uint8_t>(light::ColorCapability::COLOR_TEMPERATURE));
    color_mode = (light::ColorMode)(static_cast<uint8_t>(color_mode) ^ static_cast<uint8_t>(light::ColorCapability::COLD_WARM_WHITE));
    has_white_mode = has_white_values = false;
  }
}

void device_groups::set_light_intial_values(light::LightState *obj) {
  bool power_state;
  float brightness, color_brightness, red, green, blue, cold_white, warm_white;
  esphome::light::ColorMode color_mode = esphome::light::ColorMode::UNKNOWN;
  get_light_values(obj, power_state, brightness, color_brightness, red, green, blue, cold_white, warm_white, color_mode);

  previous_power_state = power_state;
  previous_brightness = brightness;
  previous_color_brightness = color_brightness;
  previous_red = red;
  previous_green = green;
  previous_blue = blue;
  previous_cold_white = cold_white;
  previous_warm_white = warm_white;
  previous_color_mode = color_mode;
}
#endif

void device_groups::dump_config() {
  ESP_LOGCONFIG(TAG, "Device Group %s configuration:", this->device_group_name_.c_str());
  ESP_LOGCONFIG(TAG, " - Send Mask: 0x%08x", send_mask_);
  ESP_LOGCONFIG(TAG, " - Receive Mask: 0x%08x", receive_mask_);
#ifdef USE_SWITCH
  ESP_LOGCONFIG(TAG, "Switches:");
  if (this->switches_.empty()) {
    ESP_LOGCONFIG(TAG, " - No switches mapped");
  } else {
    for (switch_::Switch *obj : this->switches_) {
      ESP_LOGCONFIG(TAG, " - %s (%s)", obj->get_object_id().c_str(), obj->get_name().c_str());
    }
  }
#else
  ESP_LOGCONFIG(TAG, "Switches not configured");
#endif
#ifdef USE_LIGHT
  ESP_LOGCONFIG(TAG, "Lights:");
  if (this->lights_.empty()) {
    ESP_LOGCONFIG(TAG, " - No lights mapped");
  } else {
    for (light::LightState *obj : this->lights_) {
      ESP_LOGCONFIG(TAG, " - %s (%s)", obj->get_object_id().c_str(), obj->get_name().c_str());
    }
  }
#else
  ESP_LOGCONFIG(TAG, "Lights not configured");
#endif

  setup_complete = true;
}

void device_groups::loop() {
  if (!this->update_)
    return;

  if (!network::is_connected()) {
    DeviceGroupsStop();
    dgr_state = DGR_STATE_UNINTIALIZED;
    return;
  }

  if (!setup_complete) {
    return;
  }

  if (dgr_state == DGR_STATE_UNINTIALIZED) {
    dgr_state = DGR_STATE_INITIALIZING;
    InitTasmotaCompatibility();
    // DeviceGroupsInit(); // automatically called by DeviceGrupsStart()
    if (DeviceGroupsStart()) {
      dgr_state = DGR_STATE_INITIALIZED;
    } else {
      dgr_state = DGR_STATE_UNINTIALIZED;
    }
  }

  if (dgr_state != DGR_STATE_INITIALIZED) {
    return;
  }

  DeviceGroupsLoop();
}

void device_groups::DeviceGroupsInit() {
  // If no module set the device group count, ...
  if (!device_group_count) {
    // If relays in separate device groups is enabled, set the device group count to highest numbered
    // button.
    /*if (Settings->flag4.multiple_device_groups) {  // SetOption88 - Enable relays in separate device groups
      for (uint32_t relay_index = 0; relay_index < MAX_RELAYS; relay_index++) {
        if (PinUsed(GPIO_REL1, relay_index)) device_group_count = relay_index + 1;
      }
    }*/

    // Set up a minimum of one device group.
    if (!device_group_count)
      device_group_count = 1;
    else if (device_group_count > MAX_DEV_GROUP_NAMES)
      device_group_count = MAX_DEV_GROUP_NAMES;
  }

  // If there are more device group names set than the number of device groups needed by the
  // module, use the device group name count as the device group count.
  /*for (; device_group_count < MAX_DEV_GROUP_NAMES; device_group_count++) {
    if (!*SettingsText(SET_DEV_GROUP_NAME1 + device_group_count)) break;
  }*/

  // Initialize the device information for each device group.
  device_groups_ = (struct device_group *) calloc(device_group_count, sizeof(struct device_group));
  if (!device_groups_) {
    ESP_LOGE(TAG, "Error allocating %u-element array", device_group_count);
    return;
  }

  struct device_group *device_group = device_groups_;
  for (uint32_t device_group_index = 0; device_group_index < device_group_count; device_group_index++, device_group++) {
    /*strcpy(device_group->group_name, SettingsText(SET_DEV_GROUP_NAME1 + device_group_index));

    // If the device group name is not set, use the MQTT group topic (with the device group index +
    // 1 appended for device group indices > 0).
    if (!device_group->group_name[0]) {
      strcpy(device_group->group_name, SettingsText(SET_MQTT_GRP_TOPIC));
      if (device_group_index) {
        snprintf_P(device_group->group_name, sizeof(device_group->group_name), PSTR("%s%u"), device_group->group_name,
    device_group_index + 1);
      }
    }*/
    strcpy(device_group->group_name, this->device_group_name_.c_str());
    device_group->message_header_length =
        sprintf_P((char *) device_group->message, PSTR("%s%s"), kDeviceGroupMessage, device_group->group_name) + 1;
    device_group->no_status_share = 0;
    device_group->last_full_status_sequence = -1;
  }

  // If both in and out shared items masks are 0, assume they're unitialized and initialize them.
  if (!Settings->device_group_share_in && !Settings->device_group_share_out) {
    Settings->device_group_share_in = Settings->device_group_share_out = 0xffffffff;
  }

  device_groups_initialized = true;
}

bool device_groups::DeviceGroupsStart() {
  if (Settings->flag4.device_groups_enabled && !device_groups_up && !TasmotaGlobal.restart_flag) {
    // If we haven't successfuly initialized device groups yet, attempt to do it now.
    if (!device_groups_initialized) {
      DeviceGroupsInit();
      if (!device_groups_initialized)
        return false;
    }

    // Subscribe to device groups multicasts.
#ifdef ESP8266
    if (!device_groups_udp.beginMulticast(WiFi.localIP(), IPAddress(DEVICE_GROUPS_ADDRESS), DEVICE_GROUPS_PORT)) {
      ESP_LOGE(TAG, "Error subscribing");
      return false;
    }
#else
    if (!device_groups_udp.beginMulticast(IPAddress(DEVICE_GROUPS_ADDRESS), DEVICE_GROUPS_PORT)) {
      ESP_LOGE(TAG, "Error subscribing");
      return false;
    }
#endif
    device_groups_up = true;

    // The WiFi was down but now it's up and device groups is initialized. (Re-)discover devices in
    // our device group(s). Load the status request message for all device groups. This message will
    // be multicast 10 times at 200ms intervals.
    next_check_time = millis() + 2000;
    struct device_group *device_group = device_groups_;
    for (uint32_t device_group_index = 0; device_group_index < device_group_count;
         device_group_index++, device_group++) {
      device_group->next_announcement_time = -1;
      device_group->message_length =
          BeginDeviceGroupMessage(device_group, DGR_FLAG_RESET | DGR_FLAG_STATUS_REQUEST) - device_group->message;
      device_group->initial_status_requests_remaining = 10;
      device_group->next_ack_check_time = next_check_time;
    }
    ESP_LOGD(TAG, "%s (Re)discovering members", this->device_group_name_.c_str());
  }

  return true;
}

void device_groups::DeviceGroupsStop() {
  device_groups_udp.flush();
  device_groups_up = false;
}

void device_groups::SendReceiveDeviceGroupMessage(struct device_group *device_group,
                                                  struct device_group_member *device_group_member, uint8_t *message,
                                                  int message_length, bool received) {
  bool item_processed = false;
  uint16_t message_sequence;
  uint16_t flags;
  int device_group_index = device_group - device_groups_;
  int log_length;
  int log_remaining;
  char *log_ptr;

  // Find the end and start of the actual message (after the header).
  uint8_t *message_end_ptr = message + message_length;
  uint8_t *message_ptr = message + strlen((char *) message) + 1;

  // Get the message sequence and flags.
  if (message_ptr + 4 > message_end_ptr)
    return;  // Malformed message - must be at least 16-bit sequence, 16-bit flags left
  message_sequence = *message_ptr++;
  message_sequence |= *message_ptr++ << 8;
  flags = *message_ptr++;
  flags |= *message_ptr++ << 8;

  // Initialize the log buffer.
  char *log_buffer = (char *) malloc(512);
  log_length = sprintf(log_buffer, PSTR("%s %s message %s %s: seq=%u, flags=%u"),
                       (received ? PSTR("Received") : PSTR("Sending")), device_group->group_name,
                       (received ? PSTR("from") : PSTR("to")),
                       (device_group_member ? IPAddressToString(device_group_member->ip_address)
                        : received          ? PSTR("local")
                                            : PSTR("network")),
                       message_sequence, flags);
  log_ptr = log_buffer + log_length;
  log_remaining = 512 - log_length;

  // If this is an announcement, just log it.
  if (flags == DGR_FLAG_ANNOUNCEMENT)
    goto write_log;

  // If this is a received ack message, save the message sequence if it's newer than the last ack we
  // received from this member.
  if (flags == DGR_FLAG_ACK) {
    if (received && device_group_member &&
        (message_sequence > device_group_member->acked_sequence ||
         device_group_member->acked_sequence - message_sequence < 64536)) {
      device_group_member->acked_sequence = message_sequence;
    }
    goto write_log;
  }

  // If this is a received message, send an ack message to the sender.
  if (device_group_member) {
    if (received) {
      if (!(flags & DGR_FLAG_MORE_TO_COME)) {
        *(message_ptr - 2) = DGR_FLAG_ACK;
        *(message_ptr - 1) = 0;
        SendReceiveDeviceGroupMessage(device_group, device_group_member, message, message_ptr - message, false);
      }
    }

    // If we're sending this message directly to a member, it's a resend.
    else {
      log_length = snprintf(log_ptr, log_remaining, PSTR(", last ack=%u"), device_group_member->acked_sequence);
      log_ptr += log_length;
      log_remaining -= log_length;
      goto write_log;
    }
  }

  // If this is a status request message, skip item processing.
  if ((flags & DGR_FLAG_STATUS_REQUEST))
    goto write_log;

  // If this is a received message, ...
  if (received) {
    // If we already processed this or a later message from this group member, ignore this message.
    if (device_group_member) {
      if (message_sequence <= device_group_member->received_sequence) {
        if (message_sequence == device_group_member->received_sequence ||
            device_group_member->received_sequence - message_sequence > 64536) {
          log_length = snprintf(log_ptr, log_remaining, PSTR(" (old)"));
          log_ptr += log_length;
          log_remaining -= log_length;
          goto write_log;
        }
      }
      device_group_member->received_sequence = message_sequence;
    }

    /*
      XdrvMailbox
      bool          grpflg
      bool          usridx
      uint16_t      command_code    Item code
      uint32_t      index           0:15 Flags, 16:31 Message sequence
      uint32_t      data_len        String item value length
      int32_t       payload         Integer item value
      char         *topic           Pointer to device group index
      char         *data            Pointer to non-integer item value
      char         *command         nullptr
    */
    XdrvMailbox.command = nullptr;  // Indicates the source is a device group update
    XdrvMailbox.index = flags | message_sequence << 16;
    if (device_group_index == 0 && first_device_group_is_local)
      XdrvMailbox.index |= DGR_FLAG_LOCAL;
    XdrvMailbox.topic = (char *) &device_group_index;
    if (flags & (DGR_FLAG_MORE_TO_COME | DGR_FLAG_DIRECT))
      TasmotaGlobal.skip_light_fade = true;

    // Set the flag to ignore device group send message request so callbacks from the drivers do not
    // send updates.
    ignore_dgr_sends = true;
  }

  uint8_t item;
  uint8_t item_flags;
  int32_t value;
  uint32_t mask;
  item_flags = 0;
  for (;;) {
    if (message_ptr >= message_end_ptr)
      goto badmsg;  // Malformed message
    item = *message_ptr++;
    if (!item)
      break;  // Done

#ifdef DEVICE_GROUPS_DEBUG
    switch (item) {
      case DGR_ITEM_FLAGS:
      case DGR_ITEM_LIGHT_FADE:
      case DGR_ITEM_LIGHT_SPEED:
      case DGR_ITEM_LIGHT_BRI:
      case DGR_ITEM_LIGHT_SCHEME:
      case DGR_ITEM_LIGHT_FIXED_COLOR:
      case DGR_ITEM_BRI_PRESET_LOW:
      case DGR_ITEM_BRI_PRESET_HIGH:
      case DGR_ITEM_BRI_POWER_ON:
      case DGR_ITEM_POWER:
      case DGR_ITEM_NO_STATUS_SHARE:
      case DGR_ITEM_EVENT:
      case DGR_ITEM_LIGHT_CHANNELS:
        break;
      default:
        ESP_LOGE(TAG, "*** Invalid item=%u", item);
    }
#endif  // DEVICE_GROUPS_DEBUG

    log_length = snprintf(log_ptr, log_remaining, ", %u=", item);
    log_ptr += log_length;
    log_remaining -= log_length;
    log_length = 0;
    if (item <= DGR_ITEM_LAST_32BIT) {
      value = *message_ptr++;
      if (item > DGR_ITEM_MAX_8BIT) {
        value |= *message_ptr++ << 8;
        if (item > DGR_ITEM_MAX_16BIT) {
          value |= *message_ptr++ << 16;
          value |= *message_ptr++ << 24;
#ifdef USE_DEVICE_GROUPS_SEND
          if (item < DGR_ITEM_LAST_32BIT)
            device_group->values_32bit[item - DGR_ITEM_MAX_16BIT - 1] =
                (item == DGR_ITEM_POWER ? value & 0xffffff : value);
#endif  // USE_DEVICE_GROUPS_SEND
        }
#ifdef USE_DEVICE_GROUPS_SEND
        else {
          if (item < DGR_ITEM_LAST_16BIT)
            device_group->values_16bit[item - DGR_ITEM_MAX_8BIT - 1] = value;
        }
#endif  // USE_DEVICE_GROUPS_SEND
      }
#ifdef USE_DEVICE_GROUPS_SEND
      else {
        if (item < DGR_ITEM_LAST_8BIT)
          device_group->values_8bit[item] = value;
      }
#endif  // USE_DEVICE_GROUPS_SEND
      log_length = snprintf(log_ptr, log_remaining, "%i", value);
    } else {
      value = *message_ptr++;
      if (received)
        XdrvMailbox.data = (char *) message_ptr;
      if (message_ptr + value >= message_end_ptr)
        goto badmsg;  // Malformed message
      if (item <= DGR_ITEM_MAX_STRING) {
        log_length = snprintf(log_ptr, log_remaining, PSTR("'%s'"), message_ptr);
      } else {
        switch (item) {
          case DGR_ITEM_LIGHT_CHANNELS:
            log_length = snprintf(log_ptr, log_remaining, PSTR("%u,%u,%u,%u,%u,%u"), *message_ptr, *(message_ptr + 1),
                                  *(message_ptr + 2), *(message_ptr + 3), *(message_ptr + 4), *(message_ptr + 5));
            break;
        }
      }
      message_ptr += value;
    }
    log_ptr += log_length;
    log_remaining -= log_length;

    if (received) {
      if (item == DGR_ITEM_FLAGS) {
        item_flags = value;
        continue;
      }

      mask = DeviceGroupSharedMask(item);
      if (item_flags & DGR_ITEM_FLAG_NO_SHARE)
        device_group->no_status_share |= mask;
      else
        device_group->no_status_share &= ~mask;

      if ((!(device_group->no_status_share & mask) || device_group_member == nullptr) &&
          (!mask || (mask & Settings->device_group_share_in))) {
        item_processed = true;
        XdrvMailbox.command_code = item;
        XdrvMailbox.payload = value;
        XdrvMailbox.data_len = value;
        *log_ptr++ = '*';
        log_remaining--;
        switch (item) {
          case DGR_ITEM_POWER:
            if (Settings->flag4.multiple_device_groups) {  // SetOption88 - Enable relays in separate device groups
              uint32_t device = Settings->device_group_tie[device_group_index];
              if (device && device <= TasmotaGlobal.devices_present) {
                bool on = (value & 1);
                if (on != ((TasmotaGlobal.power >> (device - 1)) & 1))
                  ExecuteCommandPower(device, (on ? POWER_ON : POWER_OFF), SRC_REMOTE);
              }
            } else if (XdrvMailbox.index & DGR_FLAG_LOCAL) {
              uint8_t mask_devices = value >> 24;
              if (mask_devices > TasmotaGlobal.devices_present)
                mask_devices = TasmotaGlobal.devices_present;
              for (uint32_t i = 0; i < mask_devices; i++) {
                uint32_t mask = 1 << i;
                bool on = (value & mask);
                if (on != (TasmotaGlobal.power & mask))
                  ExecuteCommandPower(i + 1, (on ? POWER_ON : POWER_OFF), SRC_REMOTE);
              }
            }
            break;
          case DGR_ITEM_NO_STATUS_SHARE:
            device_group->no_status_share = value;
            break;
#ifdef USE_RULES
          case DGR_ITEM_EVENT:
            CmndEvent();
            break;
#endif
          case DGR_ITEM_COMMAND:
            ExecuteCommand(XdrvMailbox.data, SRC_REMOTE);
            break;
          case DGR_ITEM_LIGHT_FADE:
            TasmotaGlobal.fade = XdrvMailbox.payload;
            break;
          case DGR_ITEM_LIGHT_SPEED:
            TasmotaGlobal.speed = XdrvMailbox.payload;
            break;
          case DGR_ITEM_LIGHT_SCHEME:
            TasmotaGlobal.scheme = XdrvMailbox.payload;
            break;
          case DGR_ITEM_LIGHT_FIXED_COLOR:
            break;
          case DGR_ITEM_LIGHT_BRI:
#ifdef USE_LIGHT
            for (light::LightState *obj : this->lights_) {
              auto call = obj->make_call();
              if (obj->remote_values.get_color_mode() & light::ColorCapability::RGB) {
                call.set_color_brightness_if_supported(XdrvMailbox.payload / 255.0f);
              } else {
                call.set_brightness_if_supported(XdrvMailbox.payload / 255.0f);
                call.set_color_brightness_if_supported(0);
              }
              call.perform();
            }
#endif
            break;
          case DGR_ITEM_LIGHT_CHANNELS:
#ifdef USE_LIGHT
            for (light::LightState *obj : this->lights_) {
              auto call = obj->make_call();
              const float red = (uint8_t) XdrvMailbox.data[0] / 255.0f;
              const float green = (uint8_t) XdrvMailbox.data[1] / 255.0f;
              const float blue = (uint8_t) XdrvMailbox.data[2] / 255.0f;
              const float cold_white = (uint8_t) XdrvMailbox.data[3] / 255.0f;
              const float warm_white = (uint8_t) XdrvMailbox.data[4] / 255.0f;
              const bool has_rgb = red + green + blue > 0.0f;
              const bool has_white = cold_white + warm_white > 0.0f;
              light::ColorMode color_mode = light::ColorMode::ON_OFF | light::ColorMode::BRIGHTNESS;

              if (has_rgb && obj->get_traits().supports_color_capability(light::ColorCapability::RGB)) {
                color_mode = color_mode | light::ColorCapability::RGB;
              }

              if (has_white && obj->get_traits().supports_color_capability(light::ColorCapability::COLOR_TEMPERATURE)) {
                color_mode = color_mode | light::ColorCapability::COLOR_TEMPERATURE;
              } else if (has_white && obj->get_traits().supports_color_capability(light::ColorCapability::COLD_WARM_WHITE)) {
                color_mode = color_mode | light::ColorCapability::COLD_WARM_WHITE;
              } else if (has_white && obj->get_traits().supports_color_capability(light::ColorCapability::WHITE)) {
                color_mode = color_mode | light::ColorCapability::WHITE;
              }

              call.set_color_mode_if_supported(color_mode);

              if (color_mode & light::ColorCapability::RGB) {
                call.set_red_if_supported(red);
                call.set_green_if_supported(green);
                call.set_blue_if_supported(blue);
              } else {
                call.set_red_if_supported(0);
                call.set_green_if_supported(0);
                call.set_blue_if_supported(0);
                call.set_color_brightness_if_supported(0);
              }

              if (has_white && color_mode & light::ColorCapability::COLOR_TEMPERATURE) {
                call.set_color_temperature_if_supported((warm_white * obj->get_traits().get_max_mireds()) + (cold_white * obj->get_traits().get_min_mireds()));
              } else if (has_white && color_mode & light::ColorCapability::COLD_WARM_WHITE) {
                call.set_cold_white_if_supported(cold_white);
                call.set_warm_white_if_supported(warm_white);
              } else if (has_white) {
                call.set_white_if_supported(cold_white > warm_white ? cold_white : warm_white);
              } else {
                call.set_white_if_supported(0);
              }

              call.perform();
            }
#endif
            break;
          case DGR_ITEM_STATUS:
#ifdef USE_SWITCH
            for (switch_::Switch *obj : this->switches_) {
              SendDeviceGroupMessage(1, (DevGroupMessageType) (DGR_MSGTYP_UPDATE), DGR_ITEM_POWER, obj->state);
            }
#endif
#ifdef USE_LIGHT
            for (light::LightState *obj : this->lights_) {
              float red = 0.0f, green = 0.0f, blue = 0.0f, cold_white = 0.0f, warm_white = 0.0f, brightness = 0.0f, color_brightness = 0.0f;
              brightness = obj->remote_values.get_brightness();
              color_brightness = obj->remote_values.get_color_brightness();

              if (obj->get_traits().supports_color_capability(light::ColorCapability::COLOR_TEMPERATURE)) {
                float min_mireds = 0.0f, max_mireds = 0.0f, color_temperature = 0.0f;
                color_temperature = obj->remote_values.get_color_temperature();
                min_mireds = obj->get_traits().get_min_mireds();
                max_mireds = obj->get_traits().get_max_mireds();
                warm_white = (color_temperature - min_mireds) / (max_mireds - min_mireds);
                cold_white = 1.0f - warm_white;
              } else if (obj->get_traits().supports_color_capability(light::ColorCapability::COLD_WARM_WHITE)) {
                cold_white = obj->remote_values.get_cold_white();
                warm_white = obj->remote_values.get_warm_white();
              } else if (obj->get_traits().supports_color_capability(light::ColorCapability::WHITE)) {
                warm_white = cold_white = obj->remote_values.get_white();
              }

              if (color_brightness > 0 && obj->get_traits().supports_color_capability(light::ColorCapability::RGB)) {
                red = obj->remote_values.get_red();
                green = obj->remote_values.get_green();
                blue = obj->remote_values.get_blue();
              }

              auto color_mode = obj->remote_values.get_color_mode();

              if (color_mode == light::ColorMode::RGB) {
                cold_white = warm_white = 0;
              } else if (color_mode == light::ColorMode::WHITE
                        || color_mode == light::ColorMode::COLOR_TEMPERATURE
                        || color_mode == light::ColorMode::COLD_WARM_WHITE) {
                red = green = blue = 0;
                color_brightness = 0;
              }

              uint8_t light_channels[6] = {
                (uint8_t)(red * 255),
                (uint8_t)(green * 255),
                (uint8_t)(blue * 255),
                (uint8_t)(cold_white * 255),
                (uint8_t)(warm_white * 255),
                0
              };
              SendDeviceGroupMessage(1, (DevGroupMessageType) (DGR_MSGTYP_UPDATE_MORE_TO_COME),
                                    DGR_ITEM_POWER, obj->remote_values.is_on());
              // If the light is turning off, don't send channel data, as ESPHome will have 0 for all channels in shut-off mode.
              if (obj->remote_values.is_on()) {
                SendDeviceGroupMessage(1, (DevGroupMessageType) (DGR_MSGTYP_UPDATE_MORE_TO_COME),
                                      DGR_ITEM_LIGHT_CHANNELS, light_channels);
              }
              SendDeviceGroupMessage(1, (DevGroupMessageType) (DGR_MSGTYP_UPDATE),
                                    DGR_ITEM_LIGHT_BRI, (uint8_t)(brightness * 255));
            }
#endif
            break;
        }
        XdrvCall(FUNC_DEVICE_GROUP_ITEM);
      }
      item_flags = 0;
    }

    if (item_processed) {
      XdrvMailbox.command_code = DGR_ITEM_EOL;
      XdrvCall(FUNC_DEVICE_GROUP_ITEM);
    }
  }

write_log:
  *log_ptr++ = 0;
  ESP_LOGD(TAG, "%s", log_buffer);

  // If this is a received status request message, then if the requestor didn't just ack our
  // previous full status update, send a full status update.
  if (received) {
    if ((flags & DGR_FLAG_STATUS_REQUEST)) {
      if ((flags & DGR_FLAG_RESET) || device_group_member->acked_sequence != device_group->last_full_status_sequence) {
        _SendDeviceGroupMessage(-device_group_index, DGR_MSGTYP_FULL_STATUS);
      }
    }
  }

  // If this is a message being sent, send it.
  else {
    int attempt;
    IPAddress ip_address = (device_group_member ? device_group_member->ip_address : IPAddress(DEVICE_GROUPS_ADDRESS));
    for (attempt = 1; attempt <= 5; attempt++) {
      if (device_groups_udp.beginPacket(ip_address, DEVICE_GROUPS_PORT)) {
        device_groups_udp.write(message, message_length);
        if (device_groups_udp.endPacket())
          break;
      }
      delay(10);
    }
    if (attempt > 5) {
      ESP_LOGE(TAG, "Error sending message");
    }
  }
  goto cleanup;

badmsg:
  ESP_LOGE(TAG, "%s ** incorrect length", log_buffer);

cleanup:
  free(log_buffer);
  if (received) {
    TasmotaGlobal.skip_light_fade = false;
    ignore_dgr_sends = false;
  }
}

bool device_groups::_SendDeviceGroupMessage(int32_t device, DevGroupMessageType message_type, ...) {
  // If device groups is not up, ignore this request.
  if (!device_groups_up)
    return 1;

  // Extract the flags from the message type.
  bool with_local = ((message_type & DGR_MSGTYPFLAG_WITH_LOCAL) != 0);
  message_type = (DevGroupMessageType) (message_type & 0x7F);

  // If we're currently processing a remote device message, ignore this request.
  if (ignore_dgr_sends && message_type != DGR_MSGTYPE_UPDATE_COMMAND)
    return 0;

  // If device is < 0, the device group index is the device negated. If not, get the device group
  // index for this device.
  uint8_t device_group_index = -device;
  if (device > 0) {
    device_group_index = 0;
    if (Settings->flag4.multiple_device_groups) {  // SetOption88 - Enable relays in separate device groups
      for (; device_group_index < device_group_count; device_group_index++) {
        if (Settings->device_group_tie[device_group_index] == device)
          break;
      }
    }
  }
  if (device_group_index >= device_group_count)
    return 0;

  // Get a pointer to the device information for this device.
  struct device_group *device_group = &device_groups_[device_group_index];

  // If we're still sending initial status requests, ignore this request.
  if (device_group->initial_status_requests_remaining)
    return 1;

    // Load the message header, sequence and flags.
#ifdef DEVICE_GROUPS_DEBUG
  ESP_LOGD(TAG, "Building %s %spacket", device_group->group_name,
           (message_type == DGR_MSGTYP_FULL_STATUS ? "full status " : ""));
#endif  // DEVICE_GROUPS_DEBUG
  uint16_t original_sequence = device_group->outgoing_sequence;
  uint16_t flags = 0;
  if (message_type == DGR_MSGTYP_UPDATE_MORE_TO_COME)
    flags = DGR_FLAG_MORE_TO_COME;
  else if (message_type == DGR_MSGTYP_UPDATE_DIRECT)
    flags = DGR_FLAG_DIRECT;
  uint8_t *message_ptr = BeginDeviceGroupMessage(device_group, flags,
                                                 building_status_message || message_type == DGR_MSGTYP_PARTIAL_UPDATE);

  // A full status request is a request from a remote device for the status of every item we
  // control. As long as we're building it, we may as well multicast the status update to all
  // device group members.
  if (message_type == DGR_MSGTYP_FULL_STATUS) {
    device_group->last_full_status_sequence = device_group->outgoing_sequence;
    device_group->message_length = 0;

    // Set the flag indicating we're currently building a status message. SendDeviceGroupMessage
    // will build but not send messages while this flag is set.
    building_status_message = true;

    // Call the drivers to build the status update.
    power_t power = TasmotaGlobal.power;
    if (Settings->flag4.multiple_device_groups) {  // SetOption88 - Enable relays in separate device groups
      power = (power >> (Settings->device_group_tie[device_group_index] - 1)) & 1;
    }
    SendDeviceGroupMessage(-device_group_index, DGR_MSGTYP_PARTIAL_UPDATE, DGR_ITEM_NO_STATUS_SHARE,
                           device_group->no_status_share, DGR_ITEM_POWER, power);
    XdrvMailbox.index = 0;
    if (device_group_index == 0 && first_device_group_is_local)
      XdrvMailbox.index = DGR_FLAG_LOCAL;
    XdrvMailbox.command_code = DGR_ITEM_STATUS;
    XdrvMailbox.topic = (char *) &device_group_index;
    XdrvCall(FUNC_DEVICE_GROUP_ITEM);
    building_status_message = false;

    // Set the status update flag in the outgoing message.
    *(message_ptr - 2) |= DGR_FLAG_FULL_STATUS;

    // If we have nothing to share with the other members, just send the EOL item.
    if (!device_group->message_length) {
      *message_ptr++ = 0;
      device_group->message_length = message_ptr - device_group->message;
    }
  }

  else {
#ifdef USE_DEVICE_GROUPS_SEND
    uint8_t out_buffer[128];
    bool escaped;
    char chr;
    char oper;
    uint32_t old_value;
    uint8_t *out_ptr = out_buffer;
#endif  // USE_DEVICE_GROUPS_SEND
    struct item {
      uint8_t item;
      uint8_t flags;
      uint32_t value;
      void *value_ptr;
    } item_array[32];
    bool shared;
    uint8_t item;
    uint32_t mask;
    uint32_t value = 0;
    uint8_t *value_ptr;
    uint8_t *first_item_ptr = message_ptr;
    struct item *item_ptr;
    va_list ap;

    // Build an array of all the items and values in this update.
    item_ptr = item_array;
#ifdef USE_DEVICE_GROUPS_SEND
    if (message_type == DGR_MSGTYPE_UPDATE_COMMAND) {
      value_ptr = (uint8_t *) XdrvMailbox.data;
      while ((item = strtoul((char *) value_ptr, (char **) &value_ptr, 0))) {
        item_ptr->item = item;
        if (*value_ptr == '=')
          value_ptr++;

        // If flags were specified for this item, save them.
        item_ptr->flags = 0;
        if (toupper(*value_ptr) == 'N') {
          value_ptr++;
          item_ptr->flags = DGR_ITEM_FLAG_NO_SHARE;
        }

        if (item <= DGR_ITEM_MAX_32BIT) {
          oper = 0;
          if (*value_ptr == '@') {
            oper = value_ptr[1];
            value_ptr += 2;
          }
          value = (isdigit(*value_ptr) ? strtoul((char *) value_ptr, (char **) &value_ptr, 0)
                   : oper == '^'       ? 0xffffffff
                                       : 1);
          if (oper) {
            old_value = (item <= DGR_ITEM_MAX_8BIT ? device_group->values_8bit[item]
                                                   : (item <= DGR_ITEM_MAX_16BIT
                                                          ? device_group->values_16bit[item - DGR_ITEM_MAX_8BIT - 1]
                                                          : device_group->values_32bit[item - DGR_ITEM_MAX_16BIT - 1]));
            value = (oper == '+'        ? old_value + value
                     : oper == '-'      ? old_value - value
                     : oper == '^'      ? old_value ^ value
                     : oper == '|'      ? old_value | value
                     : old_value == '&' ? old_value & value
                                        : old_value);
          }
          item_ptr->value = value;

          if (item == DGR_ITEM_STATUS) {
            if (!(item_ptr->flags & DGR_ITEM_FLAG_NO_SHARE))
              device_group->no_status_share = 0;
            _SendDeviceGroupMessage(-device_group_index, DGR_MSGTYP_FULL_STATUS);
            item_ptr--;
          }
        } else {
          item_ptr->value_ptr = out_ptr;
          if (item <= DGR_ITEM_MAX_STRING) {
            escaped = false;
            while ((chr = *value_ptr++)) {
              if (chr == ' ' && !escaped)
                break;
              if (!(escaped = (chr == '\\' && !escaped)))
                *out_ptr++ = chr;
            }
            *out_ptr++ = 0;
          } else {
            switch (item) {
              case DGR_ITEM_LIGHT_CHANNELS: {
                bool hex = false;
                char *endptr;
                if (*value_ptr == '#') {
                  value_ptr++;
                  hex = true;
                }
                for (int i = 0; i < 6; i++) {
                  *out_ptr = 0;
                  if (*value_ptr != ' ') {
                    if (hex) {
                      endptr = (char *) value_ptr + 2;
                      chr = *endptr;
                      *endptr = 0;
                      *out_ptr = strtoul((char *) value_ptr, (char **) &value_ptr, 16);
                      *endptr = chr;
                    } else {
                      *out_ptr = strtoul((char *) value_ptr, (char **) &value_ptr, 10);
                      if (*value_ptr == ',')
                        value_ptr++;
                    }
                  }
                  out_ptr++;
                }
              } break;
            }
          }
        }
        item_ptr++;
      }
    } else {
#endif  // USE_DEVICE_GROUPS_SEND
      va_start(ap, message_type);
      while ((item = va_arg(ap, int))) {
        item_ptr->item = item;
        item_ptr->flags = 0;
        if (item <= DGR_ITEM_MAX_32BIT)
          item_ptr->value = va_arg(ap, int);
        else if (item <= DGR_ITEM_MAX_STRING)
          item_ptr->value_ptr = va_arg(ap, char *);
        else {
          item_ptr->value_ptr = va_arg(ap, uint8_t *);
        }
        item_ptr++;
      }
      va_end(ap);
#ifdef USE_DEVICE_GROUPS_SEND
    }
#endif  // USE_DEVICE_GROUPS_SEND
    item_ptr->item = 0;

    // If we're still building this update or all group members haven't acknowledged the previous
    // update yet, update the message to include these new updates. First we need to rebuild the
    // previous update message to remove any items and their values that are included in this new
    // update.
    if (device_group->message_length) {
      uint8_t item_flags = 0;
      int kept_item_count = 0;

      // Rebuild the previous update message, removing any items whose values are included in this
      // new update.
      uint8_t *previous_message_ptr = message_ptr;
      while ((item = *previous_message_ptr++)) {
        // If this is the flags item, save the flags.
        if (item == DGR_ITEM_FLAGS) {
          item_flags = *previous_message_ptr++;
        }

        // Otherwise, determine the length of this item's value.
        else {
          if (item <= DGR_ITEM_MAX_32BIT) {
            value = 1;
            if (item > DGR_ITEM_MAX_8BIT) {
              value = 2;
              if (item > DGR_ITEM_MAX_16BIT) {
                value = 4;
              }
            }
          } else {
            value = *previous_message_ptr + 1;
          }

          // Search for this item in the new update.
          for (item_ptr = item_array; item_ptr->item; item_ptr++) {
            if (item_ptr->item == item)
              break;
          }

          // If this item was not found in the new update, copy it to the new update message. If the
          // item has flags, first copy the flags item to the new update message.
          if (!item_ptr->item) {
            kept_item_count++;
            if (item_flags) {
              *message_ptr++ = DGR_ITEM_FLAGS;
              *message_ptr++ = item_flags;
            }
            *message_ptr++ = item;
            memmove(message_ptr, previous_message_ptr, value);
            message_ptr += value;
          }
          item_flags = 0;
        }

        // Advance past the item value.
        previous_message_ptr += value;
      }
#ifdef DEVICE_GROUPS_DEBUG
      ESP_LOGD(TAG, "%u items carried over", kept_item_count);
#endif  // DEVICE_GROUPS_DEBUG
    }

    // Itertate through the passed items adding them and their values to the message.
    for (item_ptr = item_array; (item = item_ptr->item); item_ptr++) {
      // If this item is shared with the group add it to the message.
      shared = true;
      if ((mask = DeviceGroupSharedMask(item))) {
        if (item_ptr->flags & DGR_ITEM_FLAG_NO_SHARE)
          device_group->no_status_share |= mask;
        else if (!building_status_message)
          device_group->no_status_share &= ~mask;
        if (message_type != DGR_MSGTYPE_UPDATE_COMMAND) {
          shared = (!(mask & device_group->no_status_share) &&
                    (device_group_index || (mask & Settings->device_group_share_out)));
        }
      }
      if (shared) {
        if (item_ptr->flags) {
          *message_ptr++ = DGR_ITEM_FLAGS;
          *message_ptr++ = item_ptr->flags;
        }
        *message_ptr++ = item;

        // For integer items, add the value to the message.
        if (item <= DGR_ITEM_MAX_32BIT) {
          value = item_ptr->value;
          *message_ptr++ = value & 0xff;
          if (item > DGR_ITEM_MAX_8BIT) {
            value >>= 8;
            *message_ptr++ = value & 0xff;
            if (item > DGR_ITEM_MAX_16BIT) {
              value >>= 8;
              *message_ptr++ = value & 0xff;
              value >>= 8;
              // For the power item, the device count is overlayed onto the highest 8 bits.
              if (item == DGR_ITEM_POWER && !value)
                value =
                    (!Settings->flag4.multiple_device_groups && device_group_index == 0 && first_device_group_is_local
                         ? TasmotaGlobal.devices_present
                         : 1);
              *message_ptr++ = value;
            }
          }
        }

        // For string items and special items, get the value length.
        else {
          if (item <= DGR_ITEM_MAX_STRING) {
            value = strlen((const char *) item_ptr->value_ptr) + 1;
          } else {
            switch (item) {
              case DGR_ITEM_LIGHT_CHANNELS:
                value = 6;
                break;
            }
          }

          // Load the length and copy the value.
          *message_ptr++ = value;
          memcpy(message_ptr, item_ptr->value_ptr, value);
          message_ptr += value;
        }
      }
    }

    // If we added any items, add the EOL item code and calculate the message length.
    if (message_ptr != first_item_ptr) {
      *message_ptr++ = 0;
      device_group->message_length = message_ptr - device_group->message;
    }

    // If there's going to be more items added to this message, return.
    if (building_status_message || message_type == DGR_MSGTYP_PARTIAL_UPDATE)
      return 0;
  }

  // If there is no message, restore the sequence number and return.
  if (!device_group->message_length) {
    device_group->outgoing_sequence = original_sequence;
    return 0;
  }

  // Multicast the packet.
  device_group->multicasts_remaining = DGR_MULTICAST_REPEAT_COUNT;
  SendReceiveDeviceGroupMessage(device_group, nullptr, device_group->message, device_group->message_length, false);

#ifdef USE_DEVICE_GROUPS_SEND
  // If requested, handle this updated locally as well.
  if (with_local) {
    struct XDRVMAILBOX save_XdrvMailbox = XdrvMailbox;
    SendReceiveDeviceGroupMessage(device_group, nullptr, device_group->message, device_group->message_length, true);
    XdrvMailbox = save_XdrvMailbox;
  }
#endif  // USE_DEVICE_GROUPS_SEND

  uint32_t now = millis();
  if (message_type == DGR_MSGTYP_UPDATE_MORE_TO_COME) {
    device_group->message_length = 0;
    device_group->next_ack_check_time = 0;
  } else {
    device_group->ack_check_interval = DGR_ACK_WAIT_TIME;
    device_group->next_ack_check_time = now + device_group->ack_check_interval;
    if ((int32_t) (next_check_time - device_group->next_ack_check_time) > 0)
      next_check_time = device_group->next_ack_check_time;
    device_group->member_timeout_time = now + DGR_MEMBER_TIMEOUT;
  }

  device_group->next_announcement_time = now + DGR_ANNOUNCEMENT_INTERVAL;
  if ((int32_t) (next_check_time - device_group->next_announcement_time) > 0)
    next_check_time = device_group->next_announcement_time;
  return 0;
}

ProcessGroupMessageResult device_groups::ProcessDeviceGroupMessage(multicast_packet packet) {
  // Search for a device group with the target group name. If one isn't found, return.
  uint8_t device_group_index = 0;
  struct device_group *device_group = device_groups_;
  char *message_group_name = (char *) packet.payload + sizeof(DEVICE_GROUP_MESSAGE) - 1;
  for (;;) {
    if (!strcmp(message_group_name, device_group->group_name))
      break;
    if (++device_group_index >= device_group_count)
      return PROCESS_GROUP_MESSAGE_UNMATCHED;
    device_group++;
  }

  // Find the group member. If this is a new group member, add it.
  struct device_group_member *device_group_member;
  struct device_group_member **flink = &device_group->device_group_members;
  for (;;) {
    device_group_member = *flink;
    if (!device_group_member) {
      device_group_member = (struct device_group_member *) calloc(1, sizeof(struct device_group_member));
      if (device_group_member == nullptr) {
        ESP_LOGE(TAG, "Error allocating member block");
        return PROCESS_GROUP_MESSAGE_ERROR;
      }
      device_group_member->ip_address = packet.remoteIP;
      device_group_member->acked_sequence = device_group->outgoing_sequence;
      device_group->member_timeout_time = millis() + DGR_MEMBER_TIMEOUT;
      *flink = device_group_member;
      ESP_LOGD(TAG, "%s Member %s added", device_group->group_name, IPAddressToString(packet.remoteIP));
      break;
    } else if (device_group_member->ip_address == packet.remoteIP) {
      break;
    }
    flink = &device_group_member->flink;
  }

  SendReceiveDeviceGroupMessage(device_group, device_group_member, packet.payload, packet.length, true);
  return PROCESS_GROUP_MESSAGE_SUCCESS;
}

void device_groups::DeviceGroupStatus(uint8_t device_group_index) {
  if (Settings->flag4.device_groups_enabled && device_group_index < device_group_count) {
    char buffer[1024];
    int member_count = 0;
    struct device_group *device_group = &device_groups_[device_group_index];
    buffer[0] = buffer[1] = 0;
    for (struct device_group_member *device_group_member = device_group->device_group_members; device_group_member;
         device_group_member = device_group_member->flink) {
      snprintf_P(buffer, sizeof(buffer),
                 PSTR("%s,{\"IPAddress\":\"%s\",\"ResendCount\":%u,\"LastRcvdSeq\":%u,\"LastAckedSeq\":%u}"), buffer,
                 IPAddressToString(device_group_member->ip_address), device_group_member->unicast_count,
                 device_group_member->received_sequence, device_group_member->acked_sequence);
      member_count++;
    }
    ESP_LOGI(TAG,
             "{\"" D_CMND_DEVGROUPSTATUS
             "\":{\"Index\":%u,\"GroupName\":\"%s\",\"MessageSeq\":%u,\"MemberCount\":%d,\"Members\":[%s]}}",
             device_group_index, device_group->group_name, device_group->outgoing_sequence, member_count, &buffer[1]);
  }
}

void device_groups::DeviceGroupsLoop(void) {
  if (!device_groups_up || TasmotaGlobal.restart_flag)
    return;

#if defined(ESP8266)
  while (device_groups_udp.parsePacket()) {
    struct multicast_packet packet;
    int length = device_groups_udp.read(packet.payload, sizeof(packet.payload) - 1);
    if (length > 0) {
      packet.id = packetId++;
      packet.payload[length] = 0;
      packet.length = length;
      packet.remoteIP = device_groups_udp.remoteIP();
      received_packets.push_back(packet);
    }
  }

  for (multicast_packet packet : received_packets) {
    std::string identifier(std::string(kDeviceGroupMessage) + this->device_group_name_ + '\0');
    if (!strncmp_P((char *) packet.payload, identifier.c_str(), identifier.length())) {
      ProcessGroupMessageResult status = ProcessDeviceGroupMessage(packet);
      if (status != PROCESS_GROUP_MESSAGE_UNMATCHED) {
        received_packets.erase(std::remove_if(received_packets.begin(), received_packets.end(),
                                              [&](multicast_packet const &mcp) { return mcp.id == packet.id; }),
                               received_packets.end());
      }
    } else {
      bool isRegistered = false;
      for (std::string registered_group_name : registered_group_names) {
        std::string identifier_registered(std::string(kDeviceGroupMessage) + registered_group_name + '\0');
        if (!strncmp_P((char *) packet.payload, identifier_registered.c_str(), identifier_registered.length())) {
          isRegistered = true;
          break;
        }
      }
      if (!isRegistered) {
        ESP_LOGVV(TAG, "Removing unregistered packet identifier, %s", packet.payload);
        received_packets.erase(std::remove_if(received_packets.begin(), received_packets.end(),
                                              [&](multicast_packet const &mcp) { return mcp.id == packet.id; }),
                               received_packets.end());
      }
    }
  }
#else
  while (device_groups_udp.parsePacket()) {
    struct multicast_packet packet;
    int length = device_groups_udp.read(packet.payload, sizeof(packet.payload) - 1);
    if (length > 0) {
      packet.id = 0; // Not used.
      packet.payload[length] = 0;
      packet.length = length;
      packet.remoteIP = device_groups_udp.remoteIP();
      if (!strncmp_P((char *)packet.payload, kDeviceGroupMessage, sizeof(DEVICE_GROUP_MESSAGE) - 1)) {
        ProcessDeviceGroupMessage(packet);
      }
    }
  }
#endif

  uint32_t now = millis();

  // If it's time to check on things, iterate through the device groups.
  if ((int32_t) (now - next_check_time) >= 0) {
#ifdef DEVICE_GROUPS_DEBUG
    ESP_LOGD(TAG, "Checking next_check_time=%u, now=%u", next_check_time, now);
#endif  // DEVICE_GROUPS_DEBUG
    next_check_time = now + DGR_ANNOUNCEMENT_INTERVAL * 2;

    struct device_group *device_group = device_groups_;
    for (uint32_t device_group_index = 0; device_group_index < device_group_count;
         device_group_index++, device_group++) {
      // If we're still waiting for acks to the last update from this device group, ...
      if (device_group->next_ack_check_time) {
        // If it's time to check for acks, ...
        if ((int32_t) (now - device_group->next_ack_check_time) >= 0) {
          // If we're still sending the initial status request message, send it.
          if (device_group->initial_status_requests_remaining) {
            if (--device_group->initial_status_requests_remaining) {
#ifdef DEVICE_GROUPS_DEBUG
              ESP_LOGD(TAG, "Sending initial status request for group %s", device_group->group_name);
#endif  // DEVICE_GROUPS_DEBUG
              SendReceiveDeviceGroupMessage(device_group, nullptr, device_group->message, device_group->message_length,
                                            false);
              device_group->message[device_group->message_header_length + 2] =
                  DGR_FLAG_STATUS_REQUEST;  // The reset flag is on only for the first packet - turn it off now
              next_check_time = device_group->next_ack_check_time = now + 200;
              continue;
            }

            // If we've sent the initial status request message the set number of times, send our
            // status to all the members.
            else {
              _SendDeviceGroupMessage(-device_group_index, DGR_MSGTYP_FULL_STATUS);
            }
          }

          // If we're done initializing, iterate through the group memebers, ...
          else {
#ifdef DEVICE_GROUPS_DEBUG
            ESP_LOGD(TAG, "Checking for %s ack's", device_group->group_name);
#endif  // DEVICE_GROUPS_DEBUG
            bool acked = true;
            struct device_group_member **flink = &device_group->device_group_members;
            struct device_group_member *device_group_member;
            while ((device_group_member = *flink)) {
              // If we have not received an ack to our last message from this member, ...
              if (device_group_member->acked_sequence != device_group->outgoing_sequence) {
                // If we haven't receive an ack from this member in DGR_MEMBER_TIMEOUT ms, assume
                // they're offline and remove them from the group.
                if ((int32_t) (now - device_group->member_timeout_time) >= 0) {
                  *flink = device_group_member->flink;
                  free(device_group_member);
                  ESP_LOGD(TAG, "%s Member %s removed", device_group->group_name,
                           IPAddressToString(device_group_member->ip_address));
                  continue;
                }

                // If we have more multicasts to do, multicast the packet to all members again;
                // otherwise, unicast the message directly to this member.
                if (device_group->multicasts_remaining)
                  device_group_member = nullptr;
                SendReceiveDeviceGroupMessage(device_group, device_group_member, device_group->message,
                                              device_group->message_length, false);
                acked = false;
                if (device_group->multicasts_remaining) {
                  device_group->multicasts_remaining--;
                  break;
                }
                device_group_member->unicast_count++;
              }
              flink = &device_group_member->flink;
            }

            // If we've received an ack to the last message from all members, clear the ack check
            // time and zero-out the message length.
            if (acked) {
              device_group->next_ack_check_time = 0;
              device_group->message_length = 0;  // Let _SendDeviceGroupMessage know we're done with this update
            }

            // If there are still members we haven't received an ack from, set the next ack check
            // time. We start at DGR_ACK_WAIT_TIME ms and add 100ms each pass with a maximum
            // interval of 2 seconds.
            else {
              device_group->ack_check_interval += 100;
              if (device_group->ack_check_interval > 2000)
                device_group->ack_check_interval = 2000;
              device_group->next_ack_check_time = now + device_group->ack_check_interval;
            }
          }
        }

        if (device_group->next_ack_check_time && (int32_t) (next_check_time - device_group->next_ack_check_time) > 0)
          next_check_time = device_group->next_ack_check_time;
      }

      // If we're not still waiting for acks and it's time to send a multicast announcement for this
      // group, send it. This is to announce ourself to any members that have somehow not heard
      // about us. We send it at the announcement interval plus a random number of milliseconds so
      // that even if all the devices booted at the same time, they don't all multicast their
      // announcements at the same time.
      else {
#ifdef DEVICE_GROUPS_DEBUG
        ESP_LOGD(TAG, "next_announcement_time=%u, now=%u", device_group->next_announcement_time, now);
#endif  // DEVICE_GROUPS_DEBUG
        if ((int32_t) (now - device_group->next_announcement_time) >= 0) {
          SendReceiveDeviceGroupMessage(
              device_group, nullptr, device_group->message,
              BeginDeviceGroupMessage(device_group, DGR_FLAG_ANNOUNCEMENT, true) - device_group->message, false);
          device_group->next_announcement_time = now + DGR_ANNOUNCEMENT_INTERVAL + (random_uint32() % 10000);
        }
        if ((int32_t) (next_check_time - device_group->next_announcement_time) > 0)
          next_check_time = device_group->next_announcement_time;
      }
    }
  }
}

bool device_groups::XdrvCall(uint8_t Function) {
  bool result = true;
  return result;
}

void device_groups::ExecuteCommandPower(uint32_t device, uint32_t state, uint32_t source) {
  power_t mask = 1 << (device - 1);  // Device to control
  power_t old_power = TasmotaGlobal.power;
  if (state <= POWER_TOGGLE) {
    switch (state) {
      case POWER_OFF: {
        TasmotaGlobal.power &= (POWER_MASK ^ mask);
        break;
      }
      case POWER_ON:
        TasmotaGlobal.power |= mask;
        break;
      case POWER_TOGGLE:
        TasmotaGlobal.power ^= mask;
    }
  }

  if (TasmotaGlobal.power != old_power && SRC_REMOTE != source && SRC_RETRY != source) {
    power_t dgr_power = TasmotaGlobal.power;
    if (Settings->flag4.multiple_device_groups) {  // SetOption88 - Enable relays in separate device groups
      dgr_power = (dgr_power >> (device - 1)) & 1;
    }
    SendDeviceGroupMessage(device, DGR_MSGTYP_UPDATE, DGR_ITEM_POWER, dgr_power);
  }

  if (dgr_state < DGR_STATE_INITIALIZED) {
    return;
  }

  if (SRC_REMOTE == source) {
#ifdef USE_SWITCH
    for (switch_::Switch *obj : this->switches_) {
      if (TasmotaGlobal.power > 0) {
        obj->turn_on();
      } else {
        obj->turn_off();
      }
    }
#endif
#ifdef USE_LIGHT
    for (light::LightState *obj : this->lights_) {
      auto call = TasmotaGlobal.power > 0 ? obj->turn_on() : obj->turn_off();
      call.perform();
  }
#endif
  }
}

void device_groups::ExecuteCommand(const char *cmnd, uint32_t source) { return; }

void device_groups::InitTasmotaCompatibility() {
  Settings = (TSettings *) malloc(sizeof(TSettings));
  Settings->device_group_share_in = receive_mask_;
  Settings->device_group_share_out = send_mask_;
  Settings->flag4.device_groups_enabled = 1;
  Settings->flag4.multiple_device_groups = 0;
}

}  // namespace device_groups
}  // namespace esphome

