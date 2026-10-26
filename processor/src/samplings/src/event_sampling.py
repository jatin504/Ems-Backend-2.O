import time
from datetime import datetime
import json
from collections import defaultdict, deque
import logging
from plant_architecture_resolver import PlantArchitectureResolver
from model import get_connection
from .enum_alarm_def import ENUM_ALARMS

logger = logging.getLogger(__name__)

class EventSampling:
    def __init__(self):
        # Separate buffers for ChangeAlarm and ResolutionAlarm: {device_id: {"ChangeAlarm": {tag: deque()}, "ResolutionAlarm": {tag: deque()}}}
        self.tag_buffers_event = defaultdict(lambda: defaultdict(lambda: defaultdict(deque)))
        self.last_logged_values_event = defaultdict(dict)  # Tracks the last logged value for ChangeAlarm
        self.last_processed_time_event = defaultdict(dict)  # Tracks last processed time for ResolutionAlarm
        # self.alarm_resolver = PlantArchitectureResolver()
        self.enum_alarms = ENUM_ALARMS
        
    async def add_data(self, device_id, tag, value, timestamp, change=True, resolution=60, alarmType="normal", bitMapAlarm={}, severity="critical", isNormalReverse=False, customInfos=None):
        """
        Add data for an alarm tag.
        :param device_id: The device ID.
        :param tag: The tag name.
        :param value: The current value of the tag.
        :param timestamp: The timestamp of the value.
        :param change: Whether to log only on change (True) or based on resolution (False).
        :param resolution: The resolution period in seconds (used if change is False).
        """
        if alarmType == "custom":
            self._process_custom_based(device_id, tag, value, timestamp, customInfos)
        elif change:
            await self._process_change_based(device_id, tag, value, severity, isNormalReverse, timestamp, alarmType, bitMapAlarm)
        else:
            await self._process_resolution_based(device_id, tag, value, timestamp, resolution)

    def _process_custom_based(self, device_id, tag, value, timestamp, customInfos):
        """
        Process custom alarm data.
        Insert alarm directly based on custom information provided.
        """
        logger.info(f"Processing CustomAlarm for device: {device_id}, Tag: {tag}, Value: {value}, Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}")
        if value == 1:
            self._insert_alarm_to_db(
                device_id=device_id,
                tag_name=tag,
                value=value,
                timestamp=timestamp,
                severity=customInfos.get("severity", "info"),
                alarm_type="CustomChangeAlarm",
                remark=customInfos.get("remark", "")
            )
        else:
            self._clear_active_alarm(device_id, tag, timestamp)

    async def _process_change_based(self, device_id, tag, value, severity, isNormalReverse, timestamp, alarmType, bitMapAlarm):
        """
        Process alarm data based on immediate change detection.
        If the current value is different from the last stored value,
        log the alarm and update the last stored value.
        """
        last_value = self.last_logged_values_event[device_id].get(tag, None)
        if value != last_value:  # Log only if the value has changed
            # logger.info(
            #     f"[AlarmSampling] Device: {device_id}, Alarm Type: ChangeAlarm, Tag: '{tag}', Value: {value}, Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}"
            # )
            self.last_logged_values_event[device_id][tag] = value  # Update the last stored value

            if alarmType == "enum":
                await self._process_enum_alarm(device_id, tag, value, last_value, timestamp)
            elif alarmType == "bitwise":
                await self._process_bitwise_alarm(device_id, tag, bitMapAlarm, value, last_value, timestamp)
            else: 
                # Insert into database
                # self._insert_alarm_to_db(device_id, tag, value, timestamp, severity="info", alarm_type="NormalChangeAlarm")
                await self._process_normal_alarm(device_id, tag, value, severity, isNormalReverse, last_value, timestamp)

    async def _process_resolution_based(self, device_id, tag, value, timestamp, resolution):
        """
        Process alarm data based on resolution period.
        """
        # Initialize the buffer and last_processed_time_event if not already set
        if tag not in self.last_processed_time_event[device_id]:
            self.last_processed_time_event[device_id][tag] = timestamp

        # Add data to the buffer for resolution-based alarms
        self.tag_buffers_event[device_id]["ResolutionAlarm"][tag].append((value, timestamp))

        current_time = time.time()
        last_processed = self.last_processed_time_event[device_id][tag]
        time_diff = current_time - last_processed

        # Log the first value immediately during the first reading
        if len(self.tag_buffers_event[device_id]["ResolutionAlarm"][tag]) == 1:
            first_value = self.tag_buffers_event[device_id]["ResolutionAlarm"][tag][0][0]
            # logger.info(
            #     f"[AlarmSampling] Device: {device_id}, Alarm Type: ResolutionAlarm, Tag: '{tag}', Value: {first_value}, Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}"
            # )
            # await self.alarm_resolver.update_alarm(f"{device_id}_{tag}", {"value" : first_value, "Time_Stamp" : current_time})
            self.last_processed_time_event[device_id][tag] = current_time

            # Insert first value to DB
            self._insert_alarm_to_db(device_id, tag, first_value, current_time, severity="info", alarm_type="ResolutionAlarm")

        # Process the buffer after the resolution period elapses
        if time_diff >= resolution:
            data_points = self.tag_buffers_event[device_id]["ResolutionAlarm"][tag]
            if data_points:
                last_value = data_points[-1][0]  # Use the last value in the buffer
                # logger.info(
                #     f"[AlarmSampling] Device: {device_id}, Alarm Type: ResolutionAlarm, Tag: '{tag}', Value: {last_value}, Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}"
                # )
                # await self.alarm_resolver.update_alarm(f"{device_id}_{tag}", {"value" : last_value, "Time_Stamp" : current_time})
                self.tag_buffers_event[device_id]["ResolutionAlarm"][tag].clear()  # Clear the buffer
                self.last_processed_time_event[device_id][tag] = current_time

                # Insert into DB
                self._insert_alarm_to_db(device_id, tag, last_value, current_time, severity="info", alarm_type="ResolutionAlarm")

    async def _process_normal_alarm(self, device_id: str, tag: str, value: int, severity: str, isNormalReverse: bool, last_value: int, timestamp: float) -> None:
        """
        Processes normal binary alarms.

        Logic:
            1 -> Alarm Active
            0 -> Alarm Cleared

        If isNormalReverse is True, the logic is reversed:
            0 -> Alarm Cleared
            1 -> Alarm Active
        """

        cleared_value = 0 if not isNormalReverse else 1
        active_value = 1 if not isNormalReverse else 0

        # Alarm Activated
        if value == active_value and last_value != active_value:  # Only log if it changed to active
            logger.info(
                "NormalAlarm ACTIVE - device=%s tag=%s value=%s ts=%s",
                device_id,
                tag,
                value,
                timestamp,
            )

            self._insert_alarm_to_db(
                device_id=device_id,
                tag_name=tag,
                value=value,
                timestamp=timestamp,
                severity=severity,
                alarm_type="NormalAlarm"
            )

        # Alarm Cleared
        elif value == cleared_value and last_value == active_value: # Only log if it changed to cleared
            logger.info(
                "NormalAlarm CLEARED - device=%s tag=%s value=%s ts=%s",
                device_id,
                tag,
                value,
                timestamp,
            )

            self._clear_active_alarm(
                device_id=device_id,
                tag_name=tag,
                clr_timestamp=timestamp
            )

        else:
            logger.warning(
                "NormalAlarm received unexpected value=%s for tag=%s",
                value,
                tag
            )
        
        self.last_logged_values[device_id][tag] = value
    
    # Enum-based alarms: map numeric code to state and severity
    async def _process_enum_alarm(self, device_id: str, tag: str, value: int, last_value: int, timestamp: float) -> None:
        """
        Processes enum alarms where register values map to semantic states with severity.
        tag should match a key in self.enum_alarms.
        """
        alarm_def = self.enum_alarms.get(tag)
        if alarm_def is None:
            logger.warning("EnumAlarm received for unknown tag '%s'. Ignoring.", tag)
            return

        # Map numeric value to (state, severity)
        mapped = alarm_def.mapping.get(value)
        if mapped is None:
            # state, severity = ("UNKNOWN", "warning")
            logger.debug("EnumAlarm value %s not found in mapping for tag %s", value, tag)
            return
        else:
            state, severity = mapped

        # To Clear Raised Alarm
        last_mapped = alarm_def.mapping.get(last_value)
        if last_mapped is None:
            logger.info("No Previous EnumAlarm Found for tag %s", tag)
        else:
            last_state, last_severity = last_mapped
            # self._clear_active_alarm(device_id=device_id, tag_name=last_state, clr_timestamp=timestamp)
            pass

        logger.info(
            "EnumAlarm triggered - device=%s tag=%s code=%s state=%s severity=%s ts=%s",
            device_id,
            tag,
            value,
            state,
            severity,
            timestamp,
        )
        self.last_logged_values_event[device_id][tag] = value

        self._insert_alarm_to_db(device_id, state, value, timestamp, severity=severity, alarm_type="EnumAlarm")

    async def _process_bitwise_alarm(self, device_id, parent_tag, bitMapAlarm, value: int, last_value: int, timestamp):
        """
        Process bitwise alarms.
        Insert alarms only for bits where value == 1 (active).
        """
        logger.info(f"Processing BitwiseAlarm for device: {device_id}, Parent Tag: {parent_tag} bitMapAlarm: {bitMapAlarm}")
        for bit_alias, bit_info in bitMapAlarm.items():
            bit_value = bit_info.get("value", 0)
            severity = bit_info.get("severity", "info")

            last_value = self.last_logged_values_event[device_id].get(bit_alias)

            # Only raise alarm when the bit is active
            if bit_value == 1 and last_value != 1:
                self._insert_alarm_to_db(
                    device_id=device_id,
                    tag_name=bit_alias,
                    value=bit_value,
                    timestamp=timestamp,
                    severity=severity,
                    alarm_type="BitwiseAlarm"
                )

            # Clear alarm when bit goes to 0
            elif bit_value == 0 and last_value == 1:
                # self._clear_active_alarm(device_id, bit_alias, timestamp)
                pass

            # Update cache
            self.last_logged_values_event[device_id][bit_alias] = bit_value

    def _clear_active_alarm(self, device_id, tag_name, clr_timestamp):

        clr_time_str = datetime.fromtimestamp(clr_timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE events
                        SET status = 'CLEARED',
                            clr_timestamp = %s
                        WHERE device_id = %s
                        AND tag_name = %s
                        AND status = 'ACTIVE'
                    """, (clr_time_str, device_id, tag_name))
                    conn.commit()
        except Exception as e:
            logger.error(f"❌ Failed to clear event {device_id}:{tag_name} → {e}")


    def _insert_alarm_to_db(self, device_id, tag_name, value, timestamp, severity, alarm_type, remark=""):

        if tag_name is None or severity is None or alarm_type is None or alarm_type == "UNKNOWN":
            logger.error(f"Missing required fields for alarm insertion: tag_name={tag_name}, severity={severity}, alarm_type={alarm_type}")
            return

        time_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "device_id": device_id,
            "tag_name": tag_name,
            "value": value,
            "time_stamp": time_str,
            "severity": severity,
            "alarm_type": alarm_type,
            "remark": remark
        }

        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:

                    cursor.execute("""
                        INSERT INTO events (device_id, tag_name, value, time_stamp, severity, alarm_type, status, remark)
                        VALUES (%s, %s, %s, %s, %s, %s, 'ACTIVE', %s)
                        ON CONFLICT (time_stamp)
                        DO UPDATE SET
                            device_id = EXCLUDED.device_id,
                            tag_name = EXCLUDED.tag_name,
                            value = EXCLUDED.value,
                            severity = EXCLUDED.severity,
                            alarm_type = EXCLUDED.alarm_type,
                            status = 'ACTIVE',
                            remark = EXCLUDED.remark
                    """, (
                        row["device_id"],
                        row["tag_name"],
                        row["value"],
                        row["time_stamp"],
                        row["severity"],
                        row["alarm_type"],
                        row["remark"]
                    ))
                    conn.commit()
                    print(f"✔ Event inserted: {row}")

        except Exception as e:
            logger.error(f"❌ Failed to insert event into DB: {e}")
