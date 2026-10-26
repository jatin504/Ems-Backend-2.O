import json
import time
from collections import defaultdict
from .condition_evaluator import ConditionEvaluator


class CustomAlarmEngine:

    def __init__(self, alarm_sampling, alarm_json_path):
        self.alarm_sampling = alarm_sampling
        self.rules = self._load_rules(alarm_json_path)

        # (device_id, alarm_key) → state
        self.alarm_state = defaultdict(lambda: {
            "active": False,
            "last_change": None
        })


    def _load_rules(self, path):
        with open(path) as f:
            return json.load(f)
            

    async def process(self, device_id, device_type, latest_values, timestamp):
        """
        :param latest_values: dict(tag_name → value)
        """
        for alarm_key, rule in self.rules.items():

            if not self._device_match(rule, device_type, device_id):
                continue

            state = self.alarm_state[(device_id, alarm_key)]

            if not state["active"] and self._should_raise(rule, latest_values):
                await self._raise_alarm(device_id, alarm_key, rule, timestamp)

            elif state["active"] and self._should_clear(rule, latest_values):
                await self._clear_alarm(device_id, alarm_key, rule, timestamp)
    
    def _device_match(self, rule, device_type, device_id):
        profile = rule.get("device_profile", {})
        if profile.get("consider") == "device_type":
            return profile.get("device_type") == device_type
        elif profile.get("consider") == "device_id":
            return profile.get("device_id") == device_id
        return True

    def _evaluate_block(self, block, latest_values):
        for key in block["key_filter"]:
            name = key["name"]
            data_type = key["data_type"]

            if name not in latest_values:
                return False

            value = latest_values[name]

            for cond in key["conditions"]:
                if not ConditionEvaluator.evaluate(value, data_type, cond):
                    return False

        return True

    def _should_raise(self, rule, latest_values):
        return all(
            self._evaluate_block(block, latest_values)
            for block in rule["raise_condition"]
        )

    def _should_clear(self, rule, latest_values):
        return all(
            self._evaluate_block(block, latest_values)
            for block in rule["clear_condition"]
        )

    async def _raise_alarm(self, device_id, alarm_key, rule, timestamp):
        self.alarm_state[(device_id, alarm_key)]["active"] = True
        self.alarm_state[(device_id, alarm_key)]["last_change"] = timestamp

        await self.alarm_sampling.add_data(
            device_id=device_id,
            tag=rule["state"],
            value=1,
            timestamp=timestamp,
            change=True,
            alarmType="custom",
            customInfos={
                "severity": rule.get("severity", "info"),
                "remark": rule.get("remark", "Alarm is Raised")
            }
        )

    async def _clear_alarm(self, device_id, alarm_key, rule, timestamp):
        self.alarm_state[(device_id, alarm_key)]["active"] = False
        self.alarm_state[(device_id, alarm_key)]["last_change"] = timestamp

        await self.alarm_sampling.add_data(
            device_id=device_id,
            tag=rule["state"],
            value=0,
            timestamp=timestamp,
            change=True,
            alarmType="custom",
            customInfos={
                "severity": rule.get("severity", "info"),
                "remark": rule.get("remark", "Alarm is Cleared")
            }
        )
