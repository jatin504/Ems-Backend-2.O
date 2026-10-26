import json

from runtime_paths import DataPaths

class CloudDetection:
    def __init__(self):
        pass

    @staticmethod
    def load_json():
        with open(DataPaths.resolve("cloud/details.json"), "r") as f:
            return json.load(f)

    @staticmethod
    def detect():
        data = CloudDetection.load_json()

        if not data:
            return {
                "is_cloud_detected": False
            }
        
        cloud_module = data["cloud_module"]
        cloud = data["is_cloud_detected"]
        cloud_details = data.get("cloud_detection_details", {})
        user_decision = cloud_details.get("user_decision", "none")
        dispatch_stages = data.get("confidance_dispatch_stages", {})

        if cloud and cloud_module and user_decision["user_approved"]:
            meter_dirs = cloud_details.get("meter_direction", [])

            # 1. setpoint (only for directions present in meter_direction)
            setpoint_kw = user_decision.get("setpoint_kw", 0)

            # 2. current_stage
            current_stage = cloud_details.get("current_stage")

            # 3. dispatch percent based on current_stage
            dispatch_percent = dispatch_stages.get(current_stage)

            return {
                "is_cloud_detected": True,
                "setpoint_kw": setpoint_kw,
                "current_stage": current_stage,
                "dispatch_percent": dispatch_percent
            }
        
        return {
            "is_cloud_detected": False
        }