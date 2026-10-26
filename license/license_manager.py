"""
license_manager.py

Reusable licensing module for binding Python applications to server hardware.

Features:
- RSA signature validation
- MAC binding
- Optional hardware fingerprint binding
- Expiry validation
- Feature validation
- Clean API for integration
"""

import json
import uuid
import hashlib
import base64
import platform
import subprocess
from datetime import datetime
from typing import List, Optional
import os
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


class LicenseError(Exception):
    """Custom exception for license validation errors."""
    pass


class LicenseManager:
    """
    Handles loading and validating application license files.
    """

    def __init__(self, public_key_pem: bytes, license_path: str = "license.lic"):
        self.public_key = serialization.load_pem_public_key(public_key_pem)
        self.license_path = license_path
        self.license_data: Optional[dict] = None

    # =======================================================
    # HARDWARE INFORMATION
    # =======================================================

    @staticmethod
    def get_mac_address() -> str:
        """Return system MAC address."""
        # In CMD run => getmac /v /fo list
        # return "58-CD-C9-53-EC-16"
        mac = uuid.getnode()
        return ":".join(f"{(mac >> ele) & 0xff:02X}" for ele in range(40, -1, -8))
    
    @staticmethod
    def get_machine_uuid() -> str:
        try:
            system = platform.system()

            if system == "Windows":
                output = subprocess.check_output(
                    ["wmic", "csproduct", "get", "UUID"],
                    stderr=subprocess.DEVNULL
                ).decode().splitlines()
                return output[2].strip() if len(output) > 1 else "UNKNOWN_UUID"

            elif system == "Linux":
                if os.path.exists("/etc/machine-id"):
                    with open("/etc/machine-id", "r") as f:
                        return f.read().strip()

        except Exception:
            pass

        return "UNKNOWN_UUID"


    @staticmethod
    def get_cpu_id() -> str:
        """Return CPU ID (best effort, OS dependent)."""
        # In CMD run => wmic cpu get processorid
        # return "BFEBFBFF000906A4"
        try:
            system = platform.system()

            if system == "Windows":
                output = subprocess.check_output(
                    ["wmic", "cpu", "get", "processorid"],
                    stderr=subprocess.DEVNULL
                ).decode().splitlines()
                return output[2].strip() if len(output) > 1 else "UNKNOWN_CPU"

            elif system == "Linux":
                with open("/proc/cpuinfo", "r") as f:
                    for line in f:
                        if "model name" in line:
                            return line.split(":")[1].strip()

        except Exception:
            pass

        return "UNKNOWN_CPU"

    @staticmethod
    def get_disk_serial() -> str:
        """Return disk serial number (best effort, OS dependent)."""
        # In CMD run => wmic diskdrive get serialnumber
        # return "0000_0000_0000_0001_00A0_7524_48D7_666D"
        try:
            system = platform.system()

            if system == "Windows":
                output = subprocess.check_output(
                    ["wmic", "diskdrive", "get", "SerialNumber"],
                    stderr=subprocess.DEVNULL
                ).decode().splitlines()
                return output[2].strip() if len(output) > 1 else "UNKNOWN_DISK"

            elif system == "Linux":
                if os.path.exists("/sys/class/dmi/id/product_uuid"):
                    with open("/sys/class/dmi/id/product_uuid", "r") as f:
                        return f.read().strip()

        except Exception:
            pass

        return "UNKNOWN_DISK"

    @classmethod
    def generate_hardware_fingerprint(cls) -> str:
        """
        Generate SHA256 fingerprint from MAC + CPU + Disk serial.
        """
        raw_string = (
            cls.get_mac_address()
            + cls.get_cpu_id()
            + cls.get_disk_serial()
        )
        return hashlib.sha256(raw_string.encode()).hexdigest()

    # =======================================================
    # LICENSE VALIDATION
    # =======================================================

    def load_license(self) -> None:
        """Load license file from disk."""
        try:
            with open(self.license_path, "r") as f:
                self.license_data = json.load(f)
        except Exception as e:
            raise LicenseError(f"Unable to load license file: {e}")

    def verify_signature(self) -> None:
        """Verify RSA signature of license file."""
        if not self.license_data:
            raise LicenseError("License not loaded")

        license_copy = dict(self.license_data)

        try:
            signature = base64.b64decode(license_copy.pop("signature"))
        except KeyError:
            raise LicenseError("License signature missing")

        license_json = json.dumps(license_copy, sort_keys=True).encode()

        try:
            self.public_key.verify(
                signature,
                license_json,
                padding.PKCS1v15(),
                hashes.SHA256(),
            )
        except Exception as e:
            raise LicenseError(f"Invalid license signature: {e}")

    def verify_expiry(self) -> None:
        """Check if license is expired."""
        expiry_str = self.license_data.get("expiry")
        if not expiry_str:
            raise LicenseError("Expiry not found in license")

        expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d")
        if datetime.now() > expiry_date:
            raise LicenseError("License expired")

    def verify_mac_binding(self) -> None:
        """Validate MAC address binding (optional)."""
        licensed_mac = self.license_data.get("mac_id")
        if not licensed_mac:
            return

        current_mac = self.get_mac_address()
        if licensed_mac.upper() != current_mac.upper():
            raise LicenseError("License not valid for this MAC address")

    def verify_fingerprint_binding(self) -> None:
        """Validate hardware fingerprint binding (optional)."""
        licensed_fp = self.license_data.get("hardware_fingerprint")
        if not licensed_fp:
            return

        current_fp = self.generate_hardware_fingerprint()
        if licensed_fp != current_fp:
            raise LicenseError("License not valid for this hardware")

    def verify_features(self, required_features: Optional[List[str]] = None) -> None:
        """Validate required features exist in license."""
        if not required_features:
            return

        licensed_features = self.license_data.get("features", [])

        for feature in required_features:
            if feature not in licensed_features:
                raise LicenseError(f"Feature '{feature}' not licensed")

    # =======================================================
    # MAIN ENTRY POINT
    # =======================================================

    def validate(self, required_features: Optional[List[str]] = None) -> bool:
        """
        Validate full license.

        Raises:
            LicenseError if validation fails.
        """
        self.load_license()
        self.verify_signature()
        self.verify_expiry()
        self.verify_mac_binding()
        self.verify_fingerprint_binding()
        self.verify_features(required_features)

        return True
