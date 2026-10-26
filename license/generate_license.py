"""
generate_license.py
Creates and signs a license file.
Run this on your secure machine (with private key).
"""

import json
import base64
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def generate_license(data: dict, private_key_path: str, output_path: str):
    # Load private key
    with open(private_key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
        )

    # Create copy without signature
    license_copy = dict(data)

    # Convert to canonical JSON
    license_json = json.dumps(license_copy, sort_keys=True).encode()

    # Sign
    signature = private_key.sign(
        license_json,
        padding.PKCS1v15(),
        hashes.SHA256(),
    )

    # Attach signature
    license_copy["signature"] = base64.b64encode(signature).decode()

    # Save file
    with open(output_path, "w") as f:
        json.dump(license_copy, f, indent=4)

    print(f"License saved to {output_path}")


if __name__ == "__main__":

    license_data = {
        "customer": "GUVNL Secondary Server",
        "mac_id": "80:30:E0:42:43:D0",  # Optional
        "hardware_fingerprint": "80344e626da13ec9770ccb5eee199f5d2119183e3b571306c73fb29c65ac4be8",  # PUT_GENERATED_FP_HERE
        "expiry": "2031-09-01",
        "features": ["SCADA", "EMS"]
    }

    generate_license(
        data=license_data,
        private_key_path="private_key.pem",
        output_path="license.lic"
    )
