from license_manager import LicenseManager, LicenseError

with open("public_key.pem", "rb") as f:
    PUBLIC_KEY = f.read()

try:
    lm = LicenseManager(public_key_pem=PUBLIC_KEY, license_path="license.lic")
    lm.validate(required_features=["EMS"])
    print("License valid. Application starting...")
except LicenseError as e:
    print(f"License validation failed: {e}")
    exit(1)