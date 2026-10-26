from license_manager import LicenseManager

print("MAC:", LicenseManager.get_mac_address())
print("CPU:", LicenseManager.get_cpu_id())
print("DISK:", LicenseManager.get_disk_serial())
print("Machine UUID:", LicenseManager.get_machine_uuid())
print("Fingerprint:", LicenseManager.generate_hardware_fingerprint())
