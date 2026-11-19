"""Start the approved archive manager without starting the EMS backend."""
from victoria_archiving_manager.archive_manager import main


if __name__ == "__main__":
    # Scheduled service by default; --once/--status/--restore remain available.
    main(service_default=True)
