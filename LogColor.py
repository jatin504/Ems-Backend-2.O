# Log Colors
class LogColor:
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"

    @staticmethod
    def for_slot(slot_number):
        color_cycle = [LogColor.CYAN, LogColor.GREEN, LogColor.YELLOW, LogColor.MAGENTA, LogColor.BLUE, LogColor.RED]
        return color_cycle[(slot_number - 1) % len(color_cycle)]