

def modbus_processor_node_helper(
    devices_count: int,
    process_nodes: int,
    support_count: int,
):
    """
    Generate device ranges for each Modbus process.

    Returns:
        List of tuples:
        [
            (start_device, end_device),
            ...
        ]

    end_device = -1 means "till the last device".
    """

    # Single process handles everything
    if process_nodes <= 1:
        return [(0, -1)]

    ranges = []

    for node in range(process_nodes):
        start = node * support_count

        # Last node handles remaining devices
        if node == process_nodes - 1:
            end = -1
        else:
            end = start + support_count - 1

        # Stop creating useless processes
        if start >= devices_count:
            break

        ranges.append((start, end))

    return ranges