from datetime import timedelta

DAM_TIMING = {
    # Reference point
    "gate_closure_offset": timedelta(hours=0),

    # When DAM bid preparation should START
    "planning_start_offset": timedelta(hours=36),   # T-36h
    "planning_end_offset": timedelta(hours=6),      # T-6h

    # When DAM bids should be FINALIZED
    "finalize_start_offset": timedelta(hours=6),    # T-6h
    "finalize_end_offset": timedelta(hours=3),      # T-3h

    # Post clearing
    "clearing_offset": timedelta(hours=-3)          # T+3h (approx MCP publish)
}
