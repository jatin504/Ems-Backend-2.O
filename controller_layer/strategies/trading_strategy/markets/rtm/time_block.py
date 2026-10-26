from dataclasses import dataclass
from datetime import datetime, timedelta

from ...config.rtm_timing import RTM_TIMING

@dataclass
class TimeBlock:
    start: datetime
    end: datetime
    block_no: int

    @staticmethod
    def future_blocks(now, hours=3):
        blocks = []
        aligned = now.replace(minute=(now.minute // 30) * 30, second=0)

        for i in range(int(hours * 2)):
            start = aligned + timedelta(minutes=30 * (i + 1))
            end = start + timedelta(minutes=30)
            block_no = _get_block_no(start)
            blocks.append(TimeBlock(start, end, block_no))

        return blocks
    
def _get_block_no(dt: datetime, block_minutes=30):
    return (dt.hour * 60 + dt.minute) // block_minutes + 1
