from ...models.bid import Bid
from ...models.dam import DAMBlock, DAMPriceBand, DAMBid
from ...models.tariff import TariffBlock

from .power_distributor import DAMPowerDistributor

from uuid import uuid4
from datetime import date
from typing import List
from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class DAMBidBuilder:

    @staticmethod
    def build(
        block: DAMBlock,
        price_band: DAMPriceBand,
        bess_limits,
        direction: str,
        confidence: str = "MEDIUM"
    ) -> list[DAMBid]:

        # Compute total feasible power
        max_kw = min(
            bess_limits["max_discharge_kw"] if direction == "SELL" else bess_limits["max_charge_kw"],
            bess_limits["soc_available_kwh"] / 0.25
        )

        if max_kw <= 0:
            return []

        # Split MW into steps
        steps = DAMPowerDistributor.split(max_kw, confidence)

        # Assign prices per step
        prices = [
            price_band.low,
            price_band.mid,
            price_band.high
        ]

        bids = []

        for (step_no, qty_kw), price in zip(steps, prices):
            if qty_kw <= 0:
                continue

            bids.append(
                DAMBid(
                    block_no=block.block_no,
                    direction=direction,
                    price_rs_per_kwh=price,
                    quantity_kw=qty_kw,
                    step=step_no,
                    start_time=block.start_time,
                    end_time=block.end_time
                )
            )

        return bids


