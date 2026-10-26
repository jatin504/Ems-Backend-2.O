from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class RTMContextBuilder:

    @staticmethod
    def build(
        device_id,
        block,
        plant_forecast,
        tariff_forecast,
        bess_state
    ):
        try:
            block_time = block.start.strftime("%H:%M")

            pf = next(
                (p for p in plant_forecast if p.start_time == block_time),
                None
            )

            tf = next(
                (t for t in tariff_forecast if t.start_time == block_time),
                None
            )

            if not pf:
                raise ValueError(
                    f"No plant forecast for block {block.block_no} @ {block_time}"
                )

            if not tf:
                raise ValueError(
                    f"No tariff forecast for block {block.block_no} @ {block_time}"
                )

            return {
                "block": block,
                "forecast_power_kw": pf.plant_power_kw,
                "price": tf.price_rs_per_kwh,
                "bess": bess_state
            }

        except Exception as e:
            logger.error(
                f"{LogColor.RED} RTM Context Build Failed | "
                f"Device={device_id} Block={block.block_no} "
                f"Time={block.start} Error={e} "
                f"{LogColor.RESET}"
            )
            return None

