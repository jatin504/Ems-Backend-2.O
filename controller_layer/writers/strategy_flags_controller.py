from datetime import date
import logging

from utils.db_utils import A_DB

logger = logging.getLogger(__name__)

class StrategyFlagsController:
    def __init__(self, ess_control_state_manager):
        self.ess_control_state_manager = ess_control_state_manager

    async def _is_flag_raised_today(self, strategy_name: str, flag_name: str) -> bool:
        query = """
            SELECT 1
            FROM ems_strategy_flags
            WHERE strategy_name = $1
              AND flag_name = $2
              AND raised_date = CURRENT_DATE
            LIMIT 1;
        """

        async with A_DB() as db:
            row = await db.fetchrow(query, strategy_name, flag_name)
        return row is not None

    async def _store_flag(self, strategy_name, flag_name, flag_value):
        query = """
            INSERT INTO ems_strategy_flags (
                strategy_name,
                flag_name,
                flag_value,
                raised_date
            )
            VALUES ($1, $2, $3, CURRENT_DATE)
            ON CONFLICT DO NOTHING;
        """
        
        async with A_DB() as db:
            await db.execute(query, strategy_name, flag_name, flag_value)

    async def raise_strategy_flags_once_per_day(
        self,
        strategy_name: str,
        flags: dict
    ):
        """
        flags example:
        {
            "enable_capped_dynamic_strategy": 1,
            "enable_tou_tariff_strategy": 0,
            "enable_peak_shaving_strategy": 0
        }
        """
        for flag_name, flag_value in flags.items():
            try:
                # already_raised = await self._is_flag_raised_today(
                #     strategy_name, flag_name
                # )

                # if already_raised:
                #     logger.debug(
                #         f"Flag {flag_name} already raised today for {strategy_name}"
                #     )
                #     continue

                await self.ess_control_state_manager.raise_flag(
                    flag_name, flag_value
                )

                # await self._store_flag(
                #     strategy_name, flag_name, flag_value
                # )

                logger.info(
                    f"Raised flag {flag_name}={flag_value} for {strategy_name}"
                )

            except Exception as e:
                logger.error(
                    f"Error raising flag {flag_name} for {strategy_name}: {e}"
                )
