class RTMClearingHandler:

    async def handle(self, cleared_result):

        await ScheduleRepository.store_schedule(
            market="RTM",
            device_id=cleared_result.device_id,
            schedules=cleared_result.dispatch,
            bid_id=cleared_result.bid_id
        )

        await DispatchEngine.adjust(
            schedule=cleared_result.dispatch
        )
