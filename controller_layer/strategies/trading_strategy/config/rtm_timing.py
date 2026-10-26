from datetime import timedelta

RTM_TIMING = {
    "block_minutes": 30,

    "plan_start": timedelta(minutes=120),
    "plan_end": timedelta(minutes=60),

    "freeze_at": timedelta(minutes=60),
    "submit_at": timedelta(minutes=45),
}