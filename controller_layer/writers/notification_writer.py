import json

from utils.db_utils import A_DB

INSERT_NOTIFICATION = """
INSERT INTO notification_event (

    event_time,
    source,
    event_type,
    severity,
    title,
    message,
    entity_type,
    entity_id,
    metadata

)
VALUES (

    NOW(),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8

)
"""

async def create_bess_notification(

    source,
    event_type,
    severity,
    title,
    message=None,
    entity_type=None,
    entity_id=None,
    metadata=None
):


    # --------------------------------
    # AUTO CLEAR PREVIOUS STAGE
    # --------------------------------

    # if previous_stage is not None and metadata.get("stage") != previous_stage:

    #     await clear_previous_stage_notification(
    #         source,
    #         event_type,
    #         entity_id
    #     )
    
    if metadata is not None:
        metadata = json.dumps(metadata)

    query = """
    INSERT INTO notification_event (

        source,
        event_type,
        severity,
        title,
        message,
        entity_type,
        entity_id,
        metadata

    )
    VALUES (

        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8::jsonb

    )
    ON CONFLICT DO NOTHING
    """

    async with A_DB() as db:

        await db.execute(
            query,
            source,
            event_type,
            severity,
            title,
            message,
            entity_type,
            entity_id,
            metadata
        )


async def clear_previous_stage_notification(
    source,
    event_type,
    entity_id
):

    query = """
    UPDATE notification_event
    SET

        status = 'cleared',
        cleared_time = NOW()

    WHERE

        source = $1
        AND event_type = $2
        AND entity_id = $3
        AND status = 'active'
    """

    async with A_DB() as db:
        await db.execute(
            query,
            source,
            event_type,
            entity_id
        )