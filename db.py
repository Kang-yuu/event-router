import boto3
from boto3.dynamodb.conditions import Attr

from queue_client import REGION, TEAM_QUEUES, assumed_credentials

TABLE_NAME = "kyl_customer_inquiries"


def get_table():
    dynamodb = boto3.resource("dynamodb", region_name=REGION, **assumed_credentials())
    return dynamodb.Table(TABLE_NAME)


def list_inquiries():
    table = get_table()
    response = table.scan()
    return response.get("Items", [])


def get_inquiry(inquiry_id):
    table = get_table()
    response = table.get_item(Key={"inquiry_id": inquiry_id})
    return response.get("Item")


def _as_team_list(value):
    """confirmed_teams 를 리스트로 맞춘다.

    CSV 업로드는 행을 그대로 put_item 하므로, CSV에 confirmed_teams 칼럼이 있으면
    리스트가 아니라 문자열("" 또는 "CS팀,물류팀")로 저장된다. 그대로 쓰면 터진다.
    """
    if isinstance(value, str):
        return [t.strip() for t in value.split(",") if t.strip()]
    return list(value or [])


def confirm_team_completion(inquiry_id, team, event_id):
    """inquiry_id + event_id 가 모두 맞는 항목만 완료 처리한다.

    event_id 는 SQS 메시지에 실려온 값이다. DynamoDB 의 값과 다르면
    큐의 메시지가 지금 DB 항목과 다른 건이라는 뜻이므로 건드리지 않는다.
    """
    # ponytail: read-then-write, not atomic — fine at this app's traffic;
    # switch to a conditional list_append update_item if two teams can race.
    table = get_table()
    item = table.get_item(Key={"inquiry_id": inquiry_id}).get("Item") or {}

    if not item:
        raise ValueError(f"inquiry_id={inquiry_id} 항목이 DynamoDB에 없습니다.")

    stored_event_id = item.get("event_id", "")
    if stored_event_id != event_id:
        raise ValueError(
            f"event_id 불일치 — 큐={event_id or '없음'}, DB={stored_event_id or '없음'}"
        )

    confirmed = _as_team_list(item.get("confirmed_teams"))
    if team not in confirmed:
        confirmed = confirmed + [team]

    if len(confirmed) >= len(TEAM_QUEUES):
        new_state = "DONE"
    elif confirmed:
        new_state = "PROCESS"
    else:
        new_state = "WAIT"

    table.update_item(
        Key={"inquiry_id": inquiry_id},
        UpdateExpression="SET confirmed_teams = :ct, #s = :st",
        ConditionExpression=Attr("event_id").eq(event_id),
        ExpressionAttributeNames={"#s": "state"},
        ExpressionAttributeValues={":ct": confirmed, ":st": new_state},
    )


def bulk_upsert_inquiries(items):
    table = get_table()
    with table.batch_writer(overwrite_by_pkeys=["inquiry_id"]) as batch:
        for item in items:
            batch.put_item(Item=item)
