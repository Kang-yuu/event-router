import json
import logging
import os
import sys

import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError, BotoCoreError

load_dotenv()

REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
ROLE_ARN = os.environ.get("AWS_ROLE_ARN")
ROLE_SESSION_NAME = os.environ.get("AWS_ROLE_SESSION_NAME", "dynamo-streamlit-app")

ACCOUNT_ID = "383654655155"
TEAM_QUEUES = {
    "CS팀": f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/kyl-support-queue",
    "물류팀": f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/kyl-product-queue",
}
TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:kyl-inquiry-topic"

# 화면에 보이는 창구. 여러 창구가 한 큐를 공유할 수 있다.
# CS 1팀/2팀은 같은 큐를 보지만 SQS 점유 때문에 같은 건을 집지 않는다.
# 두 번째 값은 작업 방식. claim = 정해진 건수만, all = 대기 전량.
# 창구 이름은 화면 표시 전용이다. DB 에 기록하는 팀 이름은 첫 번째 값을 쓴다.
WORKSPACES = {
    "CS 1팀": ("CS팀", "claim"),
    "CS 2팀": ("CS팀", "claim"),
    "물류팀": ("물류팀", "all"),
}

# 워커 한 명이 한 번에 집어가는 건수. 다 처리해야 다음 건을 받는다.
CLAIM_BATCH = 2
# 집어간 메시지를 붙잡는 시간. 이 안에 완료하지 못하면 자동으로 큐에 돌아간다.
CLAIM_VISIBILITY = 600

# 아래는 전량 조회(list_pending)용.
# receive_message 는 한 번에 전량을 주지 않으므로 여러 번 반복해서 긁는다.
RECEIVE_ROUNDS = 30
# 루프가 도는 동안 이미 받은 메시지가 다시 보이면 라운드만 낭비한다.
# 루프 전체 소요시간보다 넉넉히 길게 잡는다.
RECEIVE_VISIBILITY = 60
# 새 메시지가 하나도 안 늘어난 라운드가 연속 이만큼이면 종료한다.
RECEIVE_IDLE_ROUNDS = 2

# Logs go to the Streamlit terminal/console and can also be enabled with DEBUG.
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("aws-queue")


def _mask(value, keep=4):
    if not value:
        return value
    if len(value) <= keep * 2:
        return "***"
    return f"{value[:keep]}...{value[-keep:]}"


def assumed_credentials():
    if not ROLE_ARN:
        return {}

    try:
        sts = boto3.client("sts", region_name=REGION)
        creds = sts.assume_role(
            RoleArn=ROLE_ARN,
            RoleSessionName=ROLE_SESSION_NAME,
        )["Credentials"]

        return {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
    except (ClientError, BotoCoreError) as e:
        logger.exception("[AWS] AssumeRole 실패")
        raise


def get_sqs_client():
    return boto3.client("sqs", region_name=REGION, **assumed_credentials())


def get_sns_client():
    return boto3.client("sns", region_name=REGION, **assumed_credentials())


def publish_inquiry(row):
    """SNS publish 결과와 MessageId를 상세 로그로 남긴다."""
    sns = get_sns_client()

    # 큐는 "이 건을 처리하라"는 포인터다. 표시할 값은 DynamoDB 가 원본이다.
    # 내용을 실어 보내면 발행 이후 DB 가 수정됐을 때 큐 사본이 낡은 값을 들고 있다.
    payload = {
        "inquiry_id": row.get("inquiry_id", ""),
        "event_id": row.get("event_id", ""),
    }

    message = json.dumps(payload, ensure_ascii=False)

    logger.info(
        "[SNS PUBLISH] 시작 | inquiry_id=%s | event_id=%s | topic=%s",
        payload["inquiry_id"],
        payload["event_id"],
        TOPIC_ARN,
    )
    logger.debug("[SNS PUBLISH] payload=%s", message)

    try:
        response = sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=f"kyl inquiry {payload['inquiry_id'] or 'unknown'}",
            Message=message,
        )

        logger.info(
            "[SNS PUBLISH] 성공 | MessageId=%s | inquiry_id=%s",
            response.get("MessageId"),
            payload["inquiry_id"],
        )

        return response

    except (ClientError, BotoCoreError):
        logger.exception(
            "[SNS PUBLISH] 실패 | inquiry_id=%s | event_id=%s",
            payload["inquiry_id"],
            payload["event_id"],
        )
        raise


def _parse_message(msg):
    """SNS 봉투를 벗겨 dict로 만든다. 깨진 메시지는 None을 돌려주고 로그만 남긴다."""
    try:
        envelope = json.loads(msg["Body"])
        data = json.loads(envelope["Message"])
    except (ValueError, KeyError, TypeError):
        logger.exception(
            "[SQS RECEIVE] 메시지 파싱 실패 | MessageId=%s",
            msg.get("MessageId"),
        )
        return None

    data["receipt_handle"] = msg["ReceiptHandle"]
    data["message_id"] = msg["MessageId"]
    return data


def list_pending(team, target=None):
    """큐에 보이는 메시지를 긁어모아 MessageId 기준 dict로 돌려준다.

    receive_message 는 메시지를 분산 저장한 서버 일부만 샘플링하므로
    한 번 호출해서는 전량이 오지 않는다. 여러 번 돌려야 한다.

    target 을 주면(보통 queue_depth 의 대기 건수) 그만큼 모았을 때 바로 멈춘다.
    빈 응답이 안 와도 새 메시지가 안 늘어나면 RECEIVE_IDLE_ROUNDS 회 뒤 종료한다.

    한 번에 최대 RECEIVE_ROUNDS x 10 건. 점유가 짧아 곧 다시 보이므로
    claim 과 달리 내 작업으로 잡아두는 것이 아니다.
    """
    sqs = get_sqs_client()
    queue_url = TEAM_QUEUES[team]

    items = {}
    idle = 0
    rounds = 0

    for rounds in range(1, RECEIVE_ROUNDS + 1):
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            VisibilityTimeout=RECEIVE_VISIBILITY,
        )

        before = len(items)

        for msg in response.get("Messages", []):
            data = _parse_message(msg)
            if data is not None:
                # 재수신이면 최신 receipt_handle 로 덮어쓴다.
                items[msg["MessageId"]] = data

        if len(items) == before:
            # 빈 응답이거나 전부 중복. 연속되면 더 긁어도 소용없다.
            idle += 1
            if idle >= RECEIVE_IDLE_ROUNDS:
                break
        else:
            idle = 0

        if target and len(items) >= target:
            break

    logger.info(
        "[SQS RECEIVE] team=%s | 수집 %d건 | 목표 %s | %d라운드",
        team,
        len(items),
        target if target is not None else "-",
        rounds,
    )
    return items


def claim(team, count=CLAIM_BATCH):
    """count 건을 내 작업으로 집어온다. MessageId 기준 dict 로 돌려준다.

    CLAIM_VISIBILITY 동안 다른 워커에게는 보이지 않는다. 큐 하나를 여러 명이
    같이 봐도 겹치지 않는 이유가 이것이다. 완료하면 delete, 못 하면 시간이
    지나 자동으로 큐에 돌아간다.

    요청보다 적게 올 수 있다. 버튼을 다시 누르면 된다.
    """
    sqs = get_sqs_client()

    response = sqs.receive_message(
        QueueUrl=TEAM_QUEUES[team],
        MaxNumberOfMessages=count,
        # 적게 요청할수록 빈 응답이 나오기 쉽다. long polling 으로 막는다.
        WaitTimeSeconds=5,
        VisibilityTimeout=CLAIM_VISIBILITY,
    )

    items = {}
    for msg in response.get("Messages", []):
        data = _parse_message(msg)
        if data is not None:
            items[msg["MessageId"]] = data

    logger.info(
        "[SQS CLAIM] team=%s | 요청 %d건 | 수신 %d건",
        team,
        count,
        len(items),
    )
    return items


def release(team, receipt_handle):
    """반납. 남은 점유 시간을 0으로 만들어 즉시 다른 워커에게 보이게 한다."""
    sqs = get_sqs_client()

    logger.info("[SQS RELEASE] 시작 | team=%s | receipt=%s", team, _mask(receipt_handle))

    try:
        response = sqs.change_message_visibility(
            QueueUrl=TEAM_QUEUES[team],
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=0,
        )
        logger.info("[SQS RELEASE] 성공 | team=%s", team)
        return response
    except (ClientError, BotoCoreError):
        logger.exception("[SQS RELEASE] 실패 | team=%s", team)
        raise


def queue_depth(team):
    """콘솔에 보이는 근사 건수. (대기, 처리중, 지연) 튜플."""
    sqs = get_sqs_client()

    attrs = sqs.get_queue_attributes(
        QueueUrl=TEAM_QUEUES[team],
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]

    return (
        int(attrs["ApproximateNumberOfMessages"]),
        int(attrs["ApproximateNumberOfMessagesNotVisible"]),
        int(attrs["ApproximateNumberOfMessagesDelayed"]),
    )


def complete(team, receipt_handle):
    sqs = get_sqs_client()
    queue_url = TEAM_QUEUES[team]

    logger.info(
        "[SQS DELETE] 시작 | team=%s | receipt=%s",
        team,
        _mask(receipt_handle),
    )

    try:
        response = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
        logger.info("[SQS DELETE] 성공 | team=%s", team)
        return response
    except (ClientError, BotoCoreError):
        logger.exception("[SQS DELETE] 실패 | team=%s", team)
        raise
