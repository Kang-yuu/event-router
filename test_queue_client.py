"""python test_queue_client.py 로 바로 실행되는 자체 점검."""

import json

import queue_client


def _sns_message(message_id, inquiry_id, receipt):
    return {
        "MessageId": message_id,
        "ReceiptHandle": receipt,
        "Body": json.dumps({"Message": json.dumps({"inquiry_id": inquiry_id})}),
    }


class FakeSqs:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.receive_kwargs = None
        self.visibility_kwargs = None

    def receive_message(self, **kwargs):
        self.receive_kwargs = kwargs
        return {"Messages": self.messages}

    def change_message_visibility(self, **kwargs):
        self.visibility_kwargs = kwargs
        return {}


def with_fake(fake, fn):
    original = queue_client.get_sqs_client
    queue_client.get_sqs_client = lambda: fake
    try:
        return fn()
    finally:
        queue_client.get_sqs_client = original


def test_claim_asks_for_exactly_count_and_holds_them():
    fake = FakeSqs([_sns_message("m1", "INQ-1", "r1"), _sns_message("m2", "INQ-2", "r2")])
    items = with_fake(fake, lambda: queue_client.claim("CS팀", count=2))

    assert len(items) == 2, items
    assert items["m1"]["inquiry_id"] == "INQ-1"
    assert items["m1"]["receipt_handle"] == "r1"
    # 요청 건수를 넘겨 받으면 남의 몫까지 잠그게 된다.
    assert fake.receive_kwargs["MaxNumberOfMessages"] == 2, fake.receive_kwargs
    # 점유 시간이 짧으면 작업 도중에 남에게 다시 보인다.
    assert fake.receive_kwargs["VisibilityTimeout"] == queue_client.CLAIM_VISIBILITY


def test_claim_respects_smaller_count():
    fake = FakeSqs([_sns_message("m1", "INQ-1", "r1")])
    with_fake(fake, lambda: queue_client.claim("CS팀", count=1))
    assert fake.receive_kwargs["MaxNumberOfMessages"] == 1


def test_bad_body_does_not_kill_claim():
    bad = {"MessageId": "m9", "ReceiptHandle": "r9", "Body": "not json"}
    fake = FakeSqs([bad, _sns_message("m1", "INQ-1", "r1")])
    items = with_fake(fake, lambda: queue_client.claim("CS팀"))
    assert list(items) == ["m1"], items


def test_empty_queue_returns_empty():
    fake = FakeSqs([])
    assert with_fake(fake, lambda: queue_client.claim("CS팀")) == {}


def test_release_makes_it_visible_now():
    fake = FakeSqs()
    with_fake(fake, lambda: queue_client.release("CS팀", "r1"))
    assert fake.visibility_kwargs["ReceiptHandle"] == "r1"
    # 0 이 아니면 반납해도 그만큼 아무도 못 집어간다.
    assert fake.visibility_kwargs["VisibilityTimeout"] == 0, fake.visibility_kwargs


class FakeRoundsSqs:
    """receive_message 가 전량을 한 번에 주지 않는 실제 동작을 흉내낸다."""

    def __init__(self, rounds):
        self.rounds = list(rounds)
        self.calls = 0

    def receive_message(self, **kwargs):
        self.calls += 1
        if not self.rounds:
            return {}
        return {"Messages": self.rounds.pop(0)}


class FakeSns:
    def __init__(self):
        self.kwargs = None

    def publish(self, **kwargs):
        self.kwargs = kwargs
        return {"MessageId": "sns-1"}


def test_list_pending_drains_across_rounds():
    fake = FakeRoundsSqs([
        [_sns_message("m1", "INQ-1", "r1"), _sns_message("m2", "INQ-2", "r2")],
        [_sns_message("m3", "INQ-3", "r3")],
        [],
    ])
    items = with_fake(fake, lambda: queue_client.list_pending("물류팀"))
    assert len(items) == 3, items


def test_list_pending_dedupes_and_keeps_latest_handle():
    fake = FakeRoundsSqs([
        [_sns_message("m1", "INQ-1", "old")],
        [_sns_message("m1", "INQ-1", "new")],
        [],
    ])
    items = with_fake(fake, lambda: queue_client.list_pending("물류팀"))
    assert len(items) == 1, items
    assert items["m1"]["receipt_handle"] == "new"


def test_list_pending_stops_at_round_limit():
    always = [[_sns_message(f"m{i}", f"INQ-{i}", f"r{i}")] for i in range(50)]
    fake = FakeRoundsSqs(always)
    with_fake(fake, lambda: queue_client.list_pending("물류팀"))
    assert fake.calls == queue_client.RECEIVE_ROUNDS, fake.calls


def test_publish_sends_only_the_two_keys():
    """큐에 내용을 실으면 DB 가 수정됐을 때 낡은 사본이 화면에 뜬다."""
    fake = FakeSns()
    original = queue_client.get_sns_client
    queue_client.get_sns_client = lambda: fake
    try:
        queue_client.publish_inquiry(
            {"inquiry_id": "INQ-1", "event_id": "EV-1", "subject": "제목", "message": "본문"}
        )
    finally:
        queue_client.get_sns_client = original

    payload = json.loads(fake.kwargs["Message"])
    assert set(payload) == {"inquiry_id", "event_id"}, payload
    assert payload["inquiry_id"] == "INQ-1"
    assert payload["event_id"] == "EV-1"


if __name__ == "__main__":
    test_claim_asks_for_exactly_count_and_holds_them()
    test_claim_respects_smaller_count()
    test_bad_body_does_not_kill_claim()
    test_empty_queue_returns_empty()
    test_release_makes_it_visible_now()
    test_list_pending_drains_across_rounds()
    test_list_pending_dedupes_and_keeps_latest_handle()
    test_list_pending_stops_at_round_limit()
    test_publish_sends_only_the_two_keys()
    print("ok")
