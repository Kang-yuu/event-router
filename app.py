import csv
import io
import time

import streamlit as st
from streamlit_option_menu import option_menu

from db import (
    bulk_upsert_inquiries,
    confirm_team_completion,
    get_inquiry,
    list_inquiries,
)
from queue_client import (
    CLAIM_BATCH,
    CLAIM_VISIBILITY,
    WORKSPACES,
    claim,
    complete,
    list_pending,
    publish_inquiry,
    queue_depth,
    release,
)

STATES = ["WAIT", "PROCESS", "DONE"]
STATE_LABELS = {"WAIT": "대기 중", "PROCESS": "진행 중", "DONE": "완료"}
STATE_COLORS = {"WAIT": "#d64545", "PROCESS": "#1971c2", "DONE": "#2f9e44"}
MENU_OPTIONS = ["전체 문의", "CSV 업로드", "SQS 문의"]

MENU_STYLES = {
    "nav-link-selected": {"background-color": "rgba(28, 131, 255, 0.1);", "color": "rgb(0, 84, 163);"},
}

st.set_page_config(page_title="고객 문의 관리", layout="centered")

if "view" not in st.session_state:
    st.session_state.view = "list"
if "selected_id" not in st.session_state:
    st.session_state.selected_id = None
if "pending_toast" not in st.session_state:
    st.session_state.pending_toast = None
if "detail_source" not in st.session_state:
    st.session_state.detail_source = None
if "detail_team" not in st.session_state:
    st.session_state.detail_team = None
if "detail_receipt_handle" not in st.session_state:
    st.session_state.detail_receipt_handle = None
if "detail_message_id" not in st.session_state:
    st.session_state.detail_message_id = None
if "detail_event_id" not in st.session_state:
    st.session_state.detail_event_id = None
if "detail_workspace" not in st.session_state:
    st.session_state.detail_workspace = None
if "queue_items" not in st.session_state:
    # {창구: {message_id: item}} — claim 창구는 item 에 claimed_at 이 붙는다.
    st.session_state.queue_items = {}
if "sqs_depth" not in st.session_state:
    st.session_state.sqs_depth = {}


def decode_csv_bytes(raw_bytes):
    for encoding in ("utf-8-sig", "cp949"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", raw_bytes, 0, 1, "지원하지 않는 인코딩입니다.")


@st.dialog("완료 처리")
def confirm_complete_dialog(
    inquiry_id, event_id, team, receipt_handle, message_id=None, workspace=None
):
    st.write(f"{team} 문의를 완료 처리할까요?")
    yes_col, no_col = st.columns(2)
    if yes_col.button("예", key=f"yes_{inquiry_id}_{event_id}_{team}"):
        # DynamoDB 먼저. 여기서 거부되면 메시지는 큐에 남아야 한다.
        # 순서를 뒤집으면 검증 실패 시 메시지만 삭제돼 복구가 불가능하다.
        try:
            confirm_team_completion(inquiry_id, team, event_id)
        except Exception as e:
            st.error(f"DynamoDB 업데이트 실패: {e}")
            return

        try:
            complete(team, receipt_handle)
        except Exception as e:
            st.error(
                "DynamoDB는 갱신됐지만 SQS 삭제에 실패했습니다. "
                f"새로고침 후 다시 완료 처리하세요: {e}"
            )
            return

        st.session_state.queue_items.get(workspace, {}).pop(message_id, None)
        st.session_state.pending_toast = "완료 처리되었습니다."
        st.rerun()
    if no_col.button("아니오", key=f"no_{inquiry_id}_{event_id}_{team}"):
        st.rerun()


def show_upload_page():
    st.title("CSV 업로드")

    uploaded = st.file_uploader("CSV 파일 업로드", type=["csv"])
    if uploaded is not None:
        try:
            text = decode_csv_bytes(uploaded.getvalue())
            rows = list(csv.DictReader(io.StringIO(text)))
        except (UnicodeDecodeError, csv.Error) as e:
            st.error(f"CSV를 읽는 중 오류가 발생했습니다: {e}")
            return

        if not rows:
            st.warning("CSV에 등록할 데이터가 없습니다.")
            return

        valid_rows, skipped = [], []
        for i, row in enumerate(rows, start=1):
            # inquiry_id 는 DynamoDB 파티션 키, event_id 는 큐 완료 시 대조에 쓴다.
            # 둘 중 하나라도 없으면 완료 처리를 못 하므로 등록 단계에서 거른다.
            missing = [f for f in ("inquiry_id", "event_id") if not (row.get(f) or "").strip()]
            if missing:
                skipped.append((i, row.get("inquiry_id", ""), row.get("event_id", ""), missing))
            else:
                valid_rows.append(row)

        if skipped:
            lines = "\n".join(
                f"- {i}행 (inquiry_id={iid or '없음'}, event_id={eid or '없음'}): "
                f"{', '.join(m)} 없음"
                for i, iid, eid, m in skipped
            )
            st.error(f"다음 행은 필수 값이 없어 건너뜁니다:\n{lines}")

        if not valid_rows:
            st.warning("등록할 유효한 데이터가 없습니다.")
            return

        if st.button(f"{len(valid_rows)}건 등록하기"):
            with st.spinner(f"{len(valid_rows)}건 업로드 중입니다..."):
                try:
                    st.info("1/2 DynamoDB 저장 중...")
                    bulk_upsert_inquiries(valid_rows)
                    st.success("DynamoDB 저장 완료")

                    st.info("2/2 SNS 발행 중...")
                    success_count = 0

                    for index, row in enumerate(valid_rows, start=1):
                        inquiry_id = row.get("inquiry_id", "(inquiry_id 없음)")
                        event_id = row.get("event_id", "(event_id 없음)")

                        try:
                            response = publish_inquiry(row)
                            success_count += 1
                            st.write(
                                f"✅ {index}/{len(valid_rows)} SNS 발행 성공 "
                                f"- inquiry_id={inquiry_id}, "
                                f"MessageId={response.get('MessageId')}"
                            )
                        except Exception as e:
                            st.error(
                                f"❌ {index}/{len(valid_rows)} SNS 발행 실패 "
                                f"- inquiry_id={inquiry_id}: {e}"
                            )
                            st.exception(e)

                    if success_count == len(valid_rows):
                        st.success(
                            f"{success_count}건 모두 SNS publish 성공. "
                            "이제 AWS SNS → SQS subscription을 확인하세요."
                        )
                    else:
                        st.warning(
                            f"SNS publish 성공 {success_count}건 / "
                            f"전체 {len(valid_rows)}건"
                        )

                except Exception as e:
                    st.error(f"등록 처리 중 치명적 오류: {e}")
                    st.exception(e)
                    return

            st.session_state.pending_toast = f"{len(valid_rows)}건 등록 완료"
            st.rerun()


def _open_detail(
    inquiry_id,
    source,
    team=None,
    receipt_handle=None,
    message_id=None,
    event_id=None,
    workspace=None,
):
    st.session_state.selected_id = inquiry_id
    st.session_state.detail_source = source
    st.session_state.detail_team = team
    st.session_state.detail_workspace = workspace
    st.session_state.detail_receipt_handle = receipt_handle
    st.session_state.detail_message_id = message_id
    st.session_state.detail_event_id = event_id
    st.session_state.view = "detail"
    st.rerun()


def show_list():
    st.title("고객 문의 목록")

    items = list_inquiries()
    if not items:
        st.info("등록된 문의가 없습니다.")
        return

    for item in items:
        inquiry_id = item.get("inquiry_id")
        current_event_id = item.get("event_id", "")
        with st.container(border=True):
            status_col, title_col = st.columns([2, 8])

            with status_col:
                state = item.get("state", STATES[0])
                label = STATE_LABELS.get(state, state)
                color = STATE_COLORS.get(state, "gray")
                st.markdown(
                    f'<div style="display:flex;align-items:center;justify-content:center;'
                    f'height:100%;color:{color};font-weight:600;">{label}</div>',
                    unsafe_allow_html=True,
                )

            with title_col:
                if st.button(
                    f"{inquiry_id}  {item.get('subject')}",
                    key=f"open_{inquiry_id}_{current_event_id}",
                    use_container_width=True,
                    type="tertiary",
                ):
                    _open_detail(inquiry_id, source="all")


def _drop_expired(mine):
    """점유 시간이 지난 건은 목록에서 뺀다.

    만료된 메시지는 이미 큐로 돌아가 다른 워커가 집어갔을 수 있다.
    낡은 receipt_handle 로 완료 처리하면 남의 작업을 지우는 셈이라 먼저 거른다.
    """
    deadline = time.time() - CLAIM_VISIBILITY
    expired = [mid for mid, item in mine.items() if item.get("claimed_at", 0) < deadline]
    for mid in expired:
        del mine[mid]
    return len(expired)


def show_sqs_page():
    with st.sidebar:
        workspace = option_menu(
            "창구 선택", list(WORKSPACES), key="sqs_workspace", styles=MENU_STYLES
        )
    team, mode = WORKSPACES[workspace]

    st.title(f"{workspace} 문의")

    try:
        st.session_state.sqs_depth[workspace] = queue_depth(team)
    except Exception as e:
        st.error(f"큐 지표 조회 실패: {e}")

    depth = st.session_state.sqs_depth.get(workspace)
    if depth:
        waiting, in_flight, delayed = depth
        col1, col2, col3 = st.columns(3)
        col1.metric("대기", waiting)
        col2.metric("처리중", in_flight)
        col3.metric("지연", delayed)

    if mode == "claim":
        show_claim_queue(workspace, team)
    else:
        show_all_queue(workspace, team, depth)


def show_claim_queue(workspace, team):
    """정해진 건수만 집어와 점유하고 처리한다. CS 창구."""
    mine = st.session_state.queue_items.setdefault(workspace, {})

    expired = _drop_expired(mine)
    if expired:
        st.warning(
            f"점유 시간 {CLAIM_VISIBILITY // 60}분이 지나 {expired}건이 큐로 돌아갔습니다. "
            "다시 가져오세요."
        )

    st.write(f"### 내 작업 ({len(mine)}/{CLAIM_BATCH})")

    room = CLAIM_BATCH - len(mine)
    if room > 0:
        if st.button(f"작업 {room}건 가져오기"):
            try:
                got = claim(team, count=room)
            except Exception as e:
                st.error(f"작업 가져오기 실패: {e}")
                return
            now = time.time()
            for item in got.values():
                item["claimed_at"] = now
                # 큐에는 키만 실려온다. 표시할 값은 DB 에서 읽는다.
                item["record"] = get_inquiry(item.get("inquiry_id")) or {}
            mine.update(got)
            if not got:
                st.info("가져올 문의가 없습니다.")
            else:
                st.rerun()
    else:
        st.button(
            "작업 가져오기",
            disabled=True,
            help=f"{CLAIM_BATCH}건을 먼저 완료하거나 반납하세요.",
        )

    if not mine:
        st.info("집어간 문의가 없습니다. 버튼을 눌러 가져오세요.")
        return

    for message_id, item in list(mine.items()):
        record = item.get("record") or {}
        with st.container(border=True):
            open_col, release_col = st.columns([5, 1])

            with open_col:
                _queue_row_button(workspace, team, message_id, item, record)

            with release_col:
                if st.button("반납", key=f"release_{workspace}_{message_id}"):
                    try:
                        release(team, item["receipt_handle"])
                    except Exception as e:
                        st.error(f"반납 실패: {e}")
                    else:
                        mine.pop(message_id, None)
                        st.session_state.pending_toast = "반납했습니다."
                        st.rerun()


def show_all_queue(workspace, team, depth):
    """대기 전량을 훑어본다. 물류팀 창구. 점유하지 않으므로 반납도 없다."""
    cached = st.session_state.queue_items.setdefault(workspace, {})

    if st.button("새로고침"):
        # 덮어쓰지 않고 병합한다. VisibilityTimeout 때문에 이번 조회에 안 잡힌
        # 메시지가 목록에서 사라지면 안 된다.
        waiting = depth[0] if depth else None
        try:
            cached.update(list_pending(team, target=waiting))
        except Exception as e:
            st.error(f"조회 실패: {e}")
            return
        st.rerun()

    items = list(cached.items())
    if depth and items and len(items) < depth[0]:
        st.warning(
            f"표시 {len(items)}건 / 대기 {depth[0]}건 — 다 못 긁었습니다. 새로고침을 한 번 더 누르세요."
        )
    if not items:
        st.info("조회된 문의가 없습니다. 새로고침을 눌러주세요.")
        return

    # 건건이 조회하면 그 수만큼 AssumeRole 왕복이 생긴다. 한 번 읽어 인덱스로 쓴다.
    records = {r.get("inquiry_id"): r for r in list_inquiries()}

    for message_id, item in items:
        record = records.get(item.get("inquiry_id")) or {}
        with st.container(border=True):
            _queue_row_button(workspace, team, message_id, item, record)


def _queue_row_button(workspace, team, message_id, item, record):
    """큐 목록의 한 줄. 제목은 큐가 아니라 DB 레코드에서 읽는다."""
    inquiry_id = item.get("inquiry_id")
    if not inquiry_id:
        st.write(f"(inquiry_id 없음) {message_id} — 상세 조회 불가")
        return

    if st.button(
        f"{inquiry_id}  {record.get('subject', '')}",
        key=f"sqs_{workspace}_{message_id}",
        use_container_width=True,
        type="tertiary",
    ):
        _open_detail(
            inquiry_id,
            source="sqs",
            team=team,
            receipt_handle=item["receipt_handle"],
            message_id=message_id,
            event_id=item.get("event_id", ""),
            workspace=workspace,
        )


def show_detail():
    inquiry_id = st.session_state.selected_id
    item = get_inquiry(inquiry_id)

    st.title("문의 상세")

    if st.button("← 목록으로"):
        st.session_state.view = "list"
        st.rerun()

    if not item:
        st.error("해당 문의를 찾을 수 없습니다.")
        return

    st.text_input("inquiry_id", value=item.get("inquiry_id", ""), disabled=True)
    st.text_input("event_id", value=item.get("event_id", ""), disabled=True)
    st.text_input("subject", value=item.get("subject", ""), disabled=True)
    st.text_area("message", value=item.get("message", ""), disabled=True)

    detail_state = item.get('state', STATES[0])
    st.write(f"상태: {STATE_LABELS.get(detail_state, detail_state)}")

    if st.session_state.detail_source == "sqs":
        # team 은 DB 에 기록하는 이름, workspace 는 화면에 보이는 창구 이름이다.
        team = st.session_state.detail_team
        workspace = st.session_state.detail_workspace or team
        receipt_handle = st.session_state.detail_receipt_handle
        message_id = st.session_state.detail_message_id
        # DB 값이 아니라 SQS 메시지에 실려온 event_id 를 쓴다. 대조가 목적이다.
        event_id = st.session_state.detail_event_id or ""
        st.subheader("SQS 처리")
        if event_id != item.get("event_id", ""):
            st.warning(
                f"event_id 불일치 — 큐={event_id or '없음'}, "
                f"DB={item.get('event_id') or '없음'}. 완료 처리가 거부됩니다."
            )
        if st.button(f"{workspace} 완료 처리", key=f"complete_{inquiry_id}_{workspace}"):
            confirm_complete_dialog(
                inquiry_id, event_id, team, receipt_handle, message_id, workspace
            )


if st.session_state.pending_toast:
    st.toast(st.session_state.pending_toast, icon="✅")
    st.session_state.pending_toast = None

if st.session_state.view == "detail":
    show_detail()
else:
    with st.sidebar:
        menu = option_menu("메뉴", MENU_OPTIONS, key="menu", styles=MENU_STYLES)
    if menu == "전체 문의":
        show_list()
    elif menu == "CSV 업로드":
        show_upload_page()
    else:
        show_sqs_page()
