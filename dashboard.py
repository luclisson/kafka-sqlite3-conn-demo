"""Streamlit-Frontend: Live-Ansicht der Kafka-Topics.

Start:
    .venv/bin/streamlit run dashboard.py
"""

import json
import uuid
from datetime import datetime

import streamlit as st
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
MAX_MESSAGES = 20
TOPICS = {
    "sqlite.users": "CDC — Tabelle users (Debezium)",
    "spring-topic": "Spring-Boot-App (DemoMessage)",
}

OP_LABELS = {
    "c": ("c — INSERT", "green"),
    "u": ("u — UPDATE", "orange"),
    "d": ("d — DELETE", "red"),
    "r": ("r — SNAPSHOT", "blue"),
}


def unwrap(value: bytes) -> dict | None:
    """JSON-Bytes parsen; Debezium-Envelope (schema/payload) auspacken."""
    if not value:
        return None
    data = json.loads(value)
    if isinstance(data, dict) and "payload" in data and "schema" in data:
        return data["payload"]
    return data


def make_consumer(topic: str) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"streamlit-ui-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    return consumer


def init_state() -> None:
    if "buffers" not in st.session_state:
        st.session_state.buffers = {topic: [] for topic in TOPICS}
    if "consumers" not in st.session_state:
        st.session_state.consumers = {topic: make_consumer(topic) for topic in TOPICS}
    if "errors" not in st.session_state:
        st.session_state.errors = {topic: None for topic in TOPICS}


def poll(topic: str) -> None:
    """Non-blocking Nachrichten ziehen und in den Buffer legen."""
    consumer = st.session_state.consumers[topic]
    try:
        while True:
            msg = consumer.poll(0.05)
            if msg is None:
                st.session_state.errors[topic] = None
                return
            if msg.error():
                st.session_state.errors[topic] = str(msg.error())
                continue
            st.session_state.errors[topic] = None
            value = unwrap(msg.value())
            if value is None:  # Tombstone überspringen
                continue
            entry = {
                "key": unwrap(msg.key()),
                "payload": value,
                "ts": datetime.now().strftime("%H:%M:%S"),
                "offset": msg.offset(),
            }
            st.session_state.buffers[topic].insert(0, entry)
            del st.session_state.buffers[topic][MAX_MESSAGES:]
    except KafkaException as exc:
        st.session_state.errors[topic] = str(exc)


def render_cdc_message(entry: dict) -> None:
    payload = entry["payload"]
    op = payload.get("op")
    row = payload.get("after") or payload.get("before") or {}
    label, color = OP_LABELS.get(op, (op or "?", "gray"))
    st.badge(label, color=color)
    source = payload.get("source", {})
    line = f"**id {row.get('id', entry['key'])}**"
    if "name" in row:
        line += f" · {row['name']} · {row.get('email', '')}"
    st.markdown(line)
    meta = f"`ts {entry['ts']}` · `offset {entry['offset']}` · `change_id {source.get('change_id', '—')}`"
    st.caption(meta)
    with st.expander("raw"):
        st.json(entry)


def render_plain_message(entry: dict) -> None:
    payload = entry["payload"]
    text = payload.get("text", json.dumps(payload, ensure_ascii=False))
    st.markdown(f"**{text}**")
    st.caption(f"`ts {entry['ts']}` · `offset {entry['offset']}` · `createdAt {payload.get('createdAt', '—')}`")
    with st.expander("raw"):
        st.json(entry)


def render_box(column, topic: str, description: str) -> None:
    with column:
        buffer = st.session_state.buffers[topic]
        st.subheader(topic)
        st.caption(f"{description} · {len(buffer)} Nachrichten (nur Live, keine Historie)")
        if st.session_state.errors[topic]:
            st.warning(f"Broker/Consumer: {st.session_state.errors[topic]}")
        if not buffer:
            st.info("Noch keine Nachrichten empfangen …")
        for entry in buffer:
            with st.container(border=True):
                if topic == "sqlite.users":
                    render_cdc_message(entry)
                else:
                    render_plain_message(entry)


init_state()

st.set_page_config(page_title="Kafka Topic Streams", layout="wide")
st.title("Kafka Topic Streams")
st.caption(f"Broker: {BOOTSTRAP_SERVERS} · max. {MAX_MESSAGES} Nachrichten pro Box")

with st.sidebar:
    if st.button("🗑️ Alle Boxen leeren"):
        st.session_state.buffers = {topic: [] for topic in TOPICS}


@st.fragment(run_every=1)
def streams() -> None:
    poll("sqlite.users")
    poll("spring-topic")
    left, right = st.columns(2)
    render_box(left, "sqlite.users", TOPICS["sqlite.users"])
    render_box(right, "spring-topic", TOPICS["spring-topic"])


streams()
