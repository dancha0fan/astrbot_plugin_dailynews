"""会话订阅描述解析（纯逻辑，不依赖 astrbot，便于本地测试）。

配置页里的每条订阅可以是：
- 完整 UMO：aiocqhttp:group:123456
- 类型:ID：group:123456 / private:888888
- 裸 ID：123456（默认按群聊处理）
"""
from __future__ import annotations


def parse_session_spec(spec: str, default_platform: str = "aiocqhttp") -> str | None:
    spec = (spec or "").strip()
    if not spec:
        return None
    if spec.count(":") >= 2:
        return spec  # 已是完整 UMO（platform:type:id）
    if ":" in spec:
        mtype, _, sid = spec.partition(":")
        mtype = mtype.strip().lower()
        sid = sid.strip()
        if mtype in ("group", "private") and sid:
            return f"{default_platform}:{mtype}:{sid}"
        return None
    return f"{default_platform}:group:{spec}"


def session_kind(umo: str) -> str:
    try:
        _, mtype, _ = umo.split(":", 2)
        return "群聊" if mtype == "group" else "私聊"
    except ValueError:
        return "会话"


def session_short_id(umo: str) -> str:
    parts = umo.split(":", 2)
    return parts[2] if len(parts) == 3 else umo
