"""日报成品 → 群内文字摘要。纯逻辑模块，不依赖 astrbot，便于本地测试。

解析 markdown_export 编辑版格式：
### 序号. 【领域】标题  ⭐ 分值
摘要段落
原文：[原始标题](URL) · 来源 · 时间

build_digest 支持 categories 过滤（领域订阅），无匹配时返回提示文案。
"""
from __future__ import annotations

import re

_HEAD_RE = re.compile(r"^###\s+\d+\.\s+(.*?)\s+⭐\s*([\d.]+)\s*$")
_CAT_RE = re.compile(r"^\s*【(.+?)】")
_URL_RE = re.compile(r"原文：\[.*?\]\((\S+?)\)")


def _parse_items(md_text: str) -> list[dict]:
    """返回 [{title(不含【领域】), category, score, url}]。"""
    items: list[dict] = []
    current: dict | None = None
    for line in md_text.splitlines():
        head = _HEAD_RE.match(line.strip())
        if head:
            if current:
                items.append(current)
            raw_title = head.group(1)
            cat_match = _CAT_RE.match(raw_title)
            current = {
                "title": _CAT_RE.sub("", raw_title).strip(),
                "category": cat_match.group(1) if cat_match else None,
                "score": head.group(2),
                "url": "",
            }
        elif current is not None:
            url_match = _URL_RE.search(line.strip())
            if url_match:
                current["url"] = url_match.group(1)
                items.append(current)
                current = None
    if current:
        items.append(current)
    return items


def build_digest(md_text: str, max_items: int = 10,
                 header: str | None = None,
                 categories: list[str] | None = None) -> str:
    """提取紧凑文字摘要。categories 为领域中文名列表（如 ["科技","军事"]），
    非空时只保留这些领域的条目；全部落空时返回友好提示。"""
    if categories:
        wanted = {c.strip() for c in categories if c.strip()}
        items = [it for it in _parse_items(md_text) if it["category"] in wanted]
        if not items:
            prefix = f"{header}\n\n" if header else ""
            return (prefix + f"你订阅的领域（{'、'.join(sorted(wanted))}）今天暂无入选内容，"
                    "完整版面见上方图片。")
    else:
        items = _parse_items(md_text)

    if not items:
        # 回退：解析不了结构时给前 800 字，保证有产出
        body = md_text.strip()[:800]
        return f"{header}\n\n{body}" if header else body

    lines = [header] if header else []
    for i, it in enumerate(items[:max_items], 1):
        cat = f"【{it['category']}】" if it["category"] else ""
        lines.append(f"【{i}】{cat}{it['title']}（⭐{it['score']}）")
        if it["url"]:
            lines.append(f"🔗 {it['url']}")
    total = len(items)
    if total > max_items:
        lines.append(f"…等共 {total} 条，完整版见图片卡片")
    return "\n".join(lines)
