"""AstrBot 插件：每日资讯日报（早报 + 晚报）。

职责：读取本机日报流水线同步到本服务器的成品文件（{date}-morning/evening.md 与 .png），
响应 /早报 /晚报 /日报 指令，并按订阅在每日固定时间推送（早报 08:30、晚报 18:30）。
"重新整理"（/日报刷新 或 群内说"重新整理日报/晚报"）会代跑服务器上的 run.py daily。

数据来源目录：插件数据目录（StarTools.get_data_dir）或配置 files_dir。
适配 AstrBot v4.x；API 依据 skill-astrbot-dev v4.26.8 文档。
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta
import re
from pathlib import Path

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star

try:
    from .digest import build_digest
except ImportError:  # 某些加载方式不作为包导入
    from digest import build_digest  # type: ignore

try:
    from .session_parse import parse_session_spec, session_kind, session_short_id
except ImportError:
    from session_parse import parse_session_spec, session_kind, session_short_id  # type: ignore

try:
    import yaml
except ImportError:  # AstrBot 环境自带 PyYAML；保险起见兜底
    yaml = None

try:
    from astrbot.api.star import StarTools

    _DATA_DIR = Path(StarTools.get_data_dir("astrbot_plugin_dailynews"))
except Exception:  # 旧版本没有 StarTools 时退回约定路径
    _DATA_DIR = Path("data") / "plugin_data" / "astrbot_plugin_dailynews"

PLUGIN_NAME = "astrbot_plugin_dailynews"
SUBS_FILE = "subscriptions.json"
FIRED_FILE = "fired.json"
SEEN_FILE = "seen_sessions.json"
EDITIONS = ("morning", "evening")
_EDITION_LABEL = {"morning": "早报", "evening": "晚报"}


def _load_tz(name: str):
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception:
        logger.warning(f"[{PLUGIN_NAME}] 时区 {name} 不可用，使用服务器本地时间")
        return None


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour, _, minute = str(value).strip().partition(":")
    return int(hour or 8), int(minute or 0)


class DailyNewsPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._task: asyncio.Task | None = None

    async def initialize(self):
        self.data_dir = (
            Path(str(self.config.get("files_dir") or "").strip())
            if str(self.config.get("files_dir") or "").strip()
            else _DATA_DIR
        )
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._subs_path = self.data_dir / SUBS_FILE
        self._fired_path = self.data_dir / FIRED_FILE
        self._sync_ai_override()
        self._sync_feeds_override()
        self._task = asyncio.create_task(self._push_loop())
        logger.info(f"[{PLUGIN_NAME}] v0.5 已加载，成品目录：{self.data_dir}")

    def _sync_ai_override(self):
        """把配置页的 AI 设置写入流水线 config/ai_override.yaml。

        流水线（含 cron 定时任务）读取该文件覆盖默认 AI 配置；
        配置页全部留空时删除覆盖文件，回落到流水线自带配置。
        """
        pipeline_dir = Path(str(self.config.get("pipeline_dir") or "").strip()
                            or "/home/xiajiao/news-pipeline")
        override_path = pipeline_dir / "config" / "ai_override.yaml"
        fields = {
            "ai_provider": self.config.get("ai_provider"),
            "ai_protocol": self.config.get("ai_protocol"),
            "ai_base_url": self.config.get("ai_base_url"),
            "ai_model": self.config.get("ai_model"),
            "ai_api_key": self.config.get("ai_api_key"),
            "ai_max_tokens": self.config.get("ai_max_tokens"),
            "ai_timeout_seconds": self.config.get("ai_timeout_seconds"),
        }
        payload = {k: str(v).strip() for k, v in fields.items()
                   if v is not None and str(v).strip() not in ("", "0")}
        try:
            override_path.parent.mkdir(parents=True, exist_ok=True)
            if payload:
                lines = ["# 由 AstrBot 插件配置页自动生成（勿手改，重载插件即刷新）"]
                lines += [f"{k}: \"{v}\"" for k, v in payload.items()]
                override_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                logger.info(f"[{PLUGIN_NAME}] AI 配置覆盖已写入: {override_path}（{len(payload)} 项）")
            elif override_path.exists():
                override_path.unlink()
                logger.info(f"[{PLUGIN_NAME}] AI 配置为空，已移除覆盖文件，回落流水线默认")
        except Exception as exc:
            logger.warning(f"[{PLUGIN_NAME}] AI 配置覆盖写入失败（不影响推送）：{exc}")

    def _sync_feeds_override(self):
        """把配置页的采集源清单写入流水线 config/feeds_override.yaml。

        rss 清单非空或 hackernews 被关闭时写覆盖文件；全部保持默认则移除覆盖文件。
        下一轮生成（cron 或 /日报刷新）按新清单采集。
        """
        pipeline_dir = Path(str(self.config.get("pipeline_dir") or "").strip()
                            or "/home/xiajiao/news-pipeline")
        override_path = pipeline_dir / "config" / "feeds_override.yaml"

        feeds = self.config.get("rss_feeds") or []
        if isinstance(feeds, str):
            feeds = [s.strip() for s in feeds.replace("，", ",").split(",") if s.strip()]
        feeds = [str(s).strip() for s in feeds if str(s).strip()]
        hn_enabled = bool(self.config.get("enable_hackernews", True))
        hn_hits = int(self.config.get("hackernews_hits", 30) or 30)

        all_default = (not feeds) and hn_enabled and hn_hits == 30
        try:
            override_path.parent.mkdir(parents=True, exist_ok=True)
            if all_default:
                if override_path.exists():
                    override_path.unlink()
                    logger.info(f"[{PLUGIN_NAME}] 采集源为默认，已移除覆盖文件")
                return
            if yaml is None:
                logger.warning(f"[{PLUGIN_NAME}] 无 yaml 库，采集源覆盖未写入")
                return
            payload = {"rss": feeds, "hackernews": {"enabled": hn_enabled,
                                                    "hits_per_page": hn_hits}}
            override_path.write_text(
                "# 由 AstrBot 插件配置页自动生成（勿手改，重载插件即刷新）\n"
                + yaml.dump(payload, allow_unicode=True, sort_keys=False),
                encoding="utf-8")
            logger.info(f"[{PLUGIN_NAME}] 采集源覆盖已写入: {override_path}"
                        f"（rss {len(feeds)} 条，HN {'开' if hn_enabled else '关'}）")
        except Exception as exc:
            logger.warning(f"[{PLUGIN_NAME}] 采集源覆盖写入失败（不影响推送）：{exc}")

    async def terminate(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ---------- 文件与配置 ----------

    def _now(self) -> datetime:
        tz = _load_tz(str(self.config.get("timezone") or "Asia/Shanghai"))
        return datetime.now(tz) if tz else datetime.now()

    def _edition_paths(self, edition: str, now: datetime | None = None) -> tuple[Path, Path]:
        """版次化成品路径。早报兼容旧的无后缀文件。"""
        now = now or self._now()
        md = self.data_dir / f"{now:%Y-%m-%d}-{edition}.md"
        png = self.data_dir / f"{now:%Y-%m-%d}-{edition}.png"
        if edition == "morning" and not md.exists():
            legacy_md = self.data_dir / f"{now:%Y-%m-%d}.md"
            legacy_png = self.data_dir / f"{now:%Y-%m-%d}.png"
            if legacy_md.exists():
                md, png = legacy_md, legacy_png
        return md, png

    def _latest_edition(self) -> str:
        """当前该展示哪个版次：晚报文件已生成则晚报，否则早报。"""
        md_e, _ = self._edition_paths("evening")
        if md_e.exists():
            return "evening"
        return "morning"

    def _edition_by_time(self) -> str:
        return "evening" if self._now().hour >= 14 else "morning"

    def _build_chain_parts(self, edition: str,
                           filter_labels: list[str] | None = None) -> tuple[list, str]:
        """返回 (组件列表, 状态说明)。组件列表为空时说明不可用。

        filter_labels 非空时，文字摘要只保留订阅领域（图片仍为完整版面）。
        """
        md_path, png_path = self._edition_paths(edition)
        label = _EDITION_LABEL[edition]
        if not md_path.exists() and not png_path.exists():
            return [], f"今天（{md_path.stem}）的{label}还没有同步到服务器。"
        parts: list = []
        if self.config.get("send_image", True) and png_path.exists():
            parts.append(Comp.Image.fromFileSystem(str(png_path)))
        if self.config.get("send_text", True) and md_path.exists():
            if filter_labels:
                header = (f"📢 每日资讯{label} {md_path.stem[:10]}"
                          f"（已按订阅过滤：{'、'.join(filter_labels)}）")
            else:
                header = f"📢 每日资讯{label} {md_path.stem[:10]}"
            digest = build_digest(
                md_path.read_text(encoding="utf-8"),
                max_items=int(self.config.get("digest_max_items", 10)),
                header=header,
                categories=filter_labels or None,
            )
            parts.append(Comp.Plain(digest))
        if not parts:
            return [], f"{label}文件存在但按当前配置没有可发送的内容（检查 send_image/send_text）。"
        return parts, "ok"

    # ---------- 订阅与推送状态 ----------

    def _available_categories(self) -> list[str]:
        raw = str(self.config.get("custom_categories") or
                  "人工智能,军事,外交,经济,科技,国际,文化,社会")
        return [c.strip() for c in raw.replace("，", ",").split(",") if c.strip()]

    def _config_sessions(self) -> dict[str, list[str]]:
        """配置页订阅的会话（subscribed_sessions），全部领域。"""
        specs = self.config.get("subscribed_sessions") or []
        if isinstance(specs, str):
            specs = [s for s in specs.replace("，", ",").split(",")]
        platform = str(self.config.get("default_platform") or "aiocqhttp").strip()
        result: dict[str, list[str]] = {}
        for spec in specs:
            umo = parse_session_spec(str(spec), platform)
            if umo:
                result[umo] = []
        return result

    def _file_filters(self) -> dict[str, list[str]]:
        """指令订阅的会话（subscriptions.json）。兼容旧版 {"umos": [...]}。"""
        if not self._subs_path.exists():
            return {}
        try:
            data = json.loads(self._subs_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if "filters" in data:
            return {u: list(v) for u, v in data["filters"].items()}
        return {u: [] for u in data.get("umos", [])}

    def _save_file_filters(self, filters: dict[str, list[str]]) -> None:
        self._subs_path.write_text(
            json.dumps({"filters": filters, "updated_at": int(time.time())},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_filters(self) -> dict[str, list[str]]:
        """合并来源：配置页订阅 + 指令订阅（同会话时指令侧的领域设置优先）。"""
        merged = self._config_sessions()
        merged.update(self._file_filters())
        return merged

    def _load_subs(self) -> list[str]:
        return list(self._load_filters().keys())

    def _save_subs(self, umos: list[str]) -> None:
        filters = self._file_filters()
        for umo in umos:
            filters.setdefault(umo, [])
        self._save_file_filters(filters)

    def _remember_seen(self, event: AstrMessageEvent) -> None:
        """记录 bot 见过的会话，供 /日报会话 展示与配置页复制 UMO。"""
        try:
            umo = event.unified_msg_origin
            seen_path = self.data_dir / SEEN_FILE
            data = {}
            if seen_path.exists():
                data = json.loads(seen_path.read_text(encoding="utf-8"))
            try:
                _, mtype, _ = umo.split(":", 2)
            except ValueError:
                mtype = "unknown"
            data[umo] = {"mtype": mtype, "last_seen": int(time.time())}
            data = dict(sorted(data.items(), key=lambda kv: -kv[1]["last_seen"])[:30])
            seen_path.write_text(json.dumps(data, ensure_ascii=False, indent=1),
                                 encoding="utf-8")
        except Exception:
            pass  # 记录失败不影响主流程

    def _load_fired(self) -> dict:
        if not self._fired_path.exists():
            return {}
        try:
            return json.loads(self._fired_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _mark_fired(self, date_key: str, edition: str) -> None:
        data = self._load_fired()
        data.setdefault(date_key, {})[edition] = int(time.time())
        # 只保留最近 3 天
        keep = sorted(data.keys())[-3:]
        data = {k: data[k] for k in keep}
        self._fired_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    # ---------- 指令 ----------

    async def _reply_edition(self, event: AstrMessageEvent, edition: str):
        self._remember_seen(event)
        filters = self._load_filters()
        parts, status = self._build_chain_parts(edition, filters.get(event.unified_msg_origin))
        if not parts:
            yield event.plain_result(status)
            return
        yield event.chain_result(parts)

    @filter.command("日报")
    async def cmd_digest(self, event: AstrMessageEvent):
        """查看最新一版日报（晚报生成后为晚报，否则早报）"""
        async for result in self._reply_edition(event, self._latest_edition()):
            yield result

    @filter.command("早报")
    async def cmd_morning(self, event: AstrMessageEvent):
        """查看今天的早报"""
        async for result in self._reply_edition(event, "morning"):
            yield result

    @filter.command("晚报")
    async def cmd_evening(self, event: AstrMessageEvent):
        """查看今天的晚报（暗色图片）"""
        async for result in self._reply_edition(event, "evening"):
            yield result

    @filter.command("日报订阅")
    async def cmd_subscribe(self, event: AstrMessageEvent):
        """订阅当前会话（选定这个群/私聊），每天定时推送"""
        self._remember_seen(event)
        umo = event.unified_msg_origin
        if umo in self._config_sessions():
            yield event.plain_result("该会话已在配置页的订阅清单里，无需重复订阅。")
            return
        file_filters = self._file_filters()
        if umo in file_filters:
            yield event.plain_result("本会话已订阅日报，无需重复订阅。")
            return
        file_filters[umo] = []
        self._save_file_filters(file_filters)
        mh, mm = _parse_hhmm(str(self.config.get("morning_push_time") or "08:30"))
        eh, em = _parse_hhmm(str(self.config.get("evening_push_time") or "18:30"))
        yield event.plain_result(
            f"订阅成功！本会话已选定，每天 {mh:02d}:{mm:02d} 推送早报、{eh:02d}:{em:02d} 推送晚报。"
            "可用 /订阅领域 只收指定领域，/日报退订 取消。"
        )

    @filter.command("日报退订")
    async def cmd_unsubscribe(self, event: AstrMessageEvent):
        """退订当前会话的定时日报推送"""
        self._remember_seen(event)
        umo = event.unified_msg_origin
        if umo in self._config_sessions():
            yield event.plain_result(
                "该会话是配置页订阅的：请在插件配置的 subscribed_sessions 里移除。")
            return
        file_filters = self._file_filters()
        if umo not in file_filters:
            yield event.plain_result("本会话没有订阅日报。")
            return
        del file_filters[umo]
        self._save_file_filters(file_filters)
        yield event.plain_result("已退订日报推送。")

    @filter.command("订阅领域")
    async def cmd_subscribe_categories(self, event: AstrMessageEvent):
        """设置本会话的领域订阅：/订阅领域 科技,军事；/订阅领域 全部 取消过滤"""
        arg = (event.message_str or "").replace("订阅领域", "").strip()
        available = self._available_categories()
        umo = event.unified_msg_origin
        if not arg:
            current = self._load_filters().get(umo, [])
            current_txt = "、".join(current) if current else "全部领域"
            yield event.plain_result(
                "可订阅领域：" + "、".join(available) + "\n"
                f"当前订阅：{current_txt}\n"
                "设置示例：/订阅领域 科技,军事（多领域用逗号分隔）\n"
                "取消过滤：/订阅领域 全部\n"
                "提示：领域内容量取决于信息源，军事/外交等暂缺稳定国内源，条目会偏少。")
            return
        if arg in ("全部", "所有", "all"):
            file_filters = self._file_filters()
            file_filters[umo] = []
            self._save_file_filters(file_filters)
            yield event.plain_result("已取消领域过滤，之后推送完整版面。")
            return
        wanted = [c.strip() for c in arg.replace("，", ",").split(",") if c.strip()]
        invalid = [c for c in wanted if c not in available]
        if invalid:
            yield event.plain_result(f"未知领域：{'、'.join(invalid)}\n可选：" + "、".join(available))
            return
        file_filters = self._file_filters()
        file_filters.setdefault(umo, [])
        file_filters[umo] = wanted
        self._save_file_filters(file_filters)
        yield event.plain_result(
            f"订阅成功！早报/晚报的文字摘要将只包含：{'、'.join(wanted)}。"
            "图片仍为完整版面；想看全部时发 /订阅领域 全部。")

    @filter.command("领域日报")
    async def cmd_category_digest(self, event: AstrMessageEvent):
        """按领域查看今日要闻（纯文字）：/领域日报 科技 或 /领域日报 AI"""
        arg = (event.message_str or "").replace("领域日报", "").strip()
        if not arg:
            yield event.plain_result("用法：/领域日报 领域名（如 /领域日报 科技）\n可选："
                                     + "、".join(self._available_categories()))
            return
        labels = {c for c in self._available_categories()}
        wanted = [c for c in arg.replace("，", ",").split(",") if c.strip()]
        bad = [c for c in wanted if c not in labels]
        if bad:
            yield event.plain_result(f"未知领域：{'、'.join(bad)}\n可选：" + "、".join(labels))
            return
        edition = self._latest_edition()
        md_path, _ = self._edition_paths(edition)
        if not md_path.exists():
            yield event.plain_result("今天的日报还没有同步到服务器。")
            return
        digest = build_digest(
            md_path.read_text(encoding="utf-8"),
            max_items=int(self.config.get("digest_max_items", 10)),
            header=f"📢 {arg}要闻（{_EDITION_LABEL[edition]}）",
            categories=wanted,
        )
        yield event.plain_result(digest)

    @filter.command("日报会话")
    async def cmd_list_sessions(self, event: AstrMessageEvent):
        """查看已订阅（选定）的会话清单，含配置页订阅与见过的会话"""
        self._remember_seen(event)
        config_sessions = self._config_sessions()
        file_filters = self._file_filters()
        seen = {}
        seen_path = self.data_dir / SEEN_FILE
        if seen_path.exists():
            try:
                seen = json.loads(seen_path.read_text(encoding="utf-8"))
            except Exception:
                seen = {}

        lines = ["== 配置页订阅（subscribed_sessions）=="]
        if config_sessions:
            for umo in config_sessions:
                lines.append(f"- [{session_kind(umo)}] {session_short_id(umo)}")
        else:
            lines.append("-（空）")

        lines += ["", "== 指令订阅 =="]
        if file_filters:
            for umo, cats in file_filters.items():
                cat_txt = "、".join(cats) if cats else "全部领域"
                lines.append(f"- [{session_kind(umo)}] {session_short_id(umo)}（{cat_txt}）")
        else:
            lines.append("-（空）")

        others = [u for u in seen if u not in config_sessions and u not in file_filters]
        if others:
            lines += ["", "== bot 见过但未订阅（可把 UMO 复制进配置页）=="]
            for umo in others[:10]:
                lines.append(f"- {session_kind(umo)}：`{umo}`")
        yield event.plain_result("\n".join(lines))

    @filter.command("日报推送")
    async def cmd_push_now(self, event: AstrMessageEvent):
        """立即向所有已订阅会话推送当前版次（补发/测试用）"""
        self._remember_seen(event)
        edition = "evening" if "晚" in (event.message_str or "") else \
            ("morning" if "早" in (event.message_str or "") else self._edition_by_time())
        subs = self._load_subs()
        if not subs:
            yield event.plain_result(
                "还没有任何会话订阅。\n在目标群或私聊里发 /日报订阅 选定会话后，再发 /日报推送。")
            return
        label = _EDITION_LABEL[edition]
        yield event.plain_result(f"开始向 {len(subs)} 个订阅会话推送{label}…")
        await self._push_all(edition)
        self._mark_fired(f"{self._now():%Y-%m-%d}", edition)
        yield event.plain_result(
            f"{label}推送指令执行完毕，发送 /日报状态 可核对。")

    @filter.command("日报状态")
    async def cmd_status(self, event: AstrMessageEvent):
        """查看日报插件状态（订阅数、各版次文件、推送时间）"""
        subs = self._load_subs()
        mh, mm = _parse_hhmm(str(self.config.get("morning_push_time") or "08:30"))
        eh, em = _parse_hhmm(str(self.config.get("evening_push_time") or "18:30"))
        my_filter = self._load_filters().get(event.unified_msg_origin, [])
        filter_txt = "、".join(my_filter) if my_filter else "全部领域"
        lines = [f"日报插件状态（订阅 {len(subs)} 个会话）：",
                 f"- 本会话领域订阅：{filter_txt}"]
        for edition in EDITIONS:
            md_path, png_path = self._edition_paths(edition)
            lines.append(
                f"- {_EDITION_LABEL[edition]}："
                f"文字{'已同步' if md_path.exists() else '未同步'} · "
                f"图片{'已同步' if png_path.exists() else '未同步'}"
            )
        lines.append(f"- 推送时间：早报 {mh:02d}:{mm:02d} / 晚报 {eh:02d}:{em:02d}（{self.config.get('timezone')}）")
        lines.append(f"- 成品目录：{self.data_dir}")
        yield event.plain_result("\n".join(lines))

    # ---------- 重新整理（触发服务器上的流水线） ----------

    async def _regen_daily(self, edition: str) -> tuple[bool, str]:
        """代跑一次 run.py daily --edition（采集+AI 分析+出图+落位）。含并发与冷却守卫。"""
        now = time.time()
        if getattr(self, "_regen_busy", False):
            return False, "上一次重新整理还在进行中，请稍后再试。"
        cooldown_min = int(self.config.get("regen_cooldown_minutes", 10))
        remain = int(cooldown_min * 60 - (now - getattr(self, "_last_regen", 0.0)))
        if remain > 0:
            return False, f"冷却中：约 {remain} 分钟后可再次重新整理（防止重复消耗 AI 额度）。"
        self._regen_busy = True
        self._last_regen = now
        try:
            pdir = str(self.config.get("pipeline_dir") or "/home/xiajiao/news-pipeline").rstrip("/")
            try:
                proc = await asyncio.create_subprocess_exec(
                    f"{pdir}/.venv/bin/python", f"{pdir}/run.py", "daily",
                    "--trigger", "manual", "--edition", edition,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    cwd=pdir,
                )
            except FileNotFoundError:
                return False, f"流水线不存在：{pdir}（检查插件配置 pipeline_dir）"
            try:
                out, _ = await asyncio.wait_for(proc.communicate(), timeout=900)
            except asyncio.TimeoutError:
                proc.kill()
                return False, "重新整理超时（15 分钟），已终止。请到服务器查看 ~/news-pipeline/logs/。"
            tail = out.decode("utf-8", "replace").strip().splitlines()[-1] if out else ""
            if proc.returncode == 0:
                return True, tail
            return False, f"重新整理失败：{tail[:200]}"
        finally:
            self._regen_busy = False

    async def _regen_flow(self, event: AstrMessageEvent, edition: str):
        if not self.config.get("regen_enabled", True):
            yield event.plain_result("管理员已关闭重新整理功能（regen_enabled=false）。")
            return
        label = _EDITION_LABEL[edition]
        yield event.plain_result(
            f"收到，开始重新生成今天的{label}（采集 + AI 分析 + 出图），大约需要 1~3 分钟…")
        ok, msg = await self._regen_daily(edition)
        if not ok:
            yield event.plain_result(f"✗ {msg}")
            return
        parts, status = self._build_chain_parts(edition)
        if not parts:
            yield event.plain_result(f"{label}重新生成完成，但读取成品失败：{status}")
            return
        yield event.chain_result(parts)

    @filter.command("日报刷新")
    async def cmd_digest_regen(self, event: AstrMessageEvent):
        """重新生成日报：14 点前刷新早报，之后刷新晚报；/日报刷新晚报 指定版次"""
        arg = (event.message_str or "").strip()
        edition = "evening" if "晚" in arg else ("morning" if "早" in arg else self._edition_by_time())
        async for result in self._regen_flow(event, edition):
            yield result

    # ---------- 自然语言触发 ----------

    @filter.regex(r"日报|早报|晚报")
    async def cmd_digest_nlp(self, event: AstrMessageEvent):
        """自然语言触发（收紧版）：仅当消息是"日报/早报/晚报"单独一词，
        或带有明确动作意图（整理/生成/来一份/看看/发一下等）时才响应；
        普通聊天里偶然提到"日报"不再触发。"""
        text = (event.message_str or "").strip()
        if len(text) > 30:
            return
        if text in {"日报订阅", "日报退订", "日报状态", "日报刷新", "订阅领域", "领域日报",
                    "日报推送", "日报会话"}:
            return
        bare = text in {"日报", "早报", "晚报"}
        intent = re.search(
            r"(整理|生成|刷新|推送|来一?份?|发一?下?|发个|看看|看下|看一下|查看|获取)", text)
        mentions = any(w in text for w in ("日报", "早报", "晚报"))
        if not mentions:
            return
        if not (bare or intent):
            return  # 无动作意图的普通聊天，不触发
        try:
            chain_text = "".join(
                getattr(comp, "text", "") or "" for comp in event.message_obj.message
            ).strip()
        except Exception:
            chain_text = text
        if chain_text.startswith(("/", "!", "！")):
            return
        regen = ("重新" in text or "刷新" in text) and self.config.get("regen_enabled", True)
        if "晚报" in text:
            edition = "evening"
        elif "早报" in text:
            edition = "morning"
        else:
            edition = self._edition_by_time() if regen else self._latest_edition()
        if regen:
            async for result in self._regen_flow(event, edition):
                yield result
            return
        async for result in self._reply_edition(event, edition):
            yield result

    # ---------- 定时推送（早报 + 晚报） ----------

    def _push_schedule(self) -> list[tuple[str, tuple[int, int]]]:
        schedule = [("morning", _parse_hhmm(str(self.config.get("morning_push_time") or "08:30")))]
        if self.config.get("evening_enabled", True):
            schedule.append(("evening", _parse_hhmm(str(self.config.get("evening_push_time") or "18:30"))))
        return schedule

    async def _push_loop(self):
        while True:
            try:
                await asyncio.sleep(30)
                await self._check_scheduled()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"[{PLUGIN_NAME}] 推送循环异常：{exc}")
                await asyncio.sleep(30)

    async def _check_scheduled(self):
        now = self._now()
        date_key = f"{now:%Y-%m-%d}"
        fired = self._load_fired().get(date_key, {})
        for edition, (hh, mm) in self._push_schedule():
            if edition in fired:
                continue
            target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            late_min = (now - target).total_seconds() / 60
            # 到点后 2 小时内补发；错过太久（如宕机重启）不再打扰
            if 0 <= late_min < 120:
                await self._push_all(edition)
                self._mark_fired(date_key, edition)

    async def _push_all(self, edition: str):
        filters = self._load_filters()
        if not filters:
            logger.info(f"[{PLUGIN_NAME}] 无订阅会话，跳过{_EDITION_LABEL[edition]}推送")
            return
        ok = failed = 0
        for umo, filter_labels in filters.items():
            parts, status = self._build_chain_parts(edition, filter_labels or None)
            if not parts:
                logger.warning(f"[{PLUGIN_NAME}] {_EDITION_LABEL[edition]}推送中止 {umo}：{status}")
                continue
            try:
                chain = MessageChain()
                for part in parts:
                    if isinstance(part, Comp.Plain):
                        chain = chain.message(part.text)
                    else:
                        chain = chain.file_image(getattr(part, "path", None) or part.file)
                await self.context.send_message(umo, chain)
                ok += 1
            except Exception as exc:
                failed += 1
                logger.error(f"[{PLUGIN_NAME}] {_EDITION_LABEL[edition]}推送失败 {umo}：{exc}")
        logger.info(f"[{PLUGIN_NAME}] {_EDITION_LABEL[edition]}定时推送完成：成功 {ok}，失败 {failed}")
