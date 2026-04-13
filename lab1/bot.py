import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DATE_TIME_FMT = "%d.%m.%Y %H:%M"
DATE_FMT = "%d.%m"
SHORT_DATE_FMT = "%d.%m.%y"
TASK_LOG_RETENTION_DAYS = 14
SHOPPING_LOG_RETENTION_DAYS = 14
CLEANING_LOG_RETENTION_DAYS = 14
EVENTS_LOG_RETENTION_DAYS = 14
SHOPPING_CATEGORY_ORDER = ["продукты", "бытовое", "другое"]
CLEANING_ZONES = ["кухня", "ванная", "спальня", "прихожая"]
FIXED_CLEANING_CHECKLIST = [
    "Вытереть пыль",
    "Пропылесосить",
    "Помыть пол",
    "Разложить вещи по местам",
]
FIXED_CLEANING_ORDER = {
    text.casefold(): index for index, text in enumerate(FIXED_CLEANING_CHECKLIST)
}
MONTH_NAMES = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}
SEASON_MONTHS = {
    "winter": {"label": "❄️ Зима", "months": [12, 1, 2]},
    "spring": {"label": "🌸 Весна", "months": [3, 4, 5]},
    "summer": {"label": "☀️ Лето", "months": [6, 7, 8]},
    "autumn": {"label": "🍁 Осень", "months": [9, 10, 11]},
}
SEASON_ORDER = ["winter", "spring", "summer", "autumn"]
MONTH_TO_SEASON = {
    month: season_key
    for season_key, meta in SEASON_MONTHS.items()
    for month in meta["months"]
}

LOG_CATEGORY_META = {
    "tasks": {"label": "Дела", "storage_key": "task_log"},
    "shopping": {"label": "Покупки", "storage_key": "shopping_log"},
    "cleaning": {"label": "Уборка", "storage_key": "cleaning_log"},
    "events": {"label": "События", "storage_key": "events_log"},
}

MAIN_MENU = [
    ["📝 Дела", "➕ Задача"],
    ["🛒 Покупки", "➕ Покупка"],
    ["🧹 Уборка", "➕ Уборка"],
    ["📅 События", "➕ Событие"],
    ["❤️ Помощь", "🏠 Меню"],
]


@dataclass
class Config:
    token: str
    allowed_user_ids: set[int]
    data_file: Path
    timezone: ZoneInfo


class DataStore:
    """Простое JSON-хранилище данных бота."""

    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, Any] = {}

    @staticmethod
    def _empty_shared() -> dict[str, Any]:
        return {
            "tasks": [],
            "task_log": [],
            "shopping": [],
            "shopping_log": [],
            "cleaning_log": [],
            "cleaning": [],
            "events_log": [],
            "events": [],
            "counters": {"task": 1, "item": 1, "clean": 1, "event": 1},
        }

    def _ensure_shared(self) -> None:
        defaults = self._empty_shared()
        if "shared" not in self.data or not isinstance(self.data.get("shared"), dict):
            self.data["shared"] = defaults
            return

        shared = self.data["shared"]
        for key, default_value in defaults.items():
            if key not in shared:
                if isinstance(default_value, dict):
                    shared[key] = dict(default_value)
                elif isinstance(default_value, list):
                    shared[key] = list(default_value)
                else:
                    shared[key] = default_value

        if not isinstance(shared.get("counters"), dict):
            shared["counters"] = dict(defaults["counters"])
        for counter_name, counter_default in defaults["counters"].items():
            if counter_name not in shared["counters"]:
                shared["counters"][counter_name] = counter_default

    def _migrate_done_tasks_to_log(self) -> None:
        shared = self.shared_data()
        active_tasks: list[dict[str, Any]] = []
        moved = False

        for task in shared.get("tasks", []):
            if task.get("status") == "done":
                completed_at = task.get("completed_at") or datetime.now().isoformat()
                log_entry = {
                    "text": task.get("text", "Без названия"),
                    "action": "done",
                    "logged_at": completed_at,
                    "completed_at": completed_at,
                }
                shared["task_log"].append(log_entry)
                moved = True
            else:
                active_tasks.append(task)

        if moved:
            shared["tasks"] = active_tasks
            if len(shared["task_log"]) > 200:
                shared["task_log"] = shared["task_log"][-200:]

    def _migrate_done_shopping_to_log(self) -> None:
        shared = self.shared_data()
        active_items: list[dict[str, Any]] = []
        moved = False

        for item in shared.get("shopping", []):
            if item.get("status") == "done":
                log_entry = {
                    "text": item.get("name", "Без названия"),
                    "name": item.get("name", "Без названия"),
                    "category": item.get("category", "другое"),
                    "action": "done",
                    "logged_at": datetime.now().isoformat(),
                }
                shared["shopping_log"].append(log_entry)
                moved = True
            else:
                active_items.append(item)

        if moved:
            shared["shopping"] = active_items
            if len(shared["shopping_log"]) > 500:
                shared["shopping_log"] = shared["shopping_log"][-500:]

    def _migrate_from_users_if_needed(self) -> None:
        """Поддержка старого формата: users -> user_id -> списки."""
        if "shared" in self.data:
            self._ensure_shared()
            return

        users = self.data.get("users")
        if not isinstance(users, dict) or not users:
            self._ensure_shared()
            return

        shared = self._empty_shared()

        for user_data in users.values():
            if not isinstance(user_data, dict):
                continue
            for task in user_data.get("tasks", []):
                if task.get("status") == "done":
                    log_entry = dict(task)
                    log_entry["completed_at"] = log_entry.get("completed_at") or datetime.now().isoformat()
                    shared["task_log"].append(log_entry)
                else:
                    shared["tasks"].append(task)
            shared["shopping"].extend(user_data.get("shopping", []))
            shared["cleaning"].extend(user_data.get("cleaning", []))
            shared["events"].extend(user_data.get("events", []))

        # Приводим id к общему счётчику без конфликтов.
        for key, counter_name in (
            ("tasks", "task"),
            ("shopping", "item"),
            ("cleaning", "clean"),
            ("events", "event"),
        ):
            next_id = 1
            for obj in shared[key]:
                obj["id"] = next_id
                next_id += 1
            shared["counters"][counter_name] = next_id

        self.data = {"shared": shared}

    def load(self) -> None:
        if not self.path.exists():
            self._ensure_shared()
            self.save()
            return
        try:
            with self.path.open("r", encoding="utf-8") as f:
                self.data = json.load(f)
            if not isinstance(self.data, dict):
                self.data = {}
            self._migrate_from_users_if_needed()
            self._migrate_done_tasks_to_log()
            self._migrate_done_shopping_to_log()
        except Exception as exc:
            logger.error("Не удалось загрузить JSON, будет создан новый файл: %s", exc)
            self.data = {}
            self._ensure_shared()
            self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def shared_data(self) -> dict[str, Any]:
        self._ensure_shared()
        return self.data["shared"]

    def next_id(self, counter_name: str) -> int:
        shared = self.shared_data()
        counters = shared.setdefault("counters", {})
        current = int(counters.get(counter_name, 1))
        counters[counter_name] = current + 1
        return current


# Глобальные объекты инициализируются в main()
CONFIG: Config
STORE: DataStore


def load_config() -> Config:
    load_dotenv()

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("BOT_TOKEN не задан в .env")

    allowed_raw = os.getenv("ALLOWED_USER_IDS", "").strip()
    if not allowed_raw:
        raise ValueError("ALLOWED_USER_IDS не задан. Укажи список id через запятую")
    allowed_user_ids: set[int] = set()
    for part in allowed_raw.split(","):
        part = part.strip()
        if not part:
            continue
        allowed_user_ids.add(int(part))

    data_file = Path(os.getenv("DATA_FILE", "data.json")).expanduser().resolve()
    tz_name = os.getenv("TIMEZONE", "Europe/Moscow")
    timezone = ZoneInfo(tz_name)

    return Config(
        token=token,
        allowed_user_ids=allowed_user_ids,
        data_file=data_file,
        timezone=timezone,
    )


def has_access(user_id: int) -> bool:
    return user_id in CONFIG.allowed_user_ids


def user_actor_label(user_id: int) -> str:
    return "B" if str(user_id).endswith("532") else "A"


def normalize_actor_label(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    if raw in {"A", "B"}:
        return raw
    return None


def actor_tag(value: Any) -> str:
    actor = normalize_actor_label(value)
    return f" [{actor}]" if actor else ""


async def broadcast_to_allowed_users(bot, text: str) -> None:
    for allowed_user_id in CONFIG.allowed_user_ids:
        try:
            await bot.send_message(chat_id=allowed_user_id, text=text)
        except Exception as exc:
            logger.warning("Не удалось отправить сообщение user_id=%s: %s", allowed_user_id, exc)


async def deny_access(update: Update) -> None:
    text = "У вас нет доступа к этому боту"
    if update.message:
        await update.message.reply_text(text)
    elif update.callback_query:
        await update.callback_query.answer(text, show_alert=True)


def state_set(context: ContextTypes.DEFAULT_TYPE, state: str | None, **payload: Any) -> None:
    context.user_data["state"] = state
    context.user_data["payload"] = payload


def state_get(context: ContextTypes.DEFAULT_TYPE) -> tuple[str | None, dict[str, Any]]:
    return context.user_data.get("state"), context.user_data.get("payload", {})


def clear_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
    context.user_data.pop("payload", None)


def parse_multiline_entries(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def set_last_section(context: ContextTypes.DEFAULT_TYPE, section: str) -> None:
    context.user_data["last_section"] = section


async def reply_text(update: Update, text: str, **kwargs: Any) -> None:
    if update.message:
        await update.message.reply_text(text, **kwargs)
        return
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text, **kwargs)


async def go_to_section_list(
    section: str | None, update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    if section == "tasks":
        await tasks(update, context)
        return True
    if section == "shopping":
        await shopping(update, context)
        return True
    if section == "cleaning":
        await cleaning(update, context)
        return True
    if section == "events":
        await events(update, context)
        return True
    return False


def parse_log_datetime(entry: dict[str, Any]) -> datetime | None:
    for field in ("logged_at", "completed_at"):
        raw = entry.get(field)
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(str(raw))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CONFIG.timezone)
        return dt
    return None


def get_log_storage_key(category: str) -> str:
    meta = LOG_CATEGORY_META.get(category)
    if not meta:
        return "task_log"
    return str(meta["storage_key"])


def get_week_range(now: datetime, weeks_back: int) -> tuple[datetime, datetime]:
    current_week_start_date = (now - timedelta(days=now.weekday())).date()
    start_date = current_week_start_date - timedelta(days=7 * weeks_back)
    start_dt = datetime.combine(start_date, time.min, tzinfo=CONFIG.timezone)
    end_dt = start_dt + timedelta(days=7)
    return start_dt, end_dt


def format_week_label(start_dt: datetime, end_dt: datetime) -> str:
    end_inclusive = end_dt - timedelta(days=1)
    return f"{start_dt.strftime('%d.%m.%Y')} — {end_inclusive.strftime('%d.%m.%Y')}"


def format_log_entry(category: str, entry: dict[str, Any]) -> str:
    action = str(entry.get("action", "info"))
    icon = {"done": "✅", "deleted": "🗑", "added": "➕"}.get(action, "•")
    title = entry.get("text") or entry.get("name") or "Без названия"
    suffix_parts: list[str] = []

    if category == "shopping":
        cat = entry.get("category")
        if cat:
            suffix_parts.append(str(cat).title())
    if category == "cleaning":
        zone = entry.get("zone")
        if zone:
            suffix_parts.append(f"зона: {zone}")
    if category == "events":
        event_date = entry.get("date")
        if event_date:
            suffix_parts.append(f"дата: {event_date}")
    actor = normalize_actor_label(entry.get("actor"))
    if actor:
        suffix_parts.append(f"пользователь: {actor}")

    logged_dt = parse_log_datetime(entry)
    if logged_dt:
        suffix_parts.append(logged_dt.astimezone(CONFIG.timezone).strftime("%d.%m %H:%M"))

    suffix = f" ({', '.join(suffix_parts)})" if suffix_parts else ""
    return f"{icon} {title}{suffix}"


def parse_event_date(text: str) -> tuple[int, int]:
    parsed = datetime.strptime(text.strip(), DATE_FMT)
    return parsed.day, parsed.month


def extract_event_day_month(raw_date: Any) -> tuple[int | None, int | None]:
    raw = str(raw_date or "").strip()
    if not raw:
        return None, None
    date_part = raw.split()[0]
    parts = date_part.split(".")
    if len(parts) < 2:
        return None, None
    try:
        day = int(parts[0])
        month = int(parts[1])
    except ValueError:
        return None, None
    if not (1 <= month <= 12):
        return None, None
    if not (1 <= day <= 31):
        day = None
    return day, month


def extract_event_month(raw_date: Any) -> int | None:
    _, month = extract_event_day_month(raw_date)
    return month


def events_for_month(shared_data: dict[str, Any], month: int) -> list[dict[str, Any]]:
    return [
        item
        for item in shared_data.get("events", [])
        if extract_event_month(item.get("date")) == month
    ]


def season_events_count(shared_data: dict[str, Any], season_key: str) -> int:
    meta = SEASON_MONTHS.get(season_key)
    if not meta:
        return 0
    months = set(meta["months"])
    return sum(1 for item in shared_data.get("events", []) if extract_event_month(item.get("date")) in months)


def parse_reminder_datetime(text: str) -> datetime:
    raw = text.strip()
    parts = raw.split()
    if len(parts) == 1:
        date_part = parts[0]
        hour = 9
        minute = 59
    elif len(parts) == 2:
        date_part, time_part = parts
        normalized_time = time_part.replace(":", ".")
        parsed_time = datetime.strptime(normalized_time, "%H.%M")
        hour = parsed_time.hour
        minute = parsed_time.minute
    else:
        raise ValueError("Неверный формат даты/времени")

    parsed_date = datetime.strptime(date_part, SHORT_DATE_FMT)
    return datetime(
        year=parsed_date.year,
        month=parsed_date.month,
        day=parsed_date.day,
        hour=hour,
        minute=minute,
        tzinfo=CONFIG.timezone,
    )


def cleanup_old_task_log(shared_data: dict[str, Any], now: datetime) -> int:
    cutoff = now - timedelta(days=TASK_LOG_RETENTION_DAYS)
    kept: list[dict[str, Any]] = []
    removed = 0

    for task in shared_data.get("task_log", []):
        completed_dt = parse_log_datetime(task)
        if not completed_dt:
            kept.append(task)
            continue

        if completed_dt < cutoff:
            removed += 1
        else:
            kept.append(task)

    if removed:
        shared_data["task_log"] = kept
    return removed


def add_shopping_log_entry(
    shared_data: dict[str, Any], item: dict[str, Any], action: str, actor: str | None = None
) -> None:
    shopping_log = shared_data.setdefault("shopping_log", [])
    actor_value = normalize_actor_label(actor) or normalize_actor_label(item.get("actor"))
    shopping_log.append(
        {
            "text": item.get("name", "Без названия"),
            "name": item.get("name", "Без названия"),
            "category": item.get("category", "другое"),
            "action": action,
            "actor": actor_value,
            "logged_at": datetime.now(CONFIG.timezone).isoformat(),
        }
    )
    if len(shopping_log) > 500:
        shared_data["shopping_log"] = shopping_log[-500:]


def add_task_log_entry(
    shared_data: dict[str, Any], task: dict[str, Any], action: str, actor: str | None = None
) -> None:
    task_log = shared_data.setdefault("task_log", [])
    actor_value = normalize_actor_label(actor) or normalize_actor_label(task.get("actor"))
    task_log.append(
        {
            "text": task.get("text", "Без названия"),
            "action": action,
            "actor": actor_value,
            "logged_at": datetime.now(CONFIG.timezone).isoformat(),
        }
    )
    if len(task_log) > 500:
        shared_data["task_log"] = task_log[-500:]


def add_cleaning_log_entry(
    shared_data: dict[str, Any], item: dict[str, Any], action: str, actor: str | None = None
) -> None:
    cleaning_log = shared_data.setdefault("cleaning_log", [])
    actor_value = normalize_actor_label(actor) or normalize_actor_label(item.get("actor"))
    cleaning_log.append(
        {
            "text": item.get("text", "Без названия"),
            "zone": item.get("zone", ""),
            "action": action,
            "actor": actor_value,
            "logged_at": datetime.now(CONFIG.timezone).isoformat(),
        }
    )
    if len(cleaning_log) > 500:
        shared_data["cleaning_log"] = cleaning_log[-500:]


def add_events_log_entry(
    shared_data: dict[str, Any], item: dict[str, Any], action: str, actor: str | None = None
) -> None:
    events_log = shared_data.setdefault("events_log", [])
    actor_value = normalize_actor_label(actor) or normalize_actor_label(item.get("actor"))
    events_log.append(
        {
            "text": item.get("name", "Без названия"),
            "date": item.get("date"),
            "action": action,
            "actor": actor_value,
            "logged_at": datetime.now(CONFIG.timezone).isoformat(),
        }
    )
    if len(events_log) > 500:
        shared_data["events_log"] = events_log[-500:]


def cleanup_old_shopping_log(shared_data: dict[str, Any], now: datetime) -> int:
    cutoff = now - timedelta(days=SHOPPING_LOG_RETENTION_DAYS)
    kept: list[dict[str, Any]] = []
    removed = 0

    for entry in shared_data.get("shopping_log", []):
        logged_at = entry.get("logged_at")
        if not logged_at:
            kept.append(entry)
            continue
        try:
            logged_dt = datetime.fromisoformat(logged_at)
            if logged_dt.tzinfo is None:
                logged_dt = logged_dt.replace(tzinfo=CONFIG.timezone)
        except ValueError:
            kept.append(entry)
            continue

        if logged_dt < cutoff:
            removed += 1
        else:
            kept.append(entry)

    if removed:
        shared_data["shopping_log"] = kept
    return removed


def cleanup_old_cleaning_log(shared_data: dict[str, Any], now: datetime) -> int:
    cutoff = now - timedelta(days=CLEANING_LOG_RETENTION_DAYS)
    kept: list[dict[str, Any]] = []
    removed = 0

    for entry in shared_data.get("cleaning_log", []):
        logged_dt = parse_log_datetime(entry)
        if not logged_dt:
            kept.append(entry)
            continue
        if logged_dt < cutoff:
            removed += 1
        else:
            kept.append(entry)

    if removed:
        shared_data["cleaning_log"] = kept
    return removed


def cleanup_old_events_log(shared_data: dict[str, Any], now: datetime) -> int:
    cutoff = now - timedelta(days=EVENTS_LOG_RETENTION_DAYS)
    kept: list[dict[str, Any]] = []
    removed = 0

    for entry in shared_data.get("events_log", []):
        logged_dt = parse_log_datetime(entry)
        if not logged_dt:
            kept.append(entry)
            continue
        if logged_dt < cutoff:
            removed += 1
        else:
            kept.append(entry)

    if removed:
        shared_data["events_log"] = kept
    return removed


def schedule_task_reminder(app: Application, task: dict[str, Any]) -> None:
    remind_at = task.get("remind_at")
    if not remind_at:
        return

    when = datetime.fromisoformat(remind_at)
    if when.tzinfo is None:
        when = when.replace(tzinfo=CONFIG.timezone)

    if when <= datetime.now(CONFIG.timezone) or task.get("status") == "done":
        return

    app.job_queue.run_once(
        callback=task_reminder_job,
        when=when,
        name=f"task:{task['id']}",
        data={"task_id": task["id"]},
    )


def schedule_cleaning_reminder(app: Application, item: dict[str, Any]) -> None:
    remind_at = item.get("remind_at")
    if not remind_at:
        return

    when = datetime.fromisoformat(remind_at)
    if when.tzinfo is None:
        when = when.replace(tzinfo=CONFIG.timezone)

    if when <= datetime.now(CONFIG.timezone) or item.get("status") == "done":
        return

    app.job_queue.run_once(
        callback=cleaning_reminder_job,
        when=when,
        name=f"clean:{item['id']}",
        data={"clean_id": item["id"]},
    )


def reschedule_all(app: Application) -> None:
    shared = STORE.shared_data()
    for task in shared.get("tasks", []):
        schedule_task_reminder(app, task)
    for clean in shared.get("cleaning", []):
        schedule_cleaning_reminder(app, clean)


def find_by_id(items: list[dict[str, Any]], item_id: int) -> dict[str, Any] | None:
    for item in items:
        if int(item.get("id", -1)) == item_id:
            return item
    return None


def normalize_cleaning_text(text: str) -> str:
    return " ".join(text.strip().casefold().split())


def is_fixed_cleaning_item(item: dict[str, Any]) -> bool:
    if item.get("kind") == "fixed":
        return True
    text = str(item.get("text", ""))
    return normalize_cleaning_text(text) in FIXED_CLEANING_ORDER


def cleaning_item_sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
    if is_fixed_cleaning_item(item):
        fixed_order = FIXED_CLEANING_ORDER.get(normalize_cleaning_text(str(item.get("text", ""))), 999)
        return (0, fixed_order, int(item.get("id", 0)))
    return (1, int(item.get("id", 0)), 0)


def ensure_fixed_cleaning_items_for_zone(
    shared_data: dict[str, Any], zone: str, actor: str | None = None
) -> int:
    items = shared_data.setdefault("cleaning", [])
    actor_value = normalize_actor_label(actor)
    zone_texts = {
        normalize_cleaning_text(str(item.get("text", "")))
        for item in items
        if str(item.get("zone", "")) == zone
    }
    added = 0
    for base_text in FIXED_CLEANING_CHECKLIST:
        normalized = normalize_cleaning_text(base_text)
        if normalized in zone_texts:
            continue
        items.append(
            {
                "id": STORE.next_id("clean"),
                "zone": zone,
                "text": base_text,
                "status": "todo",
                "remind_at": None,
                "kind": "fixed",
                "actor": actor_value,
            }
        )
        added += 1
        zone_texts.add(normalized)
    return added


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(MAIN_MENU, resize_keyboard=True)


def cleaning_zone_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(zone.title(), callback_data=f"clean_zone:{zone}")]
        for zone in CLEANING_ZONES
    ]
    rows.append([InlineKeyboardButton("↩️ Отмена", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(rows)


def cancel_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("↩️ Отмена", callback_data="flow_cancel")]]
    )


def menu_action(text: str) -> str | None:
    normalized = text.strip().lower()
    mapping = {
        "📝 дела": "tasks",
        "🗂 дела": "tasks",
        "➕ задача": "add_task",
        "🛒 покупки": "shopping",
        "➕ покупка": "add_item",
        "🧹 уборка": "cleaning",
        "➕ уборка": "add_cleaning",
        "📅 события": "events",
        "➕ событие": "add_event",
        "❤️ помощь": "help",
        "🏠 меню": "menu_panel",
        "отмена": "cancel",
    }
    return mapping.get(normalized)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    STORE.shared_data()
    STORE.save()

    await update.message.reply_text(
        "Привет! 👋 Я семейный ассистент.\n"
        "Помогаю с делами, покупками, уборкой и важными датами.\n\n"
        "Нажимай кнопки и добавляй дела :)",
        reply_markup=main_menu_keyboard(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    text = (
        "Можно пользоваться кнопками без `/`:\n"
        "📝 Дела, ➕ Задача\n"
        "🛒 Покупки, ➕ Покупка\n"
        "🧹 Уборка, ➕ Уборка\n"
        "📅 События, ➕ Событие\n\n"
        "Или классические команды:\n"
        "/start, /help, /tasks, /add_task, /shopping, /add_item,\n"
        "/cleaning, /add_cleaning, /events, /add_event, /cancel"
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def cat_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return
    await update.message.reply_text("Вы любимый кот!", reply_markup=main_menu_keyboard())


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    _, payload = state_get(context)
    section = payload.get("return_to") or context.user_data.get("last_section")
    clear_state(context)
    if await go_to_section_list(section, update, context):
        return
    await update.message.reply_text("Ой, не спешите 🙈", reply_markup=main_menu_keyboard())


async def flow_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return
    await query.answer()
    _, payload = state_get(context)
    section = payload.get("return_to") or context.user_data.get("last_section")
    clear_state(context)
    if await go_to_section_list(section, update, context):
        return
    await query.message.reply_text("Ой, не спешите 🙈", reply_markup=main_menu_keyboard())


async def section_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, section = query.data.split(":", 1)
    clear_state(context)
    if await go_to_section_list(section, update, context):
        return
    await query.message.reply_text("Раздел не найден.")


async def menu_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📚 Логи", callback_data="logs_menu")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="menu_cancel")],
        ]
    )
    await update.message.reply_text("Меню:", reply_markup=keyboard)


async def menu_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    section = context.user_data.get("last_section")
    if await go_to_section_list(section, update, context):
        return
    await query.message.reply_text("Возвращаемся 👌", reply_markup=main_menu_keyboard())


async def logs_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📝 Дела", callback_data="logs_cat:tasks")],
            [InlineKeyboardButton("🛒 Покупки", callback_data="logs_cat:shopping")],
            [InlineKeyboardButton("🧹 Уборка", callback_data="logs_cat:cleaning")],
            [InlineKeyboardButton("📅 События", callback_data="logs_cat:events")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="menu_cancel")],
        ]
    )
    await query.message.reply_text("Выберите категорию логов:", reply_markup=keyboard)


async def logs_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, category = query.data.split(":", 1)
    meta = LOG_CATEGORY_META.get(category)
    if not meta:
        await query.message.reply_text("Неизвестная категория логов.")
        return

    now = datetime.now(CONFIG.timezone)
    this_week_start, this_week_end = get_week_range(now, 0)
    prev_week_start, prev_week_end = get_week_range(now, 1)
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(format_week_label(this_week_start, this_week_end), callback_data=f"logs_week:{category}:0")],
            [InlineKeyboardButton(format_week_label(prev_week_start, prev_week_end), callback_data=f"logs_week:{category}:1")],
            [InlineKeyboardButton("↩️ Назад", callback_data="logs_menu")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="menu_cancel")],
        ]
    )
    await query.message.reply_text(
        f"Логи категории «{meta['label']}».\nВыберите неделю (Пн–Вс):",
        reply_markup=keyboard,
    )


async def logs_week_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, category, week_offset_str = query.data.split(":", 2)
    meta = LOG_CATEGORY_META.get(category)
    if not meta:
        await query.message.reply_text("Неизвестная категория логов.")
        return

    try:
        week_offset = int(week_offset_str)
    except ValueError:
        await query.message.reply_text("Не удалось распознать период логов.")
        return

    now = datetime.now(CONFIG.timezone)
    week_start, week_end = get_week_range(now, week_offset)
    shared_data = STORE.shared_data()
    logs = shared_data.get(get_log_storage_key(category), [])

    lines = [f"<b>📚 Логи: {meta['label']}</b>", f"<b>Неделя:</b> {format_week_label(week_start, week_end)}"]
    week_logs: list[dict[str, Any]] = []
    for entry in logs:
        logged_dt = parse_log_datetime(entry)
        if not logged_dt:
            continue
        if week_start <= logged_dt < week_end:
            week_logs.append(entry)

    if not week_logs:
        lines.append("— записей за этот период нет")
    else:
        for entry in sorted(week_logs, key=lambda x: parse_log_datetime(x) or week_start, reverse=True):
            lines.append(format_log_entry(category, entry))

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("↩️ К неделям", callback_data=f"logs_cat:{category}")],
            [InlineKeyboardButton("↩️ К категориям", callback_data="logs_menu")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="menu_cancel")],
        ]
    )
    await query.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


def format_task(task: dict[str, Any], number: int) -> str:
    priority_emoji = {
        "низкий": "🟢",
        "средний": "🟡",
        "высокий": "🔴",
    }.get(task.get("priority", "").lower(), "⚪")
    remind = task.get("remind_at")
    remind_txt = ""
    if remind:
        dt = datetime.fromisoformat(remind).astimezone(CONFIG.timezone)
        remind_txt = f" | ⏰ {dt.strftime(DATE_TIME_FMT)}"
    return f"{number}. {priority_emoji} {task['text']}{actor_tag(task.get('actor'))}{remind_txt}"


def format_task_log_item(task: dict[str, Any]) -> str:
    completed_at = task.get("completed_at")
    completed_txt = ""
    if completed_at:
        try:
            completed_dt = datetime.fromisoformat(completed_at).astimezone(CONFIG.timezone)
            completed_txt = f" ({completed_dt.strftime('%d.%m %H:%M')})"
        except ValueError:
            completed_txt = ""
    return f"✅ {task.get('text', 'Без названия')}{actor_tag(task.get('actor'))}{completed_txt}"


async def tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "tasks")
    shared_data = STORE.shared_data()
    task_list = shared_data.get("tasks", [])

    if not task_list:
        await reply_text(update, "Список дел пуст. Добавь задачу кнопкой «➕ Задача».")
        return

    lines = ["<b>📝 Дела</b>"]
    lines.append("\n<b>Активные:</b>")
    if task_list:
        for number, task in enumerate(task_list, start=1):
            lines.append(format_task(task, number))
    else:
        lines.append("— пока пусто")

    keyboard: list[list[InlineKeyboardButton]] = []
    for number, _task_item in enumerate(task_list, start=1):
        keyboard.append([
            InlineKeyboardButton(f"{number}", callback_data=f"task_pick:{number}")
        ])

    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def task_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, number_str = query.data.split(":", 1)
    try:
        number = int(number_str)
    except ValueError:
        await query.message.reply_text("Не удалось распознать номер дела.")
        return

    shared_data = STORE.shared_data()
    tasks_data = shared_data.get("tasks", [])
    if number < 1 or number > len(tasks_data):
        await query.message.reply_text("Дело с таким номером не найдено.")
        return

    item = tasks_data[number - 1]
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Готово", callback_data=f"task_action:done:{item['id']}")],
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"task_action:del:{item['id']}")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="back:tasks")],
        ]
    )
    await query.message.reply_text(
        f"Выбрано дело {number}: {item['text']}\nХотите завершить?",
        reply_markup=keyboard,
    )


async def add_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "tasks")
    state_set(context, "add_task_text", return_to="tasks")
    await update.message.reply_text(
        "✍️ Введите задачу (можно несколько, каждая с новой строки):",
        reply_markup=cancel_inline_keyboard(),
    )


async def add_task_priority_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, priority = query.data.split(":", 1)
    _, payload = state_get(context)
    payload["priority"] = priority
    state_set(context, "add_task_need_reminder", **payload)

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Да", callback_data="addtask_remind:yes")],
            [InlineKeyboardButton("❌ Нет", callback_data="addtask_remind:no")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="flow_cancel")],
        ]
    )
    await query.message.reply_text("Нужно ли напоминание?", reply_markup=keyboard)


async def add_task_reminder_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, choice = query.data.split(":", 1)
    _, payload = state_get(context)
    actor = user_actor_label(user.id)

    if choice == "no":
        shared_data = STORE.shared_data()
        task_texts = payload.get("texts") or [payload.get("text", "")]
        task_texts = [text for text in task_texts if text]
        if not task_texts:
            clear_state(context)
            await query.message.reply_text("Не удалось найти текст задачи. Начни заново через «➕ Задача».")
            return
        for task_text in task_texts:
            task = {
                "id": STORE.next_id("task"),
                "text": task_text,
                "priority": payload["priority"],
                "status": "todo",
                "remind_at": None,
                "actor": actor,
            }
            shared_data["tasks"].append(task)
            add_task_log_entry(shared_data, task, action="added")
        STORE.save()
        clear_state(context)
        if len(task_texts) == 1:
            await query.message.reply_text("Задача добавлена.")
        else:
            await query.message.reply_text(f"Добавлено задач: {len(task_texts)}.")
        return

    state_set(context, "add_task_date", **payload)
    await query.message.reply_text(
        "Введи дату напоминания:\n"
        "ДД.ММ.ГГ ЧЧ.ММ\n"
        "или ДД.ММ.ГГ (по умолчанию 09.59)",
        reply_markup=cancel_inline_keyboard(),
    )


async def task_actions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    action: str
    item_id: int
    if query.data.startswith("task_action:"):
        _, action, item_id_str = query.data.split(":", 2)
        item_id = int(item_id_str)
    else:
        # Совместимость со старыми сообщениями: task_done:<id> / task_del:<id>.
        action_raw, item_id_str = query.data.split(":", 1)
        action = action_raw.removeprefix("task_")
        item_id = int(item_id_str)

    shared_data = STORE.shared_data()
    tasks_data = shared_data.get("tasks", [])
    item = find_by_id(tasks_data, item_id)
    if not item:
        await query.message.reply_text("Задача не найдена.")
        return

    if action == "done":
        tasks_data.remove(item)
        add_task_log_entry(shared_data, item, action="done", actor=user_actor_label(user.id))
        STORE.save()
        await query.message.reply_text("Задача выполнена и перенесена в лог ✅")
    elif action == "del":
        tasks_data.remove(item)
        add_task_log_entry(shared_data, item, action="deleted", actor=user_actor_label(user.id))
        STORE.save()
        await query.message.reply_text("Задача удалена.")


async def task_log_clear_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    shared_data = STORE.shared_data()
    shared_data["task_log"] = []
    STORE.save()
    await query.message.reply_text("Лог выполненных задач очищен.")


async def shopping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "shopping")
    await render_shopping_categories(update)


def shopping_categories_map(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    categories: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        category = str(item.get("category", "другое"))
        categories.setdefault(category, []).append(item)
    return categories


def ordered_shopping_categories(categories: dict[str, list[dict[str, Any]]]) -> list[str]:
    ordered = [cat for cat in SHOPPING_CATEGORY_ORDER if cat in categories]
    ordered.extend(cat for cat in categories if cat not in ordered)
    return ordered


async def render_shopping_categories(update: Update) -> None:
    shared_data = STORE.shared_data()
    items = shared_data.get("shopping", [])
    if not items:
        await reply_text(update, "Список покупок пуст. Добавь товар кнопкой «➕ Покупка».")
        return

    lines = ["<b>🛒 Список покупок</b>", "Выбери категорию:"]
    keyboard: list[list[InlineKeyboardButton]] = []
    categories = shopping_categories_map(items)
    ordered_categories = ordered_shopping_categories(categories)
    for category in ordered_categories:
        total = len(categories[category])
        lines.append(f"• {category.title()} — {total}")
        keyboard.append([
            InlineKeyboardButton(
                f"{category.title()} ({total})", callback_data=f"shop_cat:{category}"
            )
        ])

    keyboard.append([InlineKeyboardButton("↩️ Отмена", callback_data="back:shopping")])

    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def render_shopping_category_items(update: Update, category: str) -> None:
    shared_data = STORE.shared_data()
    items = [
        item
        for item in shared_data.get("shopping", [])
        if str(item.get("category", "другое")) == category
    ]
    title = category.title()
    if not items:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("↩️ Отмена", callback_data="back:shopping")]]
        )
        await reply_text(
            update,
            f"<b>🛒 {title}</b>\nВ этой категории пока нет товаров.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        return

    lines = [f"<b>🛒 {title}</b>", "Выбери номер товара:"]
    keyboard: list[list[InlineKeyboardButton]] = []
    for number, item in enumerate(items, start=1):
        lines.append(f"{number}. {item['name']}{actor_tag(item.get('actor'))}")
        keyboard.append([
            InlineKeyboardButton(f"{number}", callback_data=f"shop_pick:{category}:{number}")
        ])
    keyboard.append(
        [
            InlineKeyboardButton("✅ Всё куплено", callback_data=f"shop_bulk:done:{category}"),
            InlineKeyboardButton("🗑 Удалить всё", callback_data=f"shop_bulk:del:{category}"),
        ]
    )
    keyboard.append([InlineKeyboardButton("↩️ Отмена", callback_data="back:shopping")])

    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def shopping_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    set_last_section(context, "shopping")
    _, category = query.data.split(":", 1)
    await render_shopping_category_items(update, category)


async def shopping_pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, category, number_str = query.data.split(":", 2)
    try:
        number = int(number_str)
    except ValueError:
        await query.message.reply_text("Не удалось распознать номер товара.")
        return

    shared_data = STORE.shared_data()
    items = [
        item
        for item in shared_data.get("shopping", [])
        if str(item.get("category", "другое")) == category
    ]
    if number < 1 or number > len(items):
        await query.message.reply_text("Товар с таким номером не найден.")
        return

    item = items[number - 1]
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Куплено", callback_data=f"shop_action:done:{item['id']}:{category}")],
            [InlineKeyboardButton("🗑 В корзину", callback_data=f"shop_action:del:{item['id']}:{category}")],
            [InlineKeyboardButton("↩️ Отмена", callback_data=f"shop_cat:{category}")],
        ]
    )
    await query.message.reply_text(
        f"Выбран товар {number}: {item['name']}\nЧто дальше?",
        reply_markup=keyboard,
    )


async def shopping_bulk_actions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, action, category = query.data.split(":", 2)
    actor = user_actor_label(user.id)

    shared_data = STORE.shared_data()
    items = shared_data.get("shopping", [])
    category_items = [
        item
        for item in items
        if str(item.get("category", "другое")) == category
    ]
    if not category_items:
        await query.message.reply_text("В этой категории уже нет товаров.")
        await render_shopping_category_items(update, category)
        return

    if action == "done":
        for item in category_items:
            add_shopping_log_entry(shared_data, item, action="done", actor=actor)
        message = f"Отмечено как куплено: {len(category_items)}."
    elif action == "del":
        for item in category_items:
            add_shopping_log_entry(shared_data, item, action="deleted", actor=actor)
        message = f"Удалено товаров: {len(category_items)}."
    else:
        await query.message.reply_text("Неизвестная групповая команда.")
        return

    shared_data["shopping"] = [
        item for item in items if str(item.get("category", "другое")) != category
    ]
    STORE.save()
    await query.message.reply_text(message)
    await render_shopping_category_items(update, category)


async def add_item(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "shopping")
    state_set(context, "add_item_category", return_to="shopping")
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Продукты", callback_data="additem_cat:продукты")],
            [InlineKeyboardButton("Бытовое", callback_data="additem_cat:бытовое")],
            [InlineKeyboardButton("Другое", callback_data="additem_cat:другое")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="flow_cancel")],
        ]
    )
    await update.message.reply_text(
        "Сначала выбери категорию:", reply_markup=keyboard
    )


async def add_item_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, category = query.data.split(":", 1)
    _, payload = state_get(context)

    # Поддержка старого шага на случай клика по старому сообщению:
    # если в payload уже есть name, сразу завершаем добавление.
    name = payload.get("name")
    if name:
        actor = user_actor_label(user.id)
        shared_data = STORE.shared_data()
        item = {
            "id": STORE.next_id("item"),
            "name": name,
            "category": category,
            "status": "todo",
            "actor": actor,
        }
        shared_data["shopping"].append(item)
        add_shopping_log_entry(shared_data, item, action="added")
        STORE.save()
        clear_state(context)
        await query.message.reply_text("Товар добавлен.")
        return

    state_set(context, "add_item_name", category=category, return_to="shopping")
    await query.message.reply_text(
        "📝 Введите товар (можно несколько, каждый с новой строки):",
        reply_markup=cancel_inline_keyboard(),
    )


async def shopping_actions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    action: str
    item_id: int
    category: str | None = None
    if query.data.startswith("shop_action:"):
        _, action, item_id_str, category = query.data.split(":", 3)
        item_id = int(item_id_str)
    else:
        action_raw, item_id_str = query.data.split(":", 1)
        action = action_raw.removeprefix("item_")
        item_id = int(item_id_str)

    shared_data = STORE.shared_data()
    items = shared_data.get("shopping", [])
    item = find_by_id(items, item_id)
    if not item:
        await query.message.reply_text("Товар не найден.")
        return

    if action == "done":
        items.remove(item)
        add_shopping_log_entry(shared_data, item, action="done", actor=user_actor_label(user.id))
        STORE.save()
        await query.message.reply_text("Товар отмечен как купленный и перенесён в лог.")
    elif action == "del":
        items.remove(item)
        add_shopping_log_entry(shared_data, item, action="deleted", actor=user_actor_label(user.id))
        STORE.save()
        await query.message.reply_text("Товар удалён и перенесён в лог.")
    else:
        await query.message.reply_text("Неизвестное действие для товара.")
        return

    if category:
        await render_shopping_category_items(update, category)


async def cleaning(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "cleaning")
    await render_cleaning_overview(update)


def get_cleaning_zone_items(shared_data: dict[str, Any], zone: str) -> list[dict[str, Any]]:
    zone_items = [
        item for item in shared_data.get("cleaning", []) if str(item.get("zone", "")) == zone
    ]
    return sorted(zone_items, key=cleaning_item_sort_key)


async def render_cleaning_overview(update: Update) -> None:
    shared_data = STORE.shared_data()
    cleaning_items = shared_data.get("cleaning", [])
    if not cleaning_items:
        await reply_text(update, "План уборки пуст. Добавь пункт кнопкой «➕ Уборка».")
        return

    lines = ["<b>🧹 План уборки:</b>"]
    keyboard: list[list[InlineKeyboardButton]] = []
    for zone in CLEANING_ZONES:
        zone_items = get_cleaning_zone_items(shared_data, zone)
        if not zone_items:
            continue
        total = len(zone_items)
        done = sum(1 for x in zone_items if x["status"] == "done")
        percent = int((done / total) * 100) if total else 0
        zone_line = f"{zone.title()} — {done}/{total} ({percent}%)"
        if percent == 100:
            zone_line = f"<s>{zone_line}</s>"
        lines.append(f"• {zone_line}")
        keyboard.append(
            [InlineKeyboardButton(f"{zone.title()} ({percent}%)", callback_data=f"clean_view_zone:{zone}")]
        )

    if not keyboard:
        await reply_text(update, "План уборки пуст. Добавь пункт кнопкой «➕ Уборка».")
        return

    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def render_cleaning_zone(update: Update, zone: str) -> None:
    shared_data = STORE.shared_data()
    zone_items = get_cleaning_zone_items(shared_data, zone)
    if not zone_items:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("↩️ К зонам", callback_data="back:cleaning")]]
        )
        await reply_text(
            update,
            f"В зоне <b>{zone.title()}</b> пока нет пунктов.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        return

    total = len(zone_items)
    done = sum(1 for x in zone_items if x["status"] == "done")
    percent = int((done / total) * 100) if total else 0
    lines = [f"<b>{zone.title()}</b> — {done}/{total} ({percent}%)"]
    keyboard_rows: list[list[InlineKeyboardButton]] = []

    for idx, item in enumerate(zone_items, start=1):
        status = "✅" if item["status"] == "done" else "⬜"
        remind_txt = ""
        if item.get("remind_at"):
            dt = datetime.fromisoformat(item["remind_at"]).astimezone(CONFIG.timezone)
            remind_txt = f" | ⏰ {dt.strftime(DATE_TIME_FMT)}"
        lines.append(f"{status} {idx}. {item['text']}{actor_tag(item.get('actor'))}{remind_txt}")

        cid = int(item["id"])
        row = [InlineKeyboardButton(f"✅ {idx}", callback_data=f"clean_done:{cid}")]
        if not is_fixed_cleaning_item(item):
            row.append(InlineKeyboardButton(f"🗑 {idx}", callback_data=f"clean_del:{cid}"))
        keyboard_rows.append(row)

    if percent == 100:
        keyboard_rows.append([InlineKeyboardButton("🗑 Удалить зону", callback_data=f"clean_zone_del:{zone}")])
    keyboard_rows.append([InlineKeyboardButton("↩️ К зонам", callback_data="back:cleaning")])
    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )


async def cleaning_zone_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, zone = query.data.split(":", 1)
    set_last_section(context, "cleaning")
    await render_cleaning_zone(update, zone)


async def cleaning_zone_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, zone = query.data.split(":", 1)
    shared_data = STORE.shared_data()
    zone_items = get_cleaning_zone_items(shared_data, zone)
    if not zone_items:
        await render_cleaning_overview(update)
        return

    total = len(zone_items)
    done = sum(1 for x in zone_items if x["status"] == "done")
    if done < total:
        await query.message.reply_text("Зону можно удалить только после выполнения всех пунктов.")
        await render_cleaning_zone(update, zone)
        return

    items = shared_data.get("cleaning", [])
    to_remove = [item for item in items if str(item.get("zone", "")) == zone]
    for item in to_remove:
        add_cleaning_log_entry(shared_data, item, action="deleted", actor=user_actor_label(user.id))
    shared_data["cleaning"] = [item for item in items if str(item.get("zone", "")) != zone]
    STORE.save()
    await query.message.reply_text(f"Зона «{zone.title()}» удалена из плана уборки.")
    await render_cleaning_overview(update)


async def add_cleaning(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "cleaning")
    clear_state(context)
    await update.message.reply_text("Выберите зону уборки 🧼:", reply_markup=cleaning_zone_keyboard())


async def add_cleaning_zone_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    clear_state(context)
    await query.message.reply_text("Выберите зону уборки 🧼:", reply_markup=cleaning_zone_keyboard())


async def add_cleaning_zone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, zone = query.data.split(":", 1)
    actor = user_actor_label(user.id)
    shared_data = STORE.shared_data()
    added_count = ensure_fixed_cleaning_items_for_zone(shared_data, zone, actor=actor)
    if added_count:
        STORE.save()
    clear_state(context)
    lines = [
        f"Зона: <b>{zone.title()}</b>",
        "",
        "Вот чек-лист для этой зоны:",
    ]
    for idx, base_text in enumerate(FIXED_CLEANING_CHECKLIST, start=1):
        lines.append(f"{idx}. {base_text}")
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Дополнительная задача", callback_data=f"clean_plus:{zone}")],
            [InlineKeyboardButton("↩️ К зонам", callback_data="clean_zone_menu")],
            [InlineKeyboardButton("↩️ Отмена", callback_data="flow_cancel")],
        ]
    )
    await query.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def add_cleaning_plus_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, zone = query.data.split(":", 1)
    state_set(context, "add_cleaning_text", zone=zone, kind="plus", return_to="cleaning")
    await query.message.reply_text(
        f"Введите дополнительный пункт для зоны «{zone.title()}»:",
        reply_markup=cancel_inline_keyboard(),
    )


async def cleaning_actions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    action, item_id_str = query.data.split(":", 1)
    item_id = int(item_id_str)

    shared_data = STORE.shared_data()
    items = shared_data.get("cleaning", [])
    item = find_by_id(items, item_id)
    if not item:
        await query.message.reply_text("Пункт не найден.")
        return

    if action == "clean_done":
        if item.get("status") == "done":
            await query.message.reply_text("Этот пункт уже отмечен как выполненный.")
            return
        item["status"] = "done"
        add_cleaning_log_entry(shared_data, item, action="done", actor=user_actor_label(user.id))
        STORE.save()
        await render_cleaning_zone(update, str(item.get("zone", "")))
        return
    elif action == "clean_del":
        if is_fixed_cleaning_item(item):
            await query.message.reply_text(
                "Фиксированный пункт нельзя удалить. Отмечай его как выполненный кнопкой ✅."
            )
            return
        zone = str(item.get("zone", ""))
        items.remove(item)
        add_cleaning_log_entry(shared_data, item, action="deleted", actor=user_actor_label(user.id))
        STORE.save()
        await render_cleaning_zone(update, zone)
        return


async def events(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "events")
    await render_events_seasons(update)


async def render_events_seasons(update: Update) -> None:
    shared_data = STORE.shared_data()
    lines = ["<b>Событие</b>", "", "Выберите сезон:"]

    keyboard_rows: list[list[InlineKeyboardButton]] = []
    for season_key in SEASON_ORDER:
        meta = SEASON_MONTHS[season_key]
        count = season_events_count(shared_data, season_key)
        keyboard_rows.append(
            [InlineKeyboardButton(f"{meta['label']} ({count})", callback_data=f"event_season:{season_key}")]
        )

    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )


async def render_events_season_months(update: Update, season_key: str) -> None:
    meta = SEASON_MONTHS.get(season_key)
    if not meta:
        await reply_text(update, "Сезон не найден.")
        return

    shared_data = STORE.shared_data()
    lines = [f"<b>{meta['label']}</b>", "", "Выберите месяц:"]
    keyboard_rows: list[list[InlineKeyboardButton]] = []

    for month in meta["months"]:
        month_name = MONTH_NAMES.get(month, str(month))
        month_count = len(events_for_month(shared_data, month))
        keyboard_rows.append(
            [InlineKeyboardButton(f"{month_name} ({month_count})", callback_data=f"event_month:{month}")]
        )

    keyboard_rows.append([InlineKeyboardButton("↩️ К сезонам", callback_data="back:events")])
    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )


async def render_events_month(update: Update, month: int) -> None:
    shared_data = STORE.shared_data()
    items = events_for_month(shared_data, month)
    month_name = MONTH_NAMES.get(month, f"Месяц {month}")
    lines = [f"<b>{month_name}</b>"]

    keyboard_rows: list[list[InlineKeyboardButton]] = []
    if items:
        sorted_items = sorted(
            items,
            key=lambda item: (
                extract_event_day_month(item.get("date"))[0] or 99,
                str(item.get("date", "")),
                int(item.get("id", 0)),
            ),
        )
        for idx, item in enumerate(sorted_items, start=1):
            lines.append(
                f"{idx}. {item['name']}{actor_tag(item.get('actor'))} — {item['date']} (напомнить за {item['remind_days_before']} дн.)"
            )
            keyboard_rows.append(
                [InlineKeyboardButton(f"🗑 {idx}", callback_data=f"event_del:{item['id']}:{month}")]
            )

    season_key = MONTH_TO_SEASON.get(month)
    if season_key:
        keyboard_rows.append(
            [InlineKeyboardButton("↩️ К месяцам", callback_data=f"event_season:{season_key}")]
        )
    keyboard_rows.append([InlineKeyboardButton("↩️ К сезонам", callback_data="back:events")])
    await reply_text(
        update,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )


async def events_season_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, season_key = query.data.split(":", 1)
    set_last_section(context, "events")
    await render_events_season_months(update, season_key)


async def events_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    _, month_raw = query.data.split(":", 1)
    try:
        month = int(month_raw)
    except ValueError:
        await reply_text(update, "Не удалось распознать месяц.")
        return
    set_last_section(context, "events")
    await render_events_month(update, month)


async def add_event(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    set_last_section(context, "events")
    state_set(context, "add_event_name", return_to="events")
    await update.message.reply_text(
        "🎉 Введите название события (например: День рождения мамы):",
        reply_markup=cancel_inline_keyboard(),
    )


async def event_actions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if not has_access(user.id):
        await deny_access(update)
        return

    await query.answer()
    parts = query.data.split(":")
    if len(parts) < 2:
        await reply_text(update, "Не удалось распознать действие.")
        return
    action = parts[0]
    event_id = int(parts[1])
    month: int | None = None
    if len(parts) >= 3:
        try:
            month = int(parts[2])
        except ValueError:
            month = None

    shared_data = STORE.shared_data()
    items = shared_data.get("events", [])
    item = find_by_id(items, event_id)
    if not item:
        await reply_text(update, "Событие не найдено.")
        return

    if action == "event_del":
        items.remove(item)
        add_events_log_entry(shared_data, item, action="deleted", actor=user_actor_label(user.id))
        STORE.save()
        if month is not None:
            await render_events_month(update, month)
            return
        await reply_text(update, "Событие удалено.")
        return

    await reply_text(update, "Неизвестное действие для события.")


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return
    actor = user_actor_label(user.id)

    if not update.message or not update.message.text:
        return

    state, payload = state_get(context)
    text = update.message.text.strip()
    action = menu_action(text)

    if action and not state:
        if action == "tasks":
            await tasks(update, context)
            return
        if action == "add_task":
            await add_task(update, context)
            return
        if action == "shopping":
            await shopping(update, context)
            return
        if action == "add_item":
            await add_item(update, context)
            return
        if action == "cleaning":
            await cleaning(update, context)
            return
        if action == "add_cleaning":
            await add_cleaning(update, context)
            return
        if action == "events":
            await events(update, context)
            return
        if action == "add_event":
            await add_event(update, context)
            return
        if action == "help":
            await cat_help(update, context)
            return
        if action == "menu_panel":
            await menu_panel(update, context)
            return
        if action == "start":
            await start(update, context)
            return
        if action == "cancel":
            await cancel(update, context)
            return

    if not state:
        await update.message.reply_text(
            "Не понял сообщение. Нажмите кнопку «❤️ Помощь» или используйте /help.",
            reply_markup=main_menu_keyboard(),
        )
        return

    try:
        if state == "add_task_priority":
            await update.message.reply_text(
                "Выбери приоритет кнопкой ниже или нажми /cancel.",
                reply_markup=cancel_inline_keyboard(),
            )
            return

        if state == "add_task_need_reminder":
            await update.message.reply_text(
                "Выбери вариант напоминания кнопкой (Да/Нет) или нажми /cancel.",
                reply_markup=cancel_inline_keyboard(),
            )
            return

        if state == "add_item_category":
            await update.message.reply_text(
                "Сначала выбери категорию кнопкой или нажми /cancel.",
                reply_markup=cancel_inline_keyboard(),
            )
            return

        if state == "add_task_text":
            task_texts = parse_multiline_entries(text)
            if not task_texts:
                await update.message.reply_text("Текст задачи не должен быть пустым.")
                return

            state_set(context, "add_task_priority", texts=task_texts, return_to="tasks")
            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🟢 Низкий", callback_data="addtask_pri:низкий")],
                    [InlineKeyboardButton("🟡 Средний", callback_data="addtask_pri:средний")],
                    [InlineKeyboardButton("🔴 Высокий", callback_data="addtask_pri:высокий")],
                    [InlineKeyboardButton("↩️ Отмена", callback_data="flow_cancel")],
                ]
            )
            await update.message.reply_text("Выбери приоритет:", reply_markup=keyboard)
            return

        if state == "add_task_date":
            remind_dt = parse_reminder_datetime(text)
            if remind_dt <= datetime.now(CONFIG.timezone):
                await update.message.reply_text("Дата должна быть в будущем. Попробуй снова.")
                return

            shared_data = STORE.shared_data()
            task_texts = payload.get("texts") or [payload.get("text", "")]
            task_texts = [task_text for task_text in task_texts if task_text]
            if not task_texts:
                clear_state(context)
                await update.message.reply_text("Не удалось найти текст задачи. Начни заново через «➕ Задача».")
                return
            added_tasks: list[dict[str, Any]] = []
            for task_text in task_texts:
                task = {
                    "id": STORE.next_id("task"),
                    "text": task_text,
                    "priority": payload["priority"],
                    "status": "todo",
                    "remind_at": remind_dt.isoformat(),
                    "actor": actor,
                }
                shared_data["tasks"].append(task)
                add_task_log_entry(shared_data, task, action="added")
                added_tasks.append(task)
            STORE.save()
            for task in added_tasks:
                schedule_task_reminder(context.application, task)
            clear_state(context)

            if len(added_tasks) == 1:
                await update.message.reply_text("Задача добавлена.")
            else:
                await update.message.reply_text(f"Добавлено задач: {len(added_tasks)}.")
            return

        if state == "add_item_name":
            item_names = parse_multiline_entries(text)
            if not item_names:
                await update.message.reply_text("Название товара не должно быть пустым.")
                return
            category = payload.get("category")
            if not category:
                clear_state(context)
                await update.message.reply_text("Сначала выбери категорию кнопкой «➕ Покупка».")
                return

            shared_data = STORE.shared_data()
            for item_name in item_names:
                item = {
                    "id": STORE.next_id("item"),
                    "name": item_name,
                    "category": category,
                    "status": "todo",
                    "actor": actor,
                }
                shared_data["shopping"].append(item)
                add_shopping_log_entry(shared_data, item, action="added")
            STORE.save()
            clear_state(context)
            if len(item_names) == 1:
                await update.message.reply_text("Товар добавлен.")
            else:
                await update.message.reply_text(f"Добавлено товаров: {len(item_names)}.")
            return

        if state == "add_cleaning_text":
            if not text:
                await update.message.reply_text("Пункт чек-листа не должен быть пустым.")
                return
            state_set(
                context,
                "add_cleaning_datetime",
                zone=payload["zone"],
                text=text,
                kind=payload.get("kind", "plus"),
                return_to="cleaning",
            )
            await update.message.reply_text(
                "Введи дату напоминания:\n"
                "ДД.ММ.ГГ ЧЧ.ММ\n"
                "или ДД.ММ.ГГ (по умолчанию 09.59)\n"
                "или '-' если напоминание не нужно.",
                reply_markup=cancel_inline_keyboard(),
            )
            return

        if state == "add_cleaning_datetime":
            remind_at_iso = None
            if text != "-":
                remind_dt = parse_reminder_datetime(text)
                if remind_dt <= datetime.now(CONFIG.timezone):
                    await update.message.reply_text("Дата должна быть в будущем. Попробуй снова.")
                    return
                remind_at_iso = remind_dt.isoformat()

            shared_data = STORE.shared_data()
            item = {
                "id": STORE.next_id("clean"),
                "zone": payload["zone"],
                "text": payload["text"],
                "status": "todo",
                "remind_at": remind_at_iso,
                "kind": payload.get("kind", "plus"),
                "actor": actor,
            }
            shared_data["cleaning"].append(item)
            add_cleaning_log_entry(shared_data, item, action="added")
            STORE.save()
            schedule_cleaning_reminder(context.application, item)
            clear_state(context)

            await update.message.reply_text("Дополнительный пункт уборки добавлен.")
            return

        if state == "add_event_name":
            if not text:
                await update.message.reply_text("Название события не должно быть пустым.")
                return
            state_set(context, "add_event_date", name=text, return_to="events")
            await update.message.reply_text(
                "Введи дату события в формате ДД.ММ (например 14.02):",
                reply_markup=cancel_inline_keyboard(),
            )
            return

        if state == "add_event_date":
            day, month = parse_event_date(text)
            state_set(
                context,
                "add_event_remind_days",
                name=payload["name"],
                date=f"{day:02d}.{month:02d}",
                return_to="events",
            )
            await update.message.reply_text(
                "За сколько дней напоминать? (число, например 1)",
                reply_markup=cancel_inline_keyboard(),
            )
            return

        if state == "add_event_remind_days":
            remind_days = int(text)
            if remind_days < 0:
                await update.message.reply_text("Число дней не может быть отрицательным.")
                return

            shared_data = STORE.shared_data()
            event = {
                "id": STORE.next_id("event"),
                "name": payload["name"],
                "date": payload["date"],
                "remind_days_before": remind_days,
                "last_notified_year": None,
                "actor": actor,
            }
            shared_data["events"].append(event)
            add_events_log_entry(shared_data, event, action="added")
            STORE.save()
            clear_state(context)
            await update.message.reply_text("Событие добавлено.")
            return

        await update.message.reply_text("Состояние не распознано. Используй /cancel")
    except ValueError:
        await update.message.reply_text("Неверный формат данных. Попробуй ещё раз или /cancel")
    except Exception as exc:
        logger.exception("Ошибка при обработке текста: %s", exc)
        await update.message.reply_text("Произошла ошибка. Попробуй ещё раз или /cancel")


async def task_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    task_id = context.job.data["task_id"]

    shared_data = STORE.shared_data()
    task = find_by_id(shared_data.get("tasks", []), task_id)
    if not task or task.get("status") == "done":
        return

    await broadcast_to_allowed_users(
        context.bot,
        f"⏰ Кажется пора выполнить: {task['text']}",
    )


async def cleaning_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    clean_id = context.job.data["clean_id"]

    shared_data = STORE.shared_data()
    item = find_by_id(shared_data.get("cleaning", []), clean_id)
    if not item or item.get("status") == "done":
        return

    await broadcast_to_allowed_users(
        context.bot,
        f"🧹 Напоминание об уборке: [{item['zone']}] {item['text']}",
    )


async def daily_event_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(CONFIG.timezone)
    today = now.date()
    shared_data = STORE.shared_data()
    changed = False

    for event in shared_data.get("events", []):
        day, month = extract_event_day_month(event.get("date"))
        if day is None or month is None:
            continue
        remind_days = int(event.get("remind_days_before", 1))

        try:
            event_this_year = datetime(year=today.year, month=month, day=day).date()
        except ValueError:
            # Например, 29.02 в невисокосный год.
            continue

        reminder_date = event_this_year - timedelta(days=remind_days)
        if reminder_date == today and event.get("last_notified_year") != today.year:
            await broadcast_to_allowed_users(
                context.bot,
                (
                    f"📅 Напоминание: скоро событие '{event['name']}' ({event['date']}). "
                    f"Осталось {remind_days} дн."
                ),
            )
            event["last_notified_year"] = today.year
            changed = True

    if changed:
        STORE.save()


async def task_log_cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(CONFIG.timezone)
    shared_data = STORE.shared_data()
    removed = cleanup_old_task_log(shared_data, now)
    if removed > 0:
        STORE.save()
        logger.info("Удалено старых записей из task_log: %s", removed)


async def shopping_log_cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(CONFIG.timezone)
    shared_data = STORE.shared_data()
    removed = cleanup_old_shopping_log(shared_data, now)
    if removed > 0:
        STORE.save()
        logger.info("Удалено старых записей из shopping_log: %s", removed)


async def cleaning_log_cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(CONFIG.timezone)
    shared_data = STORE.shared_data()
    removed = cleanup_old_cleaning_log(shared_data, now)
    if removed > 0:
        STORE.save()
        logger.info("Удалено старых записей из cleaning_log: %s", removed)


async def events_log_cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(CONFIG.timezone)
    shared_data = STORE.shared_data()
    removed = cleanup_old_events_log(shared_data, now)
    if removed > 0:
        STORE.save()
        logger.info("Удалено старых записей из events_log: %s", removed)


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not has_access(user.id):
        await deny_access(update)
        return

    await update.message.reply_text("Неизвестная команда. Используй /help")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Необработанная ошибка: %s", context.error)
    if isinstance(update, Update):
        if update.message:
            await update.message.reply_text("Произошла внутренняя ошибка. Попробуйте позже.")
        elif update.callback_query:
            await update.callback_query.answer("Произошла ошибка", show_alert=True)


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            ("start", "Приветствие"),
            ("help", "Список команд"),
            ("tasks", "Список дел"),
            ("add_task", "Добавить задачу"),
            ("shopping", "Список покупок"),
            ("add_item", "Добавить товар"),
            ("cleaning", "План уборки"),
            ("add_cleaning", "Добавить пункт уборки"),
            ("events", "Список событий"),
            ("add_event", "Добавить событие"),
            ("cancel", "Отменить текущее действие"),
        ]
    )


def build_application() -> Application:
    app = Application.builder().token(CONFIG.token).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("cancel", cancel))

    app.add_handler(CommandHandler("tasks", tasks))
    app.add_handler(CommandHandler("add_task", add_task))
    app.add_handler(CallbackQueryHandler(flow_cancel_callback, pattern=r"^flow_cancel$"))
    app.add_handler(CallbackQueryHandler(menu_cancel_callback, pattern=r"^menu_cancel$"))
    app.add_handler(CallbackQueryHandler(logs_menu_callback, pattern=r"^logs_menu$"))
    app.add_handler(CallbackQueryHandler(logs_category_callback, pattern=r"^logs_cat:"))
    app.add_handler(CallbackQueryHandler(logs_week_callback, pattern=r"^logs_week:"))
    app.add_handler(CallbackQueryHandler(section_back_callback, pattern=r"^back:(tasks|shopping|cleaning|events)$"))
    app.add_handler(CallbackQueryHandler(task_log_clear_callback, pattern=r"^task_log_clear$"))
    app.add_handler(CallbackQueryHandler(add_task_priority_callback, pattern=r"^addtask_pri:"))
    app.add_handler(CallbackQueryHandler(add_task_reminder_choice_callback, pattern=r"^addtask_remind:"))
    app.add_handler(CallbackQueryHandler(task_select_callback, pattern=r"^task_pick:"))
    app.add_handler(CallbackQueryHandler(task_actions_callback, pattern=r"^task_action:(done|del):"))
    app.add_handler(CallbackQueryHandler(task_actions_callback, pattern=r"^task_(done|del):"))

    app.add_handler(CommandHandler("shopping", shopping))
    app.add_handler(CommandHandler("add_item", add_item))
    app.add_handler(CallbackQueryHandler(shopping_category_callback, pattern=r"^shop_cat:"))
    app.add_handler(CallbackQueryHandler(shopping_pick_callback, pattern=r"^shop_pick:"))
    app.add_handler(CallbackQueryHandler(shopping_bulk_actions_callback, pattern=r"^shop_bulk:(done|del):"))
    app.add_handler(CallbackQueryHandler(shopping_actions_callback, pattern=r"^shop_action:(done|del):"))
    app.add_handler(CallbackQueryHandler(add_item_category_callback, pattern=r"^additem_cat:"))
    app.add_handler(CallbackQueryHandler(shopping_actions_callback, pattern=r"^item_(done|del):"))

    app.add_handler(CommandHandler("cleaning", cleaning))
    app.add_handler(CommandHandler("add_cleaning", add_cleaning))
    app.add_handler(CallbackQueryHandler(cleaning_zone_view_callback, pattern=r"^clean_view_zone:"))
    app.add_handler(CallbackQueryHandler(cleaning_zone_delete_callback, pattern=r"^clean_zone_del:"))
    app.add_handler(CallbackQueryHandler(add_cleaning_zone_menu_callback, pattern=r"^clean_zone_menu$"))
    app.add_handler(CallbackQueryHandler(add_cleaning_zone_callback, pattern=r"^clean_zone:"))
    app.add_handler(CallbackQueryHandler(add_cleaning_plus_callback, pattern=r"^clean_plus:"))
    app.add_handler(CallbackQueryHandler(cleaning_actions_callback, pattern=r"^clean_(done|del):"))

    app.add_handler(CommandHandler("events", events))
    app.add_handler(CommandHandler("add_event", add_event))
    app.add_handler(CallbackQueryHandler(events_season_callback, pattern=r"^event_season:"))
    app.add_handler(CallbackQueryHandler(events_month_callback, pattern=r"^event_month:"))
    app.add_handler(CallbackQueryHandler(event_actions_callback, pattern=r"^event_del:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    app.add_error_handler(error_handler)
    return app


def main() -> None:
    global CONFIG, STORE

    CONFIG = load_config()
    STORE = DataStore(CONFIG.data_file)
    STORE.load()
    shared_data = STORE.shared_data()
    now = datetime.now(CONFIG.timezone)
    removed_tasks_on_start = cleanup_old_task_log(shared_data, now)
    removed_shopping_on_start = cleanup_old_shopping_log(shared_data, now)
    removed_cleaning_on_start = cleanup_old_cleaning_log(shared_data, now)
    removed_events_on_start = cleanup_old_events_log(shared_data, now)
    if (
        removed_tasks_on_start > 0
        or removed_shopping_on_start > 0
        or removed_cleaning_on_start > 0
        or removed_events_on_start > 0
    ):
        STORE.save()
    if removed_tasks_on_start > 0:
        logger.info("При старте удалено старых записей из task_log: %s", removed_tasks_on_start)
    if removed_shopping_on_start > 0:
        logger.info(
            "При старте удалено старых записей из shopping_log: %s", removed_shopping_on_start
        )
    if removed_cleaning_on_start > 0:
        logger.info(
            "При старте удалено старых записей из cleaning_log: %s", removed_cleaning_on_start
        )
    if removed_events_on_start > 0:
        logger.info("При старте удалено старых записей из events_log: %s", removed_events_on_start)

    app = build_application()

    # Ежедневная автоочистка лога выполненных задач.
    app.job_queue.run_daily(
        task_log_cleanup_job,
        time=time(hour=0, minute=5, tzinfo=CONFIG.timezone),
        name="daily_task_log_cleanup",
    )

    # Ежедневная автоочистка лога покупок.
    app.job_queue.run_daily(
        shopping_log_cleanup_job,
        time=time(hour=0, minute=10, tzinfo=CONFIG.timezone),
        name="daily_shopping_log_cleanup",
    )

    app.job_queue.run_daily(
        cleaning_log_cleanup_job,
        time=time(hour=0, minute=15, tzinfo=CONFIG.timezone),
        name="daily_cleaning_log_cleanup",
    )

    app.job_queue.run_daily(
        events_log_cleanup_job,
        time=time(hour=0, minute=20, tzinfo=CONFIG.timezone),
        name="daily_events_log_cleanup",
    )

    # Ежедневная проверка событий.
    app.job_queue.run_daily(
        daily_event_check_job,
        time=time(hour=9, minute=0, tzinfo=CONFIG.timezone),
        name="daily_event_check",
    )

    # После рестарта восстанавливаем отложенные напоминания.
    reschedule_all(app)

    logger.info("Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
