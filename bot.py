import asyncio
import logging
import os
import re
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RATES_FILE = os.getenv("RATES_FILE", "rates_auto_rail.xlsx").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = os.getenv("SMTP_PORT", "").strip()
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", "").strip()
ORDER_RECEIVER_EMAIL = os.getenv("ORDER_RECEIVER_EMAIL", "").strip()

EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)
ALLOWED_LANGS = ("ru", "en", "cn")

REQUIRED_SETTINGS = {
    "volumetric_coef_kg_per_m3",
    "docs_fee_usd",
    "auto_agent_fee_cny",
    "pickup_small_cny",
    "pickup_large_cny",
    "fx_cny_per_usd",
    "rail_tariff_0_1_fixed_usd",
    "rail_tariff_1_5_usd_per_m3",
    "rail_tariff_5_10_usd_per_m3",
    "rail_tariff_10_plus_usd_per_m3",
}


# =========================
# TEXTS / I18N
# =========================
TEXTS: dict[str, dict[str, str]] = {
    "ru": {
        "choose_language": "Выберите язык:",
        "language_set": "Язык установлен: Русский",
        "start_intro": "Бот рассчитает стоимость LCL-перевозки Китай → Москва.",
        "choose_city": "Выберите город забора:",
        "no_cities": "Список активных городов пуст. Проверьте Excel-файл rates_auto_rail.xlsx.",
        "enter_weight": "Введите вес груза в кг. Пример: 320",
        "enter_volume": "Введите фактический объём в м³. Пример: 1.8",
        "choose_stackable": "Груз штабелируется?",
        "yes": "Да",
        "no": "Нет",
        "enter_length": "Введите длину в метрах. Пример: 1.2",
        "enter_width": "Введите ширину в метрах. Пример: 0.8",
        "calc_ready": "Расчёт готов. Выберите действие:",
        "order_btn": "Заказать",
        "menu_btn": "В меню",
        "enter_email": "Введите ваш email:",
        "bad_email": "Некорректный email. Попробуйте ещё раз.",
        "enter_name": "Введите ваше имя:",
        "confirm_order": "Проверьте данные заказа и подтвердите отправку:",
        "confirm_btn": "Подтвердить",
        "cancel_btn": "Отмена",
        "order_sent": "Заявка отправлена. Спасибо!",
        "order_failed": "Заявку не удалось отправить по email: {error}",
        "cancelled": "Действие отменено. Возвращаю в меню.",
        "back_to_menu": "Главное меню. Выберите действие:",
        "main_menu_hint": "Нажмите «Новый расчёт», чтобы начать.",
        "new_calc_btn": "Новый расчёт",
        "bad_number": "Некорректное число. Введите значение ещё раз.",
        "bad_positive_number": "Число должно быть больше нуля. Попробуйте ещё раз.",
        "excel_error": "Ошибка чтения Excel: {error}",
        "internal_error": "Произошла внутренняя ошибка. Попробуйте /start",
        "pickup_city_saved": "Город забора: {city}",
        "stackable_yes": "Да",
        "stackable_no": "Нет",
        "result_title": "Результат расчёта",
        "result_template": (
            "<b>{title}</b>\n\n"
            "Город забора: {city}\n"
            "Вес: {weight_kg:.2f} кг\n"
            "Фактический объём: {actual_volume:.3f} м³\n"
            "Штабелируемость: {stackable_text}\n"
            "Весовой объём: {weight_volume:.3f} м³\n"
            "Нештабелируемый объём: {nonstack_volume:.3f} м³\n"
            "Расчётный ЖД-объём: {rail_volume:.3f} м³\n\n"
            "ЖД база: {rail_base:.2f} USD\n"
            "Документы: {docs_fee_usd:.2f} USD\n"
            "ЖД итого: {rail_total:.2f} USD\n\n"
            "Авто по м³: {auto_by_m3:.2f} USD\n"
            "Авто по тоннам: {auto_by_ton:.2f} USD\n"
            "Авто база: {auto_base:.2f} USD\n"
            "Агентский сбор: {auto_agent_fee_usd:.2f} USD\n"
            "Pickup fee: {pickup_fee_usd:.2f} USD\n"
            "Авто итого: {auto_total:.2f} USD\n\n"
            "<b>Итого: {total:.2f} USD</b>"
        ),
        "order_summary": (
            "Имя: {name}\n"
            "Email: {email}\n"
            "Город забора: {city}\n"
            "Вес: {weight_kg:.2f} кг\n"
            "Фактический объём: {actual_volume:.3f} м³\n"
            "Штабелируемость: {stackable_text}\n"
            "Длина: {length_display}\n"
            "Ширина: {width_display}\n"
            "Расчётный ЖД-объём: {rail_volume:.3f} м³\n"
            "Итого: {total:.2f} USD"
        ),
        "email_subject": "Новая заявка LCL China → Moscow",
        "email_body": (
            "Новая заявка из Telegram-бота\n\n"
            "Имя: {name}\n"
            "Email клиента: {email}\n"
            "Язык: {lang}\n"
            "Город забора: {city}\n"
            "Вес: {weight_kg:.2f} кг\n"
            "Фактический объём: {actual_volume:.3f} м³\n"
            "Штабелируемость: {stackable_text}\n"
            "Длина: {length_display}\n"
            "Ширина: {width_display}\n"
            "Весовой объём: {weight_volume:.3f} м³\n"
            "Нештабелируемый объём: {nonstack_volume:.3f} м³\n"
            "Расчётный ЖД-объём: {rail_volume:.3f} м³\n"
            "ЖД база: {rail_base:.2f} USD\n"
            "ЖД итого: {rail_total:.2f} USD\n"
            "Авто по м³: {auto_by_m3:.2f} USD\n"
            "Авто по тоннам: {auto_by_ton:.2f} USD\n"
            "Авто база: {auto_base:.2f} USD\n"
            "Агентский сбор: {auto_agent_fee_usd:.2f} USD\n"
            "Pickup fee: {pickup_fee_usd:.2f} USD\n"
            "Авто итого: {auto_total:.2f} USD\n"
            "ИТОГО: {total:.2f} USD\n"
        ),
    },
    "en": {
        "choose_language": "Choose language:",
        "language_set": "Language set: English",
        "start_intro": "This bot calculates LCL shipping cost China → Moscow.",
        "choose_city": "Choose pickup city:",
        "no_cities": "Active pickup cities list is empty. Check rates_auto_rail.xlsx.",
        "enter_weight": "Enter cargo weight in kg. Example: 320",
        "enter_volume": "Enter actual volume in m³. Example: 1.8",
        "choose_stackable": "Is the cargo stackable?",
        "yes": "Yes",
        "no": "No",
        "enter_length": "Enter length in meters. Example: 1.2",
        "enter_width": "Enter width in meters. Example: 0.8",
        "calc_ready": "Calculation is ready. Choose an action:",
        "order_btn": "Order",
        "menu_btn": "Menu",
        "enter_email": "Enter your email:",
        "bad_email": "Invalid email. Please try again.",
        "enter_name": "Enter your name:",
        "confirm_order": "Check the order data and confirm sending:",
        "confirm_btn": "Confirm",
        "cancel_btn": "Cancel",
        "order_sent": "Order request sent. Thank you!",
        "order_failed": "Failed to send the email request: {error}",
        "cancelled": "Action cancelled. Returning to menu.",
        "back_to_menu": "Main menu. Choose an action:",
        "main_menu_hint": "Press “New calculation” to begin.",
        "new_calc_btn": "New calculation",
        "bad_number": "Invalid number. Please enter the value again.",
        "bad_positive_number": "The number must be greater than zero. Try again.",
        "excel_error": "Excel read error: {error}",
        "internal_error": "Internal error occurred. Try /start",
        "pickup_city_saved": "Pickup city: {city}",
        "stackable_yes": "Yes",
        "stackable_no": "No",
        "result_title": "Calculation result",
        "result_template": (
            "<b>{title}</b>\n\n"
            "Pickup city: {city}\n"
            "Weight: {weight_kg:.2f} kg\n"
            "Actual volume: {actual_volume:.3f} m³\n"
            "Stackable: {stackable_text}\n"
            "Weight volume: {weight_volume:.3f} m³\n"
            "Non-stackable volume: {nonstack_volume:.3f} m³\n"
            "Rail chargeable volume: {rail_volume:.3f} m³\n\n"
            "Rail base: {rail_base:.2f} USD\n"
            "Documents fee: {docs_fee_usd:.2f} USD\n"
            "Rail total: {rail_total:.2f} USD\n\n"
            "Truck by m³: {auto_by_m3:.2f} USD\n"
            "Truck by ton: {auto_by_ton:.2f} USD\n"
            "Truck base: {auto_base:.2f} USD\n"
            "Agent fee: {auto_agent_fee_usd:.2f} USD\n"
            "Pickup fee: {pickup_fee_usd:.2f} USD\n"
            "Truck total: {auto_total:.2f} USD\n\n"
            "<b>Total: {total:.2f} USD</b>"
        ),
        "order_summary": (
            "Name: {name}\n"
            "Email: {email}\n"
            "Pickup city: {city}\n"
            "Weight: {weight_kg:.2f} kg\n"
            "Actual volume: {actual_volume:.3f} m³\n"
            "Stackable: {stackable_text}\n"
            "Length: {length_display}\n"
            "Width: {width_display}\n"
            "Rail chargeable volume: {rail_volume:.3f} m³\n"
            "Total: {total:.2f} USD"
        ),
        "email_subject": "New LCL China → Moscow order request",
        "email_body": (
            "New request from Telegram bot\n\n"
            "Name: {name}\n"
            "Client email: {email}\n"
            "Language: {lang}\n"
            "Pickup city: {city}\n"
            "Weight: {weight_kg:.2f} kg\n"
            "Actual volume: {actual_volume:.3f} m³\n"
            "Stackable: {stackable_text}\n"
            "Length: {length_display}\n"
            "Width: {width_display}\n"
            "Weight volume: {weight_volume:.3f} m³\n"
            "Non-stackable volume: {nonstack_volume:.3f} m³\n"
            "Rail chargeable volume: {rail_volume:.3f} m³\n"
            "Rail base: {rail_base:.2f} USD\n"
            "Rail total: {rail_total:.2f} USD\n"
            "Truck by m³: {auto_by_m3:.2f} USD\n"
            "Truck by ton: {auto_by_ton:.2f} USD\n"
            "Truck base: {auto_base:.2f} USD\n"
            "Agent fee: {auto_agent_fee_usd:.2f} USD\n"
            "Pickup fee: {pickup_fee_usd:.2f} USD\n"
            "Truck total: {auto_total:.2f} USD\n"
            "TOTAL: {total:.2f} USD\n"
        ),
    },
    "cn": {
        "choose_language": "请选择语言：",
        "language_set": "语言已设置：中文",
        "start_intro": "此机器人用于计算中国 → 莫斯科的 LCL 拼箱运输费用。",
        "choose_city": "请选择提货城市：",
        "no_cities": "可用提货城市列表为空。请检查 rates_auto_rail.xlsx。",
        "enter_weight": "请输入货物重量（公斤）。例如：320",
        "enter_volume": "请输入实际体积（立方米）。例如：1.8",
        "choose_stackable": "货物是否可堆叠？",
        "yes": "是",
        "no": "否",
        "enter_length": "请输入长度（米）。例如：1.2",
        "enter_width": "请输入宽度（米）。例如：0.8",
        "calc_ready": "计算已完成。请选择操作：",
        "order_btn": "下单",
        "menu_btn": "菜单",
        "enter_email": "请输入您的邮箱：",
        "bad_email": "邮箱格式不正确，请重试。",
        "enter_name": "请输入您的姓名：",
        "confirm_order": "请检查订单信息并确认发送：",
        "confirm_btn": "确认",
        "cancel_btn": "取消",
        "order_sent": "订单申请已发送。谢谢！",
        "order_failed": "邮件发送失败：{error}",
        "cancelled": "操作已取消，返回菜单。",
        "back_to_menu": "主菜单。请选择操作：",
        "main_menu_hint": "点击“新计算”开始。",
        "new_calc_btn": "新计算",
        "bad_number": "数字格式不正确，请重新输入。",
        "bad_positive_number": "数字必须大于 0，请重试。",
        "excel_error": "读取 Excel 出错：{error}",
        "internal_error": "发生内部错误。请尝试 /start",
        "pickup_city_saved": "提货城市：{city}",
        "stackable_yes": "是",
        "stackable_no": "否",
        "result_title": "计算结果",
        "result_template": (
            "<b>{title}</b>\n\n"
            "提货城市：{city}\n"
            "重量：{weight_kg:.2f} kg\n"
            "实际体积：{actual_volume:.3f} m³\n"
            "可堆叠：{stackable_text}\n"
            "重量体积：{weight_volume:.3f} m³\n"
            "不可堆叠体积：{nonstack_volume:.3f} m³\n"
            "铁路计费体积：{rail_volume:.3f} m³\n\n"
            "铁路基础费：{rail_base:.2f} USD\n"
            "单证费：{docs_fee_usd:.2f} USD\n"
            "铁路合计：{rail_total:.2f} USD\n\n"
            "汽车按体积：{auto_by_m3:.2f} USD\n"
            "汽车按吨位：{auto_by_ton:.2f} USD\n"
            "汽车基础费：{auto_base:.2f} USD\n"
            "代理费：{auto_agent_fee_usd:.2f} USD\n"
            "提货费：{pickup_fee_usd:.2f} USD\n"
            "汽车合计：{auto_total:.2f} USD\n\n"
            "<b>总计：{total:.2f} USD</b>"
        ),
        "order_summary": (
            "姓名：{name}\n"
            "邮箱：{email}\n"
            "提货城市：{city}\n"
            "重量：{weight_kg:.2f} kg\n"
            "实际体积：{actual_volume:.3f} m³\n"
            "可堆叠：{stackable_text}\n"
            "长度：{length_display}\n"
            "宽度：{width_display}\n"
            "铁路计费体积：{rail_volume:.3f} m³\n"
            "总计：{total:.2f} USD"
        ),
        "email_subject": "新的 LCL 中国 → 莫斯科订单申请",
        "email_body": (
            "来自 Telegram 机器人的新申请\n\n"
            "姓名：{name}\n"
            "客户邮箱：{email}\n"
            "语言：{lang}\n"
            "提货城市：{city}\n"
            "重量：{weight_kg:.2f} kg\n"
            "实际体积：{actual_volume:.3f} m³\n"
            "可堆叠：{stackable_text}\n"
            "长度：{length_display}\n"
            "宽度：{width_display}\n"
            "重量体积：{weight_volume:.3f} m³\n"
            "不可堆叠体积：{nonstack_volume:.3f} m³\n"
            "铁路计费体积：{rail_volume:.3f} m³\n"
            "铁路基础费：{rail_base:.2f} USD\n"
            "铁路合计：{rail_total:.2f} USD\n"
            "汽车按体积：{auto_by_m3:.2f} USD\n"
            "汽车按吨位：{auto_by_ton:.2f} USD\n"
            "汽车基础费：{auto_base:.2f} USD\n"
            "代理费：{auto_agent_fee_usd:.2f} USD\n"
            "提货费：{pickup_fee_usd:.2f} USD\n"
            "汽车合计：{auto_total:.2f} USD\n"
            "总计：{total:.2f} USD\n"
        ),
    },
}

# =========================
# STATE
# =========================
class CalcStates(StatesGroup):
    waiting_for_pickup_city = State()
    waiting_for_weight = State()
    waiting_for_actual_volume = State()
    waiting_for_stackable = State()
    waiting_for_length = State()
    waiting_for_width = State()
    waiting_for_calc_confirmation = State()


class OrderStates(StatesGroup):
    waiting_for_email = State()
    waiting_for_name = State()
    waiting_for_confirmation = State()


# =========================
# DATA MODELS
# =========================
@dataclass
class AppData:
    cities: list[str]
    city_rates: dict[str, dict[str, float]]
    settings: dict[str, float]


# =========================
# GLOBAL RUNTIME DATA
# =========================
USER_LANGS: dict[int, str] = {}
APP_DATA: AppData | None = None


# =========================
# HELPERS
# =========================
def get_lang(user_id: int) -> str:
    return USER_LANGS.get(user_id, "ru")


def t(user_id: int, key: str, **kwargs: Any) -> str:
    lang = get_lang(user_id)
    text = TEXTS.get(lang, TEXTS["ru"]).get(key, key)
    return text.format(**kwargs) if kwargs else text


def parse_bool_active(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    value_str = str(value).strip().lower()
    return value_str in {"1", "true", "yes", "y", "да", "активен", "active"}


def parse_positive_float(raw: str) -> float:
    cleaned = raw.strip().replace(" ", "").replace(",", ".")
    value = float(cleaned)
    if value <= 0:
        raise ValueError("Value must be > 0")
    return value


def is_valid_email(email: str) -> bool:
    return bool(EMAIL_RE.fullmatch(email.strip()))


def smtp_is_configured() -> bool:
    return all([SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM, ORDER_RECEIVER_EMAIL])


def create_language_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Русский", callback_data="lang:ru")
    builder.button(text="English", callback_data="lang:en")
    builder.button(text="中文", callback_data="lang:cn")
    builder.adjust(1)
    return builder.as_markup()


def create_city_keyboard(cities: list[str]) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    for city in cities:
        builder.add(KeyboardButton(text=city))
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)


def create_stackable_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=t(user_id, "yes")))
    builder.add(KeyboardButton(text=t(user_id, "no")))
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)


def create_main_menu_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=t(user_id, "new_calc_btn")))
    return builder.as_markup(resize_keyboard=True)


def create_calc_actions_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=t(user_id, "order_btn")))
    builder.add(KeyboardButton(text=t(user_id, "menu_btn")))
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)


def create_confirm_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=t(user_id, "confirm_btn")))
    builder.add(KeyboardButton(text=t(user_id, "cancel_btn")))
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)


def load_excel_data(file_path: str) -> AppData:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")

    pickup_df = pd.read_excel(path, sheet_name="pickup_cities", engine="openpyxl")
    settings_df = pd.read_excel(path, sheet_name="settings", engine="openpyxl")

    required_pickup_columns = {"city", "auto_rate_usd_per_m3", "auto_rate_usd_per_ton", "active"}
    missing_pickup_columns = required_pickup_columns - set(pickup_df.columns)
    if missing_pickup_columns:
        raise ValueError(f"Missing columns in pickup_cities: {sorted(missing_pickup_columns)}")

    required_settings_columns = {"key", "value"}
    missing_settings_columns = required_settings_columns - set(settings_df.columns)
    if missing_settings_columns:
        raise ValueError(f"Missing columns in settings: {sorted(missing_settings_columns)}")

    settings: dict[str, float] = {}
    for _, row in settings_df.iterrows():
        key = str(row["key"]).strip()
        if not key:
            continue
        try:
            settings[key] = float(str(row["value"]).strip().replace(",", "."))
        except Exception as exc:
            raise ValueError(f"Invalid numeric setting for '{key}': {row['value']}") from exc

    missing_settings = REQUIRED_SETTINGS - set(settings.keys())
    if missing_settings:
        raise ValueError(f"Missing required settings: {sorted(missing_settings)}")

    active_df = pickup_df[pickup_df["active"].apply(parse_bool_active)].copy()
    active_df["city"] = active_df["city"].astype(str).str.strip()
    active_df = active_df[active_df["city"] != ""]

    if active_df.empty:
        raise ValueError("Active pickup cities list is empty")

    city_rates: dict[str, dict[str, float]] = {}
    for _, row in active_df.iterrows():
        city = str(row["city"]).strip()
        try:
            city_rates[city] = {
                "auto_rate_usd_per_m3": float(row["auto_rate_usd_per_m3"]),
                "auto_rate_usd_per_ton": float(row["auto_rate_usd_per_ton"]),
            }
        except Exception as exc:
            raise ValueError(f"Invalid rates for city '{city}'") from exc

    cities = sorted(city_rates.keys())
    return AppData(cities=cities, city_rates=city_rates, settings=settings)


def calculate_lcl(data: dict[str, Any], app_data: AppData) -> dict[str, Any]:
    city = data["pickup_city"]
    weight_kg = float(data["weight_kg"])
    actual_volume = float(data["actual_volume"])
    stackable = bool(data["stackable"])
    length = float(data.get("length") or 0)
    width = float(data.get("width") or 0)

    s = app_data.settings
    city_rate = app_data.city_rates[city]

    volumetric_coef = s["volumetric_coef_kg_per_m3"]
    weight_volume = weight_kg / volumetric_coef
    nonstack_volume = 0.0 if stackable else (length * width * 2.4)
    rail_volume = max(actual_volume, weight_volume, nonstack_volume)

    if rail_volume <= 1:
        rail_base = s["rail_tariff_0_1_fixed_usd"]
    elif rail_volume <= 5:
        rail_base = rail_volume * s["rail_tariff_1_5_usd_per_m3"]
    elif rail_volume <= 10:
        rail_base = rail_volume * s["rail_tariff_5_10_usd_per_m3"]
    else:
        rail_base = rail_volume * s["rail_tariff_10_plus_usd_per_m3"]

    docs_fee_usd = s["docs_fee_usd"]
    rail_total = rail_base + docs_fee_usd

    auto_by_m3 = actual_volume * city_rate["auto_rate_usd_per_m3"]
    auto_by_ton = (weight_kg / 1000.0) * city_rate["auto_rate_usd_per_ton"]
    auto_base = max(auto_by_m3, auto_by_ton)

    auto_agent_fee_usd = s["auto_agent_fee_cny"] / s["fx_cny_per_usd"]
    pickup_fee_cny = s["pickup_small_cny"] if actual_volume <= 1 else s["pickup_large_cny"]
    pickup_fee_usd = pickup_fee_cny / s["fx_cny_per_usd"]
    auto_total = auto_base + auto_agent_fee_usd + pickup_fee_usd

    total = rail_total + auto_total

    return {
        "pickup_city": city,
        "weight_kg": weight_kg,
        "actual_volume": actual_volume,
        "stackable": stackable,
        "length": length if not stackable else None,
        "width": width if not stackable else None,
        "weight_volume": weight_volume,
        "nonstack_volume": nonstack_volume,
        "rail_volume": rail_volume,
        "rail_base": rail_base,
        "docs_fee_usd": docs_fee_usd,
        "rail_total": rail_total,
        "auto_by_m3": auto_by_m3,
        "auto_by_ton": auto_by_ton,
        "auto_base": auto_base,
        "auto_agent_fee_usd": auto_agent_fee_usd,
        "pickup_fee_usd": pickup_fee_usd,
        "auto_total": auto_total,
        "total": total,
    }


def format_calc_result(user_id: int, result: dict[str, Any]) -> str:
    stackable_text = t(user_id, "stackable_yes") if result["stackable"] else t(user_id, "stackable_no")
    return t(
        user_id,
        "result_template",
        title=t(user_id, "result_title"),
        city=result["pickup_city"],
        weight_kg=result["weight_kg"],
        actual_volume=result["actual_volume"],
        stackable_text=stackable_text,
        weight_volume=result["weight_volume"],
        nonstack_volume=result["nonstack_volume"],
        rail_volume=result["rail_volume"],
        rail_base=result["rail_base"],
        docs_fee_usd=result["docs_fee_usd"],
        rail_total=result["rail_total"],
        auto_by_m3=result["auto_by_m3"],
        auto_by_ton=result["auto_by_ton"],
        auto_base=result["auto_base"],
        auto_agent_fee_usd=result["auto_agent_fee_usd"],
        pickup_fee_usd=result["pickup_fee_usd"],
        auto_total=result["auto_total"],
        total=result["total"],
    )


def format_order_summary(user_id: int, result: dict[str, Any], name: str, email: str) -> str:
    stackable_text = t(user_id, "stackable_yes") if result["stackable"] else t(user_id, "stackable_no")
    length_display = "-" if result["length"] is None else f"{result['length']:.3f} m"
    width_display = "-" if result["width"] is None else f"{result['width']:.3f} m"
    return t(
        user_id,
        "order_summary",
        name=name,
        email=email,
        city=result["pickup_city"],
        weight_kg=result["weight_kg"],
        actual_volume=result["actual_volume"],
        stackable_text=stackable_text,
        length_display=length_display,
        width_display=width_display,
        rail_volume=result["rail_volume"],
        total=result["total"],
    )


def send_email_sync(subject: str, body: str) -> tuple[bool, str]:
    if not smtp_is_configured():
        return False, "SMTP not configured"

    try:
        port = int(SMTP_PORT)
        msg = MIMEMultipart()
        msg["From"] = SMTP_FROM
        msg["To"] = ORDER_RECEIVER_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        if port == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, port, timeout=20) as server:
                if SMTP_USER and SMTP_PASSWORD:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, port, timeout=20) as server:
                server.ehlo()
                if port == 587:
                    server.starttls()
                    server.ehlo()
                if SMTP_USER and SMTP_PASSWORD:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg)

        return True, "OK"
    except Exception as exc:
        return False, str(exc)


async def send_email_async(subject: str, body: str) -> tuple[bool, str]:
    return await asyncio.to_thread(send_email_sync, subject, body)


def ensure_app_data() -> AppData:
    global APP_DATA
    if APP_DATA is None:
        APP_DATA = load_excel_data(RATES_FILE)
    return APP_DATA


# =========================
# HANDLERS
# =========================
async def show_language_selection(target: Message | CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    text = TEXTS["ru"]["choose_language"]
    markup = create_language_keyboard()

    if isinstance(target, CallbackQuery):
        await target.message.answer(text, reply_markup=markup)
    else:
        await target.answer(text, reply_markup=markup)


async def show_main_menu(message: Message, user_id: int) -> None:
    await message.answer(
        f"{t(user_id, 'back_to_menu')}\n{t(user_id, 'main_menu_hint')}",
        reply_markup=create_main_menu_keyboard(user_id),
    )


async def start_calculation(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    try:
        app_data = ensure_app_data()
    except Exception as exc:
        logging.exception("Excel load error")
        await message.answer(t(user_id, "excel_error", error=str(exc)), reply_markup=ReplyKeyboardRemove())
        return

    if not app_data.cities:
        await message.answer(t(user_id, "no_cities"), reply_markup=ReplyKeyboardRemove())
        return

    await state.set_state(CalcStates.waiting_for_pickup_city)
    await message.answer(
        t(user_id, "choose_city"),
        reply_markup=create_city_keyboard(app_data.cities),
    )


async def cmd_start(message: Message, state: FSMContext) -> None:
    await show_language_selection(message, state)


async def choose_language(callback: CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    lang = callback.data.split(":", 1)[1]
    if lang not in ALLOWED_LANGS:
        await callback.answer("Unsupported language", show_alert=True)
        return

    USER_LANGS[user_id] = lang
    await state.clear()

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass

    await callback.message.answer(
        f"{t(user_id, 'language_set')}\n{t(user_id, 'start_intro')}",
        reply_markup=create_main_menu_keyboard(user_id),
    )
    await callback.message.answer(t(user_id, "main_menu_hint"))
    await callback.answer()


async def new_calc_from_menu(message: Message, state: FSMContext) -> None:
    await start_calculation(message, state)


async def pickup_city_chosen(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    app_data = ensure_app_data()
    city = message.text.strip()

    if city not in app_data.city_rates:
        await message.answer(
            t(user_id, "choose_city"),
            reply_markup=create_city_keyboard(app_data.cities),
        )
        return

    await state.update_data(pickup_city=city)
    await state.set_state(CalcStates.waiting_for_weight)
    await message.answer(
        f"{t(user_id, 'pickup_city_saved', city=city)}\n{t(user_id, 'enter_weight')}",
        reply_markup=ReplyKeyboardRemove(),
    )


async def weight_received(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    try:
        weight_kg = parse_positive_float(message.text)
    except Exception:
        await message.answer(t(user_id, "bad_positive_number"))
        return

    await state.update_data(weight_kg=weight_kg)
    await state.set_state(CalcStates.waiting_for_actual_volume)
    await message.answer(t(user_id, "enter_volume"))


async def actual_volume_received(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    try:
        actual_volume = parse_positive_float(message.text)
    except Exception:
        await message.answer(t(user_id, "bad_positive_number"))
        return

    await state.update_data(actual_volume=actual_volume)
    await state.set_state(CalcStates.waiting_for_stackable)
    await message.answer(
        t(user_id, "choose_stackable"),
        reply_markup=create_stackable_keyboard(user_id),
    )


async def stackable_received(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    text = message.text.strip()
    yes_text = t(user_id, "yes")
    no_text = t(user_id, "no")

    if text == yes_text:
        await state.update_data(stackable=True, length=None, width=None)
        await finalize_calculation(message, state)
        return

    if text == no_text:
        await state.update_data(stackable=False)
        await state.set_state(CalcStates.waiting_for_length)
        await message.answer(t(user_id, "enter_length"), reply_markup=ReplyKeyboardRemove())
        return

    await message.answer(
        t(user_id, "choose_stackable"),
        reply_markup=create_stackable_keyboard(user_id),
    )


async def length_received(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    try:
        length = parse_positive_float(message.text)
    except Exception:
        await message.answer(t(user_id, "bad_positive_number"))
        return

    await state.update_data(length=length)
    await state.set_state(CalcStates.waiting_for_width)
    await message.answer(t(user_id, "enter_width"))


async def width_received(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    try:
        width = parse_positive_float(message.text)
    except Exception:
        await message.answer(t(user_id, "bad_positive_number"))
        return

    await state.update_data(width=width)
    await finalize_calculation(message, state)


async def finalize_calculation(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    try:
        app_data = ensure_app_data()
        raw_data = await state.get_data()
        result = calculate_lcl(raw_data, app_data)
    except Exception as exc:
        logging.exception("Calculation error")
        await state.clear()
        await message.answer(t(user_id, "internal_error"))
        return

    await state.update_data(calc_result=result)
    await state.set_state(CalcStates.waiting_for_calc_confirmation)

    await message.answer(
        format_calc_result(user_id, result),
        reply_markup=create_calc_actions_keyboard(user_id),
    )
    await message.answer(t(user_id, "calc_ready"))


async def calc_actions(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    text = message.text.strip()

    if text == t(user_id, "order_btn"):
        data = await state.get_data()
        if "calc_result" not in data:
            await message.answer(t(user_id, "internal_error"))
            await show_main_menu(message, user_id)
            return
        await state.set_state(OrderStates.waiting_for_email)
        await message.answer(t(user_id, "enter_email"), reply_markup=ReplyKeyboardRemove())
        return

    if text == t(user_id, "menu_btn"):
        await state.clear()
        await show_main_menu(message, user_id)
        return

    await message.answer(t(user_id, "calc_ready"), reply_markup=create_calc_actions_keyboard(user_id))


async def order_email_received(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    email = message.text.strip()
    if not is_valid_email(email):
        await message.answer(t(user_id, "bad_email"))
        return

    await state.update_data(customer_email=email)
    await state.set_state(OrderStates.waiting_for_name)
    await message.answer(t(user_id, "enter_name"))


async def order_name_received(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    name = message.text.strip()
    if not name:
        await message.answer(t(user_id, "enter_name"))
        return

    data = await state.get_data()
    calc_result = data.get("calc_result")
    if not calc_result:
        await state.clear()
        await message.answer(t(user_id, "internal_error"))
        await show_main_menu(message, user_id)
        return

    await state.update_data(customer_name=name)
    summary = format_order_summary(user_id, calc_result, name, data["customer_email"])
    await state.set_state(OrderStates.waiting_for_confirmation)
    await message.answer(
        f"{t(user_id, 'confirm_order')}\n\n{summary}",
        reply_markup=create_confirm_keyboard(user_id),
    )


async def order_confirmation(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    text = message.text.strip()

    if text == t(user_id, "cancel_btn"):
        await state.clear()
        await message.answer(t(user_id, "cancelled"), reply_markup=ReplyKeyboardRemove())
        await show_main_menu(message, user_id)
        return

    if text != t(user_id, "confirm_btn"):
        await message.answer(t(user_id, "confirm_order"), reply_markup=create_confirm_keyboard(user_id))
        return

    data = await state.get_data()
    calc_result = data.get("calc_result")
    customer_email = data.get("customer_email")
    customer_name = data.get("customer_name")

    if not calc_result or not customer_email or not customer_name:
        await state.clear()
        await message.answer(t(user_id, "internal_error"), reply_markup=ReplyKeyboardRemove())
        await show_main_menu(message, user_id)
        return

    stackable_text = t(user_id, "stackable_yes") if calc_result["stackable"] else t(user_id, "stackable_no")
    length_display = "-" if calc_result["length"] is None else f"{calc_result['length']:.3f} m"
    width_display = "-" if calc_result["width"] is None else f"{calc_result['width']:.3f} m"

    subject = t(user_id, "email_subject")
    body = t(
        user_id,
        "email_body",
        name=customer_name,
        email=customer_email,
        lang=get_lang(user_id),
        city=calc_result["pickup_city"],
        weight_kg=calc_result["weight_kg"],
        actual_volume=calc_result["actual_volume"],
        stackable_text=stackable_text,
        length_display=length_display,
        width_display=width_display,
        weight_volume=calc_result["weight_volume"],
        nonstack_volume=calc_result["nonstack_volume"],
        rail_volume=calc_result["rail_volume"],
        rail_base=calc_result["rail_base"],
        rail_total=calc_result["rail_total"],
        auto_by_m3=calc_result["auto_by_m3"],
        auto_by_ton=calc_result["auto_by_ton"],
        auto_base=calc_result["auto_base"],
        auto_agent_fee_usd=calc_result["auto_agent_fee_usd"],
        pickup_fee_usd=calc_result["pickup_fee_usd"],
        auto_total=calc_result["auto_total"],
        total=calc_result["total"],
    )

    ok, error = await send_email_async(subject, body)
    await state.clear()

    if ok:
        await message.answer(t(user_id, "order_sent"), reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer(t(user_id, "order_failed", error=error), reply_markup=ReplyKeyboardRemove())

    await show_main_menu(message, user_id)


async def fallback_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    current_state = await state.get_state()

    if current_state is None:
        await show_main_menu(message, user_id)
        return

    await message.answer(t(user_id, "internal_error"))


# =========================
# ROUTER SETUP
# =========================
def register_handlers(dp: Dispatcher) -> None:
    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_start, Command("menu"))
    dp.callback_query.register(choose_language, F.data.startswith("lang:"))

    dp.message.register(new_calc_from_menu, F.text.in_({TEXTS['ru']['new_calc_btn'], TEXTS['en']['new_calc_btn'], TEXTS['cn']['new_calc_btn']}))

    dp.message.register(pickup_city_chosen, CalcStates.waiting_for_pickup_city)
    dp.message.register(weight_received, CalcStates.waiting_for_weight)
    dp.message.register(actual_volume_received, CalcStates.waiting_for_actual_volume)
    dp.message.register(stackable_received, CalcStates.waiting_for_stackable)
    dp.message.register(length_received, CalcStates.waiting_for_length)
    dp.message.register(width_received, CalcStates.waiting_for_width)
    dp.message.register(calc_actions, CalcStates.waiting_for_calc_confirmation)

    dp.message.register(order_email_received, OrderStates.waiting_for_email)
    dp.message.register(order_name_received, OrderStates.waiting_for_name)
    dp.message.register(order_confirmation, OrderStates.waiting_for_confirmation)

    dp.message.register(fallback_handler)


# =========================
# MAIN
# =========================
async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    logging.info("RUNNING NEW BOT.PY VERSION")

    global APP_DATA
    APP_DATA = load_excel_data(RATES_FILE)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    dp = Dispatcher(storage=MemoryStorage())
    register_handlers(dp)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("Webhook removed, starting polling")
        await dp.start_polling(bot)

    except Exception as e:
        logging.exception(f"Bot crashed: {e}")