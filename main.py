import asyncio
import os
import random
import sqlite3
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, List, Union
import re
from collections import defaultdict
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from functools import wraps
from datetime import datetime, timedelta
import datetime as dt
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
import html
from aiogram.utils.markdown import hbold
from aiogram.enums import ParseMode
from aiogram.fsm.storage.base import StorageKey
from aiogram import BaseMiddleware



# === Настройки ===
BOT_TOKEN = "8441491418:AAFkXB6TjuBPtPj-zD2vIsaMiI0NyCpX8Uk"
ADMIN_IDS = [7183114490, 6556149989]
ADMIN_SESSION_TIMEOUT = 3600
ADMIN_PASSWORD = "admin123"

# Интервал для проверки лайков (в секундах)
NOTIFICATION_INTERVAL = 3 * 3600

GENDERS = ["Мужской", "Женский", "Другое"]
SEEKING_OPTIONS = ["Мужчин", "Женщин", "Всех"]

# === Настройка логгера ===
USER_LOGS_DIR = "user_logs"
os.makedirs(USER_LOGS_DIR, exist_ok=True)

error_logger = logging.getLogger('error_logger')
info_logger = logging.getLogger('info_logger')
warning_logger = logging.getLogger('warning_logger')

error_logger.setLevel(logging.ERROR)
info_logger.setLevel(logging.INFO)
warning_logger.setLevel(logging.WARNING)

for logger in [error_logger, info_logger, warning_logger]:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

error_handler = RotatingFileHandler(
    os.path.join(USER_LOGS_DIR, 'errors.log'),
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding='utf-8'
)
info_handler = RotatingFileHandler(
    os.path.join(USER_LOGS_DIR, 'info.log'),
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding='utf-8'
)
warning_handler = RotatingFileHandler(
    os.path.join(USER_LOGS_DIR, 'warnings.log'),
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding='utf-8'
)
console_handler = logging.StreamHandler()
# --- User actions logger ---
USER_LOGS_DIR = "user_logs"
os.makedirs(USER_LOGS_DIR, exist_ok=True)

user_action_logger = logging.getLogger('user_actions')
user_action_logger.setLevel(logging.INFO)
user_action_handler = RotatingFileHandler(
    os.path.join(USER_LOGS_DIR, 'user_actions.log'),
    maxBytes=20 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8'
)
# basic formatter for user actions: include user context if available via logging Filter
user_action_handler.setFormatter(logging.Formatter('%(asctime)s - USER[%(user_id)s:%(username)s] - %(message)s', datefmt='%Y-%m-%d %H:%M'))
user_action_logger.addHandler(user_action_handler)
# Also log to console so devs see actions while running locally
try:
    user_action_logger.addHandler(console_handler)
except Exception:
    pass

# Attach the same UserContextFilter to user_action_logger (UserContextFilter defined later in file)
# We'll add the filter after the class definition if necessary.
console_handler.setLevel(logging.INFO)
console_handler.encoding = 'utf-8'


class SafeFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'user_id'):
            record.user_id = 'SYSTEM'
        if not hasattr(record, 'username'):
            record.username = 'SYSTEM'
        try:
            return super().format(record)
        except Exception as e:
            return f"LOGGING ERROR: {e}"


formatter = SafeFormatter(
    '%(asctime)s - %(levelname)s - USER[%(user_id)s:%(username)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M'
)

for handler in [error_handler, info_handler, warning_handler, console_handler]:
    handler.setFormatter(formatter)

error_logger.addHandler(error_handler)
error_logger.addHandler(console_handler)
info_logger.addHandler(info_handler)
info_logger.addHandler(console_handler)
warning_logger.addHandler(warning_handler)
warning_logger.addHandler(console_handler)


class UserContextFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'user_id'):
            record.user_id = 'SYSTEM'
        if not hasattr(record, 'username'):
            record.username = 'SYSTEM'
        return True


for logger in [error_logger, info_logger, warning_logger]:
    logger.addFilter(UserContextFilter())

# === Районы и категории ===

try:
    user_action_logger.addFilter(UserContextFilter())
except Exception:
    pass
DISTRICTS = [
    "Новоленино", "Октябрьский", "Правобережный", "Центр",
    "Солнечный", "Юбилейный", "Энергетиково", "Свердловский",
    "Первомайский", "Рабочее", "Синюшина гора"
]
MEETING_TYPES = ["Прогулка", "Общение", "Отношения", "Тусовки", "Без разницы"]

# === Антиспам система ===
user_requests = defaultdict(list)
MAX_REQUESTS_PER_MINUTE = 20

admin_sessions = {}
user_status_cache = {}
STATUS_CACHE_TIMEOUT = 60
user_activity_cache = {}
ACTIVITY_TIMEOUT = 60
anonymous_chats = {}

# Очередь для анонимного чата
waiting_for_chat = {}  # user_id: timestamp


async def cleanup_rate_limits_and_sessions():
    while True:
        now = time.time()
        for user_id in list(user_requests.keys()):
            user_requests[user_id] = [t for t in user_requests[user_id] if now - t < 60]
            if not user_requests[user_id]:
                del user_requests[user_id]
        expired_sessions = [uid for uid, exp in admin_sessions.items() if now > exp]
        for uid in expired_sessions:
            del admin_sessions[uid]
        await asyncio.sleep(300)


async def cleanup_caches():
    while True:
        now = time.time()
        for user_id in list(user_requests.keys()):
            user_requests[user_id] = [t for t in user_requests[user_id] if now - t < 60]
            if not user_requests[user_id]:
                del user_requests[user_id]
                rate_limit_wait_times.pop(user_id, None)
        expired_sessions = [uid for uid, exp in admin_sessions.items() if now > exp]
        for uid in expired_sessions:
            del admin_sessions[uid]
        expired_statuses = [uid for uid, data in user_status_cache.items() if
                            now - data['timestamp'] > STATUS_CACHE_TIMEOUT]
        for uid in expired_statuses:
            del user_status_cache[uid]
        expired_activities = [uid for uid, timestamp in user_activity_cache.items() if
                              now - timestamp > ACTIVITY_TIMEOUT]
        for uid in expired_activities:
            del user_activity_cache[uid]
        await asyncio.sleep(60)


async def send_notifications():
    while True:
        await asyncio.sleep(NOTIFICATION_INTERVAL)
        now = dt.datetime.now()
        check_time = (now - timedelta(hours=3)).isoformat()

        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT p.user_id, COUNT(r.id) as like_count
                FROM profiles p
                JOIN reactions r ON p.id = r.to_profile_id
                WHERE r.reaction_type = 'like' AND r.reaction_date > ?
                GROUP BY p.user_id
                HAVING like_count >= 1
            ''', (check_time,))
            notifications = cursor.fetchall()
            info_logger.info(f"Found {len(notifications)} users to notify about likes")

            for user_id, like_count in notifications:
                if db.is_notification_enabled(user_id):
                    try:
                        keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="👀 Посмотреть лайки", callback_data="view_likes")]
                        ])
                        await bot.send_message(
                            user_id,
                            f"🔔 За последние 3 часа вашу анкету лайкнули {like_count} раз!",
                            reply_markup=keyboard
                        )
                        info_logger.info(f"Notification sent: user_id={user_id}, likes={like_count}")
                    except Exception as e:
                        error_logger.error(f"Failed to send notification to user_id={user_id}: {e}")


async def check_user_blocks():
    while True:
        info_logger.info("Starting user block check...")
        users_to_check = db.get_all_users()
        for user in users_to_check:
            user_id = user['user_id']
            try:
                await bot.send_chat_action(user_id, "typing")
            except TelegramForbiddenError:
                info_logger.info(f"User {user_id} blocked the bot. Deleting profile...")
                db.delete_user_data(user_id)
            except Exception as e:
                error_logger.error(f"Error checking user {user_id}: {e}")
        await asyncio.sleep(3600)


async def check_for_chat_partners():
    while True:
        now = time.time()
        # Clean up expired waiters
        for user_id, start_time in list(waiting_for_chat.items()):
            if now - start_time > 300:  # 5 минут
                try:
                    await bot.send_message(user_id,
                                           "😔 Не удалось найти собеседника за 5 минут. Попробуйте снова позже.",
                                           reply_markup=get_anonymous_chat_keyboard())
                except TelegramForbiddenError:
                    info_logger.info(f"User {user_id} blocked the bot, cannot send timeout message.")
                except Exception as e:
                    error_logger.error(f"Failed to send timeout message to user {user_id}: {e}")
                finally:
                    if user_id in waiting_for_chat:
                        del waiting_for_chat[user_id]

        # Match waiting users
        user_ids = list(waiting_for_chat.keys())
        random.shuffle(user_ids)

        if len(user_ids) >= 2:
            for i in range(0, len(user_ids) - 1, 2):
                user1_id = user_ids[i]
                user2_id = user_ids[i + 1]

                # Double check they are still waiting
                if user1_id in waiting_for_chat and user2_id in waiting_for_chat:
                    try:
                        db.start_anonymous_chat(user1_id, user2_id)
                        anonymous_chats[user1_id] = user2_id
                        anonymous_chats[user2_id] = user1_id

                        # Set states correctly using the dispatcher's storage
                        # Для пользователя 1
                        user1_bot = Bot(token=BOT_TOKEN)
                        # Исправлено на использование правильного объекта StorageKey
                        user1_dp_storage_key = StorageKey(bot_id=user1_bot.id, chat_id=user1_id, user_id=user1_id)
                        await dp.storage.set_state(key=user1_dp_storage_key, state=AnonymousChat.in_chat)
                        # Для пользователя 2
                        user2_bot = Bot(token=BOT_TOKEN)
                        # Исправлено на использование правильного объекта StorageKey
                        user2_dp_storage_key = StorageKey(bot_id=user2_bot.id, chat_id=user2_id, user_id=user2_id)
                        await dp.storage.set_state(key=user2_dp_storage_key, state=AnonymousChat.in_chat)

                        await bot.send_message(user1_id,
                                               "Вы нашли собеседника! Начните общение. Чтобы выйти, используйте команду /stopchat",
                                               reply_markup=types.ReplyKeyboardRemove())
                        await bot.send_message(user2_id,
                                               "Вы нашли собеседника! Начните общение. Чтобы выйти, используйте команду /stopchat",
                                               reply_markup=types.ReplyKeyboardRemove())

                        del waiting_for_chat[user1_id]
                        del waiting_for_chat[user2_id]
                        info_logger.info(f"Anonymous chat started between {user1_id} and {user2_id}")

                    except Exception as e:
                        error_logger.error(f"Failed to start anonymous chat between {user1_id} and {user2_id}: {e}")

        await asyncio.sleep(5)  # Check every 5 seconds


class UserStates(StatesGroup):
    waiting_for_post_text = State()
    waiting_for_unblock_profile_id = State()
    waiting_for_report_reason = State()
    waiting_for_name = State()
    waiting_for_age = State()
    waiting_for_gender = State()
    waiting_for_seeking = State()
    waiting_for_district = State()
    waiting_for_meeting_type = State()
    waiting_for_about = State()
    waiting_for_photo = State()
    waiting_for_edit_field = State()
    waiting_for_edit_value = State()
    waiting_for_anonymous_message = State()
    waiting_for_profile_id = State()
    waiting_for_block_user_id = State()
    waiting_for_admin_password = State()


class SupportForm(StatesGroup):
    bug_report = State()
    waiting_for_reply = State()


class AnonymousChat(StatesGroup):
    waiting_for_chat_gender = State()
    waiting_for_chat_age = State()
    waiting_for_chat_partner = State()
    in_chat = State()


class AdminPanel(StatesGroup):
    waiting_for_password = State()
    in_panel = State()
    viewing_reports = State()
    viewing_user_reports = State()
    waiting_for_user_id = State()
    waiting_for_post_text = State()
    viewing_bug_reports = State()
    replying_to_bug_report = State()


class Database:
    def __init__(self, db_path="dating_bot.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
        self.init_database()

    def init_database(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    registration_date TEXT,
                    is_blocked BOOLEAN DEFAULT 0,
                    warning_count INTEGER DEFAULT 0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    gender TEXT NOT NULL,
                    seeking TEXT NOT NULL,
                    district TEXT NOT NULL,
                    meeting_type TEXT NOT NULL,
                    about_text TEXT,
                    photo_file_id TEXT,
                    creation_date TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    keywords TEXT,
                    ignore_reactions BOOLEAN DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS reactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_user_id INTEGER,
                    to_profile_id INTEGER,
                    reaction_type TEXT,
                    reaction_date TEXT,
                    UNIQUE(from_user_id, to_profile_id),
                    FOREIGN KEY (from_user_id) REFERENCES users (user_id),
                    FOREIGN KEY (to_profile_id) REFERENCES profiles (id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_user_id INTEGER,
                    profile_id INTEGER,
                    report_date TEXT,
                    reason TEXT,
                    FOREIGN KEY (from_user_id) REFERENCES users (user_id),
                    FOREIGN KEY (profile_id) REFERENCES profiles (id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS anonymous_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_user_id INTEGER,
                    to_user_id INTEGER,
                    message_text TEXT,
                    send_date TEXT,
                    FOREIGN KEY (from_user_id) REFERENCES users (user_id),
                    FOREIGN KEY (to_user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS anonymous_chats (
                    user1_id INTEGER,
                    user2_id INTEGER,
                    chat_date TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    added_date TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    total_users INTEGER DEFAULT 0,
                    total_profiles INTEGER DEFAULT 0,
                    total_likes INTEGER DEFAULT 0,
                    total_matches INTEGER DEFAULT 0,
                    last_updated TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bug_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    bug_text TEXT,
                    created_at TEXT,
                    is_reviewed BOOLEAN DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS notification_settings (
                    user_id INTEGER PRIMARY KEY,
                    notifications_enabled BOOLEAN DEFAULT 1,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS complaints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reporter_user_id INTEGER,
                    reported_user_id INTEGER,
                    report_reason TEXT,
                    report_date TEXT,
                    is_reviewed BOOLEAN DEFAULT 0,
                    FOREIGN KEY (reporter_user_id) REFERENCES users(user_id),
                    FOREIGN KEY (reported_user_id) REFERENCES users(user_id)
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON profiles(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_reaction_from_user ON reactions(from_user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_reaction_to_profile ON reactions(to_profile_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_report_profile ON reports(profile_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bug_reports_user_id ON bug_reports(user_id)')
            conn.commit()

            cursor.execute("PRAGMA table_info(profiles)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'about_text' not in columns:
                cursor.execute("ALTER TABLE profiles ADD COLUMN about_text TEXT")
            if 'photo_file_id' not in columns:
                cursor.execute("ALTER TABLE profiles ADD COLUMN photo_file_id TEXT")
            if 'keywords' not in columns:
                cursor.execute("ALTER TABLE profiles ADD COLUMN keywords TEXT")
            if 'gender' not in columns:
                cursor.execute("ALTER TABLE profiles ADD COLUMN gender TEXT NOT NULL DEFAULT 'Other'")
            if 'seeking' not in columns:
                cursor.execute("ALTER TABLE profiles ADD COLUMN seeking TEXT NOT NULL DEFAULT 'All'")
            if 'ignore_reactions' not in columns:
                cursor.execute("ALTER TABLE profiles ADD COLUMN ignore_reactions BOOLEAN DEFAULT 0")

            cursor.execute("PRAGMA table_info(bug_reports)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'is_reviewed' not in columns:
                cursor.execute("ALTER TABLE bug_reports ADD COLUMN is_reviewed BOOLEAN DEFAULT 0")

            conn.commit()

    def get_all_users(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, username FROM users")
            return [{'user_id': r[0], 'username': r[1]} for r in cursor.fetchall()]

    def is_notification_enabled(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT notifications_enabled FROM notification_settings WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            if result is None:
                cursor.execute(
                    "INSERT OR IGNORE INTO notification_settings (user_id, notifications_enabled) VALUES (?, 1)",
                    (user_id,))
                conn.commit()
                return True
            return result[0] == 1

    def register_user(self, user_id: int, username: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            if cursor.fetchone():
                return True
            try:
                cursor.execute('''
                    INSERT INTO users (user_id, username, registration_date)
                    VALUES (?, ?, ?)
                ''', (user_id, username, datetime.now().isoformat()))
                conn.commit()
                info_logger.info(f"User registered: user_id={user_id}, username={username}")
                return True
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in register_user: {e}")
                return False

    def is_blocked(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] == 1 if result else False

    def block_user(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("UPDATE users SET is_blocked = 1 WHERE user_id = ?", (user_id,))
                cursor.execute("UPDATE profiles SET is_active = 0 WHERE user_id = ?", (user_id,))
                conn.commit()
                info_logger.info(f"User blocked: user_id={user_id}")
                return True
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in block_user: {e}")
                return False

    def create_profile(self, user_id: int, name: str, age: int, gender: str, seeking: str,
                       district: str, meeting_type: str, about_text: str, photo_file_id: str) -> bool:
        validation_errors = []
        if not (2 <= len(name) <= 30):
            validation_errors.append(f"Invalid name length: {len(name)} (must be 2-30 characters)")
        if not (12 <= age <= 99):
            validation_errors.append(f"Invalid age: {age} (must be 12-99)")
        if gender not in GENDERS:
            validation_errors.append(f"Invalid gender: {gender} (must be one of {GENDERS})")
        if seeking not in SEEKING_OPTIONS:
            validation_errors.append(f"Invalid seeking: {seeking} (must be one of {SEEKING_OPTIONS})")
        if district not in DISTRICTS:
            validation_errors.append(f"Invalid district: {district} (must be one of {DISTRICTS})")
        if meeting_type not in MEETING_TYPES:
            validation_errors.append(f"Invalid meeting_type: {meeting_type} (must be one of {MEETING_TYPES})")
        if not (len(about_text) <= 600):
            validation_errors.append(f"Invalid about_text length: {len(about_text)} (must be 600 characters)")
        if not photo_file_id:
            validation_errors.append("Missing photo_file_id")

        if validation_errors:
            error_logger.error(f"Invalid input for create_profile: user_id={user_id}, errors={validation_errors}")
            return False

        if self.is_blocked(user_id):
            warning_logger.warning(f"Blocked user attempted to create profile: user_id={user_id}")
            return False

        username = "Без username"
        self.register_user(user_id, username)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                cursor = conn.cursor()
                cursor.execute("UPDATE profiles SET is_active=0 WHERE user_id=?", (user_id,))
                cursor.execute('''
                    INSERT INTO profiles 
                    (user_id, name, age, gender, seeking, district, meeting_type, about_text, photo_file_id, creation_date, keywords, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ''', (user_id, name, age, gender, seeking, district, meeting_type, about_text, photo_file_id,
                      datetime.now().isoformat(), self.extract_keywords(about_text)))
                conn.commit()
                info_logger.info(f"Profile created/updated for user_id={user_id}")
                return True
            except sqlite3.Error as e:
                conn.rollback()
                error_logger.error(f"SQL error in create_profile: {e}")
                return False

    def extract_keywords(self, text: str) -> str:
        words = re.findall(r'\b[а-яё]+\b', text.lower())
        stop_words = {'я', 'мне', 'меня', 'мой', 'моя', 'мое', 'и', 'или', 'но', 'что', 'это', 'так', 'как', 'для',
                      'на', 'в', 'с', 'по', 'за', 'из', 'к', 'до', 'от', 'со', 'об', 'при'}
        keywords = [word for word in words if word not in stop_words and len(word) > 2]
        return ' '.join(set(keywords))

    def get_profile_by_user_id(self, user_id: int) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, name, age, gender, seeking, district, meeting_type, about_text, photo_file_id, creation_date, is_active
                FROM profiles WHERE user_id = ? AND is_active = 1
            ''', (user_id,))
            result = cursor.fetchone()
            if result:
                return {
                    'id': result[0], 'name': result[1], 'age': result[2],
                    'gender': result[3], 'seeking': result[4],
                    'district': result[5], 'meeting_type': result[6],
                    'about_text': result[7], 'photo_file_id': result[8],
                    'creation_date': result[9], 'is_active': result[10]
                }
            return None

    def get_profile_by_id(self, profile_id: int) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, user_id, name, age, gender, seeking, district, meeting_type, about_text, photo_file_id, creation_date, is_active
                FROM profiles WHERE id = ?
            ''', (profile_id,))
            result = cursor.fetchone()
            if result:
                return {
                    'id': result[0], 'user_id': result[1], 'name': result[2], 'age': result[3],
                    'gender': result[4], 'seeking': result[5], 'district': result[6],
                    'meeting_type': result[7], 'about_text': result[8], 'photo_file_id': result[9],
                    'creation_date': result[10], 'is_active': result[11]
                }
            return None

    def check_match(self, user_profile_id: int, other_profile_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT user_id FROM profiles WHERE id = ?", (user_profile_id,))
            user_id_res = cursor.fetchone()
            if not user_id_res: return False
            user_id = user_id_res[0]

            cursor.execute("SELECT user_id FROM profiles WHERE id = ?", (other_profile_id,))
            other_user_id_res = cursor.fetchone()
            if not other_user_id_res: return False
            other_user_id = other_user_id_res[0]

            cursor.execute('''
                SELECT 1 FROM reactions 
                WHERE from_user_id = ? AND to_profile_id = ? AND reaction_type = 'like'
            ''', (other_user_id, user_profile_id))

            is_liked_by_them = cursor.fetchone() is not None

            return is_liked_by_them

    def get_random_profiles(self, user_id: int, filters: dict = None, limit: int = 10) -> List[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT gender, seeking, age FROM profiles WHERE user_id = ? AND is_active = 1", (user_id,))
            user_data = cursor.fetchone()
            if not user_data:
                return []
            user_gender, user_seeking, user_age = user_data

            query = '''
                SELECT p.id, p.user_id, p.name, p.age, p.district, p.meeting_type,
                       p.about_text, p.photo_file_id, p.gender
                FROM profiles p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.user_id != ? AND p.is_active = 1 AND u.is_blocked = 0
                  AND p.id NOT IN (
                    SELECT r.to_profile_id FROM reactions r
                    WHERE r.from_user_id = ?
                  )
            '''
            params = [user_id, user_id]

            if user_seeking == "Мужчин":
                query += " AND p.gender = 'Мужской'"
            elif user_seeking == "Женщин":
                query += " AND p.gender = 'Женский'"

            seeking_map = {"Мужской": "Мужчин", "Женский": "Женщин"}
            user_gender_as_seeking = seeking_map.get(user_gender)
            if user_gender_as_seeking:
                query += f" AND (p.seeking = 'Всех' OR p.seeking = ?)"
                params.append(user_gender_as_seeking)

            query += " AND p.age BETWEEN ? AND ?"
            params.extend([user_age - 3, user_age + 3])

            if filters:
                if filters.get('meeting_type'):
                    query += " AND p.meeting_type = ?"
                    params.append(filters['meeting_type'])
                if filters.get('district'):
                    query += " AND p.district = ?"
                    params.append(filters['district'])

            query += " ORDER BY RANDOM() LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [
                {
                    'id': r[0], 'user_id': r[1], 'name': r[2], 'age': r[3],
                    'district': r[4], 'meeting_type': r[5], 'about_text': r[6],
                    'photo_file_id': r[7], 'gender': r[8]
                } for r in cursor.fetchall()
            ]

    def find_anonymous_partner(self, user_id: int, gender_filter: str) -> Optional[int]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            user_profile = self.get_profile_by_user_id(user_id)
            if not user_profile:
                return None

            user_age = user_profile['age']
            age_min = max(18, user_age - 3)
            age_max = user_age + 3

            query = '''
                SELECT p.user_id FROM profiles p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.user_id != ? AND p.is_active = 1 AND u.is_blocked = 0
            '''
            params = [user_id]

            if gender_filter:
                query += " AND p.gender = ?"
                params.append(gender_filter)

            query += " AND p.age BETWEEN ? AND ?"
            params.extend([age_min, age_max])

            query += " AND p.user_id NOT IN (SELECT user2_id FROM anonymous_chats WHERE user1_id = ?)"
            params.append(user_id)
            query += " AND p.user_id NOT IN (SELECT user1_id FROM anonymous_chats WHERE user2_id = ?)"
            params.append(user_id)

            query += " ORDER BY RANDOM() LIMIT 1"

            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else None

    def add_reaction(self, from_user_id: int, to_profile_id: int, reaction_type: str):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO reactions (from_user_id, to_profile_id, reaction_type, reaction_date)
                    VALUES (?, ?, ?, ?)
                ''', (from_user_id, to_profile_id, reaction_type, datetime.now().isoformat()))
                conn.commit()
                info_logger.info(
                    f"Reaction added: from_user_id={from_user_id}, to_profile_id={to_profile_id}, type={reaction_type}")
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in add_reaction: {e}")

    def get_user_likes(self, user_id: int) -> List[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            my_profile = self.get_profile_by_user_id(user_id)
            if not my_profile:
                return []
            my_profile_id = my_profile['id']

            cursor.execute('''
                SELECT p.id, p.name, p.age, p.district, p.user_id, u.username
                FROM reactions r
                JOIN profiles p ON r.from_user_id = p.user_id
                JOIN users u ON p.user_id = u.user_id
                WHERE r.to_profile_id = ? 
                  AND r.reaction_type = 'like'
                  AND p.is_active = 1
                  AND u.is_blocked = 0
                  AND EXISTS (
                      SELECT 1 FROM reactions r2 
                      WHERE r2.from_user_id = ? 
                        AND r2.to_profile_id = p.id
                        AND r2.reaction_type = 'like'
                  )
                ORDER BY r.reaction_date DESC
            ''', (my_profile_id, user_id))
            results = cursor.fetchall()
            return [
                {
                    'profile_id': r[0], 'name': r[1], 'age': r[2],
                    'district': r[3], 'user_id': r[4], 'username': r[5]
                } for r in results
            ]

    def send_anonymous_message(self, from_user_id: int, to_user_id: int, message_text: str):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO anonymous_messages (from_user_id, to_user_id, message_text, send_date)
                    VALUES (?, ?, ?, ?)
                ''', (from_user_id, to_user_id, message_text, datetime.now().isoformat()))
                conn.commit()
                info_logger.info(f"Anonymous message sent: from_user_id={from_user_id}, to_user_id={to_user_id}")
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in send_anonymous_message: {e}")

    def add_report(self, from_user_id: int, profile_id: int, reason: str):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                profile = self.get_profile_by_id(profile_id)
                if not profile:
                    warning_logger.warning(f"Attempt to report non-existent profile_id={profile_id}")
                    return

                reported_user_id = profile['user_id']
                cursor.execute('''
                    INSERT INTO complaints (reporter_user_id, reported_user_id, report_reason, report_date, is_reviewed)
                    VALUES (?, ?, ?, ?, 0)
                ''', (from_user_id, reported_user_id, reason, datetime.now().isoformat()))
                conn.commit()
                info_logger.info(
                    f"Complaint added: reporter_user_id={from_user_id}, reported_user_id={reported_user_id}, reason='{reason}'")
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in add_report: {e}")

    def get_unreviewed_complaints(self) -> List[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, reporter_user_id, reported_user_id, report_reason, report_date
                FROM complaints
                WHERE is_reviewed = 0
                ORDER BY report_date ASC
            ''')
            results = cursor.fetchall()
            return [{'id': r[0], 'reporter_user_id': r[1], 'reported_user_id': r[2], 'reason': r[3], 'date': r[4]} for r
                    in results]

    def get_complaints_for_user(self, reported_user_id: int) -> List[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT reporter_user_id, report_reason, report_date, is_reviewed
                FROM complaints
                WHERE reported_user_id = ?
                ORDER BY report_date ASC
            ''', (reported_user_id,))
            results = cursor.fetchall()
            return [{'reporter_user_id': r[0], 'reason': r[1], 'date': r[2], 'is_reviewed': bool(r[3])} for r in
                    results]

    def mark_complaint_as_reviewed(self, reported_user_id: int):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE complaints SET is_reviewed = 1 WHERE reported_user_id = ?
            ''', (reported_user_id,))
            conn.commit()
            info_logger.info(f"Complaints for user {reported_user_id} marked as reviewed.")

    def is_admin(self, user_id: int) -> bool:
        return user_id in ADMIN_IDS and not self.is_blocked(user_id)

    def add_admin(self, user_id: int, username: str):
        if user_id not in ADMIN_IDS:
            return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO admins (user_id, username, added_date)
                    VALUES (?, ?, ?)
                ''', (user_id, username, datetime.now().isoformat()))
                conn.commit()
                info_logger.info(f"Admin added: user_id={user_id}, username={username}")
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in add_admin: {e}")

    def get_statistics(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_blocked = 0")
            total_users = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM profiles WHERE is_active = 1")
            total_profiles = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM reactions WHERE reaction_type = 'like'")
            total_likes = cursor.fetchone()[0]
            cursor.execute('''
                SELECT COUNT(*) / 2 FROM (
                    SELECT r1.from_user_id as user1, p2.user_id as user2
                    FROM reactions r1
                    JOIN profiles p1 ON r1.to_profile_id = p1.id
                    JOIN reactions r2 ON r2.from_user_id = p1.user_id
                    JOIN profiles p2 ON r2.to_profile_id = p2.id
                    WHERE r1.reaction_type = 'like' AND r2.reaction_type = 'like'
                      AND r1.from_user_id = p2.user_id
                      AND p1.user_id = r2.from_user_id
                      AND r1.from_user_id < p1.user_id
                )
            ''')
            total_matches = cursor.fetchone()[0]
            return {
                'total_users': total_users,
                'total_profiles': total_profiles,
                'total_likes': total_likes,
                'total_matches': total_matches or 0
            }

    def get_district_statistics(self) -> Dict[str, Dict[str, int]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            district_stats = {district: {'users': 0, 'online': 0} for district in DISTRICTS}
            cursor.execute('''
                SELECT p.district, COUNT(DISTINCT p.user_id) as user_count
                FROM profiles p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.is_active = 1 AND u.is_blocked = 0
                GROUP BY p.district
            ''')
            for row in cursor.fetchall():
                district = row[0]
                if district in district_stats:
                    district_stats[district]['users'] = row[1]

            now = time.time()
            for user_id, timestamp in user_activity_cache.items():
                if now - timestamp < ACTIVITY_TIMEOUT:
                    cursor.execute('''
                        SELECT p.district
                        FROM profiles p
                        JOIN users u ON p.user_id = u.user_id
                        WHERE p.user_id = ? AND p.is_active = 1 AND u.is_blocked = 0
                    ''', (user_id,))
                    result = cursor.fetchone()
                    if result and result[0] in district_stats:
                        district_stats[result[0]]['online'] += 1
            return district_stats

    def is_registered(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            return cursor.fetchone() is not None

    def update_profile_field(self, user_id: int, field: str, value: any) -> bool:
        allowed_fields = ['name', 'age', 'gender', 'seeking', 'district', 'meeting_type', 'about_text', 'photo_file_id',
                          'keywords']
        if field not in allowed_fields:
            error_logger.error(f"Invalid field for update: field={field}, user_id={user_id}")
            return False
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if field == 'age':
                    value = int(value)
                    if not 12 <= value <= 99:
                        error_logger.error(f"Invalid age value: value={value}, user_id={user_id}")
                        return False
                elif field == 'seeking' and value not in SEEKING_OPTIONS:
                    error_logger.error(f"Invalid seeking value: value={value}, user_id={user_id}")
                    return False
                elif field == 'gender' and value not in GENDERS:
                    error_logger.error(f"Invalid gender value: value={value}, user_id={user_id}")
                    return False
                elif field == 'name' and not (2 <= len(value) <= 30):
                    error_logger.error(f"Invalid name length: value={value}, user_id={user_id}")
                    return False
                elif field == 'about_text' and not (len(value) <= 600):
                    error_logger.error(f"Invalid about_text length: value={value}, user_id={user_id}")
                    return False
                cursor.execute(f"UPDATE profiles SET {field} = ? WHERE user_id = ?", (value, user_id))
                if cursor.rowcount == 0:
                    error_logger.error(f"No profile found for update: user_id={user_id}, field={field}")
                    return False
                conn.commit()
                info_logger.info(f"Profile field updated: user_id={user_id}, field={field}")
                return True
        except sqlite3.Error as e:
            error_logger.error(
                f"Database error in update_profile_field: user_id={user_id}, field={field}, error={str(e)}")
            return False

    def toggle_notifications(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO notification_settings (user_id, notifications_enabled) VALUES (?, 0)
                    ON CONFLICT(user_id) DO UPDATE SET notifications_enabled = NOT notifications_enabled
                ''', (user_id,))
                conn.commit()
                cursor.execute("SELECT notifications_enabled FROM notification_settings WHERE user_id = ?", (user_id,))
                return cursor.fetchone()[0] == 1
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in toggle_notifications: {e}")
                return False

    def toggle_profile_visibility(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("UPDATE profiles SET is_active = NOT is_active WHERE user_id = ?", (user_id,))
                conn.commit()
                cursor.execute("SELECT is_active FROM profiles WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
                info_logger.info(f"Profile visibility toggled for user_id={user_id}, now={result[0]}")
                return result[0] == 1
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in toggle_profile_visibility: {e}")
                return False

    def unblock_profile(self, from_user_id: int, to_profile_id: int):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    DELETE FROM reactions WHERE from_user_id = ? AND to_profile_id = ? AND reaction_type = 'block'
                ''', (from_user_id, to_profile_id))
                conn.commit()
                info_logger.info(f"Profile unblocked: from_user_id={from_user_id}, to_profile_id={to_profile_id}")
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in unblock_profile: {e}")

    def get_blocked_profiles(self, user_id: int) -> List[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT p.id as profile_id, p.name, p.age, p.district
                FROM reactions r
                JOIN profiles p ON r.to_profile_id = p.id
                WHERE r.from_user_id = ? AND r.reaction_type = 'block'
            ''', (user_id,))
            return [{'profile_id': r[0], 'name': r[1], 'age': r[2], 'district': r[3]} for r in cursor.fetchall()]

    def add_warning(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("UPDATE users SET warning_count = COALESCE(warning_count, 0) + 1 WHERE user_id = ?",
                               (user_id,))
                cursor.execute("SELECT warning_count FROM users WHERE user_id = ?", (user_id,))
                warning_count = cursor.fetchone()
                warning_count = warning_count[0] if warning_count else 1
                if warning_count >= 3:
                    self.block_user(user_id)
                    info_logger.info(f"User auto-blocked after 3 warnings: user_id={user_id}")
                    return False
                conn.commit()
                info_logger.info(f"Warning added: user_id={user_id}, warning_count={warning_count}")
                return True
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in add_warning: {e}")
                return False

    def delete_user_profile(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM profiles WHERE user_id = ?", (user_id,))
                cursor.execute("DELETE FROM reactions WHERE from_user_id = ?", (user_id,))
                cursor.execute("DELETE FROM reports WHERE from_user_id = ?", (user_id,))
                cursor.execute("DELETE FROM anonymous_messages WHERE from_user_id = ? OR to_user_id = ?",
                               (user_id, user_id))
                cursor.execute("DELETE FROM notification_settings WHERE user_id = ?", (user_id,))
                # We don't delete the user from the `users` table, just their profile.
                conn.commit()
                info_logger.info(f"User profile deleted: user_id={user_id}")
                return True
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in delete_user_profile: user_id={user_id}, error={e}")
                return False

    def delete_user_data(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                profile_id_res = cursor.execute("SELECT id FROM profiles WHERE user_id = ?", (user_id,)).fetchone()
                if profile_id_res:
                    profile_id = profile_id_res[0]
                    cursor.execute("DELETE FROM reactions WHERE to_profile_id = ?", (profile_id,))
                    cursor.execute("DELETE FROM reports WHERE profile_id = ?", (profile_id,))

                cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                cursor.execute("DELETE FROM profiles WHERE user_id = ?", (user_id,))
                cursor.execute("DELETE FROM reactions WHERE from_user_id = ?", (user_id,))
                cursor.execute("DELETE FROM reports WHERE from_user_id = ?", (user_id,))
                cursor.execute("DELETE FROM anonymous_messages WHERE from_user_id = ? OR to_user_id = ?",
                               (user_id, user_id))
                cursor.execute("DELETE FROM anonymous_chats WHERE user1_id = ? OR user2_id = ?", (user_id, user_id))
                conn.commit()
                return True
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in delete_user_data: {e}")
                return False

    def add_bug_report(self, user_id: int, username: str, bug_text: str):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO bug_reports (user_id, username, bug_text, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, username, bug_text, datetime.now().isoformat()))
                conn.commit()
                info_logger.info(f"Bug report added from user_id={user_id}")
            except sqlite3.Error as e:
                error_logger.error(f"SQL error in add_bug_report: {e}")

    def get_unreviewed_bug_reports(self) -> List[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, user_id, username, bug_text, created_at FROM bug_reports WHERE is_reviewed = 0 ORDER BY created_at ASC")
            return [{'id': r[0], 'user_id': r[1], 'username': r[2], 'text': r[3], 'date': r[4]} for r in
                    cursor.fetchall()]

    def mark_bug_report_as_reviewed(self, report_id: int):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE bug_reports SET is_reviewed = 1 WHERE id = ?", (report_id,))
            conn.commit()
            info_logger.info(f"Bug report {report_id} marked as reviewed.")

    def get_online_users_count(self) -> int:
        now = time.time()
        online_users = [user_id for user_id, timestamp in user_activity_cache.items() if
                        now - timestamp < ACTIVITY_TIMEOUT]
        return len(online_users)

    def is_in_chat(self, user_id: int) -> bool:
        return user_id in anonymous_chats

    def start_anonymous_chat(self, user1_id: int, user2_id: int):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO anonymous_chats (user1_id, user2_id, chat_date)
                VALUES (?, ?, ?)
            ''', (user1_id, user2_id, datetime.now().isoformat()))
            conn.commit()
            info_logger.info(f"Anonymous chat started between {user1_id} and {user2_id}")

    def end_anonymous_chat(self, user_id: int) -> Optional[int]:
        partner_id = anonymous_chats.pop(user_id, None)
        if partner_id:
            anonymous_chats.pop(partner_id, None)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM anonymous_chats WHERE user1_id = ? OR user2_id = ?", (user_id, user_id))
            conn.commit()

        if partner_id:
            info_logger.info(f"Anonymous chat between {user_id} and {partner_id} ended.")
            return partner_id
        return None

    def get_user_stats(self, user_id: int) -> dict:
        profile = self.get_profile_by_user_id(user_id)
        if not profile:
            return {'likes_received': 0, 'likes_given': 0, 'matches': 0}

        profile_id = profile['id']
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Лайков получено
            cursor.execute("SELECT COUNT(*) FROM reactions WHERE to_profile_id = ? AND reaction_type = 'like'",
                           (profile_id,))
            likes_received = cursor.fetchone()[0]
            # Лайков поставлено
            cursor.execute("SELECT COUNT(*) FROM reactions WHERE from_user_id = ? AND reaction_type = 'like'",
                           (user_id,))
            likes_given = cursor.fetchone()[0]
            # Совпадений (взаимных лайков)
            matches = len(self.get_user_likes(user_id))

            return {
                'likes_received': likes_received,
                'likes_given': likes_given,
                'matches': matches
            }


db = Database()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- Middleware: log every action and update online status ---

# --- Decorator for user action logging ---
def log_action(action: str):
    def decorator(func):
        async def wrapper(event, *args, **kwargs):
            try:
                user = getattr(event, "from_user", None)
                if user:
                    user_action_logger.info(action, extra={'user_id': user.id, 'username': getattr(user, 'username', '')})
            except Exception:
                pass
            return await func(event, *args, **kwargs)
        return wrapper
    return decorator
class UserActionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            user = None
            if isinstance(event, types.Message):
                user = event.from_user
                # update activity cache for online status
                try:
                    user_activity_cache[user.id] = time.time()
                except Exception:
                    pass
                # log the message text (shortened)
                try:
                    txt = (event.text or '')[:800]
                    user_action_logger.info(f"message -> {txt}", extra={'user_id': user.id, 'username': getattr(user, 'username', '')})
                except Exception:
                    pass
            elif isinstance(event, types.CallbackQuery):
                user = event.from_user
                try:
                    user_activity_cache[user.id] = time.time()
                except Exception:
                    pass
                try:
                    user_action_logger.info(f"callback_query -> {event.data}", extra={'user_id': user.id, 'username': getattr(user, 'username', '')})
                except Exception:
                    pass
        except Exception as e:
            # Fail silently for middleware to avoid breaking handlers
            try:
                error_logger.error(f'UserActionMiddleware error: {e}')
            except Exception:
                pass
        return await handler(event, data)

# Register middleware for messages and callback queries (if dp is already defined)
try:
    dp.message.middleware(UserActionMiddleware())
    dp.callback_query.middleware(UserActionMiddleware())
except Exception:
    # fallback: try update-level middleware registration
    try:
        dp.update.middleware(UserActionMiddleware())
    except Exception:
        pass




def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="❤️‍🔥 Смотреть анкеты")],
        [KeyboardButton(text="✏️ Моя анкета"), KeyboardButton(text="👀 Мои лайки")],
        [KeyboardButton(text="💬 Анонимный чат"), KeyboardButton(text="⚙️ Настройки")],
        [KeyboardButton(text="📊 Моя статистика"), KeyboardButton(text="ℹ️ О боте")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_profile_creation_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="Отменить создание анкеты")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_anonymous_chat_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🔍 Найти собеседника")],
        [KeyboardButton(text="🔙 В главное меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, one_time_keyboard=True)


def get_reaction_keyboard(profile_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="❤️ Лайк", callback_data=f"like:{profile_id}"),
        InlineKeyboardButton(text="👎 Дизлайк", callback_data=f"dislike:{profile_id}")
    )
    keyboard.row(
        InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"block:{profile_id}"),
        InlineKeyboardButton(text="⚠️ Пожаловаться", callback_data=f"report:{profile_id}")
    )
    return keyboard.as_markup()


def get_edit_profile_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📝 Имя", callback_data="edit_field:name"),
        InlineKeyboardButton(text="🎂 Возраст", callback_data="edit_field:age")
    )
    keyboard.row(
        InlineKeyboardButton(text="🚻 Пол", callback_data="edit_field:gender"),
        InlineKeyboardButton(text="🎯 Кого ищу", callback_data="edit_field:seeking")
    )
    keyboard.row(
        InlineKeyboardButton(text="📍 Район", callback_data="edit_field:district"),
        InlineKeyboardButton(text="🤝 Тип встречи", callback_data="edit_field:meeting_type")
    )
    keyboard.row(
        InlineKeyboardButton(text="🖊️ О себе", callback_data="edit_field:about_text"),
        InlineKeyboardButton(text="🖼️ Фото", callback_data="edit_field:photo")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main_menu")
    )
    return keyboard.as_markup()


def get_settings_keyboard(notifications_enabled: bool = True, profile_active: bool = True) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="💬 Поддержка", callback_data="support")
    )
    toggle_text = "🔕 Отключить уведомления" if notifications_enabled else "🔔 Включить уведомления"
    keyboard.row(
        InlineKeyboardButton(text=toggle_text, callback_data="toggle_notifications")
    )

    visibility_text = "🙈 Скрыть анкету" if profile_active else "🙉 Показать анкету"
    keyboard.row(
        InlineKeyboardButton(text="❌ Удалить анкету", callback_data="delete_profile"),
        InlineKeyboardButton(text=visibility_text, callback_data="hide_profile")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main_menu")
    )
    return keyboard.as_markup()


def get_profile_keyboard(profile_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="Отменить блокировку", callback_data=f"unblock:{profile_id}")
    )
    return keyboard.as_markup()


def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton(text="✍️ Сделать рассылку", callback_data="admin_post")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔍 Поиск пользователя", callback_data="admin_find_user"),
        InlineKeyboardButton(text="🚨 Проверить жалобы", callback_data="admin_reports")
    )
    keyboard.row(
        InlineKeyboardButton(text="📨 Обращения в поддержку", callback_data="admin_bug_reports")
    )
    return keyboard.as_markup()


def get_user_search_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔎 Найти по ID", callback_data="find_user_by_id"),
        InlineKeyboardButton(text="❌ Блокировать", callback_data="block_user_by_id")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_admin_panel")
    )
    return keyboard.as_markup()


def get_back_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Назад")]], resize_keyboard=True)


rate_limit_wait_times = {}


def rate_limit(max_requests: int = MAX_REQUESTS_PER_MINUTE, time_window: int = 60):
    def decorator(func):
        @wraps(func)
        async def wrapper(message_or_callback: Union[types.Message, types.CallbackQuery], *args, **kwargs):
            user_id = message_or_callback.from_user.id
            now = time.time()
            user_requests[user_id] = [t for t in user_requests[user_id] if now - t < time_window]

            if len(user_requests[user_id]) >= max_requests:
                last_wait_time = rate_limit_wait_times.get(user_id, 0)
                if now - last_wait_time > 5:
                    if isinstance(message_or_callback, types.Message):
                        await message_or_callback.answer("🛑 Слишком много запросов. Пожалуйста, подождите немного.")
                    else:
                        await message_or_callback.answer("🛑 Слишком много запросов. Пожалуйста, подождите немного.",
                                                         show_alert=True)
                    rate_limit_wait_times[user_id] = now
                warning_logger.warning(
                    f"Rate limit exceeded: user_id={user_id}, requests={len(user_requests[user_id])}")
                return

            user_requests[user_id].append(now)
            return await func(message_or_callback, *args, **kwargs)

        return wrapper

    return decorator


async def get_current_profile_data(user_id: int):
    profile = db.get_profile_by_user_id(user_id)
    if profile:
        profile_info = (
            f"👤 Ваша анкета:\n"
            f"Имя: {profile['name']}\n"
            f"Возраст: {profile['age']}\n"
            f"Пол: {profile['gender']}\n"
            f"Ищу: {profile['seeking']}\n"
            f"Район: {profile['district']}\n"
            f"Тип встречи: {profile['meeting_type']}\n"
            f"О себе: {profile['about_text']}\n"
        )
        return profile_info, profile['photo_file_id']
    return None, None

@log_action("Entered cancel")
@dp.message(Command("cancel"))
async def cancel(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or "Без username"
    info_logger.info(f"Command /cancel from user_id={user_id}, username={username}")
    await state.clear()

@log_action("Started bot (/start)")
@dp.message(Command("start"))
@rate_limit()
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or "Без username"
    info_logger.info(f"Command /start from user_id={user_id}, username={username}")
    db.register_user(user_id, username)
    await state.clear()
    await message.answer(
        "Привет! Это бот для знакомств. Чтобы начать, создайте свою анкету.",
        reply_markup=get_main_menu_keyboard()
    )


@log_action("Entered admin panel (/admin)")
@dp.message(Command("admin"))
@rate_limit()
async def cmd_admin(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id in ADMIN_IDS:
        await state.set_state(AdminPanel.waiting_for_password)
        await message.answer("Введите пароль администратора:")
    else:
        await message.answer("У вас нет прав администратора.")


@dp.message(AdminPanel.waiting_for_password)
@rate_limit()
async def process_admin_password(message: types.Message, state: FSMContext):
    if message.text == ADMIN_PASSWORD:
        admin_sessions[message.from_user.id] = time.time() + ADMIN_SESSION_TIMEOUT
        await state.set_state(AdminPanel.in_panel)
        await message.answer("Добро пожаловать в админ-панель!", reply_markup=get_admin_panel_keyboard())
    else:
        await message.answer("Неверный пароль.")


@log_action("Admin viewed statistics")
@dp.callback_query(F.data == "admin_stats")
@rate_limit()
async def admin_stats(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return
    await callback.message.delete()
    stats = db.get_statistics()
    online_count = db.get_online_users_count()
    district_stats = db.get_district_statistics()
    district_text = "\n".join(
        [f"- {d}: {s['users']} анкет, {s['online']} онлайн" for d, s in district_stats.items()])

    response_text = (
        f"📊 **Статистика**\n"
        f"👥 Всего пользователей: {stats['total_users']}\n"
        f"📝 Всего анкет: {stats['total_profiles']}\n"
        f"❤️ Всего лайков: {stats['total_likes']}\n"
        f"🤝 Всего совпадений: {stats['total_matches']}\n"
        f"🟢 Пользователей онлайн: {online_count}\n\n"
        f"**Статистика по районам:**\n"
        f"{district_text}"
    )
    await callback.message.answer(response_text, reply_markup=get_admin_panel_keyboard(), parse_mode="Markdown")


@dp.callback_query(F.data == "admin_reports")
@rate_limit()
async def admin_reports(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return
    await callback.message.delete()
    reports = db.get_unreviewed_complaints()
    if not reports:
        await callback.message.answer("Новых жалоб нет.", reply_markup=get_admin_panel_keyboard())
        return

    report = reports[0]
    profile = db.get_profile_by_user_id(report['reported_user_id'])
    if not profile:
        db.mark_complaint_as_reviewed(report['reported_user_id'])
        await callback.message.answer(
            f"Профиль для жалобы на пользователя {report['reported_user_id']} не найден. Жалоба автоматически закрыта.")
        # Try to show next report
        await admin_reports(callback, state)
        return

    profile_text = (f"ID: {profile['id']}\nИмя: {profile['name']}\nВозраст: {profile['age']}\n"
                    f"Район: {profile['district']}\n")
    report_text = f"🚨 Жалоба от: {report['reporter_user_id']}\nПричина: {report['reason']}\n\nАнкета:\n{profile_text}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔️ Отметить как проверенное",
                              callback_data=f"resolve_report:{report['reported_user_id']}"),
         InlineKeyboardButton(text="🚫 Заблокировать пользователя",
                              callback_data=f"block_user_by_id:{report['reported_user_id']}")],
        [InlineKeyboardButton(text="🔙 Назад в админ-панель", callback_data="back_to_admin_panel")]
    ])

    await callback.message.answer(report_text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("resolve_report:"))
@rate_limit()
async def resolve_report(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return

    reported_user_id = int(callback.data.split(":")[1])
    db.mark_complaint_as_reviewed(reported_user_id)
    await callback.message.edit_text("Жалоба отмечена как проверенная.", reply_markup=None)

    reports = db.get_unreviewed_complaints()
    if reports:
        await admin_reports(callback, state)
    else:
        await callback.message.answer("Больше новых жалоб нет.", reply_markup=get_admin_panel_keyboard())


@dp.callback_query(F.data == "back_to_admin_panel")
@rate_limit()
async def back_to_admin_panel(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return
    await state.set_state(AdminPanel.in_panel)
    try:
        await callback.message.edit_text("Возвращаемся в админ-панель.", reply_markup=get_admin_panel_keyboard())
    except TelegramBadRequest:
        await callback.message.answer("Возвращаемся в админ-панель.", reply_markup=get_admin_panel_keyboard())


@dp.callback_query(F.data == "admin_post")
@rate_limit()
async def admin_post(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return
    await state.set_state(AdminPanel.waiting_for_post_text)
    await callback.message.edit_text("Введите текст для рассылки всем пользователям:", reply_markup=None)
    await callback.answer()


@dp.message(AdminPanel.waiting_for_post_text)
@rate_limit()
async def process_admin_post(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(message.from_user.id, 0):
        await message.answer("Сессия истекла или у вас нет прав.")
        return
    post_text = message.text
    users_to_send = db.get_all_users()
    sent_count = 0
    blocked_count = 0
    for user in users_to_send:
        try:
            await bot.send_message(user['user_id'], post_text)
            sent_count += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            blocked_count += 1
            info_logger.info(f"User {user['user_id']} blocked the bot. Not sending message.")
            db.delete_user_data(user['user_id'])
        except Exception as e:
            error_logger.error(f"Failed to send message to user {user['user_id']}: {e}")

    await message.answer(
        f"Рассылка завершена. Отправлено {sent_count} сообщений. Заблокировали бота {blocked_count} пользователей.")
    await state.set_state(AdminPanel.in_panel)
    await message.answer("Возвращаемся в админ-панель.", reply_markup=get_admin_panel_keyboard())


@dp.callback_query(F.data == "admin_find_user")
@rate_limit()
async def admin_find_user(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return
    await state.set_state(AdminPanel.waiting_for_user_id)
    await callback.message.edit_text("Введите Telegram ID пользователя:", reply_markup=None)


@dp.message(AdminPanel.waiting_for_user_id)
@rate_limit()
async def find_user_by_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(message.from_user.id, 0):
        await message.answer("Сессия истекла или у вас нет прав.")
        return
    try:
        user_id = int(message.text)
        profile = db.get_profile_by_user_id(user_id)
        if profile:
            user_info = (
                f"👤 Найден пользователь:\n"
                f"ID: {user_id}\n"
                f"Имя: {profile['name']}\n"
                f"Возраст: {profile['age']}\n"
                f"Пол: {profile['gender']}\n"
                f"Ищет: {profile['seeking']}\n"
                f"Район: {profile['district']}\n"
                f"Тип встречи: {profile['meeting_type']}\n"
                f"О себе: {profile['about_text']}\n"
            )
            complaints = db.get_complaints_for_user(user_id)
            if complaints:
                user_info += "\n**Жалобы на пользователя:**\n"
                for i, complaint in enumerate(complaints):
                    user_info += f"{i + 1}. Причина: {complaint['reason']} (От: {complaint['reporter_user_id']})\n"

            photo_file_id = profile['photo_file_id']
            await bot.send_photo(message.chat.id, photo_file_id, caption=user_info)
        else:
            await message.answer("Профиль с таким ID не найден.")
    except ValueError:
        await message.answer("Неверный формат ID. Пожалуйста, введите число.")
    except Exception as e:
        error_logger.error(f"Error finding user by ID: {e}")
        await message.answer("Произошла ошибка при поиске пользователя.")
    finally:
        await state.set_state(AdminPanel.in_panel)
        await message.answer("Выберите действие:", reply_markup=get_admin_panel_keyboard())


@dp.callback_query(F.data.startswith("block_user_by_id:"))
@rate_limit()
async def block_user_from_report(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return

    user_id_to_block = int(callback.data.split(":")[1])
    db.block_user(user_id_to_block)

    try:
        await bot.send_message(user_id_to_block,
                               "🚫 Ваша анкета была заблокирована администратором из-за многочисленных жалоб.")
    except TelegramForbiddenError:
        info_logger.info(f"User {user_id_to_block} blocked the bot, could not send a message.")
    except Exception as e:
        error_logger.error(f"Failed to send block message to user {user_id_to_block}: {e}")

    await callback.message.edit_text(f"Пользователь {user_id_to_block} заблокирован.", reply_markup=None)
    await callback.message.answer("Возвращаемся в админ-панель.", reply_markup=get_admin_panel_keyboard())


# --- Новые хендлеры для поддержки ---
@dp.callback_query(F.data == "admin_bug_reports")
@rate_limit()
async def admin_view_bug_reports(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS or time.time() > admin_sessions.get(callback.from_user.id, 0):
        await callback.message.answer("Сессия истекла или у вас нет прав.")
        return
    await callback.message.delete()

    reports = db.get_unreviewed_bug_reports()
    if not reports:
        await callback.message.answer("Новых обращений в поддержку нет.", reply_markup=get_admin_panel_keyboard())
        return

    await state.update_data(bug_reports=reports, bug_report_index=0)
    await show_bug_report(callback.message, state)


async def show_bug_report(message: types.Message, state: FSMContext):
    data = await state.get_data()
    reports = data.get('bug_reports')
    index = data.get('bug_report_index', 0)

    if not reports or index >= len(reports):
        await message.answer("Новых обращений нет.", reply_markup=get_admin_panel_keyboard())
        return

    report = reports[index]

    # --- THE FIX ---
    # Escape user-provided strings using Python's standard html library
    username = html.escape(report.get('username', 'N/A'))
    bug_text = html.escape(report.get('text', ''))

    # Format the text using HTML tags or helpers like hbold
    report_text = (
        f"📨 {hbold(f'Обращение #{report['id']}')}\n"
        f"От: {report['user_id']} (@{username})\n"
        f"Дата: {report['date']}\n\n"
        f"Текст: {bug_text}"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="✍️ Ответить", callback_data=f"reply_bug:{report['id']}:{report['user_id']}")
    builder.button(text="✅ Закрыть", callback_data=f"resolve_bug:{report['id']}")
    if index < len(reports) - 1:
        builder.button(text="➡️ Следующее", callback_data="next_bug")
    builder.button(text="🔙 Назад", callback_data="back_to_admin_panel")
    builder.adjust(2, 1)

    # Send the message with the correct parse mode
    await message.answer(
        report_text,
        reply_markup=builder.as_markup(),
        parse_mode=ParseMode.HTML
    )


@dp.callback_query(F.data == "next_bug")
@rate_limit()
async def next_bug_report(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    index = data.get('bug_report_index', 0)
    await state.update_data(bug_report_index=index + 1)
    await callback.message.delete()
    await show_bug_report(callback.message, state)


@dp.callback_query(F.data.startswith("resolve_bug:"))
@rate_limit()
async def resolve_bug_report(callback: types.CallbackQuery, state: FSMContext):
    report_id = int(callback.data.split(":")[1])
    db.mark_bug_report_as_reviewed(report_id)
    await callback.answer("Обращение закрыто.")

    # Refresh the list and show the next one
    await callback.message.delete()
    reports = db.get_unreviewed_bug_reports()
    if not reports:
        await callback.message.answer("Новых обращений в поддержку нет.", reply_markup=get_admin_panel_keyboard())
        return
    await state.update_data(bug_reports=reports, bug_report_index=0)
    await show_bug_report(callback.message, state)


@dp.callback_query(F.data.startswith("reply_bug:"))
@rate_limit()
async def reply_to_bug_report(callback: types.CallbackQuery, state: FSMContext):
    _, report_id, user_id = callback.data.split(":")
    await state.set_state(AdminPanel.replying_to_bug_report)
    await state.update_data(reply_to_user_id=int(user_id), reply_to_report_id=int(report_id))
    await callback.message.answer(f"Введите ваш ответ для пользователя {user_id}:")
    await callback.answer()


@dp.message(AdminPanel.replying_to_bug_report)
@rate_limit()
async def process_bug_report_reply(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = data.get('reply_to_user_id')
    report_id = data.get('reply_to_report_id')
    reply_text = message.text

    try:
        await bot.send_message(user_id, f"Ответ от поддержки:\n\n{reply_text}")
        db.mark_bug_report_as_reviewed(report_id)
        await message.answer(f"Ответ успешно отправлен пользователю {user_id}.")
    except Exception as e:
        await message.answer(f"Не удалось отправить ответ: {e}")
        error_logger.error(f"Failed to send reply to {user_id}: {e}")

    await state.clear()
    await state.set_state(AdminPanel.in_panel)

    # Refresh and show next report
    reports = db.get_unreviewed_bug_reports()
    if not reports:
        await message.answer("Новых обращений в поддержку нет.", reply_markup=get_admin_panel_keyboard())
        return
    await state.update_data(bug_reports=reports, bug_report_index=0)
    await show_bug_report(message, state)


# --- Конец новых хендлеров ---

@dp.message(F.text == "❤️‍🔥 Смотреть анкеты")
@rate_limit()
async def show_profiles(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    profile_exists = db.get_profile_by_user_id(user_id)
    if not profile_exists:
        await message.answer("Сначала создайте свою анкету.", reply_markup=get_main_menu_keyboard())
        return

    await state.update_data(current_profile_index=0, profiles=[])
    profiles = db.get_random_profiles(user_id)
    if not profiles:
        await message.answer("Пока нет анкет для просмотра. Попробуйте позже.")
        return

    await state.update_data(profiles=profiles)
    await display_profile(message, state)


async def display_profile(message: types.Message, state: FSMContext):
    data = await state.get_data()
    profiles = data.get('profiles')
    current_index = data.get('current_profile_index', 0)

    if not profiles or current_index >= len(profiles):
        await message.answer("Анкеты закончились! 😔", reply_markup=get_main_menu_keyboard())
        await state.clear()
        return

    profile = profiles[current_index]
    profile_id = profile['id']

    profile_text = (
        f"Имя: {profile['name']}, Возраст: {profile['age']}\n"
        f"Район: {profile['district']}\n"
        f"Тип встречи: {profile['meeting_type']}\n\n"
        f"О себе: {profile['about_text']}"
    )

    try:
        await bot.send_photo(
            chat_id=message.chat.id,
            photo=profile['photo_file_id'],
            caption=profile_text,
            reply_markup=get_reaction_keyboard(profile_id)
        )
    except Exception as e:
        error_logger.error(f"Failed to send photo for profile_id={profile_id}: {e}")
        await message.answer("Не удалось отобразить анкету. Пробуем следующую.")
        await state.update_data(current_profile_index=current_index + 1)
        await display_profile(message, state)


@dp.callback_query(F.data.startswith(("like:", "dislike:", "block:")))
@rate_limit()
async def process_reaction(callback: types.CallbackQuery, state: FSMContext):
    try:
        reaction_type, profile_id_str = callback.data.split(":")
        profile_id = int(profile_id_str)
        from_user_id = callback.from_user.id

        db.add_reaction(from_user_id, profile_id, reaction_type)

        if reaction_type == "like":
            user_profile = db.get_profile_by_user_id(from_user_id)
            if user_profile and db.check_match(user_profile['id'], profile_id):
                info_logger.info(f"MATCH! user1={from_user_id}, profile2={profile_id}")
                partner_profile = db.get_profile_by_id(profile_id)
                partner_user_id = partner_profile['user_id']

                my_username = callback.from_user.username
                partner_username = (await bot.get_chat(partner_user_id)).username

                await bot.send_message(from_user_id,
                                       f"🔥 У вас взаимная симпатия с {partner_profile['name']}! (@{partner_username})")
                await bot.send_message(partner_user_id,
                                       f"🔥 У вас взаимная симпатия с {user_profile['name']}! (@{my_username})")

        await callback.message.delete()
        await callback.answer(f"Ваша реакция: {reaction_type}", show_alert=False)

        data = await state.get_data()
        current_index = data.get('current_profile_index', 0)
        await state.update_data(current_profile_index=current_index + 1)
        await display_profile(callback.message, state)

    except TelegramBadRequest:
        warning_logger.warning(f"Failed to delete message for user {callback.from_user.id}. Message may be too old.")
        await callback.answer()
        data = await state.get_data()
        current_index = data.get('current_profile_index', 0)
        await state.update_data(current_profile_index=current_index + 1)
        # We call it on a new message to avoid bad request error
        await display_profile(await callback.message.answer("..."), state)
    except Exception as e:
        error_logger.error(f"Error processing reaction for user {callback.from_user.id}: {e}")
        await callback.message.answer("Произошла ошибка, попробуйте еще раз.")


@dp.callback_query(F.data.startswith("report:"))
@rate_limit()
async def report_profile_reason(callback: types.CallbackQuery, state: FSMContext):
    profile_id = int(callback.data.split(":")[1])
    await state.update_data(report_profile_id=profile_id)
    await state.set_state(UserStates.waiting_for_report_reason)
    await callback.message.answer("Пожалуйста, напишите причину жалобы:")


@log_action("Sent complaint reason")
@dp.message(UserStates.waiting_for_report_reason)
@rate_limit()
async def process_report_reason(message: types.Message, state: FSMContext):
    data = await state.get_data()
    profile_id = data.get('report_profile_id')
    reason = message.text
    db.add_report(message.from_user.id, profile_id, reason)
    await message.answer("Спасибо, ваша жалоба принята. Мы рассмотрим её в ближайшее время.")
    await state.clear()


@dp.message(F.text == "✏️ Моя анкета")
@rate_limit()
async def show_my_profile(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    profile_info, photo_file_id = await get_current_profile_data(user_id)
    if profile_info and photo_file_id:
        await bot.send_photo(
            chat_id=user_id,
            photo=photo_file_id,
            caption=profile_info,
            reply_markup=get_edit_profile_keyboard()
        )
    else:
        await message.answer(
            "У вас еще нет анкеты. Давайте создадим её.",
            reply_markup=get_profile_creation_keyboard()
        )
        await state.set_state(UserStates.waiting_for_name)
        await message.answer("Введите ваше имя:")


@dp.message(F.text == "Отменить создание анкеты")
@rate_limit()
async def cancel_profile_creation(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Создание анкеты отменено.", reply_markup=get_main_menu_keyboard())


@log_action("Filling profile: name")
@dp.message(UserStates.waiting_for_name)
@rate_limit()
async def process_name(message: types.Message, state: FSMContext):
    if not (2 <= len(message.text) <= 30):
        await message.answer("Имя должно быть от 2 до 30 символов. Попробуйте еще раз:")
        return
    await state.update_data(name=message.text)
    await state.set_state(UserStates.waiting_for_age)
    await message.answer("Введите ваш возраст (12-99):")


@log_action("Filling profile: age")
@dp.message(UserStates.waiting_for_age)
@rate_limit()
async def process_age(message: types.Message, state: FSMContext):
    try:
        age = int(message.text)
        if not (12 <= age <= 99):
            raise ValueError
        await state.update_data(age=age)
        await state.set_state(UserStates.waiting_for_gender)
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=g) for g in GENDERS]],
            resize_keyboard=True
        )
        await message.answer("Какой у вас пол?", reply_markup=keyboard)
    except ValueError:
        await message.answer("Неверный возраст. Пожалуйста, введите число от 12 до 99:")


@dp.message(F.text.in_(GENDERS), UserStates.waiting_for_gender)
@rate_limit()
async def process_gender(message: types.Message, state: FSMContext):
    await state.update_data(gender=message.text)
    await state.set_state(UserStates.waiting_for_seeking)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=s) for s in SEEKING_OPTIONS]],
        resize_keyboard=True
    )
    await message.answer("Кого вы ищете?", reply_markup=keyboard)


@dp.message(F.text.in_(SEEKING_OPTIONS), UserStates.waiting_for_seeking)
@rate_limit()
async def process_seeking(message: types.Message, state: FSMContext):
    await state.update_data(seeking=message.text)
    await state.set_state(UserStates.waiting_for_district)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=d)] for d in DISTRICTS],
        resize_keyboard=True
    )
    await message.answer("В каком вы районе?", reply_markup=keyboard)


@dp.message(F.text.in_(DISTRICTS), UserStates.waiting_for_district)
@rate_limit()
async def process_district(message: types.Message, state: FSMContext):
    await state.update_data(district=message.text)
    await state.set_state(UserStates.waiting_for_meeting_type)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t)] for t in MEETING_TYPES],
        resize_keyboard=True
    )
    await message.answer("Какой тип встречи вы ищете?", reply_markup=keyboard)


@dp.message(F.text.in_(MEETING_TYPES), UserStates.waiting_for_meeting_type)
@rate_limit()
async def process_meeting_type(message: types.Message, state: FSMContext):
    await state.update_data(meeting_type=message.text)
    await state.set_state(UserStates.waiting_for_about)
    await message.answer("Расскажите о себе (до 600 символов):", reply_markup=get_back_keyboard())


@dp.message(F.text, UserStates.waiting_for_about)
@rate_limit()
async def process_about(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.set_state(UserStates.waiting_for_meeting_type)
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=t)] for t in MEETING_TYPES],
            resize_keyboard=True
        )
        await message.answer("Выберите тип встречи:", reply_markup=keyboard)
        return

    about_text = message.text
    if len(about_text) > 600:
        await message.answer("Текст слишком длинный. Пожалуйста, сократите его до 600 символов.")
        return

    await state.update_data(about_text=about_text)
    await state.set_state(UserStates.waiting_for_photo)
    await message.answer("Теперь отправьте своё фото. Оно будет единственным в вашей анкете.",
                         reply_markup=get_back_keyboard())


@dp.message(F.text == "🔙 Назад", UserStates.waiting_for_photo)
@rate_limit()
async def back_to_about(message: types.Message, state: FSMContext):
    await state.set_state(UserStates.waiting_for_about)
    await message.answer("Расскажите о себе (до 600 символов):", reply_markup=get_back_keyboard())


@dp.message(F.photo | F.document, UserStates.waiting_for_photo)
@rate_limit()
async def process_photo(message: types.Message, state: FSMContext):
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document and "image" in message.document.mime_type:
        file_id = message.document.file_id
    else:
        await message.answer("Пожалуйста, отправьте именно фотографию.")
        return

    await state.update_data(photo_file_id=file_id)
    user_data = await state.get_data()
    db.create_profile(
        user_id=message.from_user.id,
        name=user_data['name'],
        age=user_data['age'],
        gender=user_data['gender'],
        seeking=user_data['seeking'],
        district=user_data['district'],
        meeting_type=user_data['meeting_type'],
        about_text=user_data['about_text'],
        photo_file_id=user_data['photo_file_id']
    )
    await state.clear()
    await message.answer("🎉 Ваша анкета создана!", reply_markup=get_main_menu_keyboard())


@dp.callback_query(F.data.startswith("edit_field:"))
@rate_limit()
async def edit_profile(callback: types.CallbackQuery, state: FSMContext):
    field = callback.data.split(":")[1]

    try:
        await callback.message.delete()
    except TelegramBadRequest:
        warning_logger.warning(
            f"Failed to delete message in edit_profile for user {callback.from_user.id}. Message may be too old.")
        pass

    await state.update_data(edit_field=field)
    await state.set_state(UserStates.waiting_for_edit_value)

    prompt = ""
    reply_markup = get_back_keyboard()
    if field == "name":
        prompt = "Введите новое имя:"
    elif field == "age":
        prompt = "Введите новый возраст (12-99):"
    elif field == "gender":
        prompt = "Выберите новый пол:"
        reply_markup = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=g) for g in GENDERS]], resize_keyboard=True
        )
    elif field == "seeking":
        prompt = "Выберите, кого вы ищете:"
        reply_markup = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=s) for s in SEEKING_OPTIONS]], resize_keyboard=True
        )
    elif field == "district":
        prompt = "Выберите новый район:"
        reply_markup = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=d)] for d in DISTRICTS], resize_keyboard=True
        )
    elif field == "meeting_type":
        prompt = "Выберите новый тип встречи:"
        reply_markup = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=t)] for t in MEETING_TYPES], resize_keyboard=True
        )
    elif field == "about_text":
        prompt = "Введите новое описание о себе (до 600 символов):"
    elif field == "photo":
        prompt = "Отправьте новое фото для вашей анкеты:"

    await callback.message.answer(prompt, reply_markup=reply_markup)


@dp.message(UserStates.waiting_for_edit_value)
@rate_limit()
async def process_edit_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    field = data.get('edit_field')

    if message.text == "🔙 Назад":
        await state.clear()
        await show_my_profile(message, state)
        return

    if field == "photo":
        file_id = None
        if message.photo:
            file_id = message.photo[-1].file_id
        elif message.document and "image" in message.document.mime_type:
            file_id = message.document.file_id
        else:
            await message.answer("Пожалуйста, отправьте именно фотографию.")
            return
        value = file_id
    else:
        value = message.text

    if field == 'age':
        try:
            age_val = int(value)
            if not (12 <= age_val <= 99):
                await message.answer("Неверный возраст. Пожалуйста, введите число от 12 до 99.")
                return
        except ValueError:
            await message.answer("Неверный возраст. Пожалуйста, введите число от 12 до 99.")
            return
    elif field == 'name' and not (2 <= len(value) <= 30):
        await message.answer("Имя должно быть от 2 до 30 символов.")
        return
    elif field == 'about_text' and len(value) > 600:
        await message.answer("Описание не должно превышать 600 символов.")
        return

    if db.update_profile_field(message.from_user.id, field, value):
        await message.answer("Данные обновлены!", reply_markup=get_main_menu_keyboard())
    else:
        await message.answer("Не удалось обновить данные. Возможно, вы ввели некорректное значение.")

    await state.clear()
    await show_my_profile(message, state)


@dp.message(F.text == "👀 Мои лайки")
@rate_limit()
async def show_my_likes(message: types.Message, state: FSMContext):
    await state.clear()
    likes = db.get_user_likes(message.from_user.id)
    if not likes:
        await message.answer("У вас пока нет взаимных лайков. ❤️")
        return

    response = "🤝 Вот пользователи, с которыми у вас взаимная симпатия:\n"
    for like in likes:
        username = f"@{like['username']}" if like['username'] else "скрыт"
        response += f"- {like['name']}, {like['age']} из {like['district']} ({username})\n"
    await message.answer(response)


# --- Новые хендлеры для статистики пользователя ---
@dp.message(F.text == "📊 Моя статистика")
@rate_limit()
async def show_my_stats(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    stats = db.get_user_stats(user_id)
    if not stats:
        await message.answer("Сначала вам нужно создать анкету.")
        return

    text = (
        f"📊 Ваша статистика:\n\n"
        f"❤️ Вашу анкету лайкнули: {stats['likes_received']} раз\n"
        f"👍 Вы поставили лайков: {stats['likes_given']}\n"
        f"🤝 Взаимных совпадений: {stats['matches']}"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_my_stats")]
    ])
    await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data == "refresh_my_stats")
@rate_limit(max_requests=5)  # Ограничим частоту обновления
async def refresh_my_stats(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    stats = db.get_user_stats(user_id)

    text = (
        f"📊 Ваша статистика (обновлено):\n\n"
        f"❤️ Вашу анкету лайкнули: {stats['likes_received']} раз\n"
        f"👍 Вы поставили лайков: {stats['likes_given']}\n"
        f"🤝 Взаимных совпадений: {stats['matches']}"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_my_stats")]
    ])
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer("Статистика обновлена!")
    except TelegramBadRequest:
        await callback.answer("Данные не изменились.")
    except Exception as e:
        error_logger.error(f"Error refreshing stats for {user_id}: {e}")
        await callback.answer("Ошибка обновления.")


# --- Конец новых хендлеров ---

@dp.message(F.text == "💬 Анонимный чат")
@rate_limit()
async def anonymous_chat_menu(message: types.Message, state: FSMContext):
    if db.is_in_chat(message.from_user.id):
        await message.answer(
            "Вы уже находитесь в чате. Чтобы выйти, используйте команду /stopchat",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AnonymousChat.in_chat)
    else:
        await state.clear()
        await message.answer("Добро пожаловать в анонимный чат.", reply_markup=get_anonymous_chat_keyboard())


@dp.message(F.text == "🔍 Найти собеседника")
@rate_limit()
async def find_and_start_chat(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if db.is_in_chat(user_id):
        await message.answer("Вы уже в чате. Чтобы выйти, используйте команду /stopchat",
                             reply_markup=types.ReplyKeyboardRemove())
        return

    if user_id in waiting_for_chat:
        await message.answer("Вы уже находитесь в режиме поиска.")
        return

    waiting_for_chat[user_id] = time.time()
    await state.set_state(AnonymousChat.waiting_for_chat_partner)
    await message.answer(
        "Идет поиск собеседника... Поиск будет длиться 5 минут.\n\nЧтобы отменить, введите команду /stopchat",
        reply_markup=types.ReplyKeyboardRemove())


@log_action("Stopped anonymous chat (/stopchat)")
@dp.message(Command("stopchat"))
@rate_limit()
async def end_chat_command(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    # Если пользователь был в очереди
    if user_id in waiting_for_chat:
        del waiting_for_chat[user_id]
        await state.clear()
        await message.answer("Поиск собеседника отменен.", reply_markup=get_main_menu_keyboard())
        return

    # Если пользователь был в чате
    partner_id = db.end_anonymous_chat(user_id)
    if partner_id:
        await state.clear()

        # Сброс состояния у партнера
        partner_bot = Bot(token=BOT_TOKEN)
        partner_dp_storage_key = StorageKey(bot_id=partner_bot.id, chat_id=partner_id, user_id=partner_id)
        await dp.storage.set_state(key=partner_dp_storage_key, state=None)

        await message.answer("Чат завершен.", reply_markup=get_main_menu_keyboard())
        try:
            await bot.send_message(partner_id, "Собеседник покинул чат.", reply_markup=get_main_menu_keyboard())
        except TelegramForbiddenError:
            info_logger.info(f"Could not send 'chat ended' message to user {partner_id} as they blocked the bot.")
    else:
        await message.answer("Вы не находитесь в чате или поиске.")
        await state.clear()


@dp.message(AnonymousChat.in_chat, ~F.text.startswith("/"))
@rate_limit(max_requests=30)
async def process_anonymous_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    partner_id = anonymous_chats.get(user_id)
    if partner_id:
        try:
            if message.text:
                await bot.send_message(partner_id, message.text)
            elif message.photo:
                await bot.send_photo(partner_id, message.photo[-1].file_id)
            elif message.voice:
                await bot.send_voice(partner_id, message.voice.file_id)
            elif message.sticker:
                await bot.send_sticker(partner_id, message.sticker.file_id)
            # и т.д. для других типов медиа
        except TelegramForbiddenError:
            await message.answer("Собеседник заблокировал бота. Чат завершен.", reply_markup=get_main_menu_keyboard())
            await end_chat_command(message, state)  # Завершаем чат
        except Exception as e:
            error_logger.error(f"Failed to forward message from {user_id} to {partner_id}: {e}")
    else:
        await message.answer("Вы не находитесь в чате. Возможно, ваш собеседник вышел.",
                             reply_markup=get_main_menu_keyboard())
        await state.clear()


@dp.message(F.text == "⚙️ Настройки")
@rate_limit()
async def show_settings(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    is_enabled = db.is_notification_enabled(user_id)
    profile = db.get_profile_by_user_id(user_id)
    is_active = profile['is_active'] if profile else False
    await message.answer("Настройки:", reply_markup=get_settings_keyboard(is_enabled, is_active))


@dp.callback_query(F.data == "support")
@rate_limit()
async def support(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(SupportForm.bug_report)
    await callback.message.answer("Опишите проблему, с которой вы столкнулись:",
                                  reply_markup=types.ReplyKeyboardRemove())
    await callback.answer()


@dp.message(SupportForm.bug_report)
@rate_limit()
async def process_bug_report(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or "Без username"
    bug_text = message.text
    db.add_bug_report(user_id, username, bug_text)
    await message.answer("Спасибо за ваше сообщение! Мы постараемся решить проблему.",
                         reply_markup=get_main_menu_keyboard())
    await state.clear()


@dp.callback_query(F.data == "toggle_notifications")
@rate_limit()
async def toggle_notifications(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    is_enabled = db.toggle_notifications(user_id)
    profile = db.get_profile_by_user_id(user_id)
    is_active = profile['is_active'] if profile else False
    status = "включены" if is_enabled else "отключены"

    try:
        await callback.message.edit_text(f"Уведомления теперь {status}.",
                                         reply_markup=get_settings_keyboard(is_enabled, is_active))
    except TelegramBadRequest:
        pass  # Не меняем, если сообщение то же самое
    await callback.answer()


@dp.callback_query(F.data == "delete_profile")
@rate_limit()
async def delete_profile(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    db.delete_user_profile(user_id)
    await callback.message.delete()
    await callback.message.answer("Ваша анкета удалена. Вы можете создать новую, нажав на 'Моя анкета'.",
                                  reply_markup=get_main_menu_keyboard())


@dp.callback_query(F.data == "hide_profile")
@rate_limit()
async def hide_profile(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    is_active = db.toggle_profile_visibility(user_id)
    is_enabled = db.is_notification_enabled(user_id)
    status = "видна" if is_active else "скрыта"

    try:
        await callback.message.edit_text(f"Ваша анкета теперь {status}.",
                                         reply_markup=get_settings_keyboard(is_enabled, is_active))
    except TelegramBadRequest:
        pass
    await callback.answer()


@dp.callback_query(F.data == "back_to_main_menu")
@rate_limit()
async def back_to_main_menu_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
        await callback.message.answer("Главное меню", reply_markup=get_main_menu_keyboard())
    except TelegramBadRequest:
        await callback.message.answer("Главное меню", reply_markup=get_main_menu_keyboard())


@dp.message(F.text == "🔙 В главное меню")
@rate_limit()
async def back_to_main_menu_message(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню", reply_markup=get_main_menu_keyboard())




@log_action("Viewed bot statistics")
@dp.message(F.text == "ℹ️ О боте")
@rate_limit()
async def about_bot(message: types.Message, state: FSMContext = None):
    """Краткая статистика бота: пользователи, анкеты, лайки, совпадения, онлайн."""
    try:
        stats = db.get_statistics()
        online_count = db.get_online_users_count() if hasattr(db, 'get_online_users_count') else 0
        district_stats = db.get_district_statistics() if hasattr(db, 'get_district_statistics') else {}

        district_text = "\n".join([f"- {d}: {s['users']} анкет, {s['online']} онлайн" for d, s in district_stats.items()])

        text = (
            f"📊 Статистика бота:\n"
            f"👥 Всего пользователей: {stats.get('total_users', 0)}\n"
            f"📝 Всего анкет: {stats.get('total_profiles', 0)}\n"
            f"❤️ Всего лайков: {stats.get('total_likes', 0)}\n"
            f"🤝 Всего совпадений: {stats.get('total_matches', 0)}\n"
            f"🟢 Пользователей онлайн: {online_count}\n\n"
            f"🏙️ Статистика по районам:\n{district_text}"
        )
        await message.answer(text)
    except Exception as e:
        try:
            error_logger.error(f"Failed to build about text: {e}")
        except Exception:
            pass
        await message.answer("Не удалось получить статистику. Попробуйте позже.")
async def main():
    try:
        info_logger.info("Starting bot...")
        await bot.delete_webhook(drop_pending_updates=True)
        asyncio.create_task(cleanup_rate_limits_and_sessions())
        asyncio.create_task(cleanup_caches())
        asyncio.create_task(send_notifications())
        asyncio.create_task(check_user_blocks())
        asyncio.create_task(check_for_chat_partners())
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        error_logger.critical(f"Fatal error in main: {e}", exc_info=True)
        admin_id = ADMIN_IDS[0] if ADMIN_IDS else None
        if admin_id:
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=f"🚨 Бот упал с критической ошибкой: {str(e)}",
                    parse_mode=ParseMode.HTML  # Правильное использование после импорта
                )
            except Exception as send_error:
                error_logger.error(f"Failed to notify admin about crash: {send_error}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())