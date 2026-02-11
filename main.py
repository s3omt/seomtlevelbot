import discord
from discord.ext import commands, tasks
from discord.ui import Button, View
import json
import asyncio
import datetime
from datetime import time as datetime_time
import sys
import aiohttp
from collections import defaultdict
import pytz
import math
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from typing import Dict, List, Optional, Tuple, Any
import threading
from flask import Flask, jsonify
import asyncpg
import os
import subprocess
import tempfile

# ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================
class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                os.environ.get("DATABASE_URL"),
                min_size=1,
                max_size=10
            )
        return self.pool

    async def init_db(self):
        pool = await self.connect()
        async with pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    messages INT DEFAULT 0,
                    voice_minutes INT DEFAULT 0
                )
            """)
            print("✅ Таблица users готова")

            # Таблица настроек серверов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_config (
                    guild_id BIGINT PRIMARY KEY,
                    log_channel BIGINT,
                    voice_events BOOLEAN DEFAULT TRUE,
                    role_events BOOLEAN DEFAULT TRUE,
                    member_events BOOLEAN DEFAULT TRUE,
                    channel_events BOOLEAN DEFAULT TRUE,
                    server_events BOOLEAN DEFAULT TRUE,
                    message_events BOOLEAN DEFAULT FALSE,
                    command_events BOOLEAN DEFAULT TRUE,
                    telegram_notify_role BOOLEAN DEFAULT FALSE,
                    telegram_daily_report BOOLEAN DEFAULT TRUE,
                    backup_channel BIGINT,
                    economy_enabled BOOLEAN DEFAULT TRUE,
                    achievements_enabled BOOLEAN DEFAULT TRUE
                )
            """)
            print("✅ Таблица guild_config готова")

            # Таблица предупреждений
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS warns (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    user_id BIGINT,
                    moderator_id BIGINT,
                    reason TEXT,
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """)
            print("✅ Таблица warns готова")

            # Таблица уровней
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS levels (
                    user_id BIGINT PRIMARY KEY,
                    xp INT DEFAULT 0,
                    level INT DEFAULT 0,
                    last_xp_time TIMESTAMP DEFAULT NOW()
                )
            """)
            print("✅ Таблица levels готова")

            # Таблица истории активности пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_history (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    guild_id BIGINT,
                    date DATE DEFAULT CURRENT_DATE,
                    voice_minutes INT DEFAULT 0,
                    messages INT DEFAULT 0,
                    UNIQUE(user_id, guild_id, date)
                )
            """)
            print("✅ Таблица user_history готова")

            # ----- НОВОЕ: ТАБЛИЦЫ ДЛЯ ЭКОНОМИКИ -----
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS economy (
                    user_id BIGINT PRIMARY KEY,
                    balance BIGINT DEFAULT 0,
                    total_earned BIGINT DEFAULT 0,
                    last_daily TIMESTAMP
                )
            """)
            print("✅ Таблица economy готова")

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS shop_roles (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    role_id BIGINT,
                    price BIGINT,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            print("✅ Таблица shop_roles готова")

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS purchased_roles (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    user_id BIGINT,
                    role_id BIGINT,
                    purchased_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(guild_id, user_id, role_id)
                )
            """)
            print("✅ Таблица purchased_roles готова")

            # ----- НОВОЕ: ТАБЛИЦЫ ДЛЯ ДОСТИЖЕНИЙ -----
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS achievements (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE,
                    description TEXT,
                    xp_reward INT DEFAULT 0,
                    coin_reward BIGINT DEFAULT 0,
                    icon TEXT DEFAULT '🏆',
                    hidden BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            print("✅ Таблица achievements готова")

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_achievements (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    achievement_id INT,
                    earned_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, achievement_id)
                )
            """)
            print("✅ Таблица user_achievements готова")

            # ----- НОВОЕ: ТАБЛИЦА ДЛЯ СТАТИСТИКИ СЕРВЕРА -----
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS server_history (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    date DATE DEFAULT CURRENT_DATE,
                    total_messages INT DEFAULT 0,
                    total_voice_minutes INT DEFAULT 0,
                    active_users INT DEFAULT 0,
                    new_members INT DEFAULT 0,
                    UNIQUE(guild_id, date)
                )
            """)
            print("✅ Таблица server_history готова")

    # ----- СУЩЕСТВУЮЩИЕ МЕТОДЫ (БЕЗ ИЗМЕНЕНИЙ) -----
    async def add_message(self, user_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, messages) VALUES ($1, 1)
                ON CONFLICT (user_id) DO UPDATE
                SET messages = users.messages + 1
            """, user_id)

    async def add_voice_time(self, user_id: int, minutes: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, voice_minutes) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE
                SET voice_minutes = users.voice_minutes + $2
            """, user_id, minutes)

    async def get_user_stats(self, user_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT messages, voice_minutes FROM users WHERE user_id = $1",
                user_id
            )
            if row:
                return {
                    'messages': row['messages'],
                    'voice_minutes': row['voice_minutes'],
                    'voice_hours': row['voice_minutes'] // 60,
                    'voice_remaining_minutes': row['voice_minutes'] % 60
                }
            else:
                return {
                    'messages': 0,
                    'voice_minutes': 0,
                    'voice_hours': 0,
                    'voice_remaining_minutes': 0
                }

    async def get_top_users(self, limit: int = 10):
        pool = await self.connect()
        async with pool.acquire() as conn:
            voice_rows = await conn.fetch("""
                SELECT user_id, voice_minutes FROM users
                ORDER BY voice_minutes DESC LIMIT $1
            """, limit)
            msg_rows = await conn.fetch("""
                SELECT user_id, messages FROM users
                ORDER BY messages DESC LIMIT $1
            """, limit)
            return (
                [(row['user_id'], row['voice_minutes']) for row in voice_rows],
                [(row['user_id'], row['messages']) for row in msg_rows]
            )

    async def get_total_users(self):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchval("SELECT COUNT(*) FROM users")
            return row

    async def get_total_stats(self):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    COALESCE(SUM(messages), 0) as total_messages,
                    COALESCE(SUM(voice_minutes), 0) as total_voice
                FROM users
            """)
            return {
                'total_messages': row['total_messages'],
                'total_voice': row['total_voice']
            }

    # ----- МЕТОДЫ ДЛЯ РАБОТЫ С УРОВНЯМИ -----
    async def add_xp(self, user_id: int, xp: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT xp, level FROM levels WHERE user_id = $1",
                user_id
            )
            if row:
                new_xp = row['xp'] + xp
                old_level = row['level']
            else:
                new_xp = xp
                old_level = 0
                await conn.execute(
                    "INSERT INTO levels (user_id, xp, level) VALUES ($1, 0, 0)",
                    user_id
                )

            new_level = int((math.sqrt(100 * (2 * new_xp + 25)) + 50) // 100)

            await conn.execute("""
                UPDATE levels 
                SET xp = $1, level = $2, last_xp_time = NOW()
                WHERE user_id = $3
            """, new_xp, new_level, user_id)

            return new_level > old_level, new_level

    async def get_level_info(self, user_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT xp, level FROM levels WHERE user_id = $1",
                user_id
            )
            if row:
                xp = row['xp']
                level = row['level']
                next_level_xp = int(((level + 1) * 100 - 50) ** 2 / 100)
                progress = xp / next_level_xp if next_level_xp > 0 else 0
                return {
                    'xp': xp,
                    'level': level,
                    'next_xp': next_level_xp,
                    'progress': progress,
                    'remaining': next_level_xp - xp
                }
            else:
                return {
                    'xp': 0,
                    'level': 0,
                    'next_xp': 25,
                    'progress': 0,
                    'remaining': 25
                }

    # ----- ИСТОРИЯ АКТИВНОСТИ -----
    async def save_daily_stats(self, user_id: int, guild_id: int, voice_minutes: int, messages: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO user_history (user_id, guild_id, date, voice_minutes, messages)
                VALUES ($1, $2, CURRENT_DATE, $3, $4)
                ON CONFLICT (user_id, guild_id, date) DO UPDATE
                SET voice_minutes = EXCLUDED.voice_minutes,
                    messages = EXCLUDED.messages
            """, user_id, guild_id, voice_minutes, messages)

    async def get_user_history(self, user_id: int, guild_id: int, days: int = 30):
        pool = await self.connect()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT date, voice_minutes, messages
                FROM user_history
                WHERE user_id = $1 AND guild_id = $2
                ORDER BY date DESC
                LIMIT $3
            """, user_id, guild_id, days)
            return [dict(row) for row in rows]

    # ----- НАСТРОЙКИ СЕРВЕРОВ -----
    async def get_guild_config(self, guild_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM guild_config WHERE guild_id = $1",
                guild_id
            )
            if row:
                return dict(row)
            else:
                return {
                    'guild_id': guild_id,
                    'log_channel': None,
                    'voice_events': True,
                    'role_events': True,
                    'member_events': True,
                    'channel_events': True,
                    'server_events': True,
                    'message_events': False,
                    'command_events': True,
                    'telegram_notify_role': False,
                    'telegram_daily_report': True,
                    'backup_channel': None,
                    'economy_enabled': True,
                    'achievements_enabled': True
                }

    async def set_log_channel(self, guild_id: int, channel_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_config (guild_id, log_channel)
                VALUES ($1, $2)
                ON CONFLICT (guild_id) DO UPDATE SET log_channel = $2
            """, guild_id, channel_id)

    async def set_backup_channel(self, guild_id: int, channel_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_config (guild_id, backup_channel)
                VALUES ($1, $2)
                ON CONFLICT (guild_id) DO UPDATE SET backup_channel = $2
            """, guild_id, channel_id)

    async def update_guild_config(self, guild_id: int, key: str, value):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO guild_config (guild_id, {key})
                VALUES ($1, $2)
                ON CONFLICT (guild_id) DO UPDATE SET {key} = $2
            """, guild_id, value)

    # ----- ПРЕДУПРЕЖДЕНИЯ -----
    async def add_warn(self, guild_id: int, user_id: int, moderator_id: int, reason: str):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO warns (guild_id, user_id, moderator_id, reason)
                VALUES ($1, $2, $3, $4)
            """, guild_id, user_id, moderator_id, reason)

    async def get_warns(self, guild_id: int, user_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM warns 
                WHERE guild_id = $1 AND user_id = $2 
                ORDER BY timestamp DESC
            """, guild_id, user_id)
            return [dict(row) for row in rows]

    async def clear_warns(self, guild_id: int, user_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM warns WHERE guild_id = $1 AND user_id = $2
            """, guild_id, user_id)

    async def remove_warn(self, warn_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM warns WHERE id = $1", warn_id)

    # ----- НОВОЕ: МЕТОДЫ ДЛЯ ЭКОНОМИКИ -----
    async def get_balance(self, user_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT balance FROM economy WHERE user_id = $1",
                user_id
            )
            return row['balance'] if row else 0

    async def add_coins(self, user_id: int, amount: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO economy (user_id, balance, total_earned)
                VALUES ($1, $2, $2)
                ON CONFLICT (user_id) DO UPDATE
                SET balance = economy.balance + $2,
                    total_earned = economy.total_earned + $2
            """, user_id, amount)

    async def remove_coins(self, user_id: int, amount: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE economy SET balance = balance - $1
                WHERE user_id = $2 AND balance >= $1
            """, amount, user_id)

    async def get_eco_top(self, limit: int = 10):
        pool = await self.connect()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT user_id, balance FROM economy
                ORDER BY balance DESC LIMIT $1
            """, limit)
            return [(row['user_id'], row['balance']) for row in rows]

    # ----- МАГАЗИН РОЛЕЙ -----
    async def add_shop_role(self, guild_id: int, role_id: int, price: int, description: str = None):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO shop_roles (guild_id, role_id, price, description)
                VALUES ($1, $2, $3, $4)
            """, guild_id, role_id, price, description or "Нет описания")

    async def remove_shop_role(self, role_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM shop_roles WHERE role_id = $1", role_id)

    async def get_shop_roles(self, guild_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM shop_roles WHERE guild_id = $1 ORDER BY price
            """, guild_id)
            return [dict(row) for row in rows]

    async def purchase_role(self, guild_id: int, user_id: int, role_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO purchased_roles (guild_id, user_id, role_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (guild_id, user_id, role_id) DO NOTHING
            """, guild_id, user_id, role_id)

    async def has_role_purchased(self, guild_id: int, user_id: int, role_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 1 FROM purchased_roles
                WHERE guild_id = $1 AND user_id = $2 AND role_id = $3
            """, guild_id, user_id, role_id)
            return row is not None

    # ----- НОВОЕ: МЕТОДЫ ДЛЯ ДОСТИЖЕНИЙ -----
    async def init_achievements(self):
        """Инициализирует базовый список достижений"""
        achievements = [
            ("chat_100", "Пиздaбoл", "Написать 100 сообщений", 50, 100, "💬"),
            ("chat_1000", "Графоман", "Написать 1000 сообщений", 200, 500, "📝"),
            ("voice_10h", "Микро...селебрити", "Провести 10 часов в голосовом канале", 50, 100, "🎤"),
            ("voice_100h", "Диктор Саша", "Провести 100 часов в голосовом канале", 200, 500, "📻"),
            ("level_5", "Мдэ", "Достичь 5 уровня", 0, 0, "🌱"),
            ("level_10", "Пикабушник", "Достичь 10 уровня", 0, 0, "🌿"),
            ("level_20", "Ньюдвачер", "Достичь 20 уровня", 0, 0, "⭐"),
            ("level_30", "Олд", "Достичь 30 уровня", 0, 0, "💎"),
            ("level_50", "Ничанер-придурок", "Достичь 50 уровня", 0, 0, "👑"),
            ("first_warning", "Рома дал этому доходяге по eбaлy", "Получить первое предупреждение", 0, -50, "⚠️"),
            ("first_purchase", "Шопинг для гeeв", "Купить первую роль в магазине", 20, 0, "🛒"),
        ]
        pool = await self.connect()
        async with pool.acquire() as conn:
            for name, title, desc, xp, coins, icon in achievements:
                await conn.execute("""
                    INSERT INTO achievements (name, description, xp_reward, coin_reward, icon)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (name) DO UPDATE
                    SET description = EXCLUDED.description,
                        xp_reward = EXCLUDED.xp_reward,
                        coin_reward = EXCLUDED.coin_reward,
                        icon = EXCLUDED.icon
                """, name, f"{title}: {desc}", xp, coins, icon)
        print("✅ Достижения инициализированы")

    async def check_achievement(self, user_id: int, achievement_name: str, guild: discord.Guild = None):
        """Проверяет, получено ли достижение, и выдаёт награду"""
        pool = await self.connect()
        async with pool.acquire() as conn:
            # Получаем ID достижения
            ach = await conn.fetchrow(
                "SELECT id, xp_reward, coin_reward, icon, description FROM achievements WHERE name = $1",
                achievement_name
            )
            if not ach:
                return False

            # Проверяем, не получено ли уже
            earned = await conn.fetchval(
                "SELECT 1 FROM user_achievements WHERE user_id = $1 AND achievement_id = $2",
                user_id, ach['id']
            )
            if earned:
                return False

            # Выдаём достижение
            await conn.execute("""
                INSERT INTO user_achievements (user_id, achievement_id)
                VALUES ($1, $2)
            """, user_id, ach['id'])

            # Награды: опыт и монеты
            if ach['xp_reward'] > 0:
                await self.add_xp(user_id, ach['xp_reward'])
            if ach['coin_reward'] > 0:
                await self.add_coins(user_id, ach['coin_reward'])
            elif ach['coin_reward'] < 0:
                await self.remove_coins(user_id, -ach['coin_reward'])

            # Логирование и уведомление
            if guild:
                config = await self.get_guild_config(guild.id)
                if config.get('log_channel'):
                    await Logger.log_event(
                        guild=guild,
                        event_type="achievement",
                        title="🏆 Получено достижение",
                        description=f"{ach['icon']} **{ach['description']}**",
                        color=0xffd700,
                        user=discord.utils.get(guild.members, id=user_id),
                        fields={
                            "Опыт": f"+{ach['xp_reward']}" if ach['xp_reward'] else "0",
                            "Монеты": f"+{ach['coin_reward']}" if ach['coin_reward'] else "0"
                        }
                    )
            return True

    async def get_user_achievements(self, user_id: int):
        pool = await self.connect()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT a.id, a.name, a.description, a.icon, ua.earned_at
                FROM user_achievements ua
                JOIN achievements a ON ua.achievement_id = a.id
                WHERE ua.user_id = $1
                ORDER BY ua.earned_at DESC
            """, user_id)
            return [dict(row) for row in rows]

    async def get_all_achievements(self):
        pool = await self.connect()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM achievements ORDER BY id
            """)
            return [dict(row) for row in rows]

    # ----- НОВОЕ: СТАТИСТИКА СЕРВЕРА -----
    async def save_server_stats(self, guild_id: int, date: datetime.date = None):
        """Собирает и сохраняет дневную статистику сервера"""
        if date is None:
            date = datetime.date.today()

        pool = await self.connect()
        async with pool.acquire() as conn:
            # Получаем статистику пользователей сервера
            guild = bot.get_guild(guild_id)
            if not guild:
                return

            total_messages = 0
            total_voice = 0
            active_users = 0

            for member in guild.members:
                if member.bot:
                    continue
                stats = await self.get_user_stats(member.id)
                total_messages += stats['messages']
                total_voice += stats['voice_minutes']
                if stats['messages'] > 0 or stats['voice_minutes'] > 0:
                    active_users += 1

            # Получаем количество новых участников за день
            new_members = 0
            for member in guild.members:
                if member.joined_at and member.joined_at.date() == date:
                    new_members += 1

            await conn.execute("""
                INSERT INTO server_history (guild_id, date, total_messages, total_voice_minutes, active_users, new_members)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (guild_id, date) DO UPDATE
                SET total_messages = EXCLUDED.total_messages,
                    total_voice_minutes = EXCLUDED.total_voice_minutes,
                    active_users = EXCLUDED.active_users,
                    new_members = EXCLUDED.new_members
            """, guild_id, date, total_messages, total_voice, active_users, new_members)

    async def get_server_stats(self, guild_id: int, days: int = 7):
        """Возвращает статистику сервера за последние N дней"""
        pool = await self.connect()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM server_history
                WHERE guild_id = $1
                ORDER BY date DESC
                LIMIT $2
            """, guild_id, days)
            return [dict(row) for row in rows]

db = Database()

# ==================== КОНФИГУРАЦИЯ ====================
TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TOKEN:
    print("❌ ОШИБКА: Токен Discord бота не найден!")
    sys.exit(1)

# Настройки времени (Московское время)
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def get_moscow_time(dt=None):
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    elif dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt.astimezone(MOSCOW_TZ)

def format_moscow_time(dt=None, format_str="%d.%m.%Y %H:%M:%S"):
    return get_moscow_time(dt).strftime(format_str)

# ==================== КОНФИГУРАЦИЯ РОЛЕЙ ПО УРОВНЯМ ====================
LEVEL_ROLES = {
    5: "Ньюфажина",
    10: "Нормис",
    20: "Бывалый",
    30: "Альтуха",
    40: "Опиум",
    50: "Игрок",
    60: "Тектоник",
    70: "Вайперр",
    85: "Модератор по сиськам",
    100: "Админ по ляжкам"
}

DEFAULT_ROLE_NAME = "Залётный"

# ==================== СОЗДАНИЕ БОТА ====================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.voice_states = True
intents.messages = True
intents.guilds = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    help_command=None
)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
voice_sessions = {}
guild_config_cache = {}

# ==================== TELEGRAM БОТ ====================
class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.enabled = bool(token and chat_id)
        self.session = None
        self.polling_task = None

    async def ensure_session(self):
        if self.session is None and self.enabled:
            self.session = aiohttp.ClientSession()

    async def send_message(self, text: str) -> bool:
        if not self.enabled:
            return False
        try:
            await self.ensure_session()
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "Markdown"
            }
            async with self.session.post(f"{self.base_url}/sendMessage", json=payload) as resp:
                return resp.status == 200
        except Exception as e:
            print(f"❌ Telegram send error: {e}")
            return False

    async def send_document(self, file_path: str, caption: str = "") -> bool:
        if not self.enabled:
            return False
        await self.ensure_session()
        with open(file_path, 'rb') as f:
            data = aiohttp.FormData()
            data.add_field('chat_id', self.chat_id)
            data.add_field('caption', caption)
            data.add_field('document', f, filename=os.path.basename(file_path))
            async with self.session.post(f"{self.base_url}/sendDocument", data=data) as resp:
                return resp.status == 200

    async def send_stats(self) -> bool:
        if not self.enabled:
            return False
        total_users = await db.get_total_users()
        totals = await db.get_total_stats()
        total_messages = totals['total_messages']
        total_voice_minutes_total = totals['total_voice']
        total_voice_hours = total_voice_minutes_total // 60
        total_voice_minutes = total_voice_minutes_total % 60

        voice_top, _ = await db.get_top_users(3)
        top_text = ""
        for i, (user_id, minutes) in enumerate(voice_top, 1):
            hours = minutes // 60
            mins = minutes % 60
            top_text += f"{i}. ID `{user_id}` — {hours}ч {mins}м\n"

        message = f"""
📊 *СТАТИСТИКА DISCORD БОТА*

👥 **Пользователей:** `{total_users}`
💬 **Сообщений:** `{total_messages}`
🎤 **Голосовая активность:** `{total_voice_hours}ч {total_voice_minutes}м`
🏠 **Серверов:** `{len(bot.guilds)}`

🏆 **Топ 3 по голосу:**
{top_text}
⏰ *{format_moscow_time()}*
        """
        return await self.send_message(message)

    async def send_alert(self, title: str, description: str, alert_type: str = "info") -> bool:
        if not self.enabled:
            return False
        emoji = {
            "info": "ℹ️", "success": "✅", "warning": "⚠️",
            "error": "❌", "critical": "🚨"
        }.get(alert_type, "📝")
        message = f"{emoji} *{title}*\n\n{description}\n\n⏰ {format_moscow_time()}"
        return await self.send_message(message)

    async def start_polling(self):
        if not self.enabled:
            return
        self.polling_task = asyncio.create_task(self._polling_loop())
        print("📱 Telegram polling запущен")

    async def _polling_loop(self):
        offset = 0
        await self.ensure_session()
        while True:
            try:
                params = {"offset": offset + 1, "timeout": 30}
                async with self.session.get(f"{self.base_url}/getUpdates", params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for update in data.get("result", []):
                            offset = update["update_id"]
                            await self._process_update(update)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Telegram polling error: {e}")
                await asyncio.sleep(5)

    async def _process_update(self, update):
        if "message" not in update:
            return
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        if str(chat_id) != self.chat_id:
            return
        if "text" not in msg:
            return
        text = msg["text"].strip()

        if text == "/start":
            await self.send_message(
                "🤖 *Discord Bot Telegram Monitor*\n\n"
                "Доступные команды:\n"
                "• `/stats` — статистика бота\n"
                "• `/top` — топ пользователей\n"
                "• `/roles` — список ролей по уровням\n"
                "• `/eco_top` — топ богачей\n"
                "• `/help` — помощь"
            )
        elif text == "/stats":
            await self.send_stats()
        elif text == "/top":
            voice_top, msg_top = await db.get_top_users(5)
            text_lines = ["🏆 *Топ по голосовой активности:*"]
            for i, (uid, minutes) in enumerate(voice_top, 1):
                text_lines.append(f"{i}. ID `{uid}` — {minutes//60}ч {minutes%60}м")
            text_lines.append("\n💬 *Топ по сообщениям:*")
            for i, (uid, count) in enumerate(msg_top, 1):
                text_lines.append(f"{i}. ID `{uid}` — {count} сообщ.")
            await self.send_message("\n".join(text_lines))
        elif text == "/roles":
            lines = ["🎖️ *Роли за уровни:*\n"]
            for level, role in LEVEL_ROLES.items():
                lines.append(f"**Уровень {level}** — {role}")
            await self.send_message("\n".join(lines))
        elif text == "/eco_top":
            top = await db.get_eco_top(5)
            lines = ["💰 *Топ по монетам:*"]
            for i, (uid, balance) in enumerate(top, 1):
                lines.append(f"{i}. ID `{uid}` — {balance} 🪙")
            await self.send_message("\n".join(lines))
        elif text == "/help":
            await self.send_message(
                "📚 *Команды Telegram:*\n\n"
                "`/stats` — статистика бота\n"
                "`/top` — топ пользователей\n"
                "`/roles` — список ролей\n"
                "`/eco_top` — топ богачей\n"
                "`/help` — это сообщение"
            )

    async def stop_polling(self):
        if self.polling_task:
            self.polling_task.cancel()
            try:
                await self.polling_task
            except asyncio.CancelledError:
                pass
            self.polling_task = None

    async def close(self):
        await self.stop_polling()
        if self.session:
            await self.session.close()

telegram = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
async def get_guild_config(guild_id: int):
    if guild_id not in guild_config_cache:
        config = await db.get_guild_config(guild_id)
        guild_config_cache[guild_id] = config
    return guild_config_cache[guild_id]

async def update_guild_config(guild_id: int, key: str, value):
    await db.update_guild_config(guild_id, key, value)
    if guild_id in guild_config_cache:
        guild_config_cache[guild_id][key] = value
    else:
        config = await db.get_guild_config(guild_id)
        config[key] = value
        guild_config_cache[guild_id] = config

async def set_log_channel(guild_id: int, channel_id: int):
    await db.set_log_channel(guild_id, channel_id)
    if guild_id in guild_config_cache:
        guild_config_cache[guild_id]['log_channel'] = channel_id
    else:
        config = await db.get_guild_config(guild_id)
        config['log_channel'] = channel_id
        guild_config_cache[guild_id] = config

async def set_backup_channel(guild_id: int, channel_id: int):
    await db.set_backup_channel(guild_id, channel_id)
    if guild_id in guild_config_cache:
        guild_config_cache[guild_id]['backup_channel'] = channel_id
    else:
        config = await db.get_guild_config(guild_id)
        config['backup_channel'] = channel_id
        guild_config_cache[guild_id] = config

# ==================== ЛОГГЕР ====================
class Logger:
    @staticmethod
    async def log_event(guild: discord.Guild, event_type: str, title: str, description: str,
                       color: int = None, fields: Dict = None, user: discord.Member = None,
                       target: discord.Member = None, channel: discord.abc.GuildChannel = None) -> None:
        try:
            config = await get_guild_config(guild.id)
            log_channel_id = config.get('log_channel')
            if not log_channel_id:
                return

            log_channel_obj = guild.get_channel(log_channel_id)
            if not log_channel_obj:
                return

            config_keys = {
                "voice": "voice_events", "role": "role_events",
                "member": "member_events", "channel": "channel_events",
                "server": "server_events", "message": "message_events",
                "command": "command_events",
                "achievement": "role_events",   # используем role_events или отдельный ключ
                "economy": "command_events"
            }
            if event_type in config_keys and not config.get(config_keys[event_type], True):
                return

            color_map = {
                "voice": 0x3498db, "role": 0x2ecc71, "member": 0xe67e22,
                "channel": 0x9b59b6, "server": 0xe74c3c, "command": 0x1abc9c,
                "message": 0x95a5a6, "achievement": 0xffd700, "economy": 0xf1c40f
            }

            embed = discord.Embed(
                title=f"📝 {title}",
                description=description,
                color=color or color_map.get(event_type, 0x95a5a6),
                timestamp=get_moscow_time()
            )

            event_icons = {
                "voice": "🎤", "role": "👑", "member": "👤", "channel": "📺",
                "server": "🏠", "command": "⚙️", "message": "💬",
                "achievement": "🏆", "economy": "💰"
            }

            embed.set_author(
                name=f"{event_icons.get(event_type, '📝')} {event_type.upper()}",
                icon_url=guild.icon.url if guild.icon else None
            )

            if user:
                embed.add_field(name="👤 Пользователь",
                              value=f"{user.mention}\nID: `{user.id}`", inline=True)
                embed.set_thumbnail(url=user.display_avatar.url)

            if target:
                embed.add_field(name="🎯 Цель",
                              value=f"{target.mention}\nID: `{target.id}`", inline=True)

            if channel:
                embed.add_field(name="📺 Канал",
                              value=f"{channel.mention}\nID: `{channel.id}`", inline=True)

            if fields:
                for name, value in fields.items():
                    embed.add_field(name=name, value=str(value), inline=False)

            embed.set_footer(text="Время МСК")
            await log_channel_obj.send(embed=embed)

        except Exception as e:
            print(f"❌ Logger error: {e}")

# ==================== МЕНЕДЖЕР РОЛЕЙ ====================
class RoleManager:
    @staticmethod
    async def check_hierarchy(guild: discord.Guild, role: discord.Role) -> bool:
        bot_member = guild.get_member(bot.user.id)
        if not bot_member or not bot_member.guild_permissions.manage_roles:
            return False
        return role.position < bot_member.top_role.position

    @staticmethod
    async def ensure_role_exists(guild: discord.Guild, role_name: str):
        role = discord.utils.get(guild.roles, name=role_name)
        if role:
            return role
        try:
            color = discord.Color.from_rgb(
                (hash(role_name) & 0xFF0000) >> 16,
                (hash(role_name) & 0x00FF00) >> 8,
                hash(role_name) & 0x0000FF
            )
            role = await guild.create_role(
                name=role_name,
                color=color,
                hoist=True,
                mentionable=False,
                reason="Автоматическое создание роли для уровней"
            )
            print(f"✅ Создана роль {role_name} на {guild.name}")
            await Logger.log_event(
                guild=guild,
                event_type="role",
                title="Создана новая роль",
                description=f"Роль **{role_name}** создана автоматически",
                color=0x2ecc71,
                fields={"Причина": "Система уровней"}
            )
            return role
        except Exception as e:
            print(f"❌ Ошибка создания роли {role_name}: {e}")
            return None

    @staticmethod
    async def give_default_role(member: discord.Member):
        try:
            for level_role in LEVEL_ROLES.values():
                role = discord.utils.get(member.guild.roles, name=level_role)
                if role and role in member.roles:
                    return

            role = discord.utils.get(member.guild.roles, name=DEFAULT_ROLE_NAME)
            if not role:
                role = await RoleManager.ensure_role_exists(member.guild, DEFAULT_ROLE_NAME)
            if role and role not in member.roles and await RoleManager.check_hierarchy(member.guild, role):
                await member.add_roles(role, reason="Начальная роль при входе")
                print(f"✅ Выдана начальная роль {DEFAULT_ROLE_NAME} {member}")
                await Logger.log_event(
                    guild=member.guild,
                    event_type="role",
                    title="Выдана начальная роль",
                    description=f"Пользователь {member.mention} получил роль **{DEFAULT_ROLE_NAME}**",
                    color=0x2ecc71,
                    user=member
                )
        except Exception as e:
            print(f"❌ Ошибка выдачи начальной роли: {e}")

    @staticmethod
    async def check_and_give_roles(member: discord.Member):
        try:
            level_info = await db.get_level_info(member.id)
            current_level = level_info['level']

            target_role_name = None
            for threshold in sorted(LEVEL_ROLES.keys(), reverse=True):
                if current_level >= threshold:
                    target_role_name = LEVEL_ROLES[threshold]
                    break

            if not target_role_name:
                return

            target_role = discord.utils.get(member.guild.roles, name=target_role_name)
            if not target_role:
                target_role = await RoleManager.ensure_role_exists(member.guild, target_role_name)
                if not target_role:
                    return

            if not await RoleManager.check_hierarchy(member.guild, target_role):
                print(f"⚠️ Невозможно выдать роль {target_role_name}: недостаточно прав")
                return

            if target_role in member.roles:
                return

            roles_to_remove = []
            for role_name in LEVEL_ROLES.values():
                if role_name == target_role_name:
                    continue
                old_role = discord.utils.get(member.guild.roles, name=role_name)
                if old_role and old_role in member.roles:
                    roles_to_remove.append(old_role)

            default_role = discord.utils.get(member.guild.roles, name=DEFAULT_ROLE_NAME)
            if default_role and default_role in member.roles:
                roles_to_remove.append(default_role)

            if roles_to_remove:
                await member.remove_roles(*roles_to_remove, reason="Обновление роли по уровню")

            await member.add_roles(target_role, reason=f"Достигнут уровень {current_level}")
            print(f"✅ {member} получил роль {target_role_name} (уровень {current_level})")

            await Logger.log_event(
                guild=member.guild,
                event_type="role",
                title="Получена новая роль",
                description=f"Пользователь {member.mention} получил роль **{target_role_name}**",
                color=0x2ecc71,
                user=member,
                fields={"Уровень": str(current_level), "Опыт": f"{level_info['xp']} XP"}
            )

            if telegram.enabled:
                config = await get_guild_config(member.guild.id)
                if config.get("telegram_notify_role", False):
                    await telegram.send_alert(
                        "🎉 Новая роль по уровню",
                        f"Пользователь **{member.display_name}** получил роль **{target_role_name}**\n\n"
                        f"📈 Уровень: **{current_level}**\n"
                        f"✨ Опыт: {level_info['xp']} XP",
                        "success"
                    )

        except Exception as e:
            print(f"❌ Ошибка обновления ролей по уровню: {e}")

# ==================== ЗАДАЧИ ====================
@tasks.loop(minutes=5)
async def check_voice_time():
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        for user_id, session_start in list(voice_sessions.items()):
            duration = (now - session_start).total_seconds() / 60
            member_id = int(user_id)
            for guild in bot.guilds:
                member = guild.get_member(member_id)
                if member and member.voice and member.voice.channel:
                    await db.add_voice_time(member_id, 5)
                    # Монеты за голос (1 монета за 5 минут)
                    await db.add_coins(member_id, 1)
                    leveled_up, new_level = await db.add_xp(member_id, 10)
                    if leveled_up:
                        try:
                            await member.send(f"🎉 Поздравляю! Вы достигли **{new_level} уровня**!")
                        except:
                            pass
                        await RoleManager.check_and_give_roles(member)
                    voice_sessions[user_id] = now - datetime.timedelta(minutes=duration % 5)
                    break
    except Exception as e:
        print(f"❌ Ошибка check_voice_time: {e}")

@tasks.loop(hours=24)
async def daily_report():
    try:
        for guild in bot.guilds:
            config = await get_guild_config(guild.id)
            if telegram.enabled and config.get("telegram_daily_report", True):
                await telegram.send_stats()
                print(f"📊 Ежедневный отчет отправлен в Telegram для {guild.name}")
                break
    except Exception as e:
        print(f"❌ Ошибка daily_report: {e}")

@tasks.loop(time=datetime_time(hour=17, minute=0))
async def weekly_top():
    now = get_moscow_time()
    if now.weekday() != 6:
        return

    for guild in bot.guilds:
        voice_top, msg_top = await db.get_top_users(10)

        embed = discord.Embed(
            title="📆 **Еженедельный топ**",
            description="Самые активные участники за последние 7 дней",
            color=discord.Color.gold(),
            timestamp=get_moscow_time()
        )

        voice_text = ""
        for i, (uid, minutes) in enumerate(voice_top[:5], 1):
            member = guild.get_member(uid)
            name = member.display_name if member else f"ID: {uid}"
            voice_text += f"{i}. **{name}** — {minutes // 60}ч {minutes % 60}м\n"
        embed.add_field(name="🎤 Голосовая активность (Топ 5)", 
                        value=voice_text or "Нет данных", 
                        inline=False)

        msg_text = ""
        for i, (uid, count) in enumerate(msg_top[:5], 1):
            member = guild.get_member(uid)
            name = member.display_name if member else f"ID: {uid}"
            msg_text += f"{i}. **{name}** — {count} сообщ.\n"
        embed.add_field(name="💬 Сообщения (Топ 5)", 
                        value=msg_text or "Нет данных", 
                        inline=False)

        embed.set_footer(text="Спасибо за активность! ❤️")

        channel = guild.system_channel
        if not channel or not channel.permissions_for(guild.me).send_messages:
            for ch in guild.text_channels:
                if ch.permissions_for(guild.me).send_messages:
                    channel = ch
                    break
        if channel:
            await channel.send(embed=embed)

@tasks.loop(time=datetime_time(hour=0, minute=5))
async def collect_stats():
    try:
        print("📊 Начинаем сбор дневной статистики...")
        for guild in bot.guilds:
            for member in guild.members:
                if member.bot:
                    continue
                stats = await db.get_user_stats(member.id)
                await db.save_daily_stats(
                    member.id,
                    guild.id,
                    stats['voice_minutes'],
                    stats['messages']
                )
            # Статистика сервера
            await db.save_server_stats(guild.id)
            print(f"   ✅ {guild.name}: статистика сохранена")
        print("✅ Дневная статистика успешно собрана")
    except Exception as e:
        print(f"❌ Ошибка сбора статистики: {e}")

# ----- НОВОЕ: АВТОБЭКАП БД В TELEGRAM -----
@tasks.loop(time=datetime_time(hour=3, minute=0))
async def backup_db():
    """Ежедневный бэкап базы данных в Telegram"""
    if not telegram.enabled:
        return
    try:
        # Проверяем наличие pg_dump
        pg_dump_path = subprocess.run(["which", "pg_dump"], capture_output=True, text=True).stdout.strip()
        if not pg_dump_path:
            print("⚠️ pg_dump не найден, пропускаем бэкап")
            return

        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            return

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"backup_{timestamp}.sql"

        result = subprocess.run(
            ["pg_dump", db_url, "-f", filename],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            # Отправляем в Telegram
            await telegram.send_document(filename, f"📦 Ежедневный бэкап БД\n⏰ {format_moscow_time()}")
            os.remove(filename)
            print(f"✅ Бэкап отправлен в Telegram: {filename}")

            # Также отправляем в канал логов, если указан
            for guild in bot.guilds:
                config = await get_guild_config(guild.id)
                if config.get('backup_channel'):
                    channel = guild.get_channel(config['backup_channel'])
                    if channel and channel.permissions_for(guild.me).send_messages:
                        with open(filename, 'rb') as f:
                            await channel.send(
                                f"📦 **Бэкап базы данных**\n⏰ {format_moscow_time()}",
                                file=discord.File(f, filename)
                            )
                        break
        else:
            print(f"❌ Ошибка pg_dump: {result.stderr}")

    except Exception as e:
        print(f"❌ Ошибка бэкапа: {e}")

@backup_db.before_loop
async def before_backup():
    await bot.wait_until_ready()

# ==================== СОБЫТИЯ DISCORD ====================
@bot.event
async def on_ready():
    print(f"✅ Бот {bot.user} запущен!")
    print(f"📊 Серверов: {len(bot.guilds)}")

    await db.init_db()
    await db.init_achievements()  # Инициализация достижений
    print("✅ База данных подключена")
    print(f"🐍 Python: {sys.version}")
    print(f"📱 Telegram: {'✅' if telegram.enabled else '❌'}")

    # Очистка старых слэш-команд
    try:
        bot.tree.clear_commands(guild=None)
        await bot.tree.sync()
        for guild in bot.guilds:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
        print("🧹 Слэш-команды очищены")
    except Exception as e:
        print(f"⚠️ Ошибка очистки команд: {e}")

    # Запуск задач
    if not check_voice_time.is_running():
        check_voice_time.start()
        print("⏱️ Запущена проверка голосового времени")
    if telegram.enabled and not daily_report.is_running():
        daily_report.start()
        print("📊 Запущен ежедневный отчет в Telegram")
    if telegram.enabled:
        await telegram.start_polling()
    if not weekly_top.is_running():
        weekly_top.start()
        print("📆 Запущена еженедельная отправка топов")
    if not collect_stats.is_running():
        collect_stats.start()
        print("📊 Запущен сбор дневной статистики")
    if telegram.enabled and not backup_db.is_running():
        backup_db.start()
        print("💾 Запущен ежедневный бэкап БД")

    # Создаём роли уровней на всех серверах
    for guild in bot.guilds:
        print(f"\n🔍 Сервер: {guild.name}")
        await RoleManager.ensure_role_exists(guild, DEFAULT_ROLE_NAME)
        for role_name in LEVEL_ROLES.values():
            await RoleManager.ensure_role_exists(guild, role_name)

    # Выдаём начальные роли всем участникам
    print("\n🎯 Выдача начальных ролей...")
    for guild in bot.guilds:
        members = [m for m in guild.members if not m.bot]
        print(f"   {guild.name}: {len(members)} участников")
        for member in members:
            await RoleManager.give_default_role(member)
            await asyncio.sleep(0.05)
    print("✅ Начальная выдача ролей завершена!")

    # Логирование запуска
    for guild in bot.guilds:
        await Logger.log_event(
            guild=guild,
            event_type="server",
            title="Бот запущен",
            description=f"Бот {bot.user.name} успешно запущен",
            color=0x2ecc71,
            fields={
                "Серверов": str(len(bot.guilds)),
                "Telegram": "✅" if telegram.enabled else "❌",
                "Время (МСК)": format_moscow_time()
            }
        )

    if telegram.enabled:
        total_users = await db.get_total_users()
        await telegram.send_alert(
            "🤖 Бот запущен",
            f"**{bot.user.name}** успешно запущен на Railway\n\n"
            f"🏠 Серверов: {len(bot.guilds)}\n"
            f"👥 Пользователей в базе: {total_users}\n"
            f"📅 Дата: {format_moscow_time()}",
            "success"
        )

@bot.event
async def on_member_join(member: discord.Member):
    if member.bot:
        return
    print(f"👤 Новый участник: {member}")
    await RoleManager.give_default_role(member)
    await Logger.log_event(
        guild=member.guild,
        event_type="member",
        title="Новый участник",
        description=f"Пользователь {member.mention} присоединился к серверу",
        color=0x2ecc71,
        user=member,
        fields={
            "Аккаунт создан": member.created_at.strftime("%d.%m.%Y"),
            "ID": member.id
        }
    )

@bot.event
async def on_member_remove(member: discord.Member):
    if member.bot:
        return
    print(f"👋 Участник вышел: {member}")
    await Logger.log_event(
        guild=member.guild,
        event_type="member",
        title="Участник вышел",
        description=f"Пользователь {member.mention} покинул сервер",
        color=0xe74c3c,
        user=member,
        fields={
            "Присоединился": member.joined_at.strftime("%d.%m.%Y") if member.joined_at else "Неизвестно",
            "ID": member.id
        }
    )

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if after.bot:
        return
    if before.display_name != after.display_name:
        await Logger.log_event(
            guild=after.guild,
            event_type="member",
            title="Изменен ник",
            description=f"Пользователь {after.mention} изменил ник",
            color=0xe67e22,
            user=after,
            fields={
                "Старый ник": before.display_name,
                "Новый ник": after.display_name
            }
        )

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if not message.content.startswith('!'):
        await db.add_message(message.author.id)
        # Монеты за сообщение (2 монеты)
        await db.add_coins(message.author.id, 2)
        leveled_up, new_level = await db.add_xp(message.author.id, 5)
        if leveled_up:
            try:
                await message.author.send(f"🎉 Поздравляю! Вы достигли **{new_level} уровня**!")
            except:
                pass
            await RoleManager.check_and_give_roles(message.author)
        if isinstance(message.author, discord.Member):
            await RoleManager.check_and_give_roles(message.author)

        # Проверка достижений
        if message.author.id:
            stats = await db.get_user_stats(message.author.id)
            level_info = await db.get_level_info(message.author.id)
            # За сообщения
            if stats['messages'] >= 100:
                await db.check_achievement(message.author.id, "chat_100", message.guild)
            if stats['messages'] >= 1000:
                await db.check_achievement(message.author.id, "chat_1000", message.guild)
            # За уровень
            if level_info['level'] >= 5:
                await db.check_achievement(message.author.id, "level_5", message.guild)
            if level_info['level'] >= 10:
                await db.check_achievement(message.author.id, "level_10", message.guild)
            if level_info['level'] >= 20:
                await db.check_achievement(message.author.id, "level_20", message.guild)
            if level_info['level'] >= 30:
                await db.check_achievement(message.author.id, "level_30", message.guild)
            if level_info['level'] >= 50:
                await db.check_achievement(message.author.id, "level_50", message.guild)

    await bot.process_commands(message)

@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    config = await get_guild_config(message.guild.id)
    if config.get("message_events", False):
        await Logger.log_event(
            guild=message.guild,
            event_type="message",
            title="Сообщение удалено",
            description=f"Сообщение от {message.author.mention} было удалено",
            color=0xe74c3c,
            user=message.author,
            channel=message.channel,
            fields={
                "Содержимое": message.content[:500] + ("..." if len(message.content) > 500 else "") if message.content else "*Без текста*",
                "Время удаления": format_moscow_time()
            }
        )

@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot or before.content == after.content:
        return
    config = await get_guild_config(before.guild.id)
    if config.get("message_events", False):
        await Logger.log_event(
            guild=before.guild,
            event_type="message",
            title="Сообщение отредактировано",
            description=f"Сообщение от {before.author.mention} было отредактировано",
            color=0xe67e22,
            user=before.author,
            channel=before.channel,
            fields={
                "Было": before.content[:500] + ("..." if len(before.content) > 500 else "") if before.content else "*Без текста*",
                "Стало": after.content[:500] + ("..." if len(after.content) > 500 else "") if after.content else "*Без текста*",
                "Ссылка": f"[Перейти к сообщению]({after.jump_url})"
            }
        )

@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return
    user_id = str(member.id)
    now = datetime.datetime.now(datetime.timezone.utc)

    if before.channel is None and after.channel is not None:
        voice_sessions[user_id] = now
        print(f"🎤 {member} зашел в {after.channel.name}")
        config = await get_guild_config(member.guild.id)
        if config.get("voice_events", True):
            await Logger.log_event(
                guild=member.guild,
                event_type="voice",
                title="Вход в голосовой канал",
                description=f"Пользователь {member.mention} зашел в голосовой канал",
                color=0x3498db,
                user=member,
                channel=after.channel,
                fields={
                    "Канал": after.channel.name,
                    "Время": format_moscow_time()
                }
            )

    elif before.channel is not None and after.channel is None:
        if user_id in voice_sessions:
            duration = (now - voice_sessions[user_id]).total_seconds() / 60
            if duration >= 1:
                await db.add_voice_time(member.id, int(duration))
                # Монеты за голос (1 монета за 5 минут)
                coin_gain = int(duration) // 5
                if coin_gain > 0:
                    await db.add_coins(member.id, coin_gain)
                xp_gain = int(duration) * 2
                leveled_up, new_level = await db.add_xp(member.id, xp_gain)
                if leveled_up:
                    try:
                        await member.send(f"🎉 Поздравляю! Вы достигли **{new_level} уровня**!")
                    except:
                        pass
                    await RoleManager.check_and_give_roles(member)
                await RoleManager.check_and_give_roles(member)

                # Проверка достижений за голос
                stats = await db.get_user_stats(member.id)
                if stats['voice_minutes'] >= 600:  # 10 часов
                    await db.check_achievement(member.id, "voice_10h", member.guild)
                if stats['voice_minutes'] >= 6000:  # 100 часов
                    await db.check_achievement(member.id, "voice_100h", member.guild)

                config = await get_guild_config(member.guild.id)
                if config.get("voice_events", True):
                    await Logger.log_event(
                        guild=member.guild,
                        event_type="voice",
                        title="Выход из голосового канала",
                        description=f"Пользователь {member.mention} вышел из голосового канала",
                        color=0x3498db,
                        user=member,
                        channel=before.channel,
                        fields={
                            "Канал": before.channel.name,
                            "Время в канале": f"{int(duration)} минут",
                            "Монеты": f"+{coin_gain}" if coin_gain else "0"
                        }
                    )
            del voice_sessions[user_id]

    elif before.channel is not None and after.channel is not None and before.channel != after.channel:
        if user_id in voice_sessions:
            duration = (now - voice_sessions[user_id]).total_seconds() / 60
            if duration >= 1:
                await db.add_voice_time(member.id, int(duration))
                coin_gain = int(duration) // 5
                if coin_gain > 0:
                    await db.add_coins(member.id, coin_gain)
                xp_gain = int(duration) * 2
                leveled_up, new_level = await db.add_xp(member.id, xp_gain)
                if leveled_up:
                    try:
                        await member.send(f"🎉 Поздравляю! Вы достигли **{new_level} уровня**!")
                    except:
                        pass
                    await RoleManager.check_and_give_roles(member)
            voice_sessions[user_id] = now
            config = await get_guild_config(member.guild.id)
            if config.get("voice_events", True):
                await Logger.log_event(
                    guild=member.guild,
                    event_type="voice",
                    title="Переход между каналами",
                    description=f"Пользователь {member.mention} перешел в другой канал",
                    color=0x3498db,
                    user=member,
                    channel=after.channel,
                    fields={
                        "Из канала": before.channel.name,
                        "В канал": after.channel.name,
                        "Время в предыдущем": f"{int(duration)} минут"
                    }
                )

@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    await Logger.log_event(
        guild=channel.guild,
        event_type="channel",
        title="Создан канал",
        description=f"Создан новый канал: **{channel.name}**",
        color=0x2ecc71,
        channel=channel,
        fields={
            "Тип": str(channel.type).split('.')[-1].capitalize(),
            "Категория": channel.category.name if channel.category else "Нет",
            "ID": str(channel.id)
        }
    )

@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    await Logger.log_event(
        guild=channel.guild,
        event_type="channel",
        title="Удален канал",
        description=f"Удален канал: **{channel.name}**",
        color=0xe74c3c,
        fields={
            "Тип": str(channel.type).split('.')[-1].capitalize(),
            "Категория": channel.category.name if channel.category else "Нет",
            "ID": str(channel.id)
        }
    )

# ==================== КОМАНДЫ DISCORD ====================

# ---- СТАТИСТИКА, ТОП, УРОВЕНЬ, ГРАФИК ----
@bot.command(name="статистика")
async def stats(ctx, member: discord.Member = None):
    if not member:
        member = ctx.author
    data = await db.get_user_stats(member.id)
    level_info = await db.get_level_info(member.id)
    balance = await db.get_balance(member.id)

    embed = discord.Embed(
        title=f"📊 Статистика {member.display_name}",
        color=discord.Color.blue(),
        timestamp=get_moscow_time()
    )
    embed.add_field(
        name="🎤 Голосовая активность",
        value=f"**{data['voice_hours']}ч {data['voice_remaining_minutes']}м**\nВсего: {data['voice_minutes']} минут",
        inline=True
    )
    embed.add_field(
        name="💬 Сообщений",
        value=f"**{data['messages']}**",
        inline=True
    )
    embed.add_field(
        name="📈 Уровень",
        value=f"**{level_info['level']}** (✨ {level_info['xp']} XP)",
        inline=True
    )
    embed.add_field(
        name="💰 Монеты",
        value=f"**{balance}** 🪙",
        inline=True
    )

    current_role = DEFAULT_ROLE_NAME
    for threshold in sorted(LEVEL_ROLES.keys(), reverse=True):
        if level_info['level'] >= threshold:
            current_role = LEVEL_ROLES[threshold]
            break
    embed.add_field(name="👑 Текущая роль", value=f"**{current_role}**", inline=False)

    embed.add_field(
        name=f"🎯 До уровня {level_info['level'] + 1}",
        value=f"Осталось: **{level_info['remaining']} XP**\nПрогресс: `{level_info['progress']*100:.1f}%`",
        inline=False
    )

    embed.set_thumbnail(url=member.display_avatar.url)
    embed.set_footer(text=f"ID: {member.id} • Время МСК")
    await ctx.send(embed=embed)

@bot.command(name="топ")
async def top(ctx):
    voice_top, messages_top = await db.get_top_users(10)
    total_users = await db.get_total_users()

    embed = discord.Embed(
        title="🏆 Топ активности",
        color=discord.Color.gold(),
        timestamp=get_moscow_time()
    )

    voice_text = ""
    for i, (uid, minutes) in enumerate(voice_top[:5], 1):
        user = ctx.guild.get_member(uid)
        name = user.display_name if user else f"ID: {uid}"
        voice_text += f"{i}. **{name}** — {minutes // 60}ч {minutes % 60}м\n"
    embed.add_field(name="🎤 Голос (Топ 5)", value=voice_text or "Нет данных", inline=False)

    msg_text = ""
    for i, (uid, count) in enumerate(messages_top[:5], 1):
        user = ctx.guild.get_member(uid)
        name = user.display_name if user else f"ID: {uid}"
        msg_text += f"{i}. **{name}** — {count} сообщ.\n"
    embed.add_field(name="💬 Сообщения (Топ 5)", value=msg_text or "Нет данных", inline=False)

    embed.set_footer(text=f"Всего в базе: {total_users} пользователей • Время МСК")
    await ctx.send(embed=embed)

@bot.command(name="уровень", aliases=["level", "lvl"])
async def level(ctx, member: discord.Member = None):
    if member is None:
        member = ctx.author
    info = await db.get_level_info(member.id)

    embed = discord.Embed(
        title=f"📈 Уровень {member.display_name}",
        color=discord.Color.green(),
        timestamp=get_moscow_time()
    )
    embed.add_field(name="🎖️ Уровень", value=f"**{info['level']}**", inline=True)
    embed.add_field(name="✨ Опыт", value=f"{info['xp']} / {info['next_xp']}", inline=True)

    bar_length = 15
    filled = int(bar_length * info['progress'])
    bar = '█' * filled + '░' * (bar_length - filled)
    embed.add_field(name="Прогресс", value=f"{bar} `{info['progress']*100:.1f}%`", inline=False)

    embed.set_thumbnail(url=member.display_avatar.url)
    embed.set_footer(text=f"До следующего уровня: {info['remaining']} XP • Время МСК")
    await ctx.send(embed=embed)

@bot.command(name="график", aliases=["graph", "activity"])
async def activity_graph(ctx, member: discord.Member = None):
    if member is None:
        member = ctx.author

    async with ctx.typing():
        history = await db.get_user_history(member.id, ctx.guild.id, 30)
        if not history:
            await ctx.send(f"❌ У {member.mention} недостаточно данных для построения графика.")
            return

        history.reverse()
        dates = [row['date'].strftime('%d.%m') for row in history]
        voice_data = [row['voice_minutes'] / 60 for row in history]
        msg_data = [row['messages'] for row in history]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        fig.suptitle(f'Активность {member.display_name} (последние 30 дней)', fontsize=16)

        bars1 = ax1.bar(dates, voice_data, color='#3498db', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax1.set_ylabel('Часы в голосе', fontsize=12)
        ax1.set_title('🎤 Голосовая активность', fontsize=14, pad=10)
        ax1.grid(axis='y', alpha=0.3)
        for bar, value in zip(bars1, voice_data):
            if value > 0:
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                        f'{value:.1f}', ha='center', va='bottom', fontsize=8)

        bars2 = ax2.bar(dates, msg_data, color='#2ecc71', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_ylabel('Сообщения', fontsize=12)
        ax2.set_xlabel('Дата', fontsize=12)
        ax2.set_title('💬 Сообщения', fontsize=14, pad=10)
        ax2.grid(axis='y', alpha=0.3)
        for bar, value in zip(bars2, msg_data):
            if value > 0:
                ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                        f'{value}', ha='center', va='bottom', fontsize=8)

        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
        buf.seek(0)
        plt.close()

        file = discord.File(buf, filename='activity.png')
        embed = discord.Embed(
            title=f"📈 График активности {member.display_name}",
            color=discord.Color.blue(),
            timestamp=get_moscow_time()
        )
        embed.set_image(url="attachment://activity.png")
        embed.set_footer(text=f"Запросил: {ctx.author.display_name} • Время МСК")

    await ctx.send(embed=embed, file=file)

# ---- НОВОЕ: ЭКОНОМИКА И МАГАЗИН РОЛЕЙ ----
@bot.command(name="баланс", aliases=["money", "coins"])
async def balance(ctx, member: discord.Member = None):
    """Показывает баланс монет пользователя"""
    if member is None:
        member = ctx.author
    balance = await db.get_balance(member.id)
    embed = discord.Embed(
        title=f"💰 Баланс {member.display_name}",
        description=f"**{balance}** 🪙",
        color=discord.Color.gold(),
        timestamp=get_moscow_time()
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.set_footer(text=f"ID: {member.id} • Время МСК")
    await ctx.send(embed=embed)

@bot.command(name="топ_монет", aliases=["topcoins", "topmoney"])
async def top_coins(ctx):
    """Топ пользователей по монетам"""
    top = await db.get_eco_top(10)
    embed = discord.Embed(
        title="💰 Топ по монетам",
        color=discord.Color.gold(),
        timestamp=get_moscow_time()
    )
    text = ""
    for i, (uid, balance) in enumerate(top[:5], 1):
        user = ctx.guild.get_member(uid)
        name = user.display_name if user else f"ID: {uid}"
        text += f"{i}. **{name}** — {balance} 🪙\n"
    embed.add_field(name="Топ 5 богачей", value=text or "Нет данных", inline=False)
    embed.set_footer(text="Монеты зарабатываются за активность")
    await ctx.send(embed=embed)

@bot.command(name="магазин", aliases=["shop"])
@commands.has_permissions(administrator=True)
async def shop(ctx):
    """Показать список ролей в магазине"""
    roles = await db.get_shop_roles(ctx.guild.id)
    if not roles:
        await ctx.send("🛒 Магазин пуст. Администратор может добавить роли через `!добавить_роль`.")
        return

    embed = discord.Embed(
        title="🛒 Магазин ролей",
        description="Купите роль с помощью `!купить <название_роли>`",
        color=discord.Color.blue(),
        timestamp=get_moscow_time()
    )
    for item in roles:
        role = ctx.guild.get_role(item['role_id'])
        if role:
            embed.add_field(
                name=f"{role.name}",
                value=f"**Цена:** {item['price']} 🪙\n{item['description']}\nID: `{item['role_id']}`",
                inline=False
            )
    embed.set_footer(text="Роль выдаётся навсегда")
    await ctx.send(embed=embed)

@bot.command(name="добавить_роль")
@commands.has_permissions(administrator=True)
async def add_shop_role(ctx, role: discord.Role, price: int, *, description: str = None):
    """Добавить роль в магазин (только админ)"""
    if not await RoleManager.check_hierarchy(ctx.guild, role):
        await ctx.send("❌ Я не могу выдавать эту роль (она выше моей).")
        return

    await db.add_shop_role(ctx.guild.id, role.id, price, description)
    await ctx.send(f"✅ Роль **{role.name}** добавлена в магазин за {price} 🪙")

@bot.command(name="удалить_роль")
@commands.has_permissions(administrator=True)
async def remove_shop_role(ctx, role: discord.Role):
    """Удалить роль из магазина (только админ)"""
    await db.remove_shop_role(role.id)
    await ctx.send(f"✅ Роль **{role.name}** удалена из магазина")

@bot.command(name="купить")
async def buy_role(ctx, *, role_name: str):
    """Купить роль из магазина"""
    role = discord.utils.get(ctx.guild.roles, name=role_name)
    if not role:
        await ctx.send("❌ Роль не найдена.")
        return

    # Проверяем, есть ли в магазине
    shop_roles = await db.get_shop_roles(ctx.guild.id)
    shop_item = None
    for item in shop_roles:
        if item['role_id'] == role.id:
            shop_item = item
            break

    if not shop_item:
        await ctx.send("❌ Эта роль не продаётся в магазине.")
        return

    # Проверяем баланс
    balance = await db.get_balance(ctx.author.id)
    if balance < shop_item['price']:
        await ctx.send(f"❌ Недостаточно монет! Нужно {shop_item['price']} 🪙, у вас {balance} 🪙.")
        return

    # Проверяем, не куплена ли уже
    if await db.has_role_purchased(ctx.guild.id, ctx.author.id, role.id):
        await ctx.send("❌ Вы уже купили эту роль.")
        return

    # Снимаем монеты
    await db.remove_coins(ctx.author.id, shop_item['price'])
    await db.purchase_role(ctx.guild.id, ctx.author.id, role.id)

    # Выдаём роль
    try:
        await ctx.author.add_roles(role, reason="Покупка в магазине")
        await ctx.send(f"✅ Поздравляем! Вы купили роль **{role.name}** за {shop_item['price']} 🪙!")

        # Достижение за первую покупку
        await db.check_achievement(ctx.author.id, "first_purchase", ctx.guild)

        await Logger.log_event(
            guild=ctx.guild,
            event_type="economy",
            title="Покупка в магазине",
            description=f"{ctx.author.mention} купил роль **{role.name}**",
            color=0xf1c40f,
            user=ctx.author,
            fields={"Цена": f"{shop_item['price']} 🪙"}
        )
    except discord.Forbidden:
        await ctx.send("❌ Не удалось выдать роль (недостаточно прав).")
        await db.add_coins(ctx.author.id, shop_item['price'])  # Возвращаем монеты

# ---- НОВОЕ: ДОСТИЖЕНИЯ ----
@bot.command(name="достижения", aliases=["achievements", "ach"])
async def achievements(ctx, member: discord.Member = None):
    """Показать полученные достижения пользователя"""
    if member is None:
        member = ctx.author

    user_achs = await db.get_user_achievements(member.id)
    all_achs = await db.get_all_achievements()

    embed = discord.Embed(
        title=f"🏆 Достижения {member.display_name}",
        color=discord.Color.gold(),
        timestamp=get_moscow_time()
    )
    embed.set_thumbnail(url=member.display_avatar.url)

    if user_achs:
        text = ""
        for ach in user_achs[:10]:
            text += f"{ach['icon']} **{ach['description']}** — {ach['earned_at'].strftime('%d.%m.%Y')}\n"
        embed.add_field(name="Полученные", value=text, inline=False)
    else:
        embed.add_field(name="Полученные", value="Пока нет достижений", inline=False)

    # Статистика
    unlocked = len(user_achs)
    total = len(all_achs)
    embed.set_footer(text=f"Прогресс: {unlocked}/{total} • Время МСК")

    await ctx.send(embed=embed)

@bot.command(name="все_достижения", aliases=["allach"])
async def all_achievements(ctx):
    """Показать список всех достижений"""
    all_achs = await db.get_all_achievements()
    embed = discord.Embed(
        title="🏆 Все достижения",
        description="Получайте награды за активность!",
        color=discord.Color.gold(),
        timestamp=get_moscow_time()
    )
    text = ""
    for ach in all_achs:
        text += f"{ach['icon']} **{ach['description']}**\n"
        if ach['xp_reward'] > 0 or ach['coin_reward'] != 0:
            text += f"└ Награда: {ach['xp_reward']} XP, {ach['coin_reward']} 🪙\n"
    embed.add_field(name="Список", value=text, inline=False)
    embed.set_footer(text=f"Всего: {len(all_achs)} • Время МСК")
    await ctx.send(embed=embed)

# ---- НОВОЕ: СТАТИСТИКА СЕРВЕРА ----
@bot.command(name="сервер_статистика", aliases=["serverstats", "ss"])
@commands.has_permissions(administrator=True)
async def server_stats(ctx, period: str = "week"):
    """Показать статистику сервера за период (day/week/month/all)"""
    days_map = {
        "day": 1,
        "week": 7,
        "month": 30,
        "all": 3650
    }
    days = days_map.get(period, 7)

    stats = await db.get_server_stats(ctx.guild.id, days)
    if not stats:
        await ctx.send("❌ Недостаточно данных для статистики.")
        return

    total_messages = sum(s['total_messages'] for s in stats)
    total_voice = sum(s['total_voice_minutes'] for s in stats)
    avg_active = sum(s['active_users'] for s in stats) // len(stats)
    total_new = sum(s['new_members'] for s in stats)

    embed = discord.Embed(
        title=f"📊 Статистика сервера {ctx.guild.name}",
        description=f"За последние {days} дней" if period != "all" else "За всё время",
        color=discord.Color.blue(),
        timestamp=get_moscow_time()
    )
    embed.add_field(name="💬 Сообщений", value=f"{total_messages}", inline=True)
    embed.add_field(name="🎤 Часов в голосе", value=f"{total_voice // 60}", inline=True)
    embed.add_field(name="👥 Активных (в среднем)", value=f"{avg_active}", inline=True)
    embed.add_field(name="👋 Новых участников", value=f"{total_new}", inline=True)

    if ctx.guild.icon:
        embed.set_thumbnail(url=ctx.guild.icon.url)

    embed.set_footer(text=f"ID: {ctx.guild.id} • Время МСК")
    await ctx.send(embed=embed)

# ---- СУЩЕСТВУЮЩИЕ КОМАНДЫ ЛОГИРОВАНИЯ И Т.Д. ----
@bot.command(name="логи")
@commands.has_permissions(administrator=True)
async def logs(ctx, target_channel: discord.TextChannel = None):
    if target_channel:
        await set_log_channel(ctx.guild.id, target_channel.id)
        embed = discord.Embed(
            title="✅ Лог-канал установлен",
            description=f"Лог-канал: {target_channel.mention}",
            color=discord.Color.green(),
            timestamp=get_moscow_time()
        )
        await ctx.send(embed=embed)
        await Logger.log_event(
            guild=ctx.guild,
            event_type="server",
            title="Установлен лог-канал",
            description=f"Администратор {ctx.author.mention} установил лог-канал",
            color=0x2ecc71,
            user=ctx.author,
            channel=target_channel
        )
    else:
        config = await get_guild_config(ctx.guild.id)
        log_channel_id = config.get('log_channel')
        embed = discord.Embed(
            title="📝 Управление логированием",
            color=discord.Color.purple(),
            timestamp=get_moscow_time()
        )
        if log_channel_id:
            ch = ctx.guild.get_channel(log_channel_id)
            if ch:
                embed.add_field(name="✅ Лог-канал", value=f"{ch.mention}", inline=False)
            else:
                embed.add_field(name="⚠️ Лог-канал не найден", value=f"ID: {log_channel_id}", inline=False)
        else:
            embed.add_field(name="❌ Лог-канал не установлен", value="Используйте `!логи #канал`", inline=False)

        config_text = ""
        for key, value in config.items():
            if key not in ['guild_id', 'log_channel', 'backup_channel'] and not key.startswith('telegram'):
                emoji = '✅' if value else '❌'
                config_text += f"• **{key.replace('_', ' ').title()}:** {emoji}\n"
        embed.add_field(name="⚙️ Конфигурация", value=config_text, inline=False)
        embed.set_footer(text="Используйте !настройки_логов для детальной настройки")
        await ctx.send(embed=embed)

@bot.command(name="тест_лога", aliases=["тест-лога"])
@commands.has_permissions(administrator=True)
async def test_log(ctx):
    config = await get_guild_config(ctx.guild.id)
    if not config.get('log_channel'):
        await ctx.send("❌ Лог-канал не установлен! Используйте `!логи #канал`")
        return
    await Logger.log_event(
        guild=ctx.guild,
        event_type="server",
        title="Тестовое лог-сообщение",
        description="Проверка работы системы логирования",
        color=0xf1c40f,
        user=ctx.author,
        fields={
            "Статус": "✅ Система работает",
            "Время": format_moscow_time()
        }
    )
    await ctx.send("✅ Тестовое сообщение отправлено!")

@bot.command(name="настройки_логов")
@commands.has_permissions(administrator=True)
async def log_settings(ctx, event_type: str = None, status: str = None):
    config = await get_guild_config(ctx.guild.id)
    if not event_type:
        embed = discord.Embed(
            title="⚙️ Настройки логирования",
            color=discord.Color.blue(),
            timestamp=get_moscow_time()
        )
        config_text = ""
        for key, value in config.items():
            if key in ['voice_events', 'role_events', 'member_events', 
                      'channel_events', 'server_events', 'message_events', 
                      'command_events', 'telegram_notify_role', 'telegram_daily_report',
                      'economy_enabled', 'achievements_enabled']:
                emoji = '✅ Вкл' if value else '❌ Выкл'
                config_text += f"• **{key}:** {emoji}\n"
        embed.add_field(name="Текущие настройки", value=config_text, inline=False)
        embed.add_field(
            name="📝 Доступные типы",
            value="`voice_events`, `role_events`, `member_events`, `channel_events`, `server_events`, `message_events`, `command_events`, `telegram_notify_role`, `telegram_daily_report`, `economy_enabled`, `achievements_enabled`",
            inline=False
        )
        embed.set_footer(text="Используйте: !настройки_логов [тип] [on/off]")
        await ctx.send(embed=embed)
        return

    if event_type not in config:
        await ctx.send(f"❌ Неизвестный тип события: {event_type}")
        return
    if not status or status.lower() not in ['on', 'off']:
        await ctx.send(f"❌ Укажите on или off")
        return
    new_value = (status.lower() == 'on')
    await update_guild_config(ctx.guild.id, event_type, new_value)
    await ctx.send(f"✅ {event_type} теперь {'включен' if new_value else 'выключен'}")

@bot.command(name="бэкап_канал")
@commands.has_permissions(administrator=True)
async def backup_channel(ctx, channel: discord.TextChannel = None):
    """Установить канал для автоматических бэкапов БД"""
    if channel:
        await set_backup_channel(ctx.guild.id, channel.id)
        await ctx.send(f"✅ Канал для бэкапов установлен: {channel.mention}")
    else:
        config = await get_guild_config(ctx.guild.id)
        ch_id = config.get('backup_channel')
        if ch_id:
            ch = ctx.guild.get_channel(ch_id)
            await ctx.send(f"📦 Канал для бэкапов: {ch.mention if ch else 'не найден'}")
        else:
            await ctx.send("❌ Канал для бэкапов не установлен.")

@bot.command(name="ручной_бэкап")
@commands.has_permissions(administrator=True)
async def manual_backup(ctx):
    """Создать бэкап БД вручную"""
    if not telegram.enabled:
        await ctx.send("❌ Telegram не настроен, бэкап невозможен.")
        return
    await ctx.send("⏳ Создаю бэкап...")
    await backup_db()  # вызываем задачу
    await ctx.send("✅ Бэкап создан и отправлен в Telegram.")

# ---- ТАЙМ-АУТЫ И ОЧИСТКА ----
class TimeoutView(View):
    def __init__(self, member: discord.Member, moderator: discord.Member):
        super().__init__(timeout=60)
        self.member = member
        self.moderator = moderator

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user == self.moderator

    @discord.ui.button(label="10 минут", style=discord.ButtonStyle.danger)
    async def timeout_10m(self, button: Button, interaction: discord.Interaction):
        await self.apply_timeout(datetime.timedelta(minutes=10), interaction)

    @discord.ui.button(label="1 час", style=discord.ButtonStyle.danger)
    async def timeout_1h(self, button: Button, interaction: discord.Interaction):
        await self.apply_timeout(datetime.timedelta(hours=1), interaction)

    @discord.ui.button(label="6 часов", style=discord.ButtonStyle.danger)
    async def timeout_6h(self, button: Button, interaction: discord.Interaction):
        await self.apply_timeout(datetime.timedelta(hours=6), interaction)

    @discord.ui.button(label="1 день", style=discord.ButtonStyle.danger)
    async def timeout_1d(self, button: Button, interaction: discord.Interaction):
        await self.apply_timeout(datetime.timedelta(days=1), interaction)

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.secondary)
    async def cancel(self, button: Button, interaction: discord.Interaction):
        await interaction.response.edit_message(content="❌ Операция отменена.", view=None)
        self.stop()

    async def apply_timeout(self, delta: datetime.timedelta, interaction: discord.Interaction):
        try:
            await self.member.timeout(delta, reason=f"Тайм-аут от {self.moderator}")
            await interaction.response.edit_message(
                content=f"✅ {self.member.mention} получил тайм-аут на {delta}.",
                view=None
            )
            await Logger.log_event(
                guild=interaction.guild,
                event_type="command",
                title="Тайм-аут",
                description=f"{self.moderator.mention} выдал тайм-аут {self.member.mention} на {delta}",
                color=0xe74c3c,
                user=self.moderator,
                target=self.member
            )
        except discord.Forbidden:
            await interaction.response.edit_message(
                content="❌ У меня недостаточно прав для тайм-аута.",
                view=None
            )
        self.stop()

@bot.command(name="timeout", aliases=["таймаут"])
@commands.has_permissions(moderate_members=True)
async def timeout(ctx, member: discord.Member):
    if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
        await ctx.send("❌ Вы не можете затаймаутить этого пользователя.")
        return
    if not ctx.guild.me.guild_permissions.moderate_members:
        await ctx.send("❌ У меня нет прав на выдачу тайм-аута.")
        return

    view = TimeoutView(member, ctx.author)
    await ctx.send(
        f"🕒 Выберите длительность тайм-аута для {member.mention}:",
        view=view
    )

@bot.command(name="clear", aliases=["очистить"])
@commands.has_permissions(manage_messages=True)
async def clear(ctx, amount: int):
    if amount <= 0:
        await ctx.send("❌ Укажите положительное число.")
        return
    amount = min(amount, 100)
    deleted = await ctx.channel.purge(limit=amount + 1)
    count = len(deleted) - 1
    await ctx.send(f"✅ Удалено {count} сообщений.", delete_after=5)
    await Logger.log_event(
        guild=ctx.guild,
        event_type="command",
        title="Очистка сообщений",
        description=f"{ctx.author.mention} удалил {count} сообщений в {ctx.channel.mention}",
        color=0x3498db,
        user=ctx.author,
        channel=ctx.channel
    )

@clear.error
async def clear_error(ctx, error):
    if isinstance(error, commands.BadArgument):
        await ctx.send("❌ Укажите число сообщений для удаления (например: `!clear 10`).")

# ---- ПРЕДУПРЕЖДЕНИЯ ----
@bot.command(name="warn", aliases=["пред"])
@commands.has_permissions(kick_members=True)
async def warn(ctx, member: discord.Member, *, reason="Не указана"):
    if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
        await ctx.send("❌ Вы не можете предупредить этого пользователя.")
        return
    await db.add_warn(ctx.guild.id, member.id, ctx.author.id, reason)
    warns = await db.get_warns(ctx.guild.id, member.id)
    warn_count = len(warns)

    embed = discord.Embed(
        title="⚠️ Предупреждение",
        color=discord.Color.orange(),
        timestamp=get_moscow_time()
    )
    embed.add_field(name="Пользователь", value=member.mention, inline=True)
    embed.add_field(name="Модератор", value=ctx.author.mention, inline=True)
    embed.add_field(name="Причина", value=reason, inline=False)
    embed.add_field(name="Всего предупреждений", value=warn_count, inline=True)
    embed.set_footer(text=f"ID: {member.id}")
    await ctx.send(embed=embed)

    if warn_count >= 3:
        muted_role = discord.utils.get(ctx.guild.roles, name="Muted")
        if not muted_role:
            muted_role = await ctx.guild.create_role(name="Muted")
            for channel in ctx.guild.channels:
                await channel.set_permissions(muted_role, speak=False, send_messages=False)
        try:
            await member.add_roles(muted_role, reason="3+ предупреждения")
            await ctx.send(f"🔇 Пользователь {member.mention} получил мут (3+ предупреждения).")
        except:
            pass

    # Достижение за первое предупреждение
    if warn_count == 1:
        await db.check_achievement(member.id, "first_warning", ctx.guild)

@bot.command(name="warns", aliases=["преды"])
@commands.has_permissions(kick_members=True)
async def warns(ctx, member: discord.Member):
    warns = await db.get_warns(ctx.guild.id, member.id)
    if not warns:
        await ctx.send(f"✅ У {member.mention} нет предупреждений.")
        return

    embed = discord.Embed(
        title=f"⚠️ Предупреждения {member.display_name}",
        color=discord.Color.orange(),
        timestamp=get_moscow_time()
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    for i, w in enumerate(warns[:10], 1):
        mod = ctx.guild.get_member(w['moderator_id'])
        mod_name = mod.display_name if mod else f"ID: {w['moderator_id']}"
        timestamp = w['timestamp'].strftime('%d.%m.%Y %H:%M')
        embed.add_field(
            name=f"#{i} (ID: {w['id']})",
            value=f"**Причина:** {w['reason']}\n**Модератор:** {mod_name}\n**Дата:** {timestamp}",
            inline=False
        )
    embed.set_footer(text=f"Всего: {len(warns)}")
    await ctx.send(embed=embed)

@bot.command(name="clearwarns", aliases=["снятьпреды"])
@commands.has_permissions(kick_members=True)
async def clear_warns(ctx, member: discord.Member):
    await db.clear_warns(ctx.guild.id, member.id)
    await ctx.send(f"✅ Все предупреждения сняты с {member.mention}")

@bot.command(name="delwarn", aliases=["удалитьпред"])
@commands.has_permissions(kick_members=True)
async def del_warn(ctx, warn_id: int):
    await db.remove_warn(warn_id)
    await ctx.send(f"✅ Предупреждение #{warn_id} удалено.")

# ---- TELEGRAM ----
@bot.command(name="telegram")
@commands.has_permissions(administrator=True)
async def telegram_cmd(ctx, action: str = None):
    if not telegram.enabled:
        embed = discord.Embed(
            title="❌ Telegram не настроен",
            description="Добавьте переменные окружения:\n`TELEGRAM_BOT_TOKEN`\n`TELEGRAM_CHAT_ID`",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    config = await get_guild_config(ctx.guild.id)
    if not action:
        embed = discord.Embed(
            title="📱 Telegram уведомления",
            color=discord.Color.blue(),
            timestamp=get_moscow_time()
        )
        embed.add_field(
            name="Статус",
            value=f"✅ Подключен к чату ID: `{TELEGRAM_CHAT_ID}`",
            inline=False
        )
        embed.add_field(
            name="Настройки",
            value=f"• Уведомления о ролях: {'✅' if config.get('telegram_notify_role', False) else '❌'}\n"
                  f"• Ежедневный отчет: {'✅' if config.get('telegram_daily_report', True) else '❌'}",
            inline=False
        )
        embed.add_field(
            name="Команды",
            value="`!telegram on` - включить уведомления о ролях\n"
                  "`!telegram off` - выключить уведомления о ролях\n"
                  "`!telegram daily` - переключить ежедневный отчет\n"
                  "`!telegram test` - отправить тестовое сообщение",
            inline=False
        )
        await ctx.send(embed=embed)
    elif action == "on":
        await update_guild_config(ctx.guild.id, "telegram_notify_role", True)
        await ctx.send("✅ Уведомления о новых ролях **включены**")
    elif action == "off":
        await update_guild_config(ctx.guild.id, "telegram_notify_role", False)
        await ctx.send("❌ Уведомления о новых ролях **выключены**")
    elif action == "daily":
        current = config.get("telegram_daily_report", True)
        await update_guild_config(ctx.guild.id, "telegram_daily_report", not current)
        await ctx.send(f"✅ Ежедневный отчет {'включен' if not current else 'выключен'}")
    elif action == "test":
        success = await telegram.send_alert(
            "🧪 Тестовое уведомление",
            f"Отправлено пользователем {ctx.author.display_name}\nСервер: {ctx.guild.name}",
            "info"
        )
        if success:
            await ctx.send("✅ Тестовое уведомление отправлено в Telegram!")
        else:
            await ctx.send("❌ Не удалось отправить уведомление")

@bot.command(name="очистить_команды")
@commands.has_permissions(administrator=True)
async def clear_commands(ctx):
    try:
        bot.tree.clear_commands(guild=None)
        await bot.tree.sync()
        bot.tree.clear_commands(guild=ctx.guild)
        await bot.tree.sync(guild=ctx.guild)
        await ctx.send("✅ Старые слэш-команды удалены! Используйте команды с префиксом `!`")
    except Exception as e:
        await ctx.send(f"❌ Ошибка: {e}")

@bot.command(name="помощь")
async def help_command(ctx):
    embed = discord.Embed(
        title="📚 Команды бота",
        description=f"Префикс: `{bot.command_prefix}`",
        color=discord.Color.green(),
        timestamp=get_moscow_time()
    )
    embed.add_field(
        name="👤 **Для всех**",
        value="`!статистика` - ваша статистика\n`!статистика @пользователь` - статистика пользователя\n"
              "`!топ` - топ пользователей\n`!уровень` - ваш уровень и опыт\n"
              "`!график` - график активности за 30 дней\n"
              "`!баланс` - ваши монеты\n`!топ_монет` - топ богачей\n"
              "`!магазин` - магазин ролей\n`!купить <роль>` - купить роль\n"
              "`!достижения` - ваши достижения\n`!все_достижения` - список всех достижений\n"
              "`!помощь` - это сообщение",
        inline=False
    )
    embed.add_field(
        name="👑 **Для администраторов**",
        value="`!логи` - статус лог-канала\n`!логи #канал` - установить канал для логов\n"
              "`!тест_лога` - тест системы логирования\n"
              "`!настройки_логов` - показать/изменить настройки\n"
              "`!бэкап_канал` - канал для бэкапов БД\n`!ручной_бэкап` - создать бэкап вручную\n"
              "`!telegram` - управление Telegram уведомлениями\n"
              "`!warn` - выдать предупреждение\n`!warns` - список предупреждений\n"
              "`!clearwarns` - снять все предупреждения\n`!delwarn` - удалить предупреждение\n"
              "`!timeout` - тайм-аут через кнопки\n`!clear` - очистка сообщений\n"
              "`!добавить_роль` - добавить роль в магазин\n`!удалить_роль` - удалить роль из магазина\n"
              "`!сервер_статистика` - статистика сервера\n"
              "`!очистить_команды` - удалить старые слэш-команды",
        inline=False
    )
    embed.add_field(
        name="⚙️ **Типы событий**",
        value="`voice_events` - голосовая активность\n`role_events` - события ролей\n"
              "`member_events` - вход/выход участников\n`channel_events` - создание/удаление каналов\n"
              "`server_events` - изменения сервера\n`message_events` - удаление/редактирование сообщений\n"
              "`command_events` - использование команд",
        inline=False
    )
    embed.set_footer(text=f"Бот: {bot.user.name} • Время МСК")
    await ctx.send(embed=embed)

# ==================== FLASK ДЛЯ UPTIMEROBOT ====================
app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "bot": str(bot.user) if bot.user else "starting",
        "servers": len(bot.guilds) if bot.guilds else 0,
        "users": 0,
        "time": format_moscow_time()
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy"})

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    print("=" * 60)
    print("🤖 Discord Voice Activity Bot")
    print("📱 Версия: 12.0 (Ultimate Edition + Economy + Achievements + Server Stats + Auto Backup)")
    print("⏰ Часовой пояс: Московское время (GMT+3)")
    print("📈 Система уровней и ролей за уровень")
    print("📊 Графики активности")
    print("💰 Экономика и магазин ролей")
    print("🏆 Достижения и ачивки")
    print("📦 Автобэкап БД в Telegram")
    print("📊 Статистика сервера")
    print("🕒 Тайм-ауты через кнопки")
    print("🧹 Очистка сообщений")
    print("📝 Логирование: все события (сохраняется в БД)")
    print(f"📱 Telegram: {'✅ ПОДКЛЮЧЕН (команды: /stats, /top, /roles, /eco_top, /help)' if telegram.enabled else '❌ НЕ НАСТРОЕН'}")
    print("=" * 60)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("🌐 Веб-сервер запущен")

    try:
        bot.run(TOKEN)
    except KeyboardInterrupt:
        print("🛑 Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")
    finally:
        asyncio.run(telegram.close())
