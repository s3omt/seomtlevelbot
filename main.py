import discord
from discord.ext import commands, tasks
from discord.ui import Button, View
import asyncio
import datetime
from datetime import time as datetime_time
import sys
import aiohttp
import pytz
import math
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from typing import Dict, List, Optional
import os
import subprocess
from PIL import Image, ImageDraw, ImageFont
import asyncpg
from bs4 import BeautifulSoup
from google import genai

# ==================== НАСТРОЙКА ИИ ДЛЯ ПЕРЕВОДОВ ====================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_API_KEY:
    ai_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    ai_client = None
    print("⚠️ GEMINI_API_KEY не найден. Перевод гайдов работать не будет.")

# ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================
class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Создаёт пул соединений с PostgreSQL с повторными попытками"""
        if self.pool is None:
            db_url = os.environ.get("DATABASE_URL")
            if not db_url:
                print("❌ ОШИБКА: DATABASE_URL не задан в переменных окружения!")
                return None

            for attempt in range(5):
                try:
                    self.pool = await asyncpg.create_pool(
                        db_url, min_size=1, max_size=10, command_timeout=60, ssl='require'
                    )
                    print(f"✅ Подключение к БД установлено (попытка {attempt+1})")
                    break
                except Exception as e:
                    print(f"⚠️ Попытка {attempt+1}/5 подключения к БД не удалась: {e}")
                    if attempt == 4:
                        print("❌ Не удалось подключиться к БД после 5 попыток")
                        return None
                    await asyncio.sleep(2 ** attempt)
        return self.pool

    async def init_db(self):
        pool = await self.connect()
        if pool is None: return

        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, messages INT DEFAULT 0, voice_minutes INT DEFAULT 0, reputation INT DEFAULT 0);
                CREATE TABLE IF NOT EXISTS rep_cooldowns (user_id BIGINT PRIMARY KEY, last_rep TIMESTAMP);
                CREATE TABLE IF NOT EXISTS guild_config (
                    guild_id BIGINT PRIMARY KEY, log_channel BIGINT, backup_channel BIGINT, guides_channel BIGINT,
                    voice_events BOOLEAN DEFAULT TRUE, role_events BOOLEAN DEFAULT TRUE,
                    member_events BOOLEAN DEFAULT TRUE, channel_events BOOLEAN DEFAULT TRUE,
                    server_events BOOLEAN DEFAULT TRUE, message_events BOOLEAN DEFAULT FALSE,
                    command_events BOOLEAN DEFAULT TRUE, telegram_notify_role BOOLEAN DEFAULT FALSE,
                    telegram_daily_report BOOLEAN DEFAULT TRUE, economy_enabled BOOLEAN DEFAULT TRUE,
                    achievements_enabled BOOLEAN DEFAULT TRUE
                );
                CREATE TABLE IF NOT EXISTS warns (id SERIAL PRIMARY KEY, guild_id BIGINT, user_id BIGINT, moderator_id BIGINT, reason TEXT, timestamp TIMESTAMP DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS levels (user_id BIGINT PRIMARY KEY, xp INT DEFAULT 0, level INT DEFAULT 0, last_xp_time TIMESTAMP DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS user_history (id SERIAL PRIMARY KEY, user_id BIGINT, guild_id BIGINT, date DATE DEFAULT CURRENT_DATE, voice_minutes INT DEFAULT 0, messages INT DEFAULT 0, UNIQUE(user_id, guild_id, date));
                CREATE TABLE IF NOT EXISTS economy (user_id BIGINT PRIMARY KEY, balance BIGINT DEFAULT 0, total_earned BIGINT DEFAULT 0, last_daily TIMESTAMP);
                CREATE TABLE IF NOT EXISTS shop_roles (id SERIAL PRIMARY KEY, guild_id BIGINT, role_id BIGINT, price BIGINT, description TEXT, created_at TIMESTAMP DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS purchased_roles (id SERIAL PRIMARY KEY, guild_id BIGINT, user_id BIGINT, role_id BIGINT, purchased_at TIMESTAMP DEFAULT NOW(), UNIQUE(guild_id, user_id, role_id));
                CREATE TABLE IF NOT EXISTS achievements (id SERIAL PRIMARY KEY, name TEXT UNIQUE, description TEXT, xp_reward INT DEFAULT 0, coin_reward BIGINT DEFAULT 0, icon TEXT DEFAULT '🏆', hidden BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS user_achievements (id SERIAL PRIMARY KEY, user_id BIGINT, achievement_id INT, earned_at TIMESTAMP DEFAULT NOW(), UNIQUE(user_id, achievement_id));
                CREATE TABLE IF NOT EXISTS server_history (id SERIAL PRIMARY KEY, guild_id BIGINT, date DATE DEFAULT CURRENT_DATE, total_messages INT DEFAULT 0, total_voice_minutes INT DEFAULT 0, active_users INT DEFAULT 0, new_members INT DEFAULT 0, UNIQUE(guild_id, date));
                CREATE TABLE IF NOT EXISTS profile_themes (id SERIAL PRIMARY KEY, name TEXT UNIQUE, accent_color INT, bg_color INT, card_color INT, overlay_url TEXT, style TEXT DEFAULT 'default', price BIGINT DEFAULT 0, preview_url TEXT, purchasable BOOLEAN DEFAULT TRUE);
                CREATE TABLE IF NOT EXISTS user_profile (user_id BIGINT PRIMARY KEY, theme_id INT DEFAULT 1, custom_accent_color INT, custom_bg_color INT, FOREIGN KEY (theme_id) REFERENCES profile_themes(id));
                CREATE TABLE IF NOT EXISTS posted_guides (url TEXT PRIMARY KEY, posted_at TIMESTAMP DEFAULT NOW());
            """)
            
            for col in ["backup_channel BIGINT", "guides_channel BIGINT", "economy_enabled BOOLEAN DEFAULT TRUE", "achievements_enabled BOOLEAN DEFAULT TRUE"]:
                try: await conn.execute(f"ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS {col}")
                except Exception: pass
            try: await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS reputation INT DEFAULT 0")
            except Exception: pass
            
            print("✅ База данных инициализирована")

    # --- МЕТОДЫ ДЛЯ ГАЙДОВ ---
    async def is_guide_posted(self, url: str):
        pool = await self.connect()
        if not pool: return True
        async with pool.acquire() as conn:
            return bool(await conn.fetchval("SELECT 1 FROM posted_guides WHERE url = $1", url))

    async def mark_guide_posted(self, url: str):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO posted_guides (url) VALUES ($1) ON CONFLICT DO NOTHING", url)

    async def get_all_guide_channels(self):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT guild_id, guides_channel FROM guild_config WHERE guides_channel IS NOT NULL")
            return [(r['guild_id'], r['guides_channel']) for r in rows]

    # --- МЕТОДЫ РЕПУТАЦИИ ---
    async def can_give_rep(self, user_id: int):
        pool = await self.connect()
        if not pool: return False, 0
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'UTC' - last_rep)) AS diff FROM rep_cooldowns WHERE user_id = $1", user_id)
            if not row: return True, 0
            diff = row['diff']
            if diff >= 86400: return True, 0
            else: return False, int(86400 - diff)

    async def add_reputation(self, sender_id: int, target_id: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO rep_cooldowns (user_id, last_rep) VALUES ($1, NOW() AT TIME ZONE 'UTC') ON CONFLICT (user_id) DO UPDATE SET last_rep = NOW() AT TIME ZONE 'UTC'", sender_id)
                await conn.execute("INSERT INTO users (user_id, reputation) VALUES ($1, 1) ON CONFLICT (user_id) DO UPDATE SET reputation = COALESCE(users.reputation, 0) + 1", target_id)
                return await conn.fetchval("SELECT reputation FROM users WHERE user_id = $1", target_id)
        return 0

    async def get_reputation(self, user_id: int):
        pool = await self.connect()
        if not pool: return 0
        async with pool.acquire() as conn: return await conn.fetchval("SELECT reputation FROM users WHERE user_id = $1", user_id) or 0

    # --- МЕТОДЫ ПОЛЬЗОВАТЕЛЕЙ И СТАТИСТИКИ ---
    async def add_message(self, user_id: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO users (user_id, messages) VALUES ($1, 1) ON CONFLICT (user_id) DO UPDATE SET messages = users.messages + 1", user_id)

    async def add_voice_time(self, user_id: int, minutes: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO users (user_id, voice_minutes) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET voice_minutes = users.voice_minutes + $2", user_id, minutes)

    async def get_user_stats(self, user_id: int):
        pool = await self.connect()
        if not pool: return {'messages': 0, 'voice_minutes': 0, 'voice_hours': 0, 'voice_remaining_minutes': 0}
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT messages, voice_minutes FROM users WHERE user_id = $1", user_id)
            if row: return {'messages': row['messages'], 'voice_minutes': row['voice_minutes'], 'voice_hours': row['voice_minutes'] // 60, 'voice_remaining_minutes': row['voice_minutes'] % 60}
            return {'messages': 0, 'voice_minutes': 0, 'voice_hours': 0, 'voice_remaining_minutes': 0}

    async def get_top_users(self, limit: int = 10):
        pool = await self.connect()
        if not pool: return [], []
        async with pool.acquire() as conn:
            voice = await conn.fetch("SELECT user_id, voice_minutes FROM users ORDER BY voice_minutes DESC LIMIT $1", limit)
            msg = await conn.fetch("SELECT user_id, messages FROM users ORDER BY messages DESC LIMIT $1", limit)
            return [(r['user_id'], r['voice_minutes']) for r in voice], [(r['user_id'], r['messages']) for r in msg]

    async def get_total_users(self):
        pool = await self.connect()
        if not pool: return 0
        async with pool.acquire() as conn: return await conn.fetchval("SELECT COUNT(*) FROM users") or 0

    async def get_total_stats(self):
        pool = await self.connect()
        if not pool: return {'total_messages': 0, 'total_voice': 0}
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COALESCE(SUM(messages), 0) as total_messages, COALESCE(SUM(voice_minutes), 0) as total_voice FROM users")
            return {'total_messages': row['total_messages'], 'total_voice': row['total_voice']}

    # --- МЕТОДЫ УРОВНЕЙ И ЭКОНОМИКИ ---
    async def add_xp(self, user_id: int, xp: int):
        pool = await self.connect()
        if not pool: return False, 0
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT xp, level FROM levels WHERE user_id = $1", user_id)
            new_xp, old_level = (row['xp'] + xp, row['level']) if row else (xp, 0)
            if not row: await conn.execute("INSERT INTO levels (user_id, xp, level) VALUES ($1, 0, 0)", user_id)
            new_level = int((math.sqrt(100 * (2 * new_xp + 25)) + 50) // 100)
            await conn.execute("UPDATE levels SET xp = $1, level = $2, last_xp_time = NOW() WHERE user_id = $3", new_xp, new_level, user_id)
            return new_level > old_level, new_level

    async def get_level_info(self, user_id: int):
        pool = await self.connect()
        if not pool: return {'xp': 0, 'level': 0, 'next_xp': 25, 'progress': 0, 'remaining': 25}
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT xp, level FROM levels WHERE user_id = $1", user_id)
            xp, level = (row['xp'], row['level']) if row else (0, 0)
            next_xp = int(((level + 1) * 100 - 50) ** 2 / 100)
            return {'xp': xp, 'level': level, 'next_xp': next_xp, 'progress': xp/next_xp if next_xp > 0 else 0, 'remaining': next_xp - xp}

    async def get_balance(self, user_id: int):
        pool = await self.connect()
        if not pool: return 0
        async with pool.acquire() as conn: return await conn.fetchval("SELECT balance FROM economy WHERE user_id = $1", user_id) or 0

    async def add_coins(self, user_id: int, amount: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO economy (user_id, balance, total_earned) VALUES ($1, $2, $2) ON CONFLICT (user_id) DO UPDATE SET balance = economy.balance + $2, total_earned = economy.total_earned + $2", user_id, amount)

    async def remove_coins(self, user_id: int, amount: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("UPDATE economy SET balance = balance - $1 WHERE user_id = $2 AND balance >= $1", amount, user_id)

    async def get_eco_top(self, limit: int = 10):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [(r['user_id'], r['balance']) for r in await conn.fetch("SELECT user_id, balance FROM economy ORDER BY balance DESC LIMIT $1", limit)]

    async def get_level_top(self, limit: int = 10):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [(r['user_id'], r['level'], r['xp']) for r in await conn.fetch("SELECT user_id, level, xp FROM levels ORDER BY level DESC, xp DESC LIMIT $1", limit)]

    # --- ИСТОРИЯ, НАСТРОЙКИ, МАГАЗИН И ПРЕДУПРЕЖДЕНИЯ ---
    async def save_daily_stats(self, user_id: int, guild_id: int, voice_minutes: int, messages: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("INSERT INTO user_history (user_id, guild_id, date, voice_minutes, messages) VALUES ($1, $2, CURRENT_DATE, $3, $4) ON CONFLICT (user_id, guild_id, date) DO UPDATE SET voice_minutes = EXCLUDED.voice_minutes, messages = EXCLUDED.messages", user_id, guild_id, voice_minutes, messages)

    async def get_user_history(self, user_id: int, guild_id: int, days: int = 30):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [dict(r) for r in await conn.fetch("SELECT date, voice_minutes, messages FROM user_history WHERE user_id = $1 AND guild_id = $2 ORDER BY date DESC LIMIT $3", user_id, guild_id, days)]

    async def save_server_stats(self, guild_id: int, date: datetime.date = None):
        date = date or datetime.date.today()
        pool = await self.connect()
        if not pool: return
        async with pool.acquire() as conn:
            guild = bot.get_guild(guild_id)
            if not guild: return
            tm, tv, au, nm = 0, 0, 0, sum(1 for m in guild.members if m.joined_at and m.joined_at.date() == date)
            for m in guild.members:
                if m.bot: continue
                s = await self.get_user_stats(m.id)
                tm += s['messages']; tv += s['voice_minutes']
                if s['messages'] > 0 or s['voice_minutes'] > 0: au += 1
            await conn.execute("INSERT INTO server_history (guild_id, date, total_messages, total_voice_minutes, active_users, new_members) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (guild_id, date) DO UPDATE SET total_messages = EXCLUDED.total_messages, total_voice_minutes = EXCLUDED.total_voice_minutes, active_users = EXCLUDED.active_users, new_members = EXCLUDED.new_members", guild_id, date, tm, tv, au, nm)

    async def get_server_stats(self, guild_id: int, days: int = 7):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [dict(r) for r in await conn.fetch("SELECT * FROM server_history WHERE guild_id = $1 ORDER BY date DESC LIMIT $2", guild_id, days)]

    async def get_guild_config(self, guild_id: int):
        pool = await self.connect()
        default = {'guild_id': guild_id, 'log_channel': None, 'backup_channel': None, 'guides_channel': None, 'voice_events': True, 'role_events': True, 'member_events': True, 'channel_events': True, 'server_events': True, 'message_events': False, 'command_events': True, 'telegram_notify_role': False, 'telegram_daily_report': True, 'economy_enabled': True, 'achievements_enabled': True}
        if not pool: return default
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM guild_config WHERE guild_id = $1", guild_id)
            return dict(row) if row else default

    async def update_guild_config(self, guild_id: int, key: str, value):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute(f"INSERT INTO guild_config (guild_id, {key}) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET {key} = $2", guild_id, value)
            
    async def set_log_channel(self, guild_id: int, channel_id: int):
        await self.update_guild_config(guild_id, 'log_channel', channel_id)

    async def set_backup_channel(self, guild_id: int, channel_id: int):
        await self.update_guild_config(guild_id, 'backup_channel', channel_id)

    async def get_shop_roles(self, guild_id: int):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [dict(r) for r in await conn.fetch("SELECT * FROM shop_roles WHERE guild_id = $1 ORDER BY price", guild_id)]
        
    async def add_shop_role(self, guild_id: int, role_id: int, price: int, description: str = None):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("INSERT INTO shop_roles (guild_id, role_id, price, description) VALUES ($1, $2, $3, $4)", guild_id, role_id, price, description or "Нет описания")

    async def remove_shop_role(self, role_id: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("DELETE FROM shop_roles WHERE role_id = $1", role_id)

    async def purchase_role(self, guild_id: int, user_id: int, role_id: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("INSERT INTO purchased_roles (guild_id, user_id, role_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING", guild_id, user_id, role_id)

    async def has_role_purchased(self, guild_id: int, user_id: int, role_id: int):
        pool = await self.connect()
        if not pool: return False
        async with pool.acquire() as conn: return bool(await conn.fetchval("SELECT 1 FROM purchased_roles WHERE guild_id = $1 AND user_id = $2 AND role_id = $3", guild_id, user_id, role_id))

    async def get_warns(self, guild_id: int, user_id: int):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [dict(r) for r in await conn.fetch("SELECT * FROM warns WHERE guild_id = $1 AND user_id = $2 ORDER BY timestamp DESC", guild_id, user_id)]

    async def add_warn(self, guild_id: int, user_id: int, mod_id: int, reason: str):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("INSERT INTO warns (guild_id, user_id, moderator_id, reason) VALUES ($1, $2, $3, $4)", guild_id, user_id, mod_id, reason)

    async def clear_warns(self, guild_id: int, user_id: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("DELETE FROM warns WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def remove_warn(self, warn_id: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("DELETE FROM warns WHERE id = $1", warn_id)

    # --- ДОСТИЖЕНИЯ И ТЕМЫ ---
    async def init_achievements(self):
        achievements = [
            ("chat_100", "Пиздaбoл", "Написать 100 сообщений", 50, 100, "💬"),
            ("chat_1000", "Графоман", "Написать 1000 сообщений", 200, 500, "📝"),
            ("voice_10h", "Микро...селебрити", "Провести 10 часов в голосе", 50, 100, "🎤"),
            ("voice_100h", "Диктор", "Провести 100 часов в голосе", 200, 500, "📻"),
            ("level_5", "Мдэ", "Достичь 5 уровня", 0, 0, "🌱"),
            ("level_10", "Нормис", "Достичь 10 уровня", 0, 0, "🌿"),
            ("level_20", "Бывалый", "Достичь 20 уровня", 0, 0, "⭐"),
            ("first_warning", "Доигрался", "Получить первое предупреждение", 0, -50, "⚠️"),
            ("first_purchase", "Шопоголик", "Купить первую роль", 20, 0, "🛒"),
        ]
        pool = await self.connect()
        if not pool: return
        async with pool.acquire() as conn:
            for name, title, desc, xp, coins, icon in achievements:
                await conn.execute("""
                    INSERT INTO achievements (name, description, xp_reward, coin_reward, icon)
                    VALUES ($1, $2, $3, $4, $5) ON CONFLICT (name) DO UPDATE
                    SET description = EXCLUDED.description, xp_reward = EXCLUDED.xp_reward, coin_reward = EXCLUDED.coin_reward, icon = EXCLUDED.icon
                """, name, f"{title}: {desc}", xp, coins, icon)

    async def check_achievement(self, user_id: int, achievement_name: str, guild: discord.Guild = None):
        pool = await self.connect()
        if not pool: return False
        async with pool.acquire() as conn:
            ach = await conn.fetchrow("SELECT id, xp_reward, coin_reward, icon, description FROM achievements WHERE name = $1", achievement_name)
            if not ach or await conn.fetchval("SELECT 1 FROM user_achievements WHERE user_id = $1 AND achievement_id = $2", user_id, ach['id']):
                return False
            await conn.execute("INSERT INTO user_achievements (user_id, achievement_id) VALUES ($1, $2)", user_id, ach['id'])
            if ach['xp_reward'] > 0: await self.add_xp(user_id, ach['xp_reward'])
            if ach['coin_reward'] > 0: await self.add_coins(user_id, ach['coin_reward'])
            elif ach['coin_reward'] < 0: await self.remove_coins(user_id, -ach['coin_reward'])
            
            if guild:
                config = await self.get_guild_config(guild.id)
                if config.get('log_channel'):
                    await Logger.log_event(guild, "achievement", "🏆 Получено достижение", f"{ach['icon']} **{ach['description']}**", 0xffd700, user=guild.get_member(user_id), fields={"Опыт": f"+{ach['xp_reward']}", "Монеты": f"+{ach['coin_reward']}"})
            return True

    async def get_user_achievements(self, user_id: int):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [dict(r) for r in await conn.fetch("SELECT a.id, a.name, a.description, a.icon, ua.earned_at FROM user_achievements ua JOIN achievements a ON ua.achievement_id = a.id WHERE ua.user_id = $1 ORDER BY ua.earned_at DESC", user_id)]

    async def get_all_achievements(self):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [dict(r) for r in await conn.fetch("SELECT * FROM achievements ORDER BY id")]

    async def init_profile_themes(self):
        themes = [
            ("Классическая", 0xFFD700, 0x1E1E2E, 0x14141C, None, "default", 0, None, False),
            ("Золотая", 0xFFD700, 0x2C2C3A, 0x1A1A26, None, "glow", 5000, None, True),
            ("Неоновая", 0x00FFFF, 0x0A0A1A, 0x0D0D17, None, "neon", 8000, None, True),
            ("Тёмная", 0x6A5ACD, 0x1A1A2E, 0x12121E, None, "dark", 3000, None, True)
        ]
        pool = await self.connect()
        if not pool: return
        async with pool.acquire() as conn:
            for n, a, b, c, o, s, p, pr, pur in themes:
                await conn.execute("INSERT INTO profile_themes (name, accent_color, bg_color, card_color, overlay_url, style, price, preview_url, purchasable) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING", n, a, b, c, o, s, p, pr, pur)

    async def get_user_profile(self, user_id: int):
        pool = await self.connect()
        if not pool: return {'user_id': user_id, 'theme_id': 1, 'custom_accent_color': None, 'custom_bg_color': None}
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM user_profile WHERE user_id = $1", user_id)
            if not row:
                await conn.execute("INSERT INTO user_profile (user_id, theme_id) VALUES ($1, 1)", user_id)
                return {'user_id': user_id, 'theme_id': 1, 'custom_accent_color': None, 'custom_bg_color': None}
            return dict(row)

    async def set_user_theme(self, user_id: int, theme_id: int):
        pool = await self.connect()
        if pool:
            async with pool.acquire() as conn: await conn.execute("INSERT INTO user_profile (user_id, theme_id) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET theme_id = $2", user_id, theme_id)

    async def get_theme_by_id(self, theme_id: int):
        pool = await self.connect()
        if not pool: return None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM profile_themes WHERE id = $1", theme_id)
            return dict(row) if row else None

    async def get_all_themes(self):
        pool = await self.connect()
        if not pool: return []
        async with pool.acquire() as conn: return [dict(r) for r in await conn.fetch("SELECT * FROM profile_themes ORDER BY price")]

    async def purchase_theme(self, user_id: int, theme_id: int):
        theme = await self.get_theme_by_id(theme_id)
        if not theme: return False, "Тема не найдена"
        balance = await self.get_balance(user_id)
        if balance < theme['price']: return False, f"Недостаточно монет! Нужно {theme['price']} 🪙"
        await self.remove_coins(user_id, theme['price'])
        await self.set_user_theme(user_id, theme_id)
        return True, f"✅ Тема **{theme['name']}** куплена и применена!"

db = Database()

# ==================== КОНФИГУРАЦИЯ ====================
TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TOKEN:
    print("❌ ОШИБКА: Токен Discord бота не найден!")
    sys.exit(1)

MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def get_moscow_time(dt=None):
    if dt is None: dt = datetime.datetime.now(datetime.timezone.utc)
    elif dt.tzinfo is None: dt = pytz.utc.localize(dt)
    return dt.astimezone(MOSCOW_TZ)

def format_moscow_time(dt=None, format_str="%d.%m.%Y %H:%M:%S"):
    return get_moscow_time(dt).strftime(format_str)

LEVEL_ROLES = {
    5: "Ньюфажина", 10: "Нормис", 20: "Бывалый", 30: "Альтуха",
    40: "Опиум", 50: "Игрок", 60: "Тектоник", 70: "Вайперр",
    85: "Модератор по сиськам", 100: "Админ по ляжкам"
}
DEFAULT_ROLE_NAME = "Залётный"
REP_REWARD_ROLE = "Ну крутой ля" 

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.voice_states = True
intents.messages = True
intents.guilds = True

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
        if not self.enabled: return False
        try:
            await self.ensure_session()
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}
            async with self.session.post(f"{self.base_url}/sendMessage", json=payload) as resp:
                return resp.status == 200
        except Exception as e:
            print(f"❌ Telegram send error: {e}")
            return False

    async def send_document(self, file_path: str, caption: str = "") -> bool:
        if not self.enabled: return False
        try:
            await self.ensure_session()
            with open(file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('chat_id', self.chat_id)
                data.add_field('caption', caption)
                data.add_field('document', f, filename=os.path.basename(file_path))
                async with self.session.post(f"{self.base_url}/sendDocument", data=data) as resp:
                    return resp.status == 200
        except Exception as e:
            print(f"❌ Telegram send doc error: {e}")
            return False

    async def send_stats(self) -> bool:
        if not self.enabled: return False
        total_users = await db.get_total_users()
        totals = await db.get_total_stats()
        voice_top, _ = await db.get_top_users(3)
        top_text = "".join([f"{i}. ID `{u}` — {m//60}ч {m%60}м\n" for i, (u, m) in enumerate(voice_top, 1)])
        
        msg = f"📊 *СТАТИСТИКА БОТА*\n👥 **Юзеров:** `{total_users}`\n💬 **Сообщений:** `{totals['total_messages']}`\n🎤 **Голос:** `{totals['total_voice']//60}ч {totals['total_voice']%60}м`\n🏆 **Топ 3 голос:**\n{top_text}\n⏰ *{format_moscow_time()}*"
        return await self.send_message(msg)

    async def send_alert(self, title: str, description: str, alert_type: str = "info") -> bool:
        emoji = {"info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "❌"}.get(alert_type, "📝")
        return await self.send_message(f"{emoji} *{title}*\n\n{description}\n\n⏰ {format_moscow_time()}")

    async def start_polling(self):
        if not self.enabled: return
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
            except asyncio.CancelledError: break
            except Exception as e:
                print(f"❌ Telegram polling error: {e}")
                await asyncio.sleep(5)

    async def _process_update(self, update):
        msg = update.get("message")
        if not msg or str(msg.get("chat", {}).get("id")) != self.chat_id or "text" not in msg: return
        text = msg["text"].strip()
        
        if text == "/stats": await self.send_stats()
        elif text == "/help": await self.send_message("📚 Команды: /stats, /top, /roles, /eco_top, /help")

    async def close(self):
        if self.polling_task:
            self.polling_task.cancel()
            self.polling_task = None
        if self.session: await self.session.close()

telegram = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

# ==================== СИСТЕМА ТИКЕТОВ ====================
class TicketControlsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🔒 Закрыть тикет", style=discord.ButtonStyle.danger, custom_id="close_ticket_btn")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("⚠️ Тикет будет закрыт и удален через 5 секунд...", ephemeral=False)
        await asyncio.sleep(5)
        try:
            await interaction.channel.delete(reason=f"Тикет закрыт пользователем {interaction.user}")
        except discord.Forbidden:
            pass

class TicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📩 Создать тикет", style=discord.ButtonStyle.primary, custom_id="create_ticket_btn")
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        category = discord.utils.get(guild.categories, name="Тикеты")
        if not category:
            try:
                category = await guild.create_category("Тикеты")
            except discord.Forbidden:
                return await interaction.response.send_message("❌ У меня нет прав для создания категории!", ephemeral=True)

        channel_name = f"тикет-{interaction.user.name.lower()}"
        existing_channel = discord.utils.get(guild.channels, name=channel_name)
        if existing_channel:
            return await interaction.response.send_message(f"❌ У вас уже есть открытый тикет: {existing_channel.mention}", ephemeral=True)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True, attach_files=True),
            guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
        }
        for role in guild.roles:
            if role.permissions.administrator:
                overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

        try:
            ticket_channel = await guild.create_text_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Тикет от {interaction.user}"
            )
            
            await interaction.response.send_message(f"✅ Ваш тикет успешно создан: {ticket_channel.mention}", ephemeral=True)
            
            embed = discord.Embed(
                title="Обращение в поддержку",
                description=f"Привет, {interaction.user.mention}!\nОпишите вашу проблему, и администрация ответит вам в ближайшее время.\n\nКогда вопрос будет решен, нажмите кнопку ниже.",
                color=discord.Color.blue()
            )
            await ticket_channel.send(content=f"{interaction.user.mention}", embed=embed, view=TicketControlsView())
        except discord.Forbidden:
            await interaction.response.send_message("❌ Ошибка прав: я не могу создавать каналы.", ephemeral=True)

# ==================== ПАРСЕР ГАЙДОВ С ИИ ПЕРЕВОДОМ ====================
def split_text_for_discord(text: str, max_len: int = 1900):
    chunks = []
    while len(text) > max_len:
        split_at = text.rfind('\n', 0, max_len)
        if split_at == -1:
            split_at = text.rfind(' ', 0, max_len)
            if split_at == -1:
                split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip()
    if text:
        chunks.append(text)
    return chunks

async def fetch_and_translate_guide(url: str):
    if not ai_client:
        return None, None, None
        
    try:
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200: return None, None, None
                html = await resp.text()

        soup = BeautifulSoup(html, 'lxml')
        
        # Получаем заголовок
        title_meta = soup.find('meta', property='og:title')
        en_title = title_meta['content'].replace(" | Game8", "") if title_meta else "Гайд Endfield"
        
        # Получаем главную обложку статьи (для карточки-анонса)
        img_meta = soup.find('meta', property='og:image')
        cover_url = img_meta['content'] if img_meta else None
        
        content = soup.find('div', class_='archive-style-wrapper')
        if not content:
            content = soup.find('article') or soup.body

        # 1. УБИВАЕМ МУСОРНЫЕ ССЫЛКИ, СОХРАНЯЯ ТЕКСТ
        for a_tag in content.find_all('a'):
            a_tag.unwrap() # Снимает тег <a>, оставляя текст внутри него

        # 2. УБИВАЕМ АБСОЛЮТНО ВСЕ КАРТИНКИ ИЗ ТЕЛА СТАТЬИ
        for img in content.find_all('img'):
            img.decompose()

        # 3. Удаляем мусорные блоки Game8 (рекламу, оглавления, боковые панели)
        for tag in content(['script', 'style', 'ins', 'iframe', 'nav', 'div.toc']):
            tag.decompose()
            
        for tag in content.find_all('div', class_=['a-ad', 'a-ad__container', 'a-linkHeader', 'article-bottom-links']):
            tag.decompose()

        raw_text = str(content)

        prompt = f"""
        Ты — эксперт по игре Arknights: Endfield. Переведи гайд с сайта Game8 на русский язык.

        ЗАГОЛОВОК СТАТЬИ: {en_title}

        ПРАВИЛА ОФОРМЛЕНИЯ:
        1. Переведи ВЕСЬ полезный текст статьи. Не делай кратких выжимок, сохраняй подробности!
        2. Используй Markdown Discord для красивого оформления (жирный шрифт, заголовки, списки).
        3. Если в тексте есть HTML-таблицы, преврати их в аккуратные текстовые списки.
        4. Используй правильный игровой сленг (АоЕ, Урон, Операторы, Кастер и т.д.).

        ОТВЕТ ВЫДАЙ СТРОГО В ТАКОМ ФОРМАТЕ (с разделителем ===):
        [Переведенный Заголовок]
        ===
        [Полный переведенный текст]

        ТЕКСТ ДЛЯ ПЕРЕВОДА:
        {raw_text[:40000]}
        """
        
        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model='gemini-2.5-flash',
            contents=prompt
        )
        
        parts = response.text.split('===')
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip(), cover_url
        return en_title, response.text.strip(), cover_url
        
    except Exception as e:
        print(f"Ошибка парсинга/перевода: {e}")
        return None, None, None

@tasks.loop(minutes=30)
async def auto_game8_parser():
    url = "https://game8.co/games/Arknights-Endfield"
    try:
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': 'Mozilla/5.0'}
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200: return
                html = await resp.text()

        soup = BeautifulSoup(html, 'lxml')
        links = soup.select('a.a-link') 
        
        new_guides = []
        for link in links:
            href = link.get('href')
            if href and "/games/Arknights-Endfield/archives/" in href:
                full_url = "https://game8.co" + href if href.startswith('/') else href
                if not await db.is_guide_posted(full_url):
                    new_guides.append(full_url)
                    
        if new_guides:
            target_url = new_guides[0]
            ru_title, ru_body, cover_url = await fetch_and_translate_guide(target_url)
            if not ru_title or not ru_body: return

            channels = await db.get_all_guide_channels()
            for guild_id, channel_id in channels:
                guild = bot.get_guild(guild_id)
                if guild:
                    ch = guild.get_channel(channel_id)
                    if ch:
                        try:
                            embed = discord.Embed(
                                title=f"📚 Новый гайд: {ru_title}",
                                url=target_url,
                                description="⬇️ Полный переведенный гайд читайте в ветке ниже! ⬇️",
                                color=0x00A8FF
                            )
                            if cover_url:
                                embed.set_image(url=cover_url)
                            embed.set_footer(text="Game8 • Переведено ИИ", icon_url="https://game8.co/favicon.ico")
                            
                            msg = await ch.send(embed=embed)
                            
                            thread = await msg.create_thread(name=ru_title[:100], auto_archive_duration=1440)
                            chunks = split_text_for_discord(ru_body)
                            for chunk in chunks:
                                await thread.send(chunk)
                                await asyncio.sleep(1)
                        except Exception as e:
                            print(f"Ошибка отправки ветки в Discord: {e}")
            
            await db.mark_guide_posted(target_url)

    except Exception as e:
        print(f"Ошибка фонового парсера Game8: {e}")

@auto_game8_parser.before_loop
async def before_parser():
    await bot.wait_until_ready()

# ==================== КЛАСС БОТА ====================
class ActivityBot(commands.Bot):
    async def setup_hook(self):
        self.add_view(TicketView())
        self.add_view(TicketControlsView())

    async def close(self):
        print("\n🛑 Получен сигнал на выключение. Сохраняем данные...")
        now = datetime.datetime.now(datetime.timezone.utc)
        saved_count = 0
        
        for user_id_str, session_start in list(voice_sessions.items()):
            duration = (now - session_start).total_seconds() / 60
            if duration >= 1:
                member_id = int(user_id_str)
                await db.add_voice_time(member_id, int(duration))
                coin_gain = int(duration) // 5
                if coin_gain > 0: await db.add_coins(member_id, coin_gain)
                await db.add_xp(member_id, int(duration) * 2)
                saved_count += 1
                
        print(f"✅ Сохранено голосовых сессий: {saved_count}")

        if db.pool:
            await db.pool.close()
            print("🔌 Соединение с БД корректно закрыто.")
            
        if telegram.enabled:
            await telegram.close()
            print("📱 Соединение с Telegram закрыто.")
            
        print("👋 Бот успешно завершил работу.")
        await super().close()

bot = ActivityBot(command_prefix="!", intents=intents, help_command=None)

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
async def get_guild_config(guild_id: int):
    if guild_id not in guild_config_cache:
        guild_config_cache[guild_id] = await db.get_guild_config(guild_id)
    return guild_config_cache[guild_id]

class Logger:
    @staticmethod
    async def log_event(guild, event_type, title, description, color=None, fields=None, user=None, target=None, channel=None):
        try:
            config = await get_guild_config(guild.id)
            log_ch_id = config.get('log_channel')
            if not log_ch_id: return
            
            log_channel_obj = guild.get_channel(log_ch_id)
            if not log_channel_obj: return
            
            config_keys = {"voice": "voice_events", "role": "role_events", "member": "member_events", "channel": "channel_events", "server": "server_events", "message": "message_events", "command": "command_events", "achievement": "role_events", "economy": "command_events"}
            if event_type in config_keys and not config.get(config_keys[event_type], True): return
            
            embed = discord.Embed(title=f"📝 {title}", description=description, color=color or 0x95a5a6, timestamp=get_moscow_time())
            if user: embed.add_field(name="👤 Пользователь", value=f"{user.mention}\nID: `{user.id}`", inline=True)
            if target: embed.add_field(name="🎯 Цель", value=f"{target.mention}\nID: `{target.id}`", inline=True)
            if channel: embed.add_field(name="📺 Канал", value=f"{channel.mention}\nID: `{channel.id}`", inline=True)
            if fields:
                for k, v in fields.items(): embed.add_field(name=k, value=str(v), inline=False)
            embed.set_footer(text="Время МСК")
            await log_channel_obj.send(embed=embed)
        except Exception as e: print(f"❌ Logger error: {e}")

class RoleManager:
    @staticmethod
    async def check_hierarchy(guild: discord.Guild, role: discord.Role) -> bool:
        bot_member = guild.get_member(bot.user.id)
        return bot_member and bot_member.guild_permissions.manage_roles and role.position < bot_member.top_role.position

    @staticmethod
    async def ensure_role_exists(guild: discord.Guild, role_name: str):
        role = discord.utils.get(guild.roles, name=role_name)
        if role: return role
        try:
            color = discord.Color.from_rgb((hash(role_name) & 0xFF0000) >> 16, (hash(role_name) & 0x00FF00) >> 8, hash(role_name) & 0x0000FF)
            role = await guild.create_role(name=role_name, color=color, hoist=True, reason="Авто-создание")
            await Logger.log_event(guild, "role", "Создана новая роль", f"Роль **{role_name}** создана", 0x2ecc71)
            return role
        except Exception as e: print(f"❌ Ошибка создания роли {role_name}: {e}"); return None

    @staticmethod
    async def give_default_role(member: discord.Member):
        try:
            for level_role in LEVEL_ROLES.values():
                if discord.utils.get(member.guild.roles, name=level_role) in member.roles: return
            role = await RoleManager.ensure_role_exists(member.guild, DEFAULT_ROLE_NAME)
            if role and role not in member.roles and await RoleManager.check_hierarchy(member.guild, role):
                await member.add_roles(role, reason="Начальная роль")
        except Exception as e: print(f"❌ Ошибка выдачи начальной роли: {e}")

    @staticmethod
    async def check_and_give_roles(member: discord.Member):
        try:
            level_info = await db.get_level_info(member.id)
            current_level = level_info['level']
            target_role_name = next((LEVEL_ROLES[t] for t in sorted(LEVEL_ROLES.keys(), reverse=True) if current_level >= t), None)
            if not target_role_name: return
            
            target_role = await RoleManager.ensure_role_exists(member.guild, target_role_name)
            if not target_role or target_role in member.roles or not await RoleManager.check_hierarchy(member.guild, target_role): return
            
            roles_to_remove = [discord.utils.get(member.guild.roles, name=r) for r in list(LEVEL_ROLES.values()) + [DEFAULT_ROLE_NAME] if r != target_role_name]
            roles_to_remove = [r for r in roles_to_remove if r and r in member.roles]
            
            if roles_to_remove: await member.remove_roles(*roles_to_remove, reason="Обновление уровня")
            await member.add_roles(target_role, reason=f"Достиг уровня {current_level}")
            
            if telegram.enabled and (await get_guild_config(member.guild.id)).get("telegram_notify_role"):
                await telegram.send_alert("🎉 Новая роль", f"**{member.display_name}** -> **{target_role_name}**\nУровень: {current_level}", "success")
        except Exception as e: print(f"❌ Ошибка обновления ролей: {e}")

# ==================== СИНХРОННЫЕ ФУНКЦИИ (ДЛЯ ВЫПОЛНЕНИЯ В ОТДЕЛЬНОМ ПОТОКЕ) ====================
def _generate_activity_graph_sync(member_name: str, history: list):
    dates = [row['date'].strftime('%d.%m') for row in history]
    voice_data = [row['voice_minutes'] / 60 for row in history]
    msg_data = [row['messages'] for row in history]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    fig.suptitle(f'Активность {member_name} (последние 30 дней)', fontsize=16)

    ax1.bar(dates, voice_data, color='#3498db', alpha=0.8, edgecolor='black', linewidth=0.5)
    ax1.set_ylabel('Часы в голосе', fontsize=12)
    ax1.set_title('🎤 Голосовая активность', fontsize=14, pad=10)
    ax1.grid(axis='y', alpha=0.3)

    ax2.bar(dates, msg_data, color='#2ecc71', alpha=0.8, edgecolor='black', linewidth=0.5)
    ax2.set_ylabel('Сообщения', fontsize=12)
    ax2.set_xlabel('Дата', fontsize=12)
    ax2.set_title('💬 Сообщения', fontsize=14, pad=10)
    ax2.grid(axis='y', alpha=0.3)

    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

def _generate_profile_card_sync(display_name, tag, member_id, level_info, balance, stats, achievements, current_role, avatar_bytes, theme):
    W, H = 1000, 380
    AVATAR_SIZE = 120
    AVATAR_X, AVATAR_Y = 30, 30

    def hex_to_rgb(hex_color, alpha=255):
        return ((hex_color >> 16) & 0xFF, (hex_color >> 8) & 0xFF, hex_color & 0xFF, alpha)

    BG_COLOR = hex_to_rgb(theme['bg_color'])
    CARD_COLOR = hex_to_rgb(theme['card_color'], 235)
    ACCENT_COLOR = hex_to_rgb(theme['accent_color'])[:3]
    TEXT_COLOR = (255, 255, 255)
    SECONDARY_COLOR = (200, 200, 200)

    try:
        font_large = ImageFont.truetype("Roboto-Medium.ttf", 32)
        font_medium = ImageFont.truetype("Roboto-Medium.ttf", 24)
        font_small = ImageFont.truetype("Roboto-Medium.ttf", 20)
        font_micro = ImageFont.truetype("Roboto-Medium.ttf", 14)
    except IOError:
        font_large = font_medium = font_small = font_micro = ImageFont.load_default()

    img = Image.new('RGBA', (W, H), BG_COLOR)
    draw = ImageDraw.Draw(img)

    for i in range(H):
        alpha = int(8 * (1 - i / H))
        draw.line([(0, i), (W, i)], fill=(*ACCENT_COLOR[:3], alpha))

    draw.rounded_rectangle([15, 15, W - 15, H - 15], radius=20, fill=CARD_COLOR, outline=ACCENT_COLOR, width=3)

    if avatar_bytes:
        try:
            avatar_img = Image.open(io.BytesIO(avatar_bytes)).convert('RGBA')
            avatar_img = avatar_img.resize((AVATAR_SIZE, AVATAR_SIZE), Image.LANCZOS)
            mask = Image.new('L', avatar_img.size, 0)
            ImageDraw.Draw(mask).ellipse((0, 0, AVATAR_SIZE, AVATAR_SIZE), fill=255)
            avatar_img.putalpha(mask)
            img.paste(avatar_img, (AVATAR_X, AVATAR_Y), avatar_img)
        except Exception: pass

    name_x = AVATAR_X + AVATAR_SIZE + 20
    draw.text((name_x, 40), display_name, font=font_large, fill=ACCENT_COLOR)
    draw.text((name_x, 80), tag, font=font_small, fill=SECONDARY_COLOR)

    draw.text((W - 250, 40), f"⚡ УРОВЕНЬ {level_info['level']}", font=font_medium, fill=ACCENT_COLOR)

    bar_y, bar_w = 130, 500
    draw.rounded_rectangle([name_x, bar_y, name_x + bar_w, bar_y + 26], radius=13, fill=(60, 60, 80))
    progress_w = int(bar_w * level_info['progress'])
    if progress_w > 0:
        draw.rounded_rectangle([name_x, bar_y, name_x + progress_w, bar_y + 26], radius=13, fill=ACCENT_COLOR)
    
    draw.text((name_x, 190), f"💰 {balance:,}", font=font_medium, fill=TEXT_COLOR)
    draw.text((name_x + 200, 190), f"💬 {stats['messages']:,}", font=font_medium, fill=TEXT_COLOR)
    draw.text((name_x + 400, 190), f"🎤 {stats['voice_hours']}ч {stats['voice_remaining_minutes']}м", font=font_medium, fill=TEXT_COLOR)
    draw.text((name_x, 240), f"👑 {current_role}", font=font_small, fill=ACCENT_COLOR)

    draw.text((W - 300, 130), "🏆 ДОСТИЖЕНИЯ", font=font_small, fill=TEXT_COLOR)
    achiv_y = 170
    for ach in achievements:
        desc = ach['description'][:28] + "…" if len(ach['description']) > 30 else ach['description']
        draw.text((W - 290, achiv_y), f"{ach['icon']} {desc}", font=font_micro, fill=SECONDARY_COLOR)
        achiv_y += 30

    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return buf

# ==================== ЗАДАЧИ АКТИВНОСТИ ====================
@tasks.loop(minutes=5)
async def check_voice_time():
    now = datetime.datetime.now(datetime.timezone.utc)
    for user_id_str, session_start in list(voice_sessions.items()):
        duration = (now - session_start).total_seconds() / 60
        member_id = int(user_id_str)
        for guild in bot.guilds:
            member = guild.get_member(member_id)
            if member and member.voice and member.voice.channel:
                await db.add_voice_time(member_id, 5)
                await db.add_coins(member_id, 1)
                leveled_up, new_level = await db.add_xp(member_id, 10)
                if leveled_up:
                    try: await member.send(f"🎉 Поздравляю! Вы достигли **{new_level} уровня**!")
                    except: pass
                await RoleManager.check_and_give_roles(member)
                voice_sessions[user_id_str] = now - datetime.timedelta(minutes=duration % 5)
                break

@tasks.loop(hours=24)
async def daily_report():
    if telegram.enabled:
        await telegram.send_stats()

@tasks.loop(time=datetime_time(hour=0, minute=5))
async def collect_stats():
    for guild in bot.guilds:
        for m in guild.members:
            if not m.bot:
                s = await db.get_user_stats(m.id)
                await db.save_daily_stats(m.id, guild.id, s['voice_minutes'], s['messages'])
        await db.save_server_stats(guild.id)

@tasks.loop(time=datetime_time(hour=3, minute=0))
async def backup_db():
    if not telegram.enabled: return
    pg_dump_path = subprocess.run(["which", "pg_dump"], capture_output=True, text=True).stdout.strip()
    if not pg_dump_path: return 
    
    db_url = os.environ.get("DATABASE_URL")
    if not db_url: return
    
    filename = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    res = subprocess.run(["pg_dump", db_url, "-T", "user_history", "-f", filename], capture_output=True, text=True)
    if res.returncode == 0:
        await telegram.send_document(filename, f"📦 Бэкап БД\n⏰ {format_moscow_time()}")
        os.remove(filename)

# ==================== СОБЫТИЯ DISCORD ====================
@bot.event
async def on_ready():
    print(f"✅ Бот {bot.user} запущен!")
    await db.init_db()
    await db.init_achievements()
    await db.init_profile_themes()
    
    if not check_voice_time.is_running(): check_voice_time.start()
    if telegram.enabled and not daily_report.is_running(): daily_report.start()
    if telegram.enabled: await telegram.start_polling()
    if not collect_stats.is_running(): collect_stats.start()
    if telegram.enabled and not backup_db.is_running(): backup_db.start()
    if not auto_game8_parser.is_running(): auto_game8_parser.start()

@bot.event
async def on_message(message):
    if message.author.bot: return
    if not message.content.startswith('!'):
        await db.add_message(message.author.id)
        await db.add_coins(message.author.id, 2)
        leveled_up, new_level = await db.add_xp(message.author.id, 5)
        if leveled_up:
            try: await message.author.send(f"🎉 Вы достигли **{new_level} уровня**!")
            except: pass
            
        if isinstance(message.author, discord.Member):
            await RoleManager.check_and_give_roles(message.author)
            
        s = await db.get_user_stats(message.author.id)
        if s['messages'] == 100: await db.check_achievement(message.author.id, "chat_100", message.guild)
        if s['messages'] == 1000: await db.check_achievement(message.author.id, "chat_1000", message.guild)
    await bot.process_commands(message)

@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot: return
    uid = str(member.id)
    now = datetime.datetime.now(datetime.timezone.utc)

    if before.channel is None and after.channel is not None:
        voice_sessions[uid] = now
    elif before.channel is not None and after.channel is None:
        if uid in voice_sessions:
            dur = (now - voice_sessions[uid]).total_seconds() / 60
            if dur >= 1:
                await db.add_voice_time(member.id, int(dur))
                await db.add_coins(member.id, int(dur) // 5)
                await db.add_xp(member.id, int(dur) * 2)
                await RoleManager.check_and_give_roles(member)
            del voice_sessions[uid]

# ==================== КОМАНДЫ DISCORD ====================
@bot.command(name="гайд", aliases=["guide", "game8"])
async def manual_game8_guide(ctx, url: str):
    """Ручная команда: Парсит полный гайд с Game8 и создает ветку с переводом"""
    if "game8.co" not in url:
        return await ctx.send("❌ Поддерживаются только ссылки с сайта Game8!")

    loading_msg = await ctx.send("⏳ Читаю страницу и перевожу текст (картинки вырезаны для читаемости). Это займет около 10 секунд...")

    ru_title, ru_body, cover_url = await fetch_and_translate_guide(url)
    if not ru_title or not ru_body:
        return await loading_msg.edit(content="❌ Не удалось получить или перевести гайд. Возможно, неправильная ссылка или ИИ не ответил.")

    embed = discord.Embed(
        title=f"📚 Новый гайд: {ru_title}",
        url=url,
        description="⬇️ Полный переведенный текст читайте в ветке ниже! ⬇️",
        color=0x00A8FF
    )
    if cover_url:
        embed.set_image(url=cover_url)
    embed.set_footer(text="Game8 • Переведено ИИ", icon_url="https://game8.co/favicon.ico")
    
    view = discord.ui.View()
    view.add_item(discord.ui.Button(label="Читать оригинал", style=discord.ButtonStyle.link, url=url))

    await loading_msg.delete()
    msg = await ctx.send(embed=embed, view=view)
    
    thread = await msg.create_thread(name=ru_title[:100], auto_archive_duration=1440)
    chunks = split_text_for_discord(ru_body)
    for chunk in chunks:
        await thread.send(chunk)
        await asyncio.sleep(1)

@bot.command(name="канал_гайдов")
@commands.has_permissions(administrator=True)
async def set_guides_channel(ctx, channel: discord.TextChannel):
    """Устанавливает канал, куда будут автоматически скидываться гайды с Game8"""
    await db.update_guild_config(ctx.guild.id, 'guides_channel', channel.id)
    await ctx.send(f"✅ Теперь переведенные гайды с Game8 будут автоматически публиковаться в {channel.mention}")

@bot.command(name="статистика")
async def stats(ctx, member: discord.Member = None):
    member = member or ctx.author
    data = await db.get_user_stats(member.id)
    level_info = await db.get_level_info(member.id)
    rep = await db.get_reputation(member.id)
    
    embed = discord.Embed(title=f"📊 Статистика {member.display_name}", color=discord.Color.blue())
    embed.add_field(name="🎤 Голос", value=f"{data['voice_hours']}ч {data['voice_remaining_minutes']}м", inline=True)
    embed.add_field(name="💬 Сообщений", value=f"{data['messages']}", inline=True)
    embed.add_field(name="📈 Уровень", value=f"{level_info['level']} ({level_info['xp']} XP)", inline=True)
    embed.add_field(name="⭐ Репутация", value=f"{rep}", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="rep", aliases=["реп", "репутация", "+rep"])
async def give_reputation(ctx, member: discord.Member):
    if member.bot:
        return await ctx.send("❌ Ботам репутация не нужна!")
    if member.id == ctx.author.id:
        return await ctx.send("❌ Нельзя выдать репутацию самому себе!")

    can_give, cooldown_sec = await db.can_give_rep(ctx.author.id)
    if not can_give:
        hours = cooldown_sec // 3600
        mins = (cooldown_sec % 3600) // 60
        return await ctx.send(f"⏳ Вы уже выдавали репутацию сегодня. Подождите еще **{hours}ч {mins}м**.")

    new_rep = await db.add_reputation(ctx.author.id, member.id)

    embed = discord.Embed(
        title="⭐ Плюс к репутации!",
        description=f"{ctx.author.mention} выразил уважение {member.mention}!\nТеперь у него/неё **{new_rep}** ед. репутации.",
        color=discord.Color.gold()
    )
    await ctx.send(embed=embed)

    if new_rep >= 10:
        role = discord.utils.get(ctx.guild.roles, name=REP_REWARD_ROLE)
        if not role:
            role = await RoleManager.ensure_role_exists(ctx.guild, REP_REWARD_ROLE)
            
        if role and role not in member.roles:
            try:
                await member.add_roles(role, reason="Достиг 10 единиц репутации")
                await ctx.send(f"🎉 {member.mention} получил особую роль **{REP_REWARD_ROLE}** за отличную репутацию!")
            except discord.Forbidden:
                pass

@bot.command(name="график", aliases=["graph"])
async def activity_graph(ctx, member: discord.Member = None):
    member = member or ctx.author
    async with ctx.typing():
        history = await db.get_user_history(member.id, ctx.guild.id, 30)
        if not history:
            return await ctx.send("❌ Недостаточно данных.")
        history.reverse()
        
        buf = await asyncio.to_thread(_generate_activity_graph_sync, member.display_name, history)
        
        file = discord.File(buf, filename='activity.png')
        embed = discord.Embed(title=f"📈 Активность {member.display_name}", color=discord.Color.blue())
        embed.set_image(url="attachment://activity.png")
        await ctx.send(embed=embed, file=file)

async def fetch_avatar(member: discord.Member, size: int = 256) -> bytes:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(member.display_avatar.url) as resp:
                if resp.status == 200: return await resp.read()
    except: pass
    return None

@bot.command(name="профиль", aliases=["rank", "profile"])
async def profile(ctx, member: discord.Member = None):
    member = member or ctx.author
    async with ctx.typing():
        level_info = await db.get_level_info(member.id)
        balance = await db.get_balance(member.id)
        stats = await db.get_user_stats(member.id)
        achievements = await db.get_user_achievements(member.id)
        profile_settings = await db.get_user_profile(member.id)
        theme = await db.get_theme_by_id(profile_settings['theme_id']) or await db.get_theme_by_id(1)

        current_role = next((LEVEL_ROLES[t] for t in sorted(LEVEL_ROLES.keys(), reverse=True) if level_info['level'] >= t), DEFAULT_ROLE_NAME)
        avatar_bytes = await fetch_avatar(member, 256)
        
        tag = f"{member.name}#{member.discriminator}" if member.discriminator != "0" else member.name

        buf = await asyncio.to_thread(
            _generate_profile_card_sync,
            member.display_name, tag, member.id, level_info, balance, stats, achievements[:3], current_role, avatar_bytes, theme
        )
        
        file = discord.File(buf, filename='profile.png')
        embed = discord.Embed(title=f"🖼️ Профиль {member.display_name}", color=theme['accent_color'])
        embed.set_image(url="attachment://profile.png")
        await ctx.send(embed=embed, file=file)

@bot.command(name="магазин")
async def shop(ctx):
    roles = await db.get_shop_roles(ctx.guild.id)
    embed = discord.Embed(title="🛒 Магазин ролей", color=discord.Color.blue())
    for item in roles:
        role = ctx.guild.get_role(item['role_id'])
        if role: embed.add_field(name=role.name, value=f"Цена: {item['price']} 🪙\n{item['description']}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="купить")
async def buy_role(ctx, *, role_name: str):
    role = discord.utils.get(ctx.guild.roles, name=role_name)
    if not role: return await ctx.send("❌ Роль не найдена.")
    shop_item = next((i for i in await db.get_shop_roles(ctx.guild.id) if i['role_id'] == role.id), None)
    if not shop_item: return await ctx.send("❌ Роль не продается.")
    
    bal = await db.get_balance(ctx.author.id)
    if bal < shop_item['price']: return await ctx.send("❌ Недостаточно монет!")
    if await db.has_role_purchased(ctx.guild.id, ctx.author.id, role.id): return await ctx.send("❌ Роль уже куплена.")
    
    await db.remove_coins(ctx.author.id, shop_item['price'])
    await db.purchase_role(ctx.guild.id, ctx.author.id, role.id)
    await ctx.author.add_roles(role, reason="Покупка")
    await ctx.send(f"✅ Вы купили роль **{role.name}**!")

@bot.command(name="setup_tickets", aliases=["тикеты"])
@commands.has_permissions(administrator=True)
async def setup_tickets(ctx):
    embed = discord.Embed(
        title="📩 Служба поддержки",
        description="У вас возник вопрос, проблема или жалоба?\nНажмите на кнопку ниже, чтобы создать приватный канал для связи с администрацией.",
        color=discord.Color.blurple()
    )
    await ctx.send(embed=embed, view=TicketView())
    await ctx.message.delete()

@bot.command(name="помощь", aliases=["help", "команды"])
async def help_command(ctx):
    embed = discord.Embed(
        title="📚 Список команд бота",
        description="Здесь собраны все доступные команды сервера.",
        color=discord.Color.blurple(),
        timestamp=get_moscow_time()
    )
    
    user_cmds = (
        "`!профиль` (или `!rank`) — Ваша красивая карточка профиля со статистикой\n"
        "`!статистика [@юзер]` — Подробная текстовая статистика активности\n"
        "`!график [@юзер]` — График вашей активности за последние 30 дней\n"
        "`!rep [@юзер]` (или `+rep`) — Выдать репутацию (раз в 24 часа)\n"
        "`!магазин` — Посмотреть список ролей, доступных для покупки\n"
        "`!купить <название>` — Купить роль за накопленные монеты\n"
        "`!гайд <ссылка_на_game8>` — Полный перевод гайда с сайта Game8"
    )
    embed.add_field(name="👤 Основные команды", value=user_cmds, inline=False)
    
    if ctx.author.guild_permissions.administrator:
        admin_cmds = (
            "`!ручной_бэкап` (или `!бэкап`) — Сделать бэкап базы данных в Telegram\n"
            "`!setup_tickets` — Разместить панель для создания тикетов\n"
            "`!канал_гайдов #канал` — Выбрать канал для авто-постинга гайдов Game8"
        )
        embed.add_field(name="👑 Команды администратора", value=admin_cmds, inline=False)
        
    embed.set_footer(text=f"Бот: {bot.user.name} • Время МСК", icon_url=bot.user.display_avatar.url if bot.user.display_avatar else None)
    await ctx.send(embed=embed)

@bot.command(name="ручной_бэкап", aliases=["бэкап", "backup"])
@commands.has_permissions(administrator=True)
async def manual_backup(ctx):
    if not telegram.enabled:
        await ctx.send("❌ Telegram-бот не настроен. Для получения бэкапов укажите токены в переменных окружения.")
        return
        
    await ctx.send("⏳ Создаю резервную копию базы данных...")
    
    pg_dump_path = subprocess.run(["which", "pg_dump"], capture_output=True, text=True).stdout.strip()
    if not pg_dump_path:
        await ctx.send("❌ Утилита `pg_dump` не найдена в системе. Убедитесь, что `postgresql` добавлен в Nixpacks на Railway.")
        return 
    
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        await ctx.send("❌ Не найдена переменная `DATABASE_URL`.")
        return
    
    filename = f"manual_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    res = subprocess.run(["pg_dump", db_url, "-T", "user_history", "-f", filename], capture_output=True, text=True)
    
    if res.returncode == 0:
        success = await telegram.send_document(
            filename, 
            f"📦 **Ручной бэкап БД**\nЗапросил: {ctx.author.display_name}\nСервер: {ctx.guild.name}\n⏰ {format_moscow_time()}"
        )
        os.remove(filename)
        if success: await ctx.send("✅ Бэкап успешно создан и отправлен в ваш Telegram!")
        else: await ctx.send("⚠️ Бэкап создан, но произошла ошибка при отправке в Telegram. Проверьте ID чата.")
    else:
        await ctx.send(f"❌ Ошибка при создании бэкапа:\n```text\n{res.stderr}\n```")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Пропущен аргумент: `{error.param.name}`. Введите `!помощь` для справки.")
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send(f"❌ Нет прав для использования этой команды.")

# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    bot.run(TOKEN)
