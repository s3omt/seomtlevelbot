import discord
from discord.ext import commands, tasks
import json
import asyncio
import datetime
import sys
import aiohttp
from collections import defaultdict
import pytz
from typing import Dict, List, Optional, Tuple, Any
import threading
from flask import Flask, jsonify
import asyncpg
import os

# ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================
class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Создаёт пул соединений с PostgreSQL"""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                os.environ.get("DATABASE_URL"),
                min_size=1,
                max_size=10
            )
        return self.pool

    async def init_db(self):
        """Создаёт таблицы, если их нет"""
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

    # ----- МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ -----
    async def add_message(self, user_id: int):
        """Увеличивает счётчик сообщений пользователя на 1"""
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, messages) VALUES ($1, 1)
                ON CONFLICT (user_id) DO UPDATE
                SET messages = users.messages + 1
            """, user_id)

    async def add_voice_time(self, user_id: int, minutes: int):
        """Добавляет минуты голосовой активности"""
        pool = await self.connect()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, voice_minutes) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE
                SET voice_minutes = users.voice_minutes + $2
            """, user_id, minutes)

    async def get_user_stats(self, user_id: int):
        """Возвращает статистику пользователя в виде словаря"""
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
        """Возвращает топ пользователей по голосу и сообщениям"""
        pool = await self.connect()
        async with pool.acquire() as conn:
            # Топ по голосу
            voice_rows = await conn.fetch("""
                SELECT user_id, voice_minutes FROM users
                ORDER BY voice_minutes DESC LIMIT $1
            """, limit)
            # Топ по сообщениям
            msg_rows = await conn.fetch("""
                SELECT user_id, messages FROM users
                ORDER BY messages DESC LIMIT $1
            """, limit)
            return (
                [(row['user_id'], row['voice_minutes']) for row in voice_rows],
                [(row['user_id'], row['messages']) for row in msg_rows]
            )

    async def get_total_users(self):
        """Возвращает общее количество пользователей в базе"""
        pool = await self.connect()
        async with pool.acquire() as conn:
            row = await conn.fetchval("SELECT COUNT(*) FROM users")
            return row

    async def get_total_stats(self):
        """Возвращает суммарные показатели по сообщениям и голосу"""
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

# Создаём глобальный экземпляр базы данных
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

# ==================== КОНФИГУРАЦИЯ РОЛЕЙ ====================
ROLES_CONFIG = {
    "Залётный": {"voice_minutes": 0},
    "Ньюфажина": {"voice_minutes": 300},
    "Бывалый": {"voice_minutes": 1200},
    "Додик": {"voice_minutes": 3000},
    "Дэбил": {"voice_minutes": 10000},
    "Джокер Гребанный Циник": {"voice_minutes": 30000}
}

ROLE_COLORS = {
    "Залётный": 0x9E9E9E,
    "Ньюфажина": 0x4CAF50,
    "Бывалый": 0x2196F3,
    "Додик": 0xFF9800,
    "Дэбил": 0x9C27B0,
    "Джокер Гребанный Циник": 0xFF5722
}

ROLE_ORDER = list(ROLES_CONFIG.keys())

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
log_channel = None
log_config = {
    "voice_events": True,
    "role_events": True,
    "member_events": True,
    "channel_events": True,
    "server_events": True,
    "message_events": False,
    "command_events": True,
    "telegram_notify_role": False,
    "telegram_daily_report": True
}

# ==================== TELEGRAM БОТ (С ПОЛЛИНГОМ) ====================
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

    # ========== ПОЛЛИНГ КОМАНД ==========
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
                "• `/roles` — список ролей\n"
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
            lines = ["🎖️ *Роли за голосовую активность:*\n"]
            for role in ROLE_ORDER:
                minutes = ROLES_CONFIG[role]["voice_minutes"]
                lines.append(f"**{role}** — {minutes//60}ч {minutes%60}м")
            await self.send_message("\n".join(lines))
        elif text == "/help":
            await self.send_message(
                "📚 *Команды Telegram:*\n\n"
                "`/stats` — статистика бота\n"
                "`/top` — топ пользователей\n"
                "`/roles` — список ролей\n"
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

# ==================== ЛОГГЕР ====================
class Logger:
    @staticmethod
    async def log_event(guild: discord.Guild, event_type: str, title: str, description: str,
                       color: int = None, fields: Dict = None, user: discord.Member = None,
                       channel: discord.abc.GuildChannel = None) -> None:
        global log_channel
        try:
            if not log_channel:
                return

            # Защита: если log_channel уже объект канала или число — преобразуем
            if isinstance(log_channel, discord.TextChannel):
                log_channel_id = int(log_channel.id)
            elif isinstance(log_channel, int):
                log_channel_id = log_channel
            elif isinstance(log_channel, str):
                log_channel_id = int(log_channel)
            else:
                return

            log_channel_obj = guild.get_channel(log_channel_id)
            if not log_channel_obj:
                return

            # Проверка конфигурации
            config_keys = {
                "voice": "voice_events", "role": "role_events",
                "member": "member_events", "channel": "channel_events",
                "server": "server_events", "message": "message_events",
                "command": "command_events"
            }
            if event_type in config_keys and not log_config.get(config_keys[event_type], True):
                return

            color_map = {
                "voice": 0x3498db, "role": 0x2ecc71, "member": 0xe67e22,
                "channel": 0x9b59b6, "server": 0xe74c3c, "command": 0x1abc9c,
                "message": 0x95a5a6
            }

            embed = discord.Embed(
                title=f"📝 {title}",
                description=description,
                color=color or color_map.get(event_type, 0x95a5a6),
                timestamp=get_moscow_time()
            )

            event_icons = {
                "voice": "🎤", "role": "👑", "member": "👤", "channel": "📺",
                "server": "🏠", "command": "⚙️", "message": "💬"
            }

            embed.set_author(
                name=f"{event_icons.get(event_type, '📝')} {event_type.upper()}",
                icon_url=guild.icon.url if guild.icon else None
            )

            if user:
                embed.add_field(name="👤 Пользователь",
                              value=f"{user.mention}\nID: `{user.id}`", inline=True)
                embed.set_thumbnail(url=user.display_avatar.url)

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
            color = ROLE_COLORS.get(role_name, 0x9E9E9E)
            role = await guild.create_role(
                name=role_name,
                color=discord.Color(color),
                hoist=True,
                mentionable=False,
                reason="Автоматическое создание роли"
            )
            print(f"✅ Создана роль {role_name} на {guild.name}")
            await Logger.log_event(
                guild=guild,
                event_type="role",
                title="Создана новая роль",
                description=f"Роль **{role_name}** создана автоматически",
                color=0x2ecc71,
                fields={"Цвет": f"`#{color:06x}`"}
            )
            return role
        except Exception as e:
            print(f"❌ Ошибка создания роли {role_name}: {e}")
            return None

    @staticmethod
    async def give_default_role(member: discord.Member):
        try:
            for role_name in ROLES_CONFIG.keys():
                role = discord.utils.get(member.guild.roles, name=role_name)
                if role and role in member.roles:
                    return
            role = discord.utils.get(member.guild.roles, name="Залётный")
            if not role:
                role = await RoleManager.ensure_role_exists(member.guild, "Залётный")
            if role and role not in member.roles and await RoleManager.check_hierarchy(member.guild, role):
                await member.add_roles(role, reason="Начальная роль")
                print(f"✅ Выдана роль Залётный {member}")
                await Logger.log_event(
                    guild=member.guild,
                    event_type="role",
                    title="Выдана начальная роль",
                    description=f"Пользователь {member.mention} получил роль **Залётный**",
                    color=0x2ecc71,
                    user=member
                )
        except Exception as e:
            print(f"❌ Ошибка выдачи роли: {e}")

    @staticmethod
    async def check_and_give_roles(member: discord.Member):
        try:
            stats = await db.get_user_stats(member.id)
            voice_minutes = stats['voice_minutes']

            earned_role_name = "Залётный"
            for role_name in reversed(ROLE_ORDER):
                if voice_minutes >= ROLES_CONFIG[role_name]["voice_minutes"]:
                    earned_role_name = role_name
                    break

            earned_role = discord.utils.get(member.guild.roles, name=earned_role_name)
            if not earned_role:
                earned_role = await RoleManager.ensure_role_exists(member.guild, earned_role_name)

            if not earned_role or earned_role in member.roles:
                return
            if not await RoleManager.check_hierarchy(member.guild, earned_role):
                return

            roles_to_remove = []
            for role_name in ROLES_CONFIG.keys():
                if role_name != earned_role_name:
                    old_role = discord.utils.get(member.guild.roles, name=role_name)
                    if old_role and old_role in member.roles:
                        roles_to_remove.append(old_role)
            if roles_to_remove:
                await member.remove_roles(*roles_to_remove, reason="Обновление роли")

            await member.add_roles(earned_role, reason=f"Голос: {voice_minutes} мин")
            print(f"✅ {member} получил роль {earned_role_name} ({voice_minutes} мин)")

            await Logger.log_event(
                guild=member.guild,
                event_type="role",
                title="Получена новая роль",
                description=f"Пользователь {member.mention} получил роль **{earned_role_name}**",
                color=0x2ecc71,
                user=member,
                fields={"Голосовая активность": f"{voice_minutes // 60}ч {voice_minutes % 60}м"}
            )

            if telegram.enabled and log_config.get("telegram_notify_role", False):
                await telegram.send_alert(
                    "🎉 Новая роль",
                    f"Пользователь **{member.display_name}** получил роль **{earned_role_name}**\n\n"
                    f"🎤 Голос: {voice_minutes // 60}ч {voice_minutes % 60}м\n"
                    f"💬 Сообщений: {stats['messages']}",
                    "success"
                )

        except Exception as e:
            print(f"❌ Ошибка обновления роли: {e}")

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
                    voice_sessions[user_id] = now - datetime.timedelta(minutes=duration % 5)
                    await RoleManager.check_and_give_roles(member)
                    break
    except Exception as e:
        print(f"❌ Ошибка check_voice_time: {e}")

@tasks.loop(hours=24)
async def daily_report():
    try:
        if telegram.enabled and log_config.get("telegram_daily_report", True):
            await telegram.send_stats()
            print("📊 Ежедневный отчет отправлен в Telegram")
    except Exception as e:
        print(f"❌ Ошибка daily_report: {e}")

# ==================== СОБЫТИЯ DISCORD ====================
@bot.event
async def on_ready():
    print(f"✅ Бот {bot.user} запущен!")
    print(f"📊 Серверов: {len(bot.guilds)}")

    # ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ
    await db.init_db()
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

    # Создание ролей
    for guild in bot.guilds:
        print(f"\n🔍 Сервер: {guild.name}")
        for role_name in ROLES_CONFIG.keys():
            await RoleManager.ensure_role_exists(guild, role_name)

    # Выдача начальных ролей
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
        if isinstance(message.author, discord.Member):
            await RoleManager.check_and_give_roles(message.author)
    await bot.process_commands(message)

@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    if log_config.get("message_events", False):
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
    if log_config.get("message_events", False):
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
        if log_config.get("voice_events", True):
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
                await RoleManager.check_and_give_roles(member)
                if log_config.get("voice_events", True):
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
                            "Время в канале": f"{int(duration)} минут"
                        }
                    )
            del voice_sessions[user_id]

    elif before.channel is not None and after.channel is not None and before.channel != after.channel:
        if user_id in voice_sessions:
            duration = (now - voice_sessions[user_id]).total_seconds() / 60
            if duration >= 1:
                await db.add_voice_time(member.id, int(duration))
            voice_sessions[user_id] = now
            if log_config.get("voice_events", True):
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
@bot.command(name="статистика")
async def stats(ctx, member: discord.Member = None):
    if not member:
        member = ctx.author
    data = await db.get_user_stats(member.id)

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

    current_role = "Залётный"
    for role_name in reversed(ROLE_ORDER):
        if data['voice_minutes'] >= ROLES_CONFIG[role_name]["voice_minutes"]:
            current_role = role_name
            break
    embed.add_field(name="👑 Текущая роль", value=f"**{current_role}**", inline=False)

    current_index = ROLE_ORDER.index(current_role)
    if current_index < len(ROLE_ORDER) - 1:
        next_role = ROLE_ORDER[current_index + 1]
        required = ROLES_CONFIG[next_role]["voice_minutes"]
        remaining = max(0, required - data['voice_minutes'])
        progress = (data['voice_minutes'] / required) * 100 if required > 0 else 0
        embed.add_field(
            name=f"🎯 До {next_role}",
            value=f"Осталось: **{remaining // 60}ч {remaining % 60}м**\nПрогресс: `{progress:.1f}%`",
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

@bot.command(name="логи")
@commands.has_permissions(administrator=True)
async def logs(ctx, target_channel: discord.TextChannel = None):
    """Управление системой логирования"""
    global log_channel, log_config

    if target_channel:
        log_channel = str(target_channel.id)
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
        embed = discord.Embed(
            title="📝 Управление логированием",
            color=discord.Color.purple(),
            timestamp=get_moscow_time()
        )
        if log_channel:
            ch = ctx.guild.get_channel(int(log_channel))
            if ch:
                embed.add_field(name="✅ Лог-канал", value=f"{ch.mention}", inline=False)
            else:
                embed.add_field(name="⚠️ Лог-канал не найден", value=f"ID: {log_channel}", inline=False)
        else:
            embed.add_field(name="❌ Лог-канал не установлен", value="Используйте `!логи #канал`", inline=False)

        config_text = ""
        for key, value in log_config.items():
            if not key.startswith("telegram"):
                config_text += f"• **{key.replace('_', ' ').title()}:** {'✅' if value else '❌'}\n"
        embed.add_field(name="⚙️ Конфигурация", value=config_text, inline=False)
        embed.set_footer(text="Используйте !настройки_логов для детальной настройки")
        await ctx.send(embed=embed)

@bot.command(name="тест_лога", aliases=["тест-лога"])
@commands.has_permissions(administrator=True)
async def test_log(ctx):
    """Тестирование системы логирования"""
    global log_channel

    if not log_channel:
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
    """Настройка типов событий для логирования"""
    global log_config

    if not event_type:
        embed = discord.Embed(
            title="⚙️ Настройки логирования",
            color=discord.Color.blue(),
            timestamp=get_moscow_time()
        )
        config_text = ""
        for key, value in log_config.items():
            if key.startswith("telegram"):
                config_text += f"• **{key.replace('_', ' ').title()}:** {'✅' if value else '❌'}\n"
            else:
                config_text += f"• **{key}:** {'✅ Вкл' if value else '❌ Выкл'}\n"
        embed.add_field(name="Текущие настройки", value=config_text, inline=False)
        embed.add_field(
            name="📝 Доступные типы",
            value="`voice_events`, `role_events`, `member_events`, `channel_events`, `server_events`, `message_events`, `command_events`",
            inline=False
        )
        embed.set_footer(text="Используйте: !настройки_логов [тип] [on/off]")
        await ctx.send(embed=embed)
        return

    if event_type not in log_config:
        await ctx.send(f"❌ Неизвестный тип события: {event_type}")
        return
    if not status or status.lower() not in ['on', 'off']:
        await ctx.send(f"❌ Укажите on или off")
        return
    log_config[event_type] = (status.lower() == 'on')
    await ctx.send(f"✅ {event_type} теперь {'включен' if log_config[event_type] else 'выключен'}")

@bot.command(name="telegram")
@commands.has_permissions(administrator=True)
async def telegram_cmd(ctx, action: str = None):
    """Управление Telegram уведомлениями"""
    global log_config

    if not telegram.enabled:
        embed = discord.Embed(
            title="❌ Telegram не настроен",
            description="Добавьте переменные окружения:\n`TELEGRAM_BOT_TOKEN`\n`TELEGRAM_CHAT_ID`",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

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
            value=f"• Уведомления о ролях: {'✅' if log_config.get('telegram_notify_role', False) else '❌'}\n"
                  f"• Ежедневный отчет: {'✅' if log_config.get('telegram_daily_report', True) else '❌'}",
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
        log_config["telegram_notify_role"] = True
        await ctx.send("✅ Уведомления о новых ролях **включены**")
    elif action == "off":
        log_config["telegram_notify_role"] = False
        await ctx.send("❌ Уведомления о новых ролях **выключены**")
    elif action == "daily":
        current = log_config.get("telegram_daily_report", True)
        log_config["telegram_daily_report"] = not current
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
        value="`!статистика` - ваша статистика\n`!статистика @пользователь` - статистика пользователя\n`!топ` - топ пользователей\n`!помощь` - это сообщение",
        inline=False
    )
    embed.add_field(
        name="👑 **Для администраторов**",
        value="`!логи` - статус лог-канала\n`!логи #канал` - установить канал для логов\n`!тест_лога` - тест системы логирования\n"
              "`!настройки_логов` - показать настройки\n`!настройки_логов [тип] [on/off]` - изменить настройки\n"
              "`!telegram` - управление Telegram уведомлениями\n`!очистить_команды` - удалить старые слэш-команды",
        inline=False
    )
    embed.add_field(
        name="⚙️ **Типы событий**",
        value="`voice_events` - голосовая активность\n`role_events` - события ролей\n`member_events` - вход/выход участников\n"
              "`channel_events` - создание/удаление каналов\n`server_events` - изменения сервера\n"
              "`message_events` - удаление/редактирование сообщений\n`command_events` - использование команд",
        inline=False
    )
    embed.set_footer(text=f"Бот: {bot.user.name} • Время МСК")
    await ctx.send(embed=embed)

# ==================== FLASK ДЛЯ UPTIMEROBOT ====================
app = Flask(__name__)

@app.route('/')
def home():
    # В синхронном окружении не можем использовать await, поэтому временно ставим 0
    return jsonify({
        "status": "online",
        "bot": str(bot.user) if bot.user else "starting",
        "servers": len(bot.guilds) if bot.guilds else 0,
        "users": 0,  # TODO: получать из БД асинхронно
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
    print("📱 Версия: 6.0 (PostgreSQL + Global Config)")
    print("⏰ Часовой пояс: Московское время (GMT+3)")
    print("📊 Система ролей: голосовая активность")
    print("📝 Логирование: все события")
    print(f"📱 Telegram: {'✅ ПОДКЛЮЧЕН (команды: /stats, /top, /roles, /help)' if telegram.enabled else '❌ НЕ НАСТРОЕН'}")
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
