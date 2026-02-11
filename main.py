import discord
from discord.ext import commands, tasks
import json
import asyncio
import datetime
import os
import sys
import aiohttp
from collections import defaultdict
import pytz
from typing import Dict, List, Optional, Tuple, Any
import threading
from flask import Flask, jsonify

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

# ==================== ХРАНИЛИЩЕ ДАННЫХ ====================
class Storage:
    def __init__(self):
        self.data_dir = './data'
        self.data_file = os.path.join(self.data_dir, 'data.json')
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.messages = defaultdict(int)
        self.voice_time = defaultdict(int)
        self.voice_sessions = {}
        self.log_channel = None
        self.log_config = {
            "voice_events": True,
            "role_events": True,
            "member_events": True,
            "channel_events": True,
            "server_events": True,
            "message_events": False,
            "command_events": True
        }
        self.load_data()

    def load_data(self):
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.messages = defaultdict(int, data.get('messages', {}))
                    self.voice_time = defaultdict(int, data.get('voice_time', {}))
                    self.log_channel = data.get('log_channel')
                    self.log_config.update(data.get('log_config', {}))
                print(f"✅ Данные загружены из {self.data_file}")
                print(f"📊 Пользователей в базе: {len(self.voice_time)}")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки данных: {e}")
            self.save_data()

    def save_data(self):
        try:
            data = {
                'messages': dict(self.messages),
                'voice_time': dict(self.voice_time),
                'log_channel': self.log_channel,
                'log_config': self.log_config,
                'last_save': datetime.datetime.now().isoformat()
            }
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"💾 Данные сохранены")
        except Exception as e:
            print(f"❌ Ошибка сохранения: {e}")

    def add_message(self, user_id: int):
        self.messages[str(user_id)] += 1
        self.save_data()

    def add_voice_time(self, user_id: int, minutes: int):
        self.voice_time[str(user_id)] += minutes
        self.save_data()
        
    def get_user_stats(self, user_id: int) -> Dict:
        uid = str(user_id)
        minutes = self.voice_time.get(uid, 0)
        return {
            'messages': self.messages.get(uid, 0),
            'voice_minutes': minutes,
            'voice_hours': minutes // 60,
            'voice_remaining_minutes': minutes % 60
        }
    
    def get_top_users(self, limit: int = 10) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
        voice_top = sorted(
            [(int(uid), minutes) for uid, minutes in self.voice_time.items()],
            key=lambda x: x[1],
            reverse=True
        )[:limit]
        
        messages_top = sorted(
            [(int(uid), count) for uid, count in self.messages.items()],
            key=lambda x: x[1],
            reverse=True
        )[:limit]
        
        return voice_top, messages_top

storage = Storage()

# ==================== TELEGRAM БОТ ====================
class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.enabled = bool(token and chat_id)
        self.session = None
    
    async def ensure_session(self):
        if self.session is None:
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
            print(f"❌ Telegram ошибка: {e}")
            return False
    
    async def send_stats(self) -> bool:
        if not self.enabled:
            return False
        
        total_users = len(storage.voice_time)
        total_messages = sum(storage.messages.values())
        total_voice_hours = sum(storage.voice_time.values()) // 60
        total_voice_minutes = sum(storage.voice_time.values()) % 60
        
        # Топ 3 пользователей
        voice_top = storage.get_top_users(3)[0]
        top_text = ""
        for i, (user_id, minutes) in enumerate(voice_top, 1):
            hours = minutes // 60
            mins = minutes % 60
            top_text += f"{i}. ID `{user_id}` - {hours}ч {mins}м\n"
        
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
            "info": "ℹ️",
            "success": "✅",
            "warning": "⚠️",
            "error": "❌",
            "critical": "🚨"
        }.get(alert_type, "📝")
        
        message = f"{emoji} *{title}*\n\n{description}\n\n⏰ {format_moscow_time()}"
        return await self.send_message(message)
    
    async def close(self):
        if self.session:
            await self.session.close()

telegram = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

# ==================== ЛОГГЕР ====================
class Logger:
    @staticmethod
    async def log_event(guild: discord.Guild, event_type: str, title: str, description: str,
                       color: int = None, fields: Dict = None, user: discord.Member = None,
                       channel: discord.abc.GuildChannel = None) -> None:
        try:
            if not storage.log_channel:
                return

            log_channel = guild.get_channel(int(storage.log_channel))
            if not log_channel:
                return

            # Проверяем конфигурацию
            config_keys = {
                "voice": "voice_events",
                "role": "role_events",
                "member": "member_events",
                "channel": "channel_events",
                "server": "server_events",
                "message": "message_events",
                "command": "command_events"
            }
            
            if event_type in config_keys and not storage.log_config.get(config_keys[event_type], True):
                return

            color_map = {
                "voice": 0x3498db,
                "role": 0x2ecc71,
                "member": 0xe67e22,
                "channel": 0x9b59b6,
                "server": 0xe74c3c,
                "command": 0x1abc9c,
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

            embed.set_footer(text=f"Время МСК")
            await log_channel.send(embed=embed)

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
            # Проверяем, есть ли уже роль из системы
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
            user_id = str(member.id)
            voice_minutes = storage.voice_time.get(user_id, 0)

            # Определяем заслуженную роль
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

            # Удаляем старые роли
            roles_to_remove = []
            for role_name in ROLES_CONFIG.keys():
                if role_name != earned_role_name:
                    old_role = discord.utils.get(member.guild.roles, name=role_name)
                    if old_role and old_role in member.roles:
                        roles_to_remove.append(old_role)
            
            if roles_to_remove:
                await member.remove_roles(*roles_to_remove, reason="Обновление роли")

            # Выдаем новую роль
            await member.add_roles(earned_role, reason=f"Голос: {voice_minutes} мин")
            print(f"✅ {member} получил роль {earned_role_name} ({voice_minutes} мин)")
            
            # Логируем
            await Logger.log_event(
                guild=member.guild,
                event_type="role",
                title="Получена новая роль",
                description=f"Пользователь {member.mention} получил роль **{earned_role_name}**",
                color=0x2ecc71,
                user=member,
                fields={"Голосовая активность": f"{voice_minutes // 60}ч {voice_minutes % 60}м"}
            )
            
            # Отправляем уведомление в Telegram
            if telegram.enabled and storage.log_config.get("telegram_notify_role", False):
                await telegram.send_alert(
                    "🎉 Новая роль",
                    f"Пользователь **{member.display_name}** получил роль **{earned_role_name}**\n\n"
                    f"🎤 Голос: {voice_minutes // 60}ч {voice_minutes % 60}м\n"
                    f"💬 Сообщений: {storage.messages.get(user_id, 0)}",
                    "success"
                )

        except Exception as e:
            print(f"❌ Ошибка обновления роли: {e}")

# ==================== ЗАДАЧИ ====================
@tasks.loop(minutes=5)
async def check_voice_time():
    """Проверка голосового времени каждые 5 минут"""
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        for user_id, session_start in list(storage.voice_sessions.items()):
            duration = (now - session_start).total_seconds() / 60
            member_id = int(user_id)
            
            for guild in bot.guilds:
                member = guild.get_member(member_id)
                if member and member.voice and member.voice.channel:
                    storage.add_voice_time(member_id, 5)
                    storage.voice_sessions[user_id] = now - datetime.timedelta(minutes=duration % 5)
                    await RoleManager.check_and_give_roles(member)
                    break
    except Exception as e:
        print(f"❌ Ошибка check_voice_time: {e}")

@tasks.loop(hours=24)
async def daily_report():
    """Ежедневный отчет в Telegram"""
    try:
        if telegram.enabled and storage.log_config.get("telegram_daily_report", True):
            await telegram.send_stats()
            print("📊 Ежедневный отчет отправлен в Telegram")
    except Exception as e:
        print(f"❌ Ошибка daily_report: {e}")

# ==================== СОБЫТИЯ DISCORD ====================
@bot.event
async def on_ready():
    print(f"✅ Бот {bot.user} запущен!")
    print(f"📊 Серверов: {len(bot.guilds)}")
    print(f"🐍 Python: {sys.version}")
    print(f"📱 Telegram: {'✅' if telegram.enabled else '❌'}")
    
    # Очищаем старые слэш-команды
    try:
        bot.tree.clear_commands(guild=None)
        await bot.tree.sync()
        for guild in bot.guilds:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
        print("🧹 Слэш-команды очищены")
    except Exception as e:
        print(f"⚠️ Ошибка очистки команд: {e}")
    
    # Запускаем задачи
    if not check_voice_time.is_running():
        check_voice_time.start()
        print("⏱️ Запущена проверка голосового времени")
    
    if telegram.enabled and not daily_report.is_running():
        daily_report.start()
        print("📊 Запущен ежедневный отчет в Telegram")
    
    # Создаем роли на всех серверах
    for guild in bot.guilds:
        print(f"\n🔍 Сервер: {guild.name}")
        for role_name in ROLES_CONFIG.keys():
            await RoleManager.ensure_role_exists(guild, role_name)
    
    # Выдаем начальные роли
    print("\n🎯 Выдача начальных ролей...")
    for guild in bot.guilds:
        members = [m for m in guild.members if not m.bot]
        print(f"   {guild.name}: {len(members)} участников")
        for member in members:
            await RoleManager.give_default_role(member)
            await asyncio.sleep(0.05)
    
    print("✅ Начальная выдача ролей завершена!")
    
    # Логируем запуск
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
    
    # Отправляем в Telegram
    if telegram.enabled:
        await telegram.send_alert(
            "🤖 Бот запущен",
            f"**{bot.user.name}** успешно запущен на Railway\n\n"
            f"🏠 Серверов: {len(bot.guilds)}\n"
            f"👥 Пользователей в базе: {len(storage.voice_time)}\n"
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
    
    # Изменение ника
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
        storage.add_message(message.author.id)
        if isinstance(message.author, discord.Member):
            await RoleManager.check_and_give_roles(message.author)
    
    await bot.process_commands(message)

@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    
    if storage.log_config.get("message_events", False):
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
    
    if storage.log_config.get("message_events", False):
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
    
    # Зашел в голосовой
    if before.channel is None and after.channel is not None:
        storage.voice_sessions[user_id] = now
        print(f"🎤 {member} зашел в {after.channel.name}")
        
        if storage.log_config.get("voice_events", True):
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
    
    # Вышел из голосового
    elif before.channel is not None and after.channel is None:
        if user_id in storage.voice_sessions:
            duration = (now - storage.voice_sessions[user_id]).total_seconds() / 60
            if duration >= 1:
                storage.add_voice_time(member.id, int(duration))
                await RoleManager.check_and_give_roles(member)
                
                if storage.log_config.get("voice_events", True):
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
                            "Общее время": f"{storage.voice_time.get(user_id, 0)} минут"
                        }
                    )
            del storage.voice_sessions[user_id]
    
    # Переход между каналами
    elif before.channel is not None and after.channel is not None and before.channel != after.channel:
        if user_id in storage.voice_sessions:
            duration = (now - storage.voice_sessions[user_id]).total_seconds() / 60
            if duration >= 1:
                storage.add_voice_time(member.id, int(duration))
            storage.voice_sessions[user_id] = now
            
            if storage.log_config.get("voice_events", True):
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
    """Создание канала"""
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
    """Удаление канала"""
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
    """Показать статистику пользователя"""
    if not member:
        member = ctx.author
    
    stats_data = storage.get_user_stats(member.id)
    
    embed = discord.Embed(
        title=f"📊 Статистика {member.display_name}",
        color=discord.Color.blue(),
        timestamp=get_moscow_time()
    )
    
    embed.add_field(
        name="🎤 Голосовая активность",
        value=f"**{stats_data['voice_hours']}ч {stats_data['voice_remaining_minutes']}м**\nВсего: {stats_data['voice_minutes']} минут",
        inline=True
    )
    
    embed.add_field(
        name="💬 Сообщений",
        value=f"**{stats_data['messages']}**",
        inline=True
    )
    
    # Текущая роль
    current_role = "Залётный"
    for role_name in reversed(ROLE_ORDER):
        if stats_data['voice_minutes'] >= ROLES_CONFIG[role_name]["voice_minutes"]:
            current_role = role_name
            break
    
    embed.add_field(
        name="👑 Текущая роль",
        value=f"**{current_role}**",
        inline=False
    )
    
    # Прогресс до следующей роли
    current_index = ROLE_ORDER.index(current_role)
    if current_index < len(ROLE_ORDER) - 1:
        next_role = ROLE_ORDER[current_index + 1]
        required = ROLES_CONFIG[next_role]["voice_minutes"]
        remaining = max(0, required - stats_data['voice_minutes'])
        progress = (stats_data['voice_minutes'] / required) * 100 if required > 0 else 0
        
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
    """Топ пользователей по активности"""
    voice_top, messages_top = storage.get_top_users(10)
    
    embed = discord.Embed(
        title="🏆 Топ активности",
        color=discord.Color.gold(),
        timestamp=get_moscow_time()
    )
    
    # Топ голоса
    voice_text = ""
    for i, (user_id, minutes) in enumerate(voice_top[:5], 1):
        user = ctx.guild.get_member(user_id)
        name = user.display_name if user else f"ID: {user_id}"
        voice_text += f"{i}. **{name}** - {minutes // 60}ч {minutes % 60}м\n"
    
    embed.add_field(
        name="🎤 Голосовая активность (Топ 5)",
        value=voice_text or "Нет данных",
        inline=False
    )
    
    # Топ сообщений
    messages_text = ""
    for i, (user_id, count) in enumerate(messages_top[:5], 1):
        user = ctx.guild.get_member(user_id)
        name = user.display_name if user else f"ID: {user_id}"
        messages_text += f"{i}. **{name}** - {count} сообщ.\n"
    
    embed.add_field(
        name="💬 Сообщения (Топ 5)",
        value=messages_text or "Нет данных",
        inline=False
    )
    
    embed.set_footer(text=f"Всего в базе: {len(storage.voice_time)} пользователей • Время МСК")
    
    await ctx.send(embed=embed)

@bot.command(name="логи")
@commands.has_permissions(administrator=True)
async def logs(ctx, channel: discord.TextChannel = None):
    """Управление системой логирования"""
    
    if channel:
        storage.log_channel = str(channel.id)
        storage.save_data()
        
        embed = discord.Embed(
            title="✅ Лог-канал установлен",
            description=f"Лог-канал: {channel.mention}",
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
            channel=channel
        )
    else:
        embed = discord.Embed(
            title="📝 Управление логированием",
            color=discord.Color.purple(),
            timestamp=get_moscow_time()
        )
        
        if storage.log_channel:
            ch = ctx.guild.get_channel(int(storage.log_channel))
            if ch:
                embed.add_field(name="✅ Лог-канал", value=f"{ch.mention}", inline=False)
            else:
                embed.add_field(name="⚠️ Лог-канал не найден", value=f"ID: {storage.log_channel}", inline=False)
        else:
            embed.add_field(name="❌ Лог-канал не установлен", value="Используйте `!логи #канал`", inline=False)
        
        config_text = ""
        for key, value in storage.log_config.items():
            if not key.startswith("telegram"):
                config_text += f"• **{key.replace('_', ' ').title()}:** {'✅' if value else '❌'}\n"
        
        embed.add_field(name="⚙️ Конфигурация", value=config_text, inline=False)
        embed.set_footer(text="Используйте !настройки_логов для детальной настройки")
        await ctx.send(embed=embed)

@bot.command(name="тест_лога")
@commands.has_permissions(administrator=True)
async def test_log(ctx):
    """Тестирование системы логирования"""
    if not storage.log_channel:
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
    
    if not event_type:
        embed = discord.Embed(
            title="⚙️ Настройки логирования",
            color=discord.Color.blue(),
            timestamp=get_moscow_time()
        )
        
        config_text = ""
        for key, value in storage.log_config.items():
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
    
    if event_type not in storage.log_config:
        await ctx.send(f"❌ Неизвестный тип события: {event_type}")
        return
    
    if not status or status.lower() not in ['on', 'off']:
        await ctx.send(f"❌ Укажите on или off")
        return
    
    storage.log_config[event_type] = (status.lower() == 'on')
    storage.save_data()
    
    await ctx.send(f"✅ {event_type} теперь {'включен' if storage.log_config[event_type] else 'выключен'}")

@bot.command(name="telegram")
@commands.has_permissions(administrator=True)
async def telegram_cmd(ctx, action: str = None):
    """Управление Telegram уведомлениями"""
    
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
        
        # Добавляем настройки если их нет
        if "telegram_notify_role" not in storage.log_config:
            storage.log_config["telegram_notify_role"] = False
        if "telegram_daily_report" not in storage.log_config:
            storage.log_config["telegram_daily_report"] = True
        storage.save_data()
        
        embed.add_field(
            name="Статус",
            value=f"✅ Подключен к чату ID: `{TELEGRAM_CHAT_ID}`",
            inline=False
        )
        
        embed.add_field(
            name="Настройки",
            value=f"• Уведомления о ролях: {'✅' if storage.log_config.get('telegram_notify_role', False) else '❌'}\n"
                  f"• Ежедневный отчет: {'✅' if storage.log_config.get('telegram_daily_report', True) else '❌'}",
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
        storage.log_config["telegram_notify_role"] = True
        storage.save_data()
        await ctx.send("✅ Уведомления о новых ролях **включены**")
        
    elif action == "off":
        storage.log_config["telegram_notify_role"] = False
        storage.save_data()
        await ctx.send("❌ Уведомления о новых ролях **выключены**")
        
    elif action == "daily":
        current = storage.log_config.get("telegram_daily_report", True)
        storage.log_config["telegram_daily_report"] = not current
        storage.save_data()
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
    """Очищает старые слэш-команды"""
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
    """Показать список команд"""
    
    embed = discord.Embed(
        title="📚 Команды бота",
        description=f"Префикс: `{bot.command_prefix}`",
        color=discord.Color.green(),
        timestamp=get_moscow_time()
    )
    
    embed.add_field(
        name="👤 **Для всех**",
        value="""
`!статистика` - ваша статистика
`!статистика @пользователь` - статистика пользователя
`!топ` - топ пользователей
`!помощь` - это сообщение
        """,
        inline=False
    )
    
    embed.add_field(
        name="👑 **Для администраторов**",
        value="""
`!логи` - статус лог-канала
`!логи #канал` - установить канал для логов
`!тест_лога` - тест системы логирования
`!настройки_логов` - показать настройки
`!настройки_логов [тип] [on/off]` - изменить настройки
`!telegram` - управление Telegram уведомлениями
`!очистить_команды` - удалить старые слэш-команды
        """,
        inline=False
    )
    
    embed.add_field(
        name="⚙️ **Типы событий**",
        value="""
`voice_events` - голосовая активность
`role_events` - события ролей
`member_events` - вход/выход участников
`channel_events` - создание/удаление каналов
`server_events` - изменения сервера
`message_events` - удаление/редактирование сообщений
`command_events` - использование команд
        """,
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
        "users": len(storage.voice_time),
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
    print("📱 Версия: 5.0 (ПОЛНЫЙ ФУНКЦИОНАЛ)")
    print("⏰ Часовой пояс: Московское время (GMT+3)")
    print("📊 Система ролей: голосовая активность")
    print("📝 Логирование: все события")
    print(f"📱 Telegram: {'✅ ПОДКЛЮЧЕН' if telegram.enabled else '❌ НЕ НАСТРОЕН'}")
    print("=" * 60)
    
    # Запускаем Flask
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("🌐 Веб-сервер запущен")
    
    # Запускаем бота
    try:
        bot.run(TOKEN)
    except KeyboardInterrupt:
        print("🛑 Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")
    finally:
        # Закрываем соединения
        asyncio.run(telegram.close())
