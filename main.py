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
from typing import Dict, List, Optional, Tuple
import threading
from flask import Flask, jsonify

# Конфигурация
TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
if not TOKEN:
    print("❌ ОШИБКА: Токен Discord бота не найден!")
    sys.exit(1)

# Настройки времени
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def get_moscow_time(dt=None):
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    elif dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt.astimezone(MOSCOW_TZ)

def format_moscow_time(dt=None, format_str="%d.%m.%Y %H:%M:%S"):
    return get_moscow_time(dt).strftime(format_str)

# Конфигурация ролей
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

# Создаем бота
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

# Хранилище данных
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
                print(f"✅ Данные загружены")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки: {e}")

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

# Задача для проверки голосового времени
@tasks.loop(minutes=5)
async def check_voice_time():
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
                    break
    except Exception as e:
        print(f"❌ Ошибка: {e}")

# RoleManager
class RoleManager:
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
                reason="Автосоздание роли"
            )
            return role
        except Exception as e:
            print(f"❌ Ошибка создания роли {role_name}: {e}")
            return None

    @staticmethod
    async def give_default_role(member: discord.Member):
        try:
            role = discord.utils.get(member.guild.roles, name="Залётный")
            if not role:
                role = await RoleManager.ensure_role_exists(member.guild, "Залётный")
            if role and role not in member.roles:
                await member.add_roles(role, reason="Начальная роль")
                print(f"✅ Выдана роль Залётный {member}")
        except Exception as e:
            print(f"❌ Ошибка: {e}")

    @staticmethod
    async def check_and_give_roles(member: discord.Member):
        try:
            user_id = str(member.id)
            voice_minutes = storage.voice_time.get(user_id, 0)

            earned_role_name = "Залётный"
            for role_name in reversed(ROLE_ORDER):
                if voice_minutes >= ROLES_CONFIG[role_name]["voice_minutes"]:
                    earned_role_name = role_name
                    break

            earned_role = discord.utils.get(member.guild.roles, name=earned_role_name)
            if not earned_role:
                earned_role = await RoleManager.ensure_role_exists(member.guild, earned_role_name)

            if earned_role and earned_role not in member.roles:
                roles_to_remove = []
                for role_name in ROLES_CONFIG.keys():
                    if role_name != earned_role_name:
                        old_role = discord.utils.get(member.guild.roles, name=role_name)
                        if old_role and old_role in member.roles:
                            roles_to_remove.append(old_role)
                
                if roles_to_remove:
                    await member.remove_roles(*roles_to_remove, reason="Обновление роли")

                await member.add_roles(ed_role, reason=f"Голос: {voice_minutes} мин")
                print(f"✅ Роль обновлена: {member} -> {earned_role_name}")

        except Exception as e:
            print(f"❌ Ошибка: {e}")

# События
@bot.event
async def on_ready():
    print(f"✅ Бот {bot.user} запущен!")
    print(f"📊 Серверов: {len(bot.guilds)}")
    
    # ОЧИСТКА СТАРЫХ СЛЭШ-КОМАНД
    try:
        # Очищаем глобальные команды
        bot.tree.clear_commands(guild=None)
        await bot.tree.sync()
        print("🧹 Глобальные слэш-команды очищены")
        
        # Очищаем команды на каждом сервере
        for guild in bot.guilds:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
            print(f"🧹 Слэш-команды очищены на сервере: {guild.name}")
    except Exception as e:
        print(f"⚠️ Ошибка при очистке команд: {e}")
    
    # Запускаем задачу
    if not check_voice_time.is_running():
        check_voice_time.start()
        print("⏱️ Запущена проверка голосового времени")
    
    # Создаем роли
    for guild in bot.guilds:
        for role_name in ROLES_CONFIG.keys():
            await RoleManager.ensure_role_exists(guild, role_name)
    
    # Выдаем начальные роли
    for guild in bot.guilds:
        for member in guild.members:
            if not member.bot:
                await RoleManager.give_default_role(member)

@bot.event
async def on_member_join(member: discord.Member):
    if member.bot:
        return
    await RoleManager.give_default_role(member)

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
async def on_voice_state_update(member, before, after):
    if member.bot:
        return
    user_id = str(member.id)
    now = datetime.datetime.now(datetime.timezone.utc)
    
    if before.channel is None and after.channel is not None:
        storage.voice_sessions[user_id] = now
        print(f"🎤 {member} зашел в {after.channel.name}")
    elif before.channel is not None and after.channel is None:
        if user_id in storage.voice_sessions:
            duration = (now - storage.voice_sessions[user_id]).total_seconds() / 60
            if duration >= 1:
                storage.add_voice_time(member.id, int(duration))
                await RoleManager.check_and_give_roles(member)
            del storage.voice_sessions[user_id]
    elif before.channel is not None and after.channel is not None and before.channel != after.channel:
        if user_id in storage.voice_sessions:
            duration = (now - storage.voice_sessions[user_id]).total_seconds() / 60
            if duration >= 1:
                storage.add_voice_time(member.id, int(duration))
            storage.voice_sessions[user_id] = now

# Команды
@bot.command(name="статистика")
async def stats(ctx, member: discord.Member = None):
    if not member:
        member = ctx.author
    stats = storage.get_user_stats(member.id)
    
    embed = discord.Embed(
        title=f"📊 Статистика {member.display_name}",
        color=discord.Color.blue(),
        timestamp=get_moscow_time()
    )
    embed.add_field(name="🎤 Голос", value=f"{stats['voice_hours']}ч {stats['voice_remaining_minutes']}м", inline=True)
    embed.add_field(name="💬 Сообщения", value=str(stats['messages']), inline=True)
    
    # Определяем роль
    earned_role_name = "Залётный"
    for role_name in reversed(ROLE_ORDER):
        if stats['voice_minutes'] >= ROLES_CONFIG[role_name]["voice_minutes"]:
            earned_role_name = role_name
            break
    embed.add_field(name="👑 Роль", value=f"**{earned_role_name}**", inline=False)
    
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.set_footer(text="Время МСК")
    await ctx.send(embed=embed)

@bot.command(name="топ")
async def top(ctx):
    voice_top, messages_top = storage.get_top_users(10)
    
    embed = discord.Embed(
        title="🏆 Топ активности",
        color=discord.Color.gold(),
        timestamp=get_moscow_time()
    )
    
    voice_text = ""
    for i, (uid, minutes) in enumerate(voice_top[:5], 1):
        user = ctx.guild.get_member(uid)
        name = user.display_name if user else f"ID: {uid}"
        voice_text += f"{i}. **{name}** - {minutes // 60}ч {minutes % 60}м\n"
    embed.add_field(name="🎤 Голос (Топ 5)", value=voice_text or "Нет данных", inline=False)
    
    messages_text = ""
    for i, (uid, count) in enumerate(messages_top[:5], 1):
        user = ctx.guild.get_member(uid)
        name = user.display_name if user else f"ID: {uid}"
        messages_text += f"{i}. **{name}** - {count} сообщ.\n"
    embed.add_field(name="💬 Сообщения (Топ 5)", value=messages_text or "Нет данных", inline=False)
    
    embed.set_footer(text=f"Всего: {len(storage.voice_time)} пользователей")
    await ctx.send(embed=embed)

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
    embed = discord.Embed(
        title="📚 Команды бота",
        description="Префикс: `!`",
        color=discord.Color.green(),
        timestamp=get_moscow_time()
    )
    embed.add_field(
        name="👤 Для всех",
        value="`!статистика` - ваша статистика\n`!статистика @пользователь` - статистика пользователя\n`!топ` - топ пользователей\n`!помощь` - это сообщение",
        inline=False
    )
    embed.add_field(
        name="👑 Для администраторов",
        value="`!очистить_команды` - удалить старые слэш-команды",
        inline=False
    )
    embed.set_footer(text=f"Бот: {bot.user.name}")
    await ctx.send(embed=embed)

# Flask для UptimeRobot
app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "bot": str(bot.user) if bot.user else "starting",
        "time": format_moscow_time()
    })

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

# Запуск
if __name__ == "__main__":
    print("=" * 50)
    print("🤖 Discord Voice Activity Bot")
    print("📱 Версия: 4.0 (СТАБИЛЬНАЯ)")
    print("⏰ Часовой пояс: Московское время (GMT+3)")
    print("=" * 50)
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("🌐 Веб-сервер запущен")
    
    bot.run(TOKEN)
