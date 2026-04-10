import asyncio
import logging
import os
import io
import re
from datetime import datetime, timedelta
from typing import Optional
from contextlib import suppress

from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, CommandObject, or_f
from aiogram.types import Message, BotCommand, BotCommandScopeAllPrivateChats
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
from flask import Flask
import threading

import aiosqlite
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# Flask для Render
app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Бот работает! Активен в группах."

# Загрузка конфига
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден!")

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Бот и диспетчер
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

DB_PATH = "stats.db"

# ----------------------------------------------------------------------
# База данных
# ----------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id INTEGER, chat_id INTEGER,
                username TEXT, first_name TEXT,
                msg_count INTEGER DEFAULT 0,
                last_msg_date TIMESTAMP,
                warn_count INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS msg_daily (
                user_id INTEGER, chat_id INTEGER,
                date DATE, count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, chat_id, date)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                warn_limit INTEGER DEFAULT 3
            )
        """)
        await db.commit()

# ----------------------------------------------------------------------
# Права доступа
# ----------------------------------------------------------------------
async def has_moder_rights(user_id: int, chat_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]
    except:
        return False

# ----------------------------------------------------------------------
# Сбор сообщений (РАБОТАЕТ ВСЕГДА)
# ----------------------------------------------------------------------
@router.message(F.chat.type.in_(["group", "supergroup"]))
async def count_message(message: Message):
    if not message.from_user or message.from_user.is_bot:
        return
    
    user = message.from_user
    chat_id = message.chat.id
    now = datetime.now()
    today = now.date().isoformat()
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверка бана
        cursor = await db.execute(
            "SELECT is_banned FROM user_stats WHERE user_id = ? AND chat_id = ?", 
            (user.id, chat_id)
        )
        row = await cursor.fetchone()
        if row and row[0] == 1:
            with suppress(Exception):
                await message.delete()
            return
        
        # Обновление статистики
        await db.execute("""
            INSERT INTO user_stats (user_id, chat_id, username, first_name, msg_count, last_msg_date)
            VALUES (?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id, chat_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            msg_count = msg_count + 1,
            last_msg_date = excluded.last_msg_date
        """, (user.id, chat_id, user.username, user.first_name, now))
        
        await db.execute("""
            INSERT INTO msg_daily (user_id, chat_id, date, count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id, chat_id, date) DO UPDATE SET
            count = count + 1
        """, (user.id, chat_id, today))
        
        await db.commit()

# ----------------------------------------------------------------------
# Команды (реагируют и на / и на текст)
# ----------------------------------------------------------------------
@router.message(or_f(Command("profile"), F.text.lower().in_(["мой профиль", "профиль"])))
async def cmd_profile(message: Message):
    if message.chat.type == "private":
        await message.answer("📊 Эта команда работает только в группах!")
        return
    
    user = message.from_user
    chat_id = message.chat.id
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT msg_count, warn_count, last_msg_date 
            FROM user_stats WHERE user_id = ? AND chat_id = ?
        """, (user.id, chat_id))
        row = await cursor.fetchone()
        
        if not row:
            await message.reply(f"👤 {user.full_name}, вы ещё не писали в этом чате!")
            return
        
        msg_count, warns, last_date = row
        
        # Статистика за сегодня
        today = datetime.now().date().isoformat()
        cursor = await db.execute("""
            SELECT count FROM msg_daily 
            WHERE user_id = ? AND chat_id = ? AND date = ?
        """, (user.id, chat_id, today))
        today_count = (await cursor.fetchone())
        today_count = today_count[0] if today_count else 0
        
        text = (
            f"📊 <b>Профиль {user.full_name}</b>\n\n"
            f"💬 Всего сообщений: {msg_count}\n"
            f"📅 Сегодня: {today_count}\n"
            f"⚠️ Предупреждений: {warns}\n"
            f"🕒 Последнее: {last_date}"
        )
        
        await message.reply(text)

@router.message(or_f(Command("top"), F.text.lower().startswith(("статистика", "стата"))))
async def cmd_top(message: Message):
    if message.chat.type == "private":
        await message.answer("📊 Эта команда работает только в группах!")
        return
    
    chat_id = message.chat.id
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT first_name, msg_count 
            FROM user_stats 
            WHERE chat_id = ? AND is_banned = 0
            ORDER BY msg_count DESC
            LIMIT 10
        """, (chat_id,))
        rows = await cursor.fetchall()
        
        if not rows:
            await message.reply("📊 Пока нет статистики. Начните общаться!")
            return
        
        text = "🏆 <b>Топ-10 болтунов чата:</b>\n\n"
        for i, (name, count) in enumerate(rows, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            text += f"{medal} {name or 'Без имени'}: {count} сообщ.\n"
        
        await message.reply(text)

@router.message(or_f(Command("warn"), F.text.lower().startswith("варн")))
async def cmd_warn(message: Message):
    if not await has_moder_rights(message.from_user.id, message.chat.id):
        await message.reply("❌ Нет прав модератора!")
        return
    
    if not message.reply_to_message:
        await message.reply("❗ Ответьте на сообщение пользователя!")
        return
    
    target = message.reply_to_message.from_user
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Лимит варнов
        cursor = await db.execute("SELECT warn_limit FROM chat_settings WHERE chat_id = ?", (message.chat.id,))
        row = await cursor.fetchone()
        warn_limit = row[0] if row else 3
        
        # Добавляем варн
        await db.execute("""
            UPDATE user_stats SET warn_count = warn_count + 1 
            WHERE user_id = ? AND chat_id = ?
        """, (target.id, message.chat.id))
        
        cursor = await db.execute(
            "SELECT warn_count FROM user_stats WHERE user_id = ? AND chat_id = ?", 
            (target.id, message.chat.id)
        )
        row = await cursor.fetchone()
        current_warns = row[0] if row else 1
        
        await db.commit()
        
        if current_warns >= warn_limit:
            with suppress(Exception):
                await message.chat.ban(user_id=target.id)
                await db.execute(
                    "UPDATE user_stats SET is_banned = 1 WHERE user_id = ? AND chat_id = ?", 
                    (target.id, message.chat.id)
                )
                await db.commit()
            await message.reply(f"🚨 {target.full_name} забанен! ({current_warns}/{warn_limit} варнов)")
        else:
            await message.reply(f"⚠️ {target.full_name} получает варн ({current_warns}/{warn_limit})")

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я бот статистики для групп.\n\n"
        "Добавь меня в группу и дай права админа, затем используй:\n"
        "• профиль — твоя статистика\n"
        "• статистика — топ чата\n"
        "• варн (ответом) — модерация\n\n"
        "Подробнее: /help"
    )

@router.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "📋 <b>Команды бота:</b>\n\n"
        "👤 <b>Для всех:</b>\n"
        "/profile или «профиль» — ваша статистика\n"
        "/top или «статистика» — рейтинг чата\n\n"
        "🔨 <b>Для админов:</b>\n"
        "/warn или «варн» — предупреждение (ответом)\n"
        "/setwarn 5 — лимит варнов до бана\n"
    )
    await message.answer(text)

# ----------------------------------------------------------------------
# Запуск
# ----------------------------------------------------------------------
async def set_commands():
    commands = [
        BotCommand(command="profile", description="📊 Мой профиль"),
        BotCommand(command="top", description="🏆 Рейтинг чата"),
        BotCommand(command="help", description="❓ Помощь"),
    ]
    await bot.set_my_commands(commands)

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

async def main():
    await init_db()
    await set_commands()
    
    # Flask в потоке
    threading.Thread(target=run_flask, daemon=True).start()
    
    # Запуск бота
    logger.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
