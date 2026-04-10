import asyncio
import logging
import os
import io
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
from contextlib import suppress

from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, CommandObject, or_f
from aiogram.types import Message, ChatMemberUpdated, InlineKeyboardButton, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

import aiosqlite
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Чтобы не требовалось окно вывода

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Не указан BOT_TOKEN в .env файле")

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# Путь к БД
DB_PATH = "stats.db"

# ----------------------------------------------------------------------
# 1. Работа с Базой Данных
# ----------------------------------------------------------------------
async def init_db():
    """Создание таблиц, если их нет."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица пользователей и их сообщений
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id INTEGER,
                chat_id INTEGER,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                msg_count INTEGER DEFAULT 0,
                last_msg_date TIMESTAMP,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_banned INTEGER DEFAULT 0,
                warn_count INTEGER DEFAULT 0,
                mute_until TIMESTAMP,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        
        # Таблица для истории сообщений (по дням) - для детальной статистики
        await db.execute("""
            CREATE TABLE IF NOT EXISTS msg_daily (
                user_id INTEGER,
                chat_id INTEGER,
                date DATE,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, chat_id, date)
            )
        """)
        
        # Таблица настроек чата (лимит варнов и т.д.)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                warn_limit INTEGER DEFAULT 3,
                mute_default_time INTEGER DEFAULT 60
            )
        """)
        
        # Таблица модераторов (дополнительная роль, помимо админов ТГ)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS moderators (
                user_id INTEGER,
                chat_id INTEGER,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        await db.commit()

async def get_db_connection():
    return await aiosqlite.connect(DB_PATH)

# ----------------------------------------------------------------------
# 2. Утилиты для прав доступа
# ----------------------------------------------------------------------
class UserRole:
    OWNER = "owner"  # Создатель бота (можно прописать ID в конфиге)
    ADMIN = "admin"  # Администратор чата в Telegram
    MODER = "moder"  # Пользователь, добавленный через /addmod
    USER = "user"

OWNER_ID = 123456789  # ЗАМЕНИ НА СВОЙ ID!

async def get_user_role(user_id: int, chat_id: int) -> str:
    """Определяет роль пользователя в чате."""
    if user_id == OWNER_ID:
        return UserRole.OWNER
    
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        if member.status in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]:
            return UserRole.ADMIN
    except Exception:
        pass
    
    async with await get_db_connection() as db:
        cursor = await db.execute("SELECT 1 FROM moderators WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
        if await cursor.fetchone():
            return UserRole.MODER
    
    return UserRole.USER

async def has_moder_rights(user_id: int, chat_id: int) -> bool:
    role = await get_user_role(user_id, chat_id)
    return role in [UserRole.OWNER, UserRole.ADMIN, UserRole.MODER]

# ----------------------------------------------------------------------
# 3. Обработчики сообщений (сбор статистики)
# ----------------------------------------------------------------------
@router.message(F.chat.type.in_(["group", "supergroup"]))
async def count_message(message: Message):
    """Считает каждое сообщение."""
    if not message.from_user or message.from_user.is_bot:
        return
    
    user = message.from_user
    chat_id = message.chat.id
    now = datetime.now()
    today = now.date().isoformat()
    
    async with await get_db_connection() as db:
        # Проверка бана
        cursor = await db.execute("SELECT is_banned FROM user_stats WHERE user_id = ? AND chat_id = ?", (user.id, chat_id))
        row = await cursor.fetchone()
        if row and row[0] == 1:
            with suppress(Exception):
                await message.delete()
            return
        
        # Обновление счетчика
        await db.execute("""
            INSERT INTO user_stats (user_id, chat_id, username, first_name, last_name, msg_count, last_msg_date)
            VALUES (?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id, chat_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            msg_count = msg_count + 1,
            last_msg_date = excluded.last_msg_date
        """, (user.id, chat_id, user.username, user.first_name, user.last_name, now))
        
        await db.execute("""
            INSERT INTO msg_daily (user_id, chat_id, date, count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id, chat_id, date) DO UPDATE SET
            count = count + 1
        """, (user.id, chat_id, today))
        
        await db.commit()

# ----------------------------------------------------------------------
# 4. Генерация графиков
# ----------------------------------------------------------------------
async def generate_stats_chart(chat_id: int, period: str, title: str) -> io.BytesIO:
    """Генерирует круговую диаграмму топ-10 участников."""
    now = datetime.now()
    start_date = None
    
    if period == "day":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        start_date = now - timedelta(days=now.weekday())
        start_date = start_date.replace(hour=0, minute=0, second=0)
    elif period == "month":
        start_date = now.replace(day=1, hour=0, minute=0, second=0)
    elif period == "year":
        start_date = now.replace(month=1, day=1, hour=0, minute=0, second=0)
    
    async with await get_db_connection() as db:
        if start_date:
            cursor = await db.execute("""
                SELECT u.first_name, SUM(d.count) as total
                FROM msg_daily d
                JOIN user_stats u ON d.user_id = u.user_id AND d.chat_id = u.chat_id
                WHERE d.chat_id = ? AND d.date >= ? AND u.is_banned = 0
                GROUP BY d.user_id
                ORDER BY total DESC
                LIMIT 10
            """, (chat_id, start_date.date().isoformat()))
        else:
            cursor = await db.execute("""
                SELECT first_name, msg_count as total
                FROM user_stats
                WHERE chat_id = ? AND is_banned = 0
                ORDER BY total DESC
                LIMIT 10
            """, (chat_id,))
        
        rows = await cursor.fetchall()
        
        if not rows:
            return None
            
        names = [row[0] if row[0] else f"ID:{row[1]}" for row in rows]
        counts = [row[1] for row in rows]
        
        plt.figure(figsize=(10, 6))
        plt.pie(counts, labels=names, autopct='%1.1f%%')
        plt.title(title)
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()
        return buf

# ----------------------------------------------------------------------
# 5. Команды для всех пользователей
# ----------------------------------------------------------------------
@router.message(or_f(Command("profile"), F.text.lower().in_(["мой профиль", "профиль"])))
async def cmd_profile(message: Message):
    if message.chat.type == "private":
        await message.answer("Эта команда только для групп.")
        return
    
    user = message.from_user
    chat_id = message.chat.id
    
    async with await get_db_connection() as db:
        cursor = await db.execute("""
            SELECT msg_count, warn_count, last_msg_date, first_seen 
            FROM user_stats WHERE user_id = ? AND chat_id = ?
        """, (user.id, chat_id))
        row = await cursor.fetchone()
        
        if not row:
            await message.answer("Вы еще не отправляли сообщений в этом чате.")
            return
            
        msg_count, warns, last_date, first_date = row
        
        # Статистика за день/неделю
        now = datetime.now()
        today = now.date().isoformat()
        week_ago = (now - timedelta(days=7)).date().isoformat()
        
        cursor = await db.execute("""
            SELECT SUM(count) FROM msg_daily 
            WHERE user_id = ? AND chat_id = ? AND date >= ?
        """, (user.id, chat_id, week_ago))
        week_count = (await cursor.fetchone())[0] or 0
        
        cursor = await db.execute("""
            SELECT count FROM msg_daily 
            WHERE user_id = ? AND chat_id = ? AND date = ?
        """, (user.id, chat_id, today))
        today_count = (await cursor.fetchone())[0] or 0
        
        text = (
            f"📊 <b>Профиль {user.full_name}</b>\n\n"
            f"💬 Всего сообщений: {msg_count}\n"
            f"📅 Сегодня: {today_count}\n"
            f"📆 За неделю: {week_count}\n"
            f"⚠️ Предупреждений: {warns}\n"
            f"🕒 Первое сообщение: {first_date}\n"
            f"🕒 Последнее: {last_date}"
        )
        
        # Попытка сгенерить график активности за неделю
        buf = await generate_activity_chart(user.id, chat_id)
        if buf:
            await message.answer_photo(types.BufferedInputFile(buf.read(), filename="activity.png"), caption=text)
        else:
            await message.answer(text)

async def generate_activity_chart(user_id: int, chat_id: int) -> Optional[io.BytesIO]:
    """График активности пользователя за последние 7 дней."""
    async with await get_db_connection() as db:
        cursor = await db.execute("""
            SELECT date, count FROM msg_daily 
            WHERE user_id = ? AND chat_id = ? AND date >= date('now', '-7 days')
            ORDER BY date
        """, (user_id, chat_id))
        rows = await cursor.fetchall()
        
        if not rows:
            return None
            
        dates = [row[0] for row in rows]
        counts = [row[1] for row in rows]
        
        plt.figure(figsize=(8, 4))
        plt.bar(dates, counts, color='skyblue')
        plt.xticks(rotation=45)
        plt.title("Активность за 7 дней")
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()
        return buf

@router.message(or_f(Command("top"), F.text.lower() == "статистика"))
async def cmd_top(message: Message, command: CommandObject = None):
    if message.chat.type == "private":
        await message.answer("Эта команда только для групп.")
        return
    
    # Определяем период из аргументов или текста
    period = "all"
    title = "Общая статистика чата"
    
    raw_text = message.text or message.caption or ""
    if "день" in raw_text or "day" in raw_text:
        period = "day"
        title = "Статистика за день"
    elif "недел" in raw_text or "week" in raw_text:
        period = "week"
        title = "Статистика за неделю"
    elif "месяц" in raw_text or "month" in raw_text:
        period = "month"
        title = "Статистика за месяц"
    elif "год" in raw_text or "year" in raw_text:
        period = "year"
        title = "Статистика за год"
        
    buf = await generate_stats_chart(message.chat.id, period, title)
    if buf:
        await message.answer_photo(types.BufferedInputFile(buf.read(), filename="stats.png"))
    else:
        await message.answer("Недостаточно данных для построения статистики.")

# ----------------------------------------------------------------------
# 6. Команды для Модерации
# ----------------------------------------------------------------------
class WarnStates(StatesGroup):
    waiting_for_reason = State()

@router.message(or_f(Command("warn"), F.text.lower().startswith("варн")))
async def cmd_warn(message: Message, command: CommandObject = None):
    if not await has_moder_rights(message.from_user.id, message.chat.id):
        await message.reply("❌ У вас нет прав модератора.")
        return
    
    # Получаем цель
    target = None
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    elif command and command.args:
        try:
            user_id = int(command.args.split()[0])
            target = await bot.get_chat_member(message.chat.id, user_id)
            target = target.user
        except:
            await message.reply("Неверный формат. Используйте: /warn <user_id> или ответьте на сообщение.")
            return
    else:
        await message.reply("Ответьте на сообщение пользователя или укажите ID.")
        return
    
    if target.id == message.from_user.id:
        await message.reply("Нельзя выдать варн самому себе.")
        return
        
    async with await get_db_connection() as db:
        # Получаем настройки чата
        cursor = await db.execute("SELECT warn_limit FROM chat_settings WHERE chat_id = ?", (message.chat.id,))
        row = await cursor.fetchone()
        warn_limit = row[0] if row else 3
        
        # Добавляем варн
        await db.execute("""
            UPDATE user_stats SET warn_count = warn_count + 1 
            WHERE user_id = ? AND chat_id = ?
        """, (target.id, message.chat.id))
        
        cursor = await db.execute("SELECT warn_count FROM user_stats WHERE user_id = ? AND chat_id = ?", (target.id, message.chat.id))
        row = await cursor.fetchone()
        current_warns = row[0] if row else 1
        
        await db.commit()
        
        if current_warns >= warn_limit:
            # Бан
            with suppress(Exception):
                await message.chat.ban(user_id=target.id)
                await db.execute("UPDATE user_stats SET is_banned = 1 WHERE user_id = ? AND chat_id = ?", (target.id, message.chat.id))
                await db.commit()
                await message.answer(f"🚨 {target.full_name} получил {current_warns}/{warn_limit} варнов и был забанен!")
        else:
            await message.answer(f"⚠️ {target.full_name} получил предупреждение ({current_warns}/{warn_limit}).")

@router.message(or_f(Command("mute"), F.text.lower().startswith("мут")))
async def cmd_mute(message: Message, command: CommandObject = None):
    if not await has_moder_rights(message.from_user.id, message.chat.id):
        return
    
    # Логика парсинга времени (пример: /mute 10m спам)
    # Для простоты сделаем /mute 60 (минут) по умолчанию
    duration = 60
    target = None
    
    if message.reply_to_message:
        target = message.reply_to_message.from_user
        if command and command.args:
            try:
                duration = int(command.args.split()[0])
            except:
                pass
    else:
        await message.reply("Ответьте на сообщение пользователя.")
        return
    
    until = datetime.now() + timedelta(minutes=duration)
    with suppress(Exception):
        await message.chat.restrict(user_id=target.id, permissions=types.ChatPermissions(can_send_messages=False), until_date=until)
        await message.answer(f"🔇 {target.full_name} замучен на {duration} минут.")

@router.message(or_f(Command("ban"), F.text.lower().startswith("бан")))
async def cmd_ban(message: Message):
    if not await has_moder_rights(message.from_user.id, message.chat.id):
        return
    # ... реализация бана (по аналогии с варном)

# ----------------------------------------------------------------------
# 7. Поиск неактивных
# ----------------------------------------------------------------------
@router.message(or_f(Command("inactive"), F.text.lower().startswith("кто неактивен")))
async def cmd_inactive(message: Message, command: CommandObject = None):
    if not await has_moder_rights(message.from_user.id, message.chat.id):
        await message.reply("❌ Нет прав.")
        return
    
    # Парсим дни. По умолчанию 7 дней
    days = 7
    if command and command.args:
        try:
            days = int(command.args.split()[0])
        except:
            pass
            
    threshold = datetime.now() - timedelta(days=days)
    
    async with await get_db_connection() as db:
        cursor = await db.execute("""
            SELECT user_id, first_name, last_msg_date 
            FROM user_stats 
            WHERE chat_id = ? AND is_banned = 0 AND last_msg_date < ?
            ORDER BY last_msg_date ASC
        """, (message.chat.id, threshold))
        rows = await cursor.fetchall()
        
        if not rows:
            await message.answer(f"Все активны за последние {days} дней!")
            return
            
        text = f"👻 Неактивны более {days} дней:\n\n"
        for user_id, name, last_date in rows[:20]:  # Топ-20
            text += f"• {name}: {last_date}\n"
            
        await message.answer(text)

# ----------------------------------------------------------------------
# 8. Настройка меню команд (slash commands)
# ----------------------------------------------------------------------
async def set_bot_commands():
    # Общие команды
    commands = [
        types.BotCommand(command="profile", description="📊 Мой профиль и статистика"),
        types.BotCommand(command="top", description="🏆 Рейтинг чата (за всё время)"),
    ]
    await bot.set_my_commands(commands)
    
    # Команды для админов (отобразятся в группах, но пользователи без прав увидят ошибку)
    # В aiogram 3.x можно задать scope, но для простоты просто добавим.

@router.message(Command("help"))
async def cmd_help(message: Message):
    role = await get_user_role(message.from_user.id, message.chat.id)
    text = "📋 <b>Доступные команды:</b>\n\n"
    text += "/profile - Статистика профиля\n"
    text += "/top - Общий рейтинг\n"
    
    if role in [UserRole.ADMIN, UserRole.OWNER, UserRole.MODER]:
        text += "\n🔨 <b>Модерация:</b>\n"
        text += "/warn - Выдать предупреждение\n"
        text += "/mute - Замутить\n"
        text += "/ban - Забанить\n"
        text += "/inactive [дни] - Кто неактивен\n"
    
    await message.answer(text)

# ----------------------------------------------------------------------
# 9. Запуск
# ----------------------------------------------------------------------
async def main():
    await init_db()
    await set_bot_commands()
    await dp.start_polling(bot, allowed_updates=["message", "chat_member"])

if __name__ == "__main__":
    asyncio.run(main())
