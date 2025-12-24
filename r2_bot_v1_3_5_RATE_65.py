
# ================== R2 BUYER BOT — ALL IN ONE (FULL & FIXED) ==================
# aiogram 3.x | FULL PRODUCTION | SINGLE FILE
# ============================================================================

import asyncio
import time
import logging
import traceback
import aiosqlite

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

# ================== CONFIG ==================

import os

BOT_TOKEN = "8451331821:AAFQxOAyCFGSKhkNeW8ULa9mDjOkCI_vcfw"
ADMIN_ID = 6216901670
GROUP_ID = -5010059640

DB = "database.db"

SERVER_RATES = {
    "R2 Rise": {
        "UAH": 65,
        "USDT": 1.4
    }
}

RATE_UAH = SERVER_RATES["R2 Rise"]["UAH"]
RATE_USDT = SERVER_RATES["R2 Rise"]["USDT"]

BAN_SECONDS = 15 * 60
TIMER_SECONDS = 10 * 60
SPAM_DELAY = 1.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ================== TEXTS ==================

TEXT = {
    "menu_title": (
        "🏦 <b>R2 Silver Trade</b>\n\n"
        "Официальный сервис безопасных сделок серебра R2 Online.\n"
        "Все операции проходят под контролем администратора."
    ),
    "rate": (
        "💱 <b>Актуальный курс (сервер: R2 Rise)</b>\n\n"
        "💴 Гривны: <b>{uah} грн</b> за 1кк\n"
        "💵 USDT: <b>{usdt} USDT</b> за 1кк\n\n"
        "Курс может изменяться. Уточняйте перед подтверждением сделки."
    ),
    "about": (
        "ℹ️ <b>О сервисе</b>\n\n"
        "Данный бот предназначен для оформления сделок по продаже серебра "
        "в игре R2 Online.\n\n"
        "🔐 Все сделки сопровождаются администратором.\n"
        "🎮 Передача серебра и оплата осуществляются исключительно в игре.\n"
        "⚠️ Бот не принимает платежи."
    ),
    "deal_sent": (
        "🆕 <b>Заявка принята</b>\n\n"
        "Администратор получил вашу заявку и в ближайшее время "
        "назначит время сделки.\n\n"
        "Пожалуйста, ожидайте."
    ),
    "enter_number_error": "⚠️ Пожалуйста, введите корректное числовое значение.",
    "deal_finished": (
        "🎉 <b>Сделка успешно завершена</b>\n\n"
        "Благодарим за использование сервиса."
    ),
}

# ================== DATABASE ==================

async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                banned_until INTEGER DEFAULT 0
            )"""
        )

        await db.execute(
            """CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                target TEXT,
                created_at INTEGER
            )"""
        )

        await db.execute(
            """CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                currency TEXT,
                bank TEXT,
                initials TEXT,
                usdt_net TEXT,
                amount_kk INTEGER,
                deal_time TEXT,
                nick TEXT,
                status TEXT,
                created_at INTEGER
            )"""
        )

        # ---- migration: timer_until ----
        cur = await db.execute("PRAGMA table_info(deals)")
        cols = [row[1] for row in await cur.fetchall()]
        if "timer_until" not in cols:
            await db.execute("ALTER TABLE deals ADD COLUMN timer_until INTEGER")

        # ---- migration: usdt_net ----
        cur = await db.execute("PRAGMA table_info(deals)")
        cols = [row[1] for row in await cur.fetchall()]
        if "usdt_net" not in cols:
            await db.execute("ALTER TABLE deals ADD COLUMN usdt_net TEXT")

        await db.commit()

async def db_exec(sql, params=(), fetch=False, one=False):
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(sql, params)
        await conn.commit()
        if fetch:
            return await (cur.fetchone() if one else cur.fetchall())

async def is_banned(uid):
    r = await db_exec("SELECT banned_until FROM users WHERE user_id=?", (uid,), True, True)
    return r and r[0] > int(time.time())

async def has_active(uid):
    # active only if amount_kk IS NOT NULL and status active
    r = await db_exec(
        "SELECT id FROM deals WHERE user_id=? AND status IN ('new','time_set','time_confirmed','nick_set','buyer_created','paid')",
        (uid,), True, True
    )
    return bool(r)


async def log_admin(admin_id, action, target):
    await db_exec(
        "INSERT INTO admin_logs (admin_id,action,target,created_at) VALUES (?,?,?,?)",
        (admin_id, action, target, int(time.time()))
    )

# ================== KEYBOARDS ==================

def reply_kb(*texts):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t)] for t in texts],
        resize_keyboard=True
    )

def inline_kb(pairs):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=t, callback_data=d)] for t, d in pairs]
    )

# ================== HELPERS ==================

def kk_fmt(k):
    if k is None:
        return "—"
    return f"{k}кк ({k*1_000_000:,}".replace(",", ".") + ")"

def sum_fmt(cur, k):
    return f"{k*RATE_UAH} грн" if cur == "UAH" else f"{k*RATE_USDT:.2f} USDT"

_last_action = {}

async def anti_spam(uid):
    now = time.time()
    if uid in _last_action and now - _last_action[uid] < SPAM_DELAY:
        return False
    _last_action[uid] = now
    return True

# ================== FSM ==================

class DealFSM(StatesGroup):
    server = State()
    currency = State()
    bank = State()
    initials = State()
    usdt_net = State()
    amount = State()
    preview = State()
    admin_time = State()
    admin_nick = State()

# ================== BOT INIT ==================

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())


@dp.message(F.text == "🛒 R2 Рынок")
async def r2_market(msg: Message):
    await msg.answer(
        "Раздел находится в разработке.",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )
    return


# ================== MENU ==================

@dp.message(F.text == "/start")
@dp.message(F.text == "⬅ ГЛАВНОЕ МЕНЮ")
async def menu(msg: Message):
    buttons = [
            "💱 ПРОВЕРИТЬ КУРС",
            "📂 МОИ АКТИВНЫЕ СДЕЛКИ",
            "📜 ИСТОРИЯ",
            "🛒 R2 Рынок"
        ]
    if msg.from_user.id == ADMIN_ID:
        buttons = ["🛠 АДМИН ПАНЕЛЬ"] + buttons
    else:
        buttons = ["🟢 ОСТАВИТЬ ЗАЯВКУ"] + buttons + ["ℹ️ О БОТЕ"]
    if msg.from_user.id == ADMIN_ID:
        buttons.insert(4, "🧹 ОЧИСТИТЬ АКТИВНЫЕ ЗАКАЗЫ")
    await msg.answer(
        TEXT['menu_title'],
        reply_markup=reply_kb(*buttons)
    )

# ================== STATIC ==================

@dp.message(F.text == "💱 ПРОВЕРИТЬ КУРС")
async def rate(msg: Message):
    await msg.answer(
        TEXT['rate'].format(uah=RATE_UAH, usdt=RATE_USDT),
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )

@dp.message(F.text == "ℹ️ О БОТЕ")
async def about(msg: Message):
    await msg.answer(
        "🤖 <b>О БОТЕ</b>\n\n"
        "Бот предназначен для создания сделок.\n"
        "Все оплаты и подтверждения проходят <b>ТОЛЬКО В ИГРЕ</b>.",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )

# ================== ACTIVE DEALS ==================

@dp.message(F.text == "📂 МОИ АКТИВНЫЕ СДЕЛКИ")
async def my_active(msg: Message):
    rows = await db_exec(
        "SELECT id,status,amount_kk,currency FROM deals "
        "WHERE user_id=? AND status IN ('new','time_set','time_confirmed','nick_set','buyer_created','paid')",
        (msg.from_user.id,), True
    )
    if not rows:
        return await msg.answer("📂 У вас нет активных сделок.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    for did, st, kk, cur in rows:
        await msg.answer(
            f"📂 <b>Сделка #{did}</b>\n"
            f"📦 {kk_fmt(kk)}\n"
            f"📌 Статус: <i>{st}</i>",
            reply_markup=inline_kb([
                ("❌ ОТМЕНИТЬ СДЕЛКУ", f"user_cancel:{did}")
            ])
        )

# ================== HISTORY ==================

@dp.message(F.text == "📜 ИСТОРИЯ")
async def history(msg: Message):
    if msg.from_user.id == ADMIN_ID:
        rows = await db_exec(
            "SELECT id,user_id,amount_kk,currency FROM deals "
            "WHERE status='done' ORDER BY id DESC LIMIT 10",
            fetch=True
        )
        if not rows:
            return await msg.answer("📜 История пуста.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
        text = "📜 <b>История заказов (ADMIN)</b>\n\n"
        for did, uid, kk, cur in rows:
            text += f"• #{did} | UID {uid} | {kk_fmt(kk)} | {sum_fmt(cur,kk)}\n"
    else:
        rows = await db_exec(
            "SELECT id,amount_kk,currency FROM deals "
            "WHERE user_id=? AND status='done' ORDER BY id DESC LIMIT 10",
            (msg.from_user.id,), True
        )
        if not rows:
            return await msg.answer("📜 История пуста.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
        text = "📜 <b>История сделок</b>\n\n"
        for did, kk, cur in rows:
            text += f"• #{did} — {kk_fmt(kk)} — {sum_fmt(cur,kk)}\n"
    await msg.answer(text, reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))


@dp.message(DealFSM.server)
async def choose_server(msg: Message, state: FSMContext):
    if msg.text != "R2 Rise":
        return await msg.answer("Пожалуйста, выберите сервер кнопкой ниже.")
    await state.update_data(server="R2 Rise")
    await msg.answer(
        "Выберите валюту:",
        reply_markup=reply_kb("💴 ГРН", "💵 USDT", "⬅ ГЛАВНОЕ МЕНЮ")
    )
    await state.set_state(DealFSM.currency)

# ================== DEAL CREATION ==================

@dp.message(F.text == "🟢 ОСТАВИТЬ ЗАЯВКУ")
async def create(msg: Message, state: FSMContext):
    if await is_banned(msg.from_user.id):
        r = await db_exec("SELECT banned_until FROM users WHERE user_id=?", (msg.from_user.id,), True, True)
        if r:
            left = max(0, r[0] - int(time.time()))
            mins = left // 60
            return await msg.answer(
                f"⛔ Вы заблокированы. Осталось: {mins} мин.",
                reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
            )
        return await msg.answer("⛔ Временный бан.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    if await has_active(msg.from_user.id):
        return await msg.answer("⚠️ У вас уже есть активная сделка.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    await state.clear()
    await msg.answer(
        "Выберите пожалуйста сервер для продолжения создания заявки:",
        reply_markup=reply_kb("R2 Rise", "⬅ ГЛАВНОЕ МЕНЮ")
    )
    await state.set_state(DealFSM.server)

@dp.message(DealFSM.currency)
async def choose_currency(msg: Message, state: FSMContext):
    if msg.text == "💴 ГРН":
        await state.update_data(currency="UAH")
        await msg.answer("Выберите банк:", reply_markup=reply_kb("Приват24", "Монобанк", "Другие", "⬅ ГЛАВНОЕ МЕНЮ"))
        await state.set_state(DealFSM.bank)
    elif msg.text == "💵 USDT":
        await state.update_data(currency="USDT")
        await msg.answer("Выберите сеть:", reply_markup=reply_kb("Binance ID", "BEP20", "TRC20", "⬅ ГЛАВНОЕ МЕНЮ"))
        await state.set_state(DealFSM.usdt_net)

@dp.message(DealFSM.bank)
async def choose_bank(msg: Message, state: FSMContext):
    await state.update_data(bank=msg.text)
    await msg.answer("Введите инициалы:", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    await state.set_state(DealFSM.initials)

@dp.message(DealFSM.initials)
async def initials(msg: Message, state: FSMContext):
    await state.update_data(initials=msg.text)
    await msg.answer("Введите количество (кк):", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    await state.set_state(DealFSM.amount)

@dp.message(DealFSM.usdt_net)
async def choose_net(msg: Message, state: FSMContext):
    await state.update_data(usdt_net=msg.text)
    await msg.answer("Введите количество (кк):", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    await state.set_state(DealFSM.amount)


@dp.message(DealFSM.amount)
async def amount(msg: Message, state: FSMContext):
    try:
        k = int(msg.text)
    except:
        return await msg.answer(TEXT["enter_number_error"])
    data = await state.get_data()

    if data.get("currency") == "UAH" and k < 10:
        return await msg.answer("Минимум 10кк.")
    if data.get("usdt_net") == "BEP20" and k < 10:
        return await msg.answer("Минимум 10кк.")
    if data.get("usdt_net") == "TRC20" and k < 50:
        return await msg.answer("Минимум 50кк.")

    await state.update_data(amount=k)

    preview_text = (
        "📝 <b>Предосмотр заявки</b>\n\n"
        f"💱 Валюта: <b>{data.get('currency')}</b>\n"
        f"🏦 Банк / сеть: <b>{data.get('bank') or data.get('usdt_net')}</b>\n"
        f"✍️ Инициалы: <b>{data.get('initials')}</b>\n"
        f"📦 Количество: <b>{kk_fmt(k)}</b>\n"
        f"💰 Сумма: <b>{sum_fmt(data.get('currency'), k)}</b>\n\n"
        "Проверьте данные заявки."
    )

    await msg.answer(
        preview_text,
        reply_markup=inline_kb([
            ("✅ ПОДТВЕРЖДАЮ", "deal_confirm"),
            ("🔄 ВЕРНУТЬСЯ В НАЧАЛО", "deal_restart")
        ])
    )
    await state.set_state(DealFSM.preview)


# ================== ADMIN / BUYER CHAIN ==================
# (same as production version, unchanged)

@dp.callback_query(F.data.startswith("time:"))
async def admin_time(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await cb.message.edit_reply_markup(None)
    await state.update_data(deal_id=int(cb.data.split(":")[1]))
    await cb.message.answer("Введите время сделки (HH:MM):", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    await state.set_state(DealFSM.admin_time)

@dp.message(DealFSM.admin_time)
async def save_time(msg: Message, state: FSMContext):
    deal_id = (await state.get_data())["deal_id"]
    await db_exec("UPDATE deals SET deal_time=?, status=? WHERE id=?", (msg.text, "time_set", deal_id))
    uid = (await db_exec("SELECT user_id FROM deals WHERE id=?", (deal_id,), True, True))[0]
    await bot.send_message(
        uid,
        f"⏱ Время сделки: <b>{msg.text}</b>",
        reply_markup=inline_kb([
            ("✅ ПОДТВЕРДИТЬ", f"confirm:{deal_id}"),
            ("❌ ОТМЕНИТЬ", f"user_cancel:{deal_id}")
        ])
    )
    await state.clear()

@dp.callback_query(F.data.startswith("confirm:"))
async def confirm_time(cb: CallbackQuery):
    deal_id = int(cb.data.split(":")[1])
    await cb.answer()
    await cb.message.edit_reply_markup(None)
    await db_exec("UPDATE deals SET status=? WHERE id=?", ("time_confirmed", deal_id))
    await bot.send_message(
        ADMIN_ID,
        f"⏱ Время подтверждено по сделке #{deal_id}",
        reply_markup=inline_kb([("✏️ ВВЕСТИ НИК", f"nick:{deal_id}")])
    )

@dp.callback_query(F.data.startswith("nick:"))
async def ask_nick(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await cb.message.edit_reply_markup(None)
    await state.update_data(deal_id=int(cb.data.split(":")[1]))
    await cb.message.answer("Введите ник покупателя:", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    await state.set_state(DealFSM.admin_nick)

@dp.message(DealFSM.admin_nick)
async def save_nick(msg: Message, state: FSMContext):
    deal_id = (await state.get_data())["deal_id"]
    await db_exec(
        "UPDATE deals SET nick=?, status=?, timer_until=? WHERE id=?",
        (msg.text, "nick_set", int(time.time()) + TIMER_SECONDS, deal_id)
    )
    uid = (await db_exec("SELECT user_id FROM deals WHERE id=?", (deal_id,), True, True))[0]
    await bot.send_message(
        uid,
        f"👤 Ник для сделки: <b>{msg.text}</b>\nСоздайте сделку в игре.",
        reply_markup=inline_kb([("🟢 СДЕЛКУ СОЗДАЛ", f"created:{deal_id}")])
    )
    await state.clear()

@dp.callback_query(F.data.startswith("created:"))
async def buyer_created(cb: CallbackQuery):
    deal_id = int(cb.data.split(":")[1])
    await cb.answer()
    await cb.message.edit_reply_markup(None)
    await db_exec("UPDATE deals SET status=? WHERE id=?", ("buyer_created", deal_id))
    await bot.send_message(
        ADMIN_ID,
        f"💸 Сделка #{deal_id}: покупатель создал сделку",
        reply_markup=inline_kb([("💰 ОПЛАТИЛ", f"paid:{deal_id}")])
    )

@dp.callback_query(F.data.startswith("paid:"))
async def admin_paid(cb: CallbackQuery):
    deal_id = int(cb.data.split(":")[1])
    await cb.answer()
    await cb.message.edit_reply_markup(None)
    await db_exec("UPDATE deals SET status=? WHERE id=?", ("paid", deal_id))
    uid = (await db_exec("SELECT user_id FROM deals WHERE id=?", (deal_id,), True, True))[0]
    await bot.send_message(
        uid,
        "💸 Средства переведены. Подтвердите получение.",
        reply_markup=inline_kb([("✅ СДЕЛКУ ПОДТВЕРДИЛ", f"user_confirm:{deal_id}")])
    )

@dp.callback_query(F.data.startswith("user_confirm:"))
async def buyer_confirm(cb: CallbackQuery):
    deal_id = int(cb.data.split(":")[1])
    await cb.answer()
    await cb.message.edit_reply_markup(None)
    await db_exec("UPDATE deals SET status=? WHERE id=?", ("buyer_confirmed", deal_id))
    await bot.send_message(
        ADMIN_ID,
        f"✅ Покупатель подтвердил сделку #{deal_id}",
        reply_markup=inline_kb([("🏁 ЗАВЕРШИТЬ СДЕЛКУ", f"finish:{deal_id}")])
    )

@dp.callback_query(F.data.startswith("finish:"))
async def finish(cb: CallbackQuery):
    deal_id = int(cb.data.split(":")[1])
    await cb.answer()
    await cb.message.edit_reply_markup(None)
    await db_exec("UPDATE deals SET status=? WHERE id=?", ("done", deal_id))
    uid = (await db_exec("SELECT user_id FROM deals WHERE id=?", (deal_id,), True, True))[0]
    await bot.send_message(uid, TEXT['deal_finished'])
    await bot.send_message(ADMIN_ID, f"🎉 Сделка #{deal_id} завершена.")
    await log_admin(cb.from_user.id, "finish_deal", str(deal_id))


@dp.message(F.text == "🧹 ОЧИСТИТЬ АКТИВНЫЕ ЗАКАЗЫ")
async def admin_clear_active(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return

    rows = await db_exec(
        "SELECT DISTINCT user_id FROM deals "
        "WHERE status IN ('new','time_set','time_confirmed','nick_set','buyer_created','paid')",
        fetch=True
    )

    if not rows:
        return await msg.answer(
            "🧹 Активных сделок нет.",
            reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
        )

    count = 0
    for (uid,) in rows:
        await db_exec(
            "UPDATE deals SET status='cancelled' "
            "WHERE user_id=? AND status IN ('new','time_set','time_confirmed','nick_set','buyer_created','paid')",
            (uid,)
        )
        await db_exec(
            "INSERT OR IGNORE INTO users (user_id,banned_until) VALUES (?,0)",
            (uid,)
        )
        count += 1

    await msg.answer(
        f"🧹 <b>Очистка завершена</b>\n"
        f"Пользователей очищено: <b>{count}</b>",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )



# ================== CANCEL DEAL ==================

@dp.callback_query(F.data.startswith("user_cancel:"))
async def user_cancel(cb: CallbackQuery):
    deal_id = int(cb.data.split(":")[1])
    uid = cb.from_user.id
    await cb.answer()
    deal = await db_exec(
        "SELECT user_id,status FROM deals WHERE id=?",
        (deal_id,), True, True
    )
    if not deal:
        return
    if deal[0] != uid:
        return
    if deal[1] == "done":
        return
    await db_exec("UPDATE deals SET status='cancelled' WHERE id=?", (deal_id,))
    await cb.message.edit_reply_markup(None)
    await bot.send_message(uid, f"❌ Сделка #{deal_id} отменена.")
    await bot.send_message(ADMIN_ID, f"❌ Пользователь отменил сделку #{deal_id}.")

@dp.callback_query(F.data.startswith("cancel:"))
async def admin_cancel(cb: CallbackQuery):
    deal_id = int(cb.data.split(":")[1])
    await cb.answer()
    await db_exec("UPDATE deals SET status='cancelled' WHERE id=?", (deal_id,))
    await cb.message.edit_reply_markup(None)
    uid = (await db_exec("SELECT user_id FROM deals WHERE id=?", (deal_id,), True, True))[0]
    await bot.send_message(uid, f"❌ Администратор отменил сделку #{deal_id}.")
    await log_admin(cb.from_user.id, "cancel_deal", str(deal_id))



# ================== ADMIN PANEL ==================

@dp.message(F.text == "🛠 АДМИН ПАНЕЛЬ")
async def admin_panel(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer(
        "🛠 <b>АДМИН ПАНЕЛЬ</b>",
        reply_markup=reply_kb(
            "📊 СТАТИСТИКА",
            "📂 АКТИВНЫЕ СДЕЛКИ",
            "🚫 ЗАБАНЕННЫЕ",
            "📜 ЛОГИ СДЕЛОК",
            "⚠️ ФЛУДЕРЫ",
            "⛔ ЗАБАНИТЬ",
            "♻️ РАЗБАНИТЬ",
            "⬅ ГЛАВНОЕ МЕНЮ"
        )
    )

@dp.message(F.text == "📊 СТАТИСТИКА")
async def admin_stats(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    total = (await db_exec("SELECT COUNT(*) FROM deals", fetch=True, one=True))[0]
    done = (await db_exec("SELECT COUNT(*) FROM deals WHERE status='done'", fetch=True, one=True))[0]
    active = (await db_exec(
        "SELECT COUNT(*) FROM deals WHERE status IN ('new','time_set','time_confirmed','nick_set','buyer_created','paid')",
        fetch=True, one=True
    ))[0]
    await msg.answer(
        "📊 <b>Статистика</b>\n"
        f"Всего сделок: <b>{total}</b>\n"
        f"Активных: <b>{active}</b>\n"
        f"Завершённых: <b>{done}</b>",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )

@dp.message(F.text == "📂 АКТИВНЫЕ СДЕЛКИ")
async def admin_active(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    rows = await db_exec(
        "SELECT id,user_id,amount_kk,currency,status FROM deals "
        "WHERE status IN ('new','time_set','time_confirmed','nick_set','buyer_created','paid') "
        "ORDER BY id DESC LIMIT 20",
        fetch=True
    )
    if not rows:
        return await msg.answer("📂 Активных сделок нет.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    for did, uid, kk, cur, st in rows:
        await msg.answer(
            f"📂 <b>#{did}</b> | UID {uid}\n"
            f"📦 {kk_fmt(kk)} | {sum_fmt(cur,kk)}\n"
            f"📌 <i>{st}</i>",
            reply_markup=inline_kb([
                ("❌ ОТМЕНИТЬ", f"cancel:{did}"), ("⛔ ЗАБАНИТЬ", f"admin_ban:{uid}")
            ])
        )

@dp.message(F.text == "🚫 ЗАБАНЕННЫЕ")
async def admin_banned(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    rows = await db_exec(
        "SELECT user_id,banned_until FROM users WHERE banned_until>?",
        (int(time.time()),),
        fetch=True
    )
    if not rows:
        return await msg.answer("🚫 Забаненных нет.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    text = "🚫 <b>Забаненные пользователи</b>\n\n"
    now = int(time.time())
    for uid, until in rows:
        mins = (until - now) // 60
        text += f"• UID {uid} — {mins} мин.\n"
    await msg.answer(text, reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))




# ================== ADMIN BAN / UNBAN ==================

class AdminBanFSM(StatesGroup):
    ban_uid = State()
    ban_minutes = State()
    unban_uid = State()

@dp.message(F.text == "⛔ ЗАБАНИТЬ")
async def admin_ban_start(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer(
        "Введите UID пользователя для бана:",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )
    await state.set_state(AdminBanFSM.ban_uid)

@dp.message(AdminBanFSM.ban_uid)
async def admin_ban_uid(msg: Message, state: FSMContext):
    if not msg.text.isdigit():
        await msg.answer("UID должен быть числом.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
        return
    await state.update_data(uid=int(msg.text))
    await msg.answer(
        "Введите время бана в минутах:",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )
    await state.set_state(AdminBanFSM.ban_minutes)

@dp.message(AdminBanFSM.ban_minutes)
async def admin_ban_minutes(msg: Message, state: FSMContext):
    if not msg.text.isdigit():
        await msg.answer("Минуты должны быть числом.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
        return
    data = await state.get_data()
    uid = data["uid"]
    minutes = int(msg.text)
    until = int(time.time()) + minutes * 60

    await db_exec(
        "INSERT OR REPLACE INTO users (user_id,banned_until) VALUES (?,?)",
        (uid, until)
    )

    await msg.answer(
        f"⛔ Пользователь {uid} забанен на {minutes} минут.",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )
    await log_admin(msg.from_user.id, "ban_user", str(uid))
    try:
        await bot.send_message(uid, "⛔ Вы были заблокированы администратором.")
    except:
        pass
    await state.clear()

@dp.message(F.text == "♻️ РАЗБАНИТЬ")
async def admin_unban_start(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer(
        "Введите UID пользователя для разбана:",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )
    await state.set_state(AdminBanFSM.unban_uid)

@dp.message(AdminBanFSM.unban_uid)
async def admin_unban_uid(msg: Message, state: FSMContext):
    if not msg.text.isdigit():
        await msg.answer("UID должен быть числом.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
        return
    uid = int(msg.text)

    await db_exec(
        "INSERT OR REPLACE INTO users (user_id,banned_until) VALUES (?,0)",
        (uid,)
    )

    await msg.answer(
        f"♻️ Пользователь {uid} разбанен.",
        reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ")
    )
    await log_admin(msg.from_user.id, "unban_user", str(uid))
    try:
        await bot.send_message(uid, "♻️ Вы были разблокированы администратором.")
    except:
        pass
    await state.clear()


# ================== ADMIN DEAL LOGS ==================

@dp.message(F.text == "📜 ЛОГИ СДЕЛОК")
async def admin_deal_logs(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    rows = await db_exec(
        "SELECT id,user_id,amount_kk,currency,status,created_at FROM deals "
        "ORDER BY id DESC LIMIT 20",
        fetch=True
    )
    if not rows:
        return await msg.answer("📜 Логов нет.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    text = "📜 <b>Последние сделки</b>\n\n"
    for did, uid, kk, cur, st, ts in rows:
        t = time.strftime("%d.%m %H:%M", time.localtime(ts))
        text += f"#{did} | UID {uid} | {kk_fmt(kk)} | {st} | {t}\n"
    await msg.answer(text, reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))

@dp.message(F.text == "⚠️ ФЛУДЕРЫ")
async def admin_flooders(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    rows = await db_exec(
        "SELECT user_id, COUNT(*) as c FROM deals "
        "WHERE status='cancelled' "
        "GROUP BY user_id HAVING c>=3 "
        "ORDER BY c DESC LIMIT 10",
        fetch=True
    )
    if not rows:
        return await msg.answer("⚠️ Флудеров не найдено.", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    text = "⚠️ <b>Потенциальные флудеры</b>\n\n"
    for uid, c in rows:
        await msg.answer(
            f"UID {uid} — отмен: {c}",
            reply_markup=inline_kb([
                ("⛔ ЗАБАНИТЬ", f"admin_ban:{uid}")
            ])
        )
    await msg.answer("⬅ Возврат в меню", reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))



# ================== INLINE ADMIN BAN ==================

@dp.callback_query(F.data.startswith("admin_ban:"))
async def inline_admin_ban(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("Нет доступа", show_alert=True)
        return
    uid = int(cb.data.split(":")[1])
    until = int(time.time()) + 60 * 60
    await db_exec(
        "INSERT OR REPLACE INTO users (user_id,banned_until) VALUES (?,?)",
        (uid, until)
    )
    await log_admin(cb.from_user.id, "inline_ban", str(uid))
    try:
        await bot.send_message(uid, "⛔ Вы были заблокированы администратором.")
    except:
        pass
    await cb.answer("Пользователь забанен на 60 минут", show_alert=True)


# ================== TIMER ==================


async def timer_watcher():
    while True:
        now = int(time.time())
        rows = await db_exec(
            "SELECT id,user_id FROM deals WHERE status='nick_set' AND timer_until<?",
            (now,), True
        )
        for deal_id, uid in rows:
            await db_exec("UPDATE deals SET status='cancelled' WHERE id=?", (deal_id,))
            await db_exec(
                "INSERT OR REPLACE INTO users (user_id,banned_until) VALUES (?,?)",
                (uid, now + BAN_SECONDS)
            )
            await bot.send_message(uid, "⏱ Время истекло. Сделка отменена.")
            await bot.send_message(ADMIN_ID, f"⛔ Сделка #{deal_id} отменена по таймеру.")
        await asyncio.sleep(5)

# ================== ERRORS ==================

@dp.errors()
async def errors_handler(event, exception: Exception):
    logging.error("Exception: %s", exception)
    traceback.print_exc()
    return True


# ================== PREVIEW FIX OVERRIDES ==================

@dp.callback_query(F.data == "deal_restart")
async def deal_restart_fix(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    await state.clear()
    await cb.message.answer(
        "🔄 Создание заявки начато заново.",
        reply_markup=reply_kb("🟢 ОСТАВИТЬ ЗАЯВКУ", "⬅ ГЛАВНОЕ МЕНЮ")
    )

@dp.callback_query(F.data == "deal_confirm")
async def deal_confirm_fix(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except:
        pass

    data = await state.get_data()

    await db_exec(
        "INSERT INTO deals (user_id,currency,bank,initials,usdt_net,amount_kk,status,created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            cb.from_user.id,
            data.get("currency"),
            data.get("bank"),
            data.get("initials"),
            data.get("usdt_net"),
            data.get("amount"),
            "new",
            int(time.time())
        )
    )

    deal_id = (await db_exec("SELECT MAX(id) FROM deals", fetch=True, one=True))[0]

    await bot.send_message(
        GROUP_ID,
        f"🆕 <b>Заявка #{deal_id}</b>\n"
        f"👤 UID: {cb.from_user.id}\n"
        f"📦 {kk_fmt(data.get('amount'))}\n"
        f"💵 {sum_fmt(data.get('currency'), data.get('amount'))}",
        reply_markup=inline_kb([
            ("⏱ УКАЗАТЬ ВРЕМЯ", f"time:{deal_id}"),
            ("❌ ОТМЕНИТЬ", f"cancel:{deal_id}")
        ])
    )

    await cb.message.answer(TEXT["deal_sent"], reply_markup=reply_kb("⬅ ГЛАВНОЕ МЕНЮ"))
    await state.clear()



# ================== GLOBAL RECOVERY BUTTON ==================

@dp.message(F.text.in_({"/menu", "МЕНЮ", "🏠 МЕНЮ"}))
async def force_menu(msg: Message):
    """
    Глобальная аварийная точка возврата меню.
    Работает всегда, даже если FSM сломан или клавиатура пропала.
    """
    try:
        await dp.storage.clear(key=msg.from_user.id)
    except:
        pass
    await menu(msg)

# Автовосстановление меню при любом неизвестном сообщении
@dp.message()
async def fallback_recover(msg: Message):
    """
    Если пользователь потерял кнопки — возвращаем главное меню.
    """
    await menu(msg)






# ================== MAIN ==================

async def main():
    await init_db()
    asyncio.create_task(timer_watcher())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())


# ================== VERSION ==================
# VERSION: 1.3.5-RATE-65
# CHANGELOG:
# 1.3.5
# ~ Updated UAH rate to 65 for R2 Rise
#

# 1.3.4
# ! Fixed R2 Market opening text (development message only)
# ! Prevented menu override on market open
# ~ Based on 1.3.2 logic
#

# 1.3.3
# + Added 'R2 Рынок' section to main menu
# + Added development description stub
# ~ Based strictly on 1.3.2
#

# 1.3.2
# + Currency rates are now server-bound (R2 Rise)
# + Prepared structure for multi-server rates
# ~ Monolith preserved
#

# 1.3.1
# + Added server selection step (R2 Rise) before deal creation
# ~ No architecture changes
#

# + Added monolith extension scaffold (registries, hooks, feature flags)
# + Added internal metrics placeholders for future patches
# ~ No architecture changes (single-file preserved)

# ================== MONOLITH EXTENSION CORE ==================

FEATURE_FLAGS = {
    "metrics": False,
    "audit": False,
    "future_roles": False,
}

MONOLITH_REGISTRY = {
    "hooks": {},       # event_name -> [callables]
    "services": {},    # name -> object
    "metrics": {},     # key -> int/float
}

def register_hook(event: str, func):
    MONOLITH_REGISTRY.setdefault("hooks", {}).setdefault(event, []).append(func)

async def emit_hook(event: str, *args, **kwargs):
    for fn in MONOLITH_REGISTRY.get("hooks", {}).get(event, []):
        try:
            res = fn(*args, **kwargs)
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            logging.error("Hook error %s: %s", event, e)

def metric_inc(key: str, value: int = 1):
    MONOLITH_REGISTRY.setdefault("metrics", {})[key] = MONOLITH_REGISTRY["metrics"].get(key, 0) + value

# Example future hook points (not wired yet):
# await emit_hook("deal_created", deal_id=deal_id)
# metric_inc("deals_created")

