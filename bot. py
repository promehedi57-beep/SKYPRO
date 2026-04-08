import telebot
from telebot import types
import json
import os
import time
import logging
import random
from datetime import datetime

# --- ⚙️ ADVANCED CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - 🤖 SKY BOT - %(levelname)s - %(message)s')

API_TOKEN = '8647348457:AAEegJoC7xixJ5-ZHh1p1eYrZ675u1JXP0E'  # আপনার বটের টোকেন দিন
MAIN_ADMIN_ID = 7689218221          # মেইন এডমিনের আইডি দিন

bot = telebot.TeleBot(API_TOKEN, parse_mode='HTML')
DB_FILE = 'sky_pro_database.json'

admin_sessions = {} # Temporary dictionary to store file number counts and real numbers

# --- 💾 DATABASE MANAGER CLASS (PRO LEVEL) - UPDATED FOR REAL NUMBERS ---
class SkyDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.data = self.load()

    def load(self):
        if not os.path.exists(self.db_path):
            default_data = {
                "admins": [MAIN_ADMIN_ID],
                "banned_users": [],
                "users": {},
                "services": {
                    "📱 Telegram": {"Bangladesh": {"price": 0.50, "stock": 150, "number_list": []}, 
                                   "USA": {"price": 1.20, "stock": 50, "number_list": []}},
                    "💬 WhatsApp": {"India": {"price": 0.30, "stock": 200, "number_list": []},
                                   "Brazil": {"price": 0.80, "stock": 0, "number_list": []}},
                    "🎮 FreeFire / Games": {"Global": {"price": 0.15, "stock": 500, "number_list": []}}
                },
                "settings": {
                    "bot_status": True,
                    "maintenance_msg": "⚠️ <b>The bot is currently down for maintenance.</b>\nPlease try again later.",
                    "welcome_msg": "Welcome to SKY SMS PRO!",
                    "currency": "$",
                    "req_channel": "" 
                }
            }
            with open(self.db_path, 'w', encoding='utf-8') as f:
                json.dump(default_data, f, indent=4)
            return default_data
        
        with open(self.db_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if "req_channel" not in data.get("settings", {}):
                data["settings"]["req_channel"] = ""
            # Ensure number_list exists for old data
            for svc in data.get("services", {}).values():
                for c in svc.values():
                    if "number_list" not in c:
                        c["number_list"] = []
            return data

    def save(self):
        with open(self.db_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=4)

    def get_user(self, user_id):
        uid = str(user_id)
        if uid not in self.data["users"]:
            self.data["users"][uid] = {
                "balance": 0.0,
                "total_spent": 0.0,
                "numbers_bought": 0,
                "join_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "active_numbers": None # Added for tracking change number
            }
            self.save()
        return self.data["users"][uid]

db = SkyDatabase(DB_FILE)

# --- 🎨 ADVANCED UI KEYBOARDS (UPGRADED FONTS & LAYOUT) ---
def sky_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn_get_num = types.KeyboardButton("🚀 Get Number")
    btn_profile = types.KeyboardButton("👤 My Profile")
    btn_wallet = types.KeyboardButton("💳 My Balance") 
    btn_history = types.KeyboardButton("📜 Transaction History")
    btn_support = types.KeyboardButton("📞 Help & Support")
    
    markup.add(btn_get_num)
    markup.add(btn_profile, btn_wallet)
    markup.add(btn_history, btn_support)
    
    if user_id in db.data["admins"]:
        markup.add(types.KeyboardButton("👑 Admin Panel"))
    return markup

def inline_back_btn(callback_data="home"):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=callback_data))
    return markup

def get_admin_panel_markup():
    markup = types.InlineKeyboardMarkup(row_width=2)
    # New Advanced Layout with Single Number Delete
    markup.add(
        types.InlineKeyboardButton("📁 Add File (TXT)", callback_data="adm_add_country"), 
        types.InlineKeyboardButton("🗑️ Delete Target Number", callback_data="adm_del_single_num") # NEW FEATURE
    )
    markup.add(
        types.InlineKeyboardButton("➕ Add Category", callback_data="adm_add_service"),
        types.InlineKeyboardButton("⚙️ Manage Categories", callback_data="adm_manage_num")
    )
    markup.add(
        types.InlineKeyboardButton("📢 Global Broadcast", callback_data="adm_broadcast"),
        types.InlineKeyboardButton("💸 Withdrawals", callback_data="adm_withdraw")
    )
    markup.add(
        types.InlineKeyboardButton("🛠️ Core Settings", callback_data="adm_settings"),
        types.InlineKeyboardButton("👥 Admins List", callback_data="adm_admin_list")
    )
    markup.add(
        types.InlineKeyboardButton("🔒 Force Channel", callback_data="adm_req_chan"),
        types.InlineKeyboardButton("❌ Close UI", callback_data="close_ui")
    )
    return markup

# --- 🛡️ MIDDLEWARE & CHECKS ---
def check_ban_and_maintenance(message):
    uid = message.from_user.id
    if uid in db.data.get("banned_users", []):
        bot.send_message(message.chat.id, "🚫 <b>You are banned from using SKY SMS PRO.</b>")
        return False
    if not db.data["settings"]["bot_status"] and uid not in db.data["admins"]:
        bot.send_message(message.chat.id, db.data["settings"]["maintenance_msg"])
        return False
    
    req_chan = db.data["settings"].get("req_channel", "")
    if req_chan and uid not in db.data["admins"]:
        try:
            status = bot.get_chat_member(req_chan, uid).status
            if status in ['left', 'kicked']:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{req_chan.replace('@', '')}"))
                bot.send_message(message.chat.id, f"⚠️ <b>Please join our channel to use the bot!</b>", reply_markup=markup)
                return False
        except Exception:
            pass 
            
    return True

# --- 🚀 START COMMAND ---
@bot.message_handler(commands=['start'])
def start_cmd(message):
    if not check_ban_and_maintenance(message): return
    
    user = message.from_user
    user_data = db.get_user(user.id)
    
    welcome_text = (
        f"👋 <b>{db.data['settings']['welcome_msg']}</b> ✨\n"
        f"<i>The best and fastest OTP service</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Name:</b> {user.first_name}\n"
        f"🆔 <b>Account ID:</b> <code>{user.id}</code>\n"
        f"💰 <b>Current Balance:</b> {db.data['settings']['currency']}<b>{user_data['balance']:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <i>To get a number, click on <b>'🚀 Get Number'</b> below 👇</i>"
    )
    bot.send_message(message.chat.id, welcome_text, reply_markup=sky_main_menu(user.id))

# --- 👤 PROFILE SYSTEM ---
@bot.message_handler(func=lambda m: m.text == "👤 My Profile")
def user_profile(message):
    if not check_ban_and_maintenance(message): return
    uid = message.from_user.id
    udata = db.get_user(uid)
    
    rank = "👑 VIP Member" if udata["total_spent"] > 10 else "👤 Regular User"
    
    profile_msg = (
        f"👤 <b>Your Profile Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📛 <b>Name:</b> {message.from_user.first_name}\n"
        f"🆔 <b>User ID:</b> <code>{uid}</code>\n"
        f"🌟 <b>Status:</b> {rank}\n\n"
        f"💰 <b>Balance:</b> {db.data['settings']['currency']}<b>{udata['balance']:.2f}</b>\n"
        f"🔢 <b>Numbers Bought:</b> {udata['numbers_bought']} pcs\n"
        f"📅 <b>Join Date:</b> {udata['join_date'][:10]}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    bot.send_message(message.chat.id, profile_msg)

# --- 📊 STATUS (STOCK) SYSTEM ---
@bot.message_handler(func=lambda m: m.text == "💳 My Balance")
def status_system(message):
    if not check_ban_and_maintenance(message): return
    
    msg = "📊 <b>Current Stock Status</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    has_stock = False
    
    for svc_name, countries in db.data["services"].items():
        if countries:
            msg += f"📦 <b>{svc_name}:</b>\n"
            for c_name, details in countries.items():
                msg += f"  🌍 ➔ {c_name}: <b>{details['stock']} pcs</b>\n"
                has_stock = True
            msg += "\n"
            
    if not has_stock:
        msg += "<i>Sorry, no services are in stock.</i>"
        
    bot.send_message(message.chat.id, msg)

# --- 📜 HISTORY SYSTEM ---
@bot.message_handler(func=lambda m: m.text == "📜 Transaction History")
def history_system(message):
    if not check_ban_and_maintenance(message): return
    uid = message.from_user.id
    udata = db.get_user(uid)
    
    msg = (
        "📜 <b>Your Transaction History</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 <b>Total Numbers Bought:</b> {udata['numbers_bought']} pcs\n"
        f"💳 <b>Current Balance:</b> {db.data['settings']['currency']}{udata['balance']:.2f}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "<i>*Contact the support team for details.</i>"
    )
    bot.send_message(message.chat.id, msg)

# --- 📞 SUPPORT SYSTEM ---
@bot.message_handler(func=lambda m: m.text == "📞 Help & Support")
def support_system(message):
    if not check_ban_and_maintenance(message): return
    
    msg = (
        "📞 <b>SKY 24/7 SUPPORT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "For any issues, payments, or support, click the links below to message:\n\n"
        "👨‍💻 <b>Main Admin:</b> @FBSKYSUPPORT\n"
        "💬 <b>Official Support:</b> @ONLYALLSUPPORT"
    )
    bot.send_message(message.chat.id, msg)

# --- 🛒 GET NUMBER ---
@bot.message_handler(func=lambda m: m.text == "🚀 Get Number")
def buy_number_categories(message):
    if not check_ban_and_maintenance(message): return
    
    services = db.data["services"]
    if not services:
        bot.send_message(message.chat.id, "⚠️ <b>Sorry! No services are currently available.</b>")
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    for s_name in services.keys():
        markup.add(types.InlineKeyboardButton(f"📦 {s_name}", callback_data=f"sky_svc_{s_name}"))
        
    bot.send_message(message.chat.id, "🛒 <b>Select Service:</b>\n<i>Choose your desired service below:</i>", reply_markup=markup)

# --- 👑 SKY MASTER ADMIN PANEL ---
@bot.message_handler(func=lambda m: m.text == "👑 Admin Panel")
def admin_panel(message):
    if message.from_user.id not in db.data["admins"]: return
    
    markup = get_admin_panel_markup()
    bot.send_message(message.chat.id, "👑 <b>Main Admin Panel</b>\n<i>Select an advanced option below:</i>", reply_markup=markup)

# --- 🔄 CALLBACK QUERY HANDLER ---
@bot.callback_query_handler(func=lambda call: True)
def handle_sky_callbacks(call):
    uid = str(call.from_user.id)
    
    try:
        if call.data == "close_ui":
            bot.delete_message(call.message.chat.id, call.message.message_id)
            
        elif call.data == "back_to_admin":
            markup = get_admin_panel_markup()
            bot.edit_message_text("👑 <b>Main Admin Panel</b>", call.message.chat.id, call.message.message_id, reply_markup=markup)

        elif call.data == "req_withdraw":
            bot.answer_callback_query(call.id, "⚠️ This feature is currently disabled!", show_alert=True)

        elif call.data.startswith("sky_svc_"):
            svc_name = call.data.replace("sky_svc_", "")
            countries = db.data["services"].get(svc_name, {})
            
            if not countries:
                bot.answer_callback_query(call.id, "⚠️ There are no countries in this service!", show_alert=True)
                return

            markup = types.InlineKeyboardMarkup(row_width=2)
            for c_name, details in countries.items():
                stock_emoji = "✅" if details['stock'] > 0 else "❌ Out of Stock"
                markup.add(types.InlineKeyboardButton(
                    f"{c_name} ({details['stock']} pcs) {stock_emoji}", 
                    callback_data=f"sky_buy_{svc_name}_{c_name}"
                ))
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back_to_main_svc"))
            bot.edit_message_text(f"📦 <b>Service: {svc_name}</b>\n<i>Select Country:</i>", call.message.chat.id, call.message.message_id, reply_markup=markup)
            
        elif call.data == "back_to_main_svc":
            markup = types.InlineKeyboardMarkup(row_width=2)
            for s_name in db.data["services"].keys():
                markup.add(types.InlineKeyboardButton(f"📦 {s_name}", callback_data=f"sky_svc_{s_name}"))
            bot.edit_message_text("🛒 <b>Select Service:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup)
            
        elif call.data.startswith("sky_cp_"):
             num = call.data.replace("sky_cp_", "")
             bot.answer_callback_query(call.id, "✅ Copied successfully!", show_alert=False)
             bot.send_message(call.message.chat.id, f"📱 <b>Your number is given below:</b>\n<code>{num}</code>")

        elif call.data.startswith("sky_buy_") or call.data == "sky_change_num":
            if call.data == "sky_change_num":
                active = db.data["users"][uid].get("active_numbers")
                if not active:
                    bot.answer_callback_query(call.id, "⚠️ No active numbers!", show_alert=True)
                    return
                svc_name = active["svc"]
                c_name = active["c"]
                price = active["price"]
                
            else:
                parts = call.data.split("_")
                svc_name = parts[2]
                c_name = parts[3]
                price = float(db.data["services"][svc_name][c_name]["price"])

            item = db.data["services"][svc_name][c_name]
            
            if item["stock"] < 3:
                bot.answer_callback_query(call.id, "⚠️ Insufficient stock! (Inform Admin)", show_alert=True)
                return
            
            if call.data != "sky_change_num":
                db.data["users"][uid]["numbers_bought"] += 1
            
            db.data["services"][svc_name][c_name]["stock"] -= 3
            
            given_numbers = []
            if "number_list" in item and len(item["number_list"]) >= 3:
                for _ in range(3):
                    given_numbers.append(item["number_list"].pop(0))
            else:
                given_numbers = ["+1 800-SKY-PRO1", "+1 800-SKY-PRO2", "+1 800-SKY-PRO3"]
                
            if "active_numbers" not in db.data["users"][uid]:
                db.data["users"][uid]["active_numbers"] = {}
                
            db.data["users"][uid]["active_numbers"] = {
                "svc": svc_name,
                "c": c_name,
                "nums": given_numbers,
                "price": price
            }
                
            db.save()
            
            if call.data == "sky_change_num":
                bot.answer_callback_query(call.id, "✅ Number changed!")
            else:
                bot.answer_callback_query(call.id, "✅ Number Generated!")
            
            markup = types.InlineKeyboardMarkup()
            
            for num in given_numbers:
                markup.add(types.InlineKeyboardButton(f"📱 {num}", copy_text=types.CopyTextButton(text=num)))
            
            btn_get_otp = types.InlineKeyboardButton("💬 Get OTP", url="https://t.me/SKYOTPPRO1")
            btn_change_num = types.InlineKeyboardButton("🔄 Change Number", callback_data="sky_change_num")
            markup.row(btn_get_otp, btn_change_num)
            
            btn_change_country = types.InlineKeyboardButton("🌍 Change Country", callback_data=f"sky_svc_{svc_name}")
            markup.row(btn_change_country)
            
            msg = (
                f"✅ <b>Number purchased successfully!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🌍 <b>Country:</b> {c_name}\n\n"
                f"<i>* Copy the number and paste it in the app\nand click 'Get OTP' to receive the code *</i>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"⏳ <b>Time left:</b> 20 minutes\n"
            )
            bot.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup)

        elif call.data.startswith("cancel_sms_"):
            parts = call.data.split("_")
            svc_name = parts[3]
            c_name = parts[4]
            
            active = db.data["users"][uid].get("active_numbers")
            returned_nums = []
            if active and active["svc"] == svc_name and active["c"] == c_name:
                returned_nums = active["nums"]
                db.data["users"][uid]["active_numbers"] = None
            
            if svc_name in db.data["services"] and c_name in db.data["services"][svc_name]:
                db.data["services"][svc_name][c_name]["stock"] += 3
                if returned_nums:
                    db.data["services"][svc_name][c_name]["number_list"].extend(returned_nums)
                
            db.save()
            bot.edit_message_text(f"✅ <b>Number canceled!</b>\nThe balance has been refunded to your account", call.message.chat.id, call.message.message_id)

        # --- NEW: TARGET NUMBER DELETION SYSTEM ---
        elif call.data == "adm_del_single_num":
            msg = bot.send_message(call.message.chat.id, "🗑️ <b>Delete Target Number</b>\n<i>Send the number you want to delete (e.g., +123456789):</i>", reply_markup=inline_back_btn("back_to_admin"))
            bot.register_next_step_handler(msg, delete_single_number_step)

        elif call.data == "adm_manage_num":
            markup = types.InlineKeyboardMarkup(row_width=2)
            for s_name in db.data["services"].keys():
                markup.add(types.InlineKeyboardButton(f"🗑️ {s_name}", callback_data=f"adm_delsvc_{s_name}"))
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back_to_admin"))
            bot.edit_message_text("⚙️ <b>Manage Categories</b>\nClick on any category below to delete it:", call.message.chat.id, call.message.message_id, reply_markup=markup)

        elif call.data.startswith("adm_delsvc_"):
            svc = call.data.replace("adm_delsvc_", "")
            if svc in db.data["services"]:
                del db.data["services"][svc]
                db.save()
                bot.answer_callback_query(call.id, f"✅ {svc} Deleted!", show_alert=True)
                handle_sky_callbacks(call) 

        elif call.data == "adm_withdraw":
            bot.answer_callback_query(call.id, "⚠️ Withdrawal is manual. Check user payment requests and process them.", show_alert=True)

        elif call.data == "adm_admin_list":
            admin_text = "\n".join([f"👤 <code>{a}</code>" for a in db.data["admins"]])
            msg = f"👥 <b>Current Admins:</b>\n\n{admin_text}"
            
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("➕ Add Admin", callback_data="adm_add_admin"),
                types.InlineKeyboardButton("➖ Remove Admin", callback_data="adm_rem_admin")
            )
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back_to_admin"))
            
            bot.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup)

        elif call.data == "adm_add_admin":
            msg = bot.send_message(call.message.chat.id, "➕ <b>Send the Telegram User ID of the new admin:</b>", reply_markup=inline_back_btn("adm_admin_list"))
            bot.register_next_step_handler(msg, add_admin_step)

        elif call.data == "adm_rem_admin":
            msg = bot.send_message(call.message.chat.id, "➖ <b>Send the Telegram User ID of the admin to remove:</b>", reply_markup=inline_back_btn("adm_admin_list"))
            bot.register_next_step_handler(msg, rem_admin_step)

        elif call.data == "adm_req_chan":
            current_chan = db.data["settings"].get("req_channel", "Not Set")
            msg = bot.send_message(call.message.chat.id, f"🔒 <b>Required Channel</b>\nCurrent: {current_chan}\n\n<i>Send the channel username (e.g., @SkySmsPro) or type 'off' to disable:</i>", reply_markup=inline_back_btn("back_to_admin"))
            bot.register_next_step_handler(msg, set_req_channel_step)

        elif call.data == "adm_settings":
            bot_power = "🟢 ON" if db.data["settings"]["bot_status"] else "🔴 OFF"
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("📊 System Stats", callback_data="adm_stats"),
                types.InlineKeyboardButton(f"⚙️ Bot Power ({bot_power})", callback_data="adm_power")
            )
            markup.add(types.InlineKeyboardButton("💰 Manage Balance", callback_data="adm_balance_mgr"))
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back_to_admin"))
            bot.edit_message_text("🛠️ <b>Advanced Settings Engine</b>", call.message.chat.id, call.message.message_id, reply_markup=markup)

        elif call.data == "adm_stats":
            total_users = len(db.data["users"])
            msg = (f"📊 <b>SKY SYSTEM STATS</b>\n👥 <b>Total Users:</b> {total_users}\n🤖 <b>Bot Status:</b> {'🟢 ON' if db.data['settings']['bot_status'] else '🔴 OFF'}")
            bot.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=inline_back_btn("adm_settings"))

        elif call.data == "adm_broadcast":
            msg = bot.send_message(call.message.chat.id, "📢 <b>Global Broadcast:</b>\n<i>Type and send the message:</i>", reply_markup=inline_back_btn("back_to_admin"))
            bot.register_next_step_handler(msg, sky_broadcast_system)

        elif call.data == "adm_power":
            db.data["settings"]["bot_status"] = not db.data["settings"]["bot_status"]
            db.save()
            call.data = "adm_settings" 
            handle_sky_callbacks(call)

        elif call.data == "adm_balance_mgr":
            msg = bot.send_message(call.message.chat.id, "💰 <b>Balance Manager</b>\n<i>Type User ID and Amount separated by a space\ne.g., 123456789 5.50</i>", reply_markup=inline_back_btn("adm_settings"))
            bot.register_next_step_handler(msg, add_balance_step)

        elif call.data == "adm_add_service":
            msg = bot.send_message(call.message.chat.id, "➕ <b>Add Category</b>\n<i>Enter the new service name (e.g., 👻 Snapchat)</i>", reply_markup=inline_back_btn("back_to_admin"))
            bot.register_next_step_handler(msg, add_service_step)

        elif call.data == "adm_add_country":
            msg = bot.send_message(call.message.chat.id, "📁 <b>Send Numbers File (.txt)</b>\n<i>Send your numbers .txt file here (one number per line):</i>", reply_markup=inline_back_btn("back_to_admin"))
            bot.register_next_step_handler(msg, process_numbers_file)
            
        elif call.data.startswith("adm_file_svc_"):
            svc_name = call.data.replace("adm_file_svc_", "")
            msg = bot.send_message(call.message.chat.id, f"📦 <b>Service Selected: {svc_name}</b>\n\n🌍 <b>Enter new Country Name (with emoji):</b>\n<i>e.g., 🇧🇩 Bangladesh</i>", reply_markup=inline_back_btn("back_to_admin"))
            bot.register_next_step_handler(msg, ask_country_step, svc_name)
            
        elif call.data == "adm_file_new":
            msg = bot.send_message(call.message.chat.id, "➕ <b>New Category Name</b>\n<i>Enter the new category name (e.g., 👻 Snapchat)</i>", reply_markup=inline_back_btn("back_to_admin"))
            bot.register_next_step_handler(msg, create_svc_from_file_step)

    except Exception as e:
        logging.error(f"Callback Error: {e}")
        bot.answer_callback_query(call.id, "⚠️ System Error Occurred.")

# --- 🗑️ NEW TARGET DELETE FUNCTION ---
def delete_single_number_step(message):
    if message.text == "🔙 Go Back": return
    target_num = message.text.strip()
    deleted_count = 0

    for svc, countries in db.data["services"].items():
        for c_name, details in countries.items():
            if "number_list" in details and target_num in details["number_list"]:
                while target_num in details["number_list"]:
                    details["number_list"].remove(target_num)
                    details["stock"] -= 1
                    deleted_count += 1

    if deleted_count > 0:
        db.save()
        bot.send_message(message.chat.id, f"✅ <b>Successfully deleted!</b>\n🎯 Number: <code>{target_num}</code>\nRemoved from {deleted_count} places")
    else:
        bot.send_message(message.chat.id, f"⚠️ <b>Number not found in database:</b>\n<code>{target_num}</code>\n\n<i>Please verify if the number is correct.</i>")

# --- 📢 ADVANCED BROADCAST & GENERAL FUNCTIONS ---
def sky_broadcast_system(message):
    if message.text == "🔙 Go Back": return
    success, failed = 0, 0
    status_msg = bot.send_message(message.chat.id, "⏳ <i>Broadcasting...</i>")
    for uid in db.data["users"].keys():
        try:
            bot.copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=message.message_id)
            success += 1
            time.sleep(0.05)
        except: failed += 1
    bot.edit_message_text(f"✅ <b>Broadcast Complete!</b>\n🟢 Success: {success}\n🔴 Failed: {failed}", message.chat.id, status_msg.message_id)

def set_req_channel_step(message):
    if message.text == "🔙 Go Back": return
    chan = "" if message.text.lower() == "off" else message.text
    db.data["settings"]["req_channel"] = chan
    db.save()
    bot.send_message(message.chat.id, f"✅ <b>Required Channel Updated!</b>\nStatus: {chan if chan else 'Disabled'}")

def add_balance_step(message):
    if message.text == "🔙 Go Back": return
    try:
        parts = message.text.split()
        target_uid = parts[0]
        amount = float(parts[1])
        if target_uid in db.data["users"]:
            db.data["users"][target_uid]["balance"] += amount
            db.save()
            bot.send_message(message.chat.id, f"✅ Success! {amount}$ added.")
            bot.send_message(target_uid, f"🎉 <b>Congratulations!</b> {amount}$ has been added to your account")
        else: bot.send_message(message.chat.id, "⚠️ User ID not found!")
    except: bot.send_message(message.chat.id, "⚠️ <b>Invalid Format!</b>")

def add_service_step(message):
    if message.text == "🔙 Go Back": return
    svc_name = message.text
    if svc_name not in db.data["services"]:
        db.data["services"][svc_name] = {}
        db.save()
        bot.send_message(message.chat.id, f"✅ <b>Service Added:</b> {svc_name}")
    else: bot.send_message(message.chat.id, "⚠️ Already exists!")

def add_admin_step(message):
    if message.text == "🔙 Go Back": return
    try:
        new_admin_id = int(message.text)
        if new_admin_id not in db.data["admins"]:
            db.data["admins"].append(new_admin_id)
            db.save()
            bot.send_message(message.chat.id, f"✅ <b>Successfully made admin:</b> <code>{new_admin_id}</code>")
        else:
            bot.send_message(message.chat.id, "⚠️ This user is already in the admin list!")
    except ValueError:
        bot.send_message(message.chat.id, "⚠️ <b>Invalid input!</b> Please provide a correct ID")

def rem_admin_step(message):
    if message.text == "🔙 Go Back": return
    try:
        rem_admin_id = int(message.text)
        if rem_admin_id == MAIN_ADMIN_ID:
            bot.send_message(message.chat.id, "⚠️ Cannot remove the Main Admin!")
            return
        if rem_admin_id in db.data["admins"]:
            db.data["admins"].remove(rem_admin_id)
            db.save()
            bot.send_message(message.chat.id, f"✅ <b>Admin removed:</b> <code>{rem_admin_id}</code>")
        else:
            bot.send_message(message.chat.id, "⚠️ This ID is not in the admin list!")
    except ValueError:
        bot.send_message(message.chat.id, "⚠️ <b>Invalid input!</b> Please provide a correct ID")

# --- FILE UPLOAD SYSTEM HANDLERS (STEP BY STEP NO API CODE) ---
def process_numbers_file(message):
    if message.text == "🔙 Go Back": return
    if not message.document or not message.document.file_name.endswith('.txt'):
        bot.send_message(message.chat.id, "⚠️ <b>Please send only .txt files!</b>")
        return
    
    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        lines = downloaded_file.decode('utf-8').strip().split('\n')
        numbers = [line.strip() for line in lines if line.strip()]
        num_count = len(numbers)
        
        if num_count == 0:
            bot.send_message(message.chat.id, "⚠️ No numbers found in the file!")
            return
            
        admin_sessions[message.from_user.id] = {"numbers": numbers, "count": num_count}
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        for s_name in db.data["services"].keys():
            markup.add(types.InlineKeyboardButton(f"📦 {s_name}", callback_data=f"adm_file_svc_{s_name}"))
        markup.add(types.InlineKeyboardButton("➕ Create New Category", callback_data="adm_file_new"))
        
        bot.send_message(message.chat.id, f"✅ <b>File read successfully!</b>\nTotal numbers found: <b>{num_count} pcs</b>\n\nNow select a service from the buttons below:", reply_markup=markup)
        
    except Exception as e:
        bot.send_message(message.chat.id, f"⚠️ <b>Error reading file:</b> {e}")

def create_svc_from_file_step(message):
    if message.text == "🔙 Go Back": return
    svc_name = message.text
    if svc_name not in db.data["services"]:
        db.data["services"][svc_name] = {}
        db.save()
    msg = bot.send_message(message.chat.id, f"✅ <b>Category Created: {svc_name}</b>\n\n🌍 <b>Enter new Country Name (with emoji):</b>\n<i>e.g., 🇧🇩 Bangladesh</i>", reply_markup=inline_back_btn("back_to_admin"))
    bot.register_next_step_handler(msg, ask_country_step, svc_name)

def ask_country_step(message, svc_name):
    if message.text == "🔙 Go Back": return
    c_name = message.text
    admin_sessions[message.from_user.id]["c_name"] = c_name
    msg = bot.send_message(message.chat.id, f"🌍 Country selected: {c_name}\n\n💲 <b>Enter price per number (in $):</b>\n<i>e.g., 0.50</i>", reply_markup=inline_back_btn("back_to_admin"))
    bot.register_next_step_handler(msg, add_price_and_save_step, svc_name)

def add_price_and_save_step(message, svc_name):
    if message.text == "🔙 Go Back": return
    try:
        price = float(message.text.strip())
        
        session_data = admin_sessions.get(message.from_user.id)
        if not session_data:
            bot.send_message(message.chat.id, "⚠️ Session expired! Please send the file again")
            return
            
        c_name = session_data["c_name"]
        num_count = session_data["count"]
        numbers_list = session_data["numbers"]
        
        if c_name in db.data["services"][svc_name]:
            db.data["services"][svc_name][c_name]["stock"] += num_count
            db.data["services"][svc_name][c_name]["price"] = price
            # Ensure list exists
            if "number_list" not in db.data["services"][svc_name][c_name]:
                 db.data["services"][svc_name][c_name]["number_list"] = []
            db.data["services"][svc_name][c_name]["number_list"].extend(numbers_list)
        else:
            db.data["services"][svc_name][c_name] = {"price": price, "stock": num_count, "number_list": numbers_list}
            
        db.save()
        del admin_sessions[message.from_user.id] # Clear session
        
        bot.send_message(message.chat.id, f"✅ <b>Numbers successfully added to stock!</b>\n\n📦 <b>Service:</b> {svc_name}\n🌍 <b>Country:</b> {c_name}\n🔢 <b>Total Numbers:</b> {num_count} pcs")
    except ValueError:
        bot.send_message(message.chat.id, "⚠️ <b>Invalid input!</b> Please enter a correct price (e.g., 0.50)")

# --- 🚀 SYSTEM BOOT ---
if __name__ == "__main__":
    logging.info("Starting SKY SMS PRO Power Engine...")
    print("====================================")
    print("   🚀 SKY SMS PRO IS NOW ONLINE!   ")
    print("====================================")
    
    try:
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except Exception as e:
        logging.critical(f"Bot crashed! Error: {e}")
