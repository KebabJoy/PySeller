import logging
import os
import sys
import threading

import sqlalchemy
import sqlalchemy.ext.declarative as sed
import sqlalchemy.orm
import telegram

import database
import duckbot
import localization
import nuconfig
import worker

try:
    import coloredlogs
except ImportError:
    coloredlogs = None


def main():
    
    
    threading.current_thread().name = "Core"

    
    log = logging.getLogger("core")
    logging.root.setLevel("INFO")
    log.debug("Set logging level to INFO while the config is being loaded")

    
    if not os.path.isfile("config/template_config.toml"):
        log.fatal("config/template_config.toml does not exist!")
        exit(254)

    
    if not os.path.isfile("config/config.toml"):
        log.debug("config/config.toml does not exist.")

        with open("config/template_config.toml", encoding="utf8") as template_cfg_file, \
                open("config/config.toml", "w", encoding="utf8") as user_cfg_file:
            
            user_cfg_file.write(template_cfg_file.read())

        log.fatal("A config file has been created in config/config.toml."
                  " Customize it, then restart greed!")
        exit(1)

    
    with open("config/template_config.toml", encoding="utf8") as template_cfg_file, \
            open("config/config.toml", encoding="utf8") as user_cfg_file:
        template_cfg = nuconfig.NuConfig(template_cfg_file)
        user_cfg = nuconfig.NuConfig(user_cfg_file)
        if not template_cfg.cmplog(user_cfg):
            log.fatal("There were errors while parsing the config.toml file. Please fix them and restart greed!")
            exit(2)
        else:
            log.debug("Configuration parsed successfully!")

    
    logging.root.setLevel(user_cfg["Logging"]["level"])
    stream_handler = logging.StreamHandler()
    if coloredlogs is not None:
        stream_handler.formatter = coloredlogs.ColoredFormatter(user_cfg["Logging"]["format"], style="{")
    else:
        stream_handler.formatter = logging.Formatter(user_cfg["Logging"]["format"], style="{")
    logging.root.handlers.clear()
    logging.root.addHandler(stream_handler)
    log.debug("Logging setup successfully!")

    
    logging.getLogger("telegram").setLevel("ERROR")

    
    log.debug("Creating the sqlalchemy engine...")
    engine = sqlalchemy.create_engine(user_cfg["Database"]["engine"])
    log.debug("Binding metadata to the engine...")
    database.TableDeclarativeBase.metadata.bind = engine
    log.debug("Creating all missing tables...")
    database.TableDeclarativeBase.metadata.create_all()
    log.debug("Preparing the tables through deferred reflection...")
    sed.DeferredReflection.prepare(engine)

    
    bot = duckbot.factory(user_cfg)()

    
    log.debug("Testing bot token...")
    me = bot.get_me()
    if me is None:
        logging.fatal("The token you have entered in the config file is invalid. Fix it, then restart greed.")
        sys.exit(1)
    log.debug("Bot token is valid!")

    
    default_language = user_cfg["Language"]["default_language"]
    
    default_loc = localization.Localization(language=default_language, fallback=default_language)

    
    
    chat_workers = {}

    
    next_update = None

    
    log.info(f"@{me.username} is starting!")

    
    while True:
        
        update_timeout = user_cfg["Telegram"]["long_polling_timeout"]
        log.debug(f"Getting updates from Telegram with a timeout of {update_timeout} seconds")
        updates = bot.get_updates(offset=next_update,
                                  timeout=update_timeout)
        
        for update in updates:
            
            if update.message is not None:
                
                if update.message.chat.type != "private":
                    log.debug(f"Received a message from a non-private chat: {update.message.chat.id}")
                    
                    bot.send_message(update.message.chat.id, default_loc.get("error_nonprivate_chat"))
                    
                    continue
                
                if isinstance(update.message.text, str) and update.message.text.startswith("/start"):
                    log.info(f"Received /start from: {update.message.chat.id}")
                    
                    old_worker = chat_workers.get(update.message.chat.id)
                    
                    if old_worker:
                        log.debug(f"Received request to stop {old_worker.name}")
                        old_worker.stop("request")
                    
                    new_worker = worker.Worker(bot=bot,
                                               chat=update.message.chat,
                                               telegram_user=update.message.from_user,
                                               cfg=user_cfg,
                                               engine=engine,
                                               daemon=True)
                    
                    log.debug(f"Starting {new_worker.name}")
                    new_worker.start()
                    
                    chat_workers[update.message.chat.id] = new_worker
                    
                    continue
                
                receiving_worker = chat_workers.get(update.message.chat.id)
                
                if receiving_worker is None:
                    log.debug(f"Received a message in a chat without worker: {update.message.chat.id}")
                    
                    bot.send_message(update.message.chat.id, default_loc.get("error_no_worker_for_chat"),
                                     reply_markup=telegram.ReplyKeyboardRemove())
                    
                    continue
                
                if not receiving_worker.is_ready():
                    log.debug(f"Received a message in a chat where the worker wasn't ready yet: {update.message.chat.id}")
                    
                    bot.send_message(update.message.chat.id, default_loc.get("error_worker_not_ready"),
                                     reply_markup=telegram.ReplyKeyboardRemove())
                    
                    continue
                
                if update.message.text == receiving_worker.loc.get("menu_cancel"):
                    log.debug(f"Forwarding CancelSignal to {receiving_worker}")
                    
                    receiving_worker.queue.put(worker.CancelSignal())
                else:
                    log.debug(f"Forwarding message to {receiving_worker}")
                    
                    receiving_worker.queue.put(update)
            
            if isinstance(update.callback_query, telegram.CallbackQuery):
                
                receiving_worker = chat_workers.get(update.callback_query.from_user.id)
                
                if receiving_worker is None:
                    log.debug(
                        f"Received a callback query in a chat without worker: {update.callback_query.from_user.id}")
                    
                    bot.send_message(update.callback_query.from_user.id, default_loc.get("error_no_worker_for_chat"))
                    
                    continue
                
                if update.callback_query.data == "cmd_cancel":
                    log.debug(f"Forwarding CancelSignal to {receiving_worker}")
                    
                    receiving_worker.queue.put(worker.CancelSignal())
                    
                    bot.answer_callback_query(update.callback_query.id)
                else:
                    log.debug(f"Forwarding callback query to {receiving_worker}")
                    
                    receiving_worker.queue.put(update)
            
            if isinstance(update.pre_checkout_query, telegram.PreCheckoutQuery):
                
                receiving_worker = chat_workers.get(update.pre_checkout_query.from_user.id)
                
                if receiving_worker is None or \
                        update.pre_checkout_query.invoice_payload != receiving_worker.invoice_payload:
                    
                    log.debug(f"Received a pre-checkout query for an expired invoice in: {update.pre_checkout_query.from_user.id}")
                    try:
                        bot.answer_pre_checkout_query(update.pre_checkout_query.id,
                                                      ok=False,
                                                      error_message=default_loc.get("error_invoice_expired"))
                    except telegram.error.BadRequest:
                        log.error("pre-checkout query expired before an answer could be sent!")
                    
                    continue
                log.debug(f"Forwarding pre-checkout query to {receiving_worker}")
                
                receiving_worker.queue.put(update)
        
        if len(updates):
            
            next_update = updates[-1].update_id + 1



if __name__ == "__main__":
    main()
