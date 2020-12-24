import datetime
import logging
import os
import queue as queuem
import re
import sys
import threading
import traceback
import uuid
from html import escape
from typing import *

import requests
import sqlalchemy.orm
import telegram

import database as db
import localization
import nuconfig

log = logging.getLogger(__name__)


class StopSignal:
    def __init__(self, reason: str = ""):
        self.reason = reason


class CancelSignal:
    pass


class Worker(threading.Thread):
    def __init__(self,
                 bot,
                 chat: telegram.Chat,
                 telegram_user: telegram.User,
                 cfg: nuconfig.NuConfig,
                 engine,
                 *args,
                 **kwargs):
        super().__init__(name=f"Worker {chat.id}", *args, **kwargs)
        self.bot = bot
        self.chat: telegram.Chat = chat
        self.telegram_user: telegram.User = telegram_user
        self.cfg = cfg
        self.loc = None
        log.debug(f"Opening new database session for {self.name}")
        self.session = sqlalchemy.orm.sessionmaker(bind=engine)()
        self.user: Optional[db.User] = None
        self.admin: Optional[db.Admin] = None
        self.queue = queuem.Queue()
        self.invoice_payload = None
        self.Price = self.price_factory()

    def __repr__(self):
        return f"<{self.__class__.__qualname__} {self.chat.id}>"

    def price_factory(worker):
        class Price:

            def __init__(self, value: Union[int, float, str, "Price"]):
                if isinstance(value, int):
                    self.value = int(value)
                elif isinstance(value, float):
                    self.value = int(value * (10 ** worker.cfg["Payments"]["currency_exp"]))
                elif isinstance(value, str):
                    self.value = int(float(value.replace(",", ".")) * (10 ** worker.cfg["Payments"]["currency_exp"]))
                elif isinstance(value, Price):
                    self.value = value.value

            def __repr__(self):
                return f"<{self.__class__.__qualname__} of value {self.value}>"

            def __str__(self):
                return worker.loc.get(
                    "currency_format_string",
                    symbol=worker.cfg["Payments"]["currency_symbol"],
                    value="{0:.2f}".format(self.value / (10 ** worker.cfg["Payments"]["currency_exp"]))
                )

            def __int__(self):
                return self.value

            def __float__(self):
                return self.value / (10 ** worker.cfg["Payments"]["currency_exp"])

            def __ge__(self, other):
                return self.value >= Price(other).value

            def __le__(self, other):
                return self.value <= Price(other).value

            def __eq__(self, other):
                return self.value == Price(other).value

            def __gt__(self, other):
                return self.value > Price(other).value

            def __lt__(self, other):
                return self.value < Price(other).value

            def __add__(self, other):
                return Price(self.value + Price(other).value)

            def __sub__(self, other):
                return Price(self.value - Price(other).value)

            def __mul__(self, other):
                return Price(int(self.value * other))

            def __floordiv__(self, other):
                return Price(int(self.value // other))

            def __radd__(self, other):
                return self.__add__(other)

            def __rsub__(self, other):
                return Price(Price(other).value - self.value)

            def __rmul__(self, other):
                return self.__mul__(other)

            def __iadd__(self, other):
                self.value += Price(other).value
                return self

            def __isub__(self, other):
                self.value -= Price(other).value
                return self

            def __imul__(self, other):
                self.value *= other
                self.value = int(self.value)
                return self

            def __ifloordiv__(self, other):
                self.value //= other
                return self

        return Price

    def run(self):
        
        log.debug("Starting conversation")
        
        self.user = self.session.query(db.User).filter(db.User.user_id == self.chat.id).one_or_none()
        self.admin = self.session.query(db.Admin).filter(db.Admin.user_id == self.chat.id).one_or_none()
        
        if self.user is None:
            
            will_be_owner = (self.session.query(db.Admin).first() is None)
            
            self.user = db.User(w=self)
            
            self.session.add(self.user)
            
            self.session.flush()
            
            if will_be_owner:
                
                self.admin = db.Admin(user_id=self.user.user_id,
                                      edit_products=True,
                                      receive_orders=True,
                                      create_transactions=True,
                                      display_on_help=True,
                                      is_owner=True,
                                      live_mode=False)
                
                self.session.add(self.admin)
            
            self.session.commit()
            log.info(f"Created new user: {self.user}")
            if will_be_owner:
                log.warning(f"User was auto-promoted to Admin as no other admins existed: {self.user}")
        
        self.__create_localization()
        
        
        try:
            
            if self.cfg["Appearance"]["display_welcome_message"] == "yes":
                self.bot.send_message(self.chat.id, self.loc.get("conversation_after_start"))
            
            if self.admin is None:
                self.__user_menu()
            
            else:
                
                self.admin.live_mode = False
                
                self.session.commit()
                
                self.__admin_menu()
        except Exception as e:
            
            
            try:
                self.bot.send_message(self.chat.id, self.loc.get("fatal_conversation_exception"))
            except Exception as ne:
                log.error(f"Failed to notify the user of a conversation exception: {ne}")
            log.error(f"Exception in {self}: {e}")
            traceback.print_exception(*sys.exc_info())

    def is_ready(self):
        
        return self.loc is not None

    def stop(self, reason: str = ""):
        
        
        self.queue.put(StopSignal(reason))
        
        self.join()

    def update_user(self) -> db.User:
        
        log.debug("Fetching updated user data from the database")
        self.user = self.session.query(db.User).filter(db.User.user_id == self.chat.id).one_or_none()
        return self.user

    
    def __receive_next_update(self) -> telegram.Update:
        
        If no update is found, block the process until one is received.
        If a stop signal is sent, try to gracefully stop the thread.
        
        try:
            data = self.queue.get(timeout=self.cfg["Telegram"]["conversation_timeout"])
        except queuem.Empty:
            
            self.__graceful_stop(StopSignal("timeout"))
        
        if isinstance(data, StopSignal):
            
            self.__graceful_stop(data)
        
        return data

    def __wait_for_specific_message(self,
                                    items: List[str],
                                    cancellable: bool = False) -> Union[str, CancelSignal]:
        
        log.debug("Waiting for a specific message...")
        while True:
            
            update = self.__receive_next_update()
            
            if isinstance(update, CancelSignal):
                
                if cancellable:
                    
                    return update
                else:
                    
                    continue
            
            if update.message is None:
                continue
            
            if update.message.text is None:
                continue
            
            if update.message.text not in items:
                continue
            
            return update.message.text

    def __wait_for_regex(self, regex: str, cancellable: bool = False) -> Union[str, CancelSignal]:
        
        log.debug("Waiting for a regex...")
        while True:
            
            update = self.__receive_next_update()
            
            if isinstance(update, CancelSignal):
                
                if cancellable:
                    
                    return update
                else:
                    
                    continue
            
            if update.message is None:
                continue
            
            if update.message.text is None:
                continue
            
            match = re.search(regex, update.message.text)
            
            if match is None:
                continue
            
            return match.group(1)

    def __wait_for_precheckoutquery(self,
                                    cancellable: bool = False) -> Union[telegram.PreCheckoutQuery, CancelSignal]:
        
        The payload is checked by the core before forwarding the message.
        log.debug("Waiting for a PreCheckoutQuery...")
        while True:
            
            update = self.__receive_next_update()
            
            if isinstance(update, CancelSignal):
                
                if cancellable:
                    
                    return update
                else:
                    
                    continue
            
            if update.pre_checkout_query is None:
                continue
            
            return update.pre_checkout_query

    def __wait_for_successfulpayment(self,
                                     cancellable: bool = False) -> Union[telegram.SuccessfulPayment, CancelSignal]:
        
        log.debug("Waiting for a SuccessfulPayment...")
        while True:
            
            update = self.__receive_next_update()
            
            if isinstance(update, CancelSignal):
                
                if cancellable:
                    
                    return update
                else:
                    
                    continue
            
            if update.message is None:
                continue
            
            if update.message.successful_payment is None:
                continue
            
            return update.message.successful_payment

    def __wait_for_photo(self, cancellable: bool = False) -> Union[List[telegram.PhotoSize], CancelSignal]:
        
        log.debug("Waiting for a photo...")
        while True:
            
            update = self.__receive_next_update()
            
            if isinstance(update, CancelSignal):
                
                if cancellable:
                    
                    return update
                else:
                    
                    continue
            
            if update.message is None:
                continue
            
            if update.message.photo is None:
                continue
            
            return update.message.photo

    def __wait_for_inlinekeyboard_callback(self, cancellable: bool = False) \
            -> Union[telegram.CallbackQuery, CancelSignal]:
        
        log.debug("Waiting for a CallbackQuery...")
        while True:
            
            update = self.__receive_next_update()
            
            if isinstance(update, CancelSignal):
                
                if cancellable:
                    
                    return update
                else:
                    
                    continue
            
            if update.callback_query is None:
                continue
            
            self.bot.answer_callback_query(update.callback_query.id)
            
            return update.callback_query

    def __user_select(self) -> Union[db.User, CancelSignal]:
        
        log.debug("Waiting for a user selection...")
        
        users = self.session.query(db.User).order_by(db.User.user_id).all()
        
        keyboard_buttons = [[self.loc.get("menu_cancel")]]
        
        for user in users:
            keyboard_buttons.append([user.identifiable_str()])
        
        keyboard = telegram.ReplyKeyboardMarkup(keyboard_buttons, one_time_keyboard=True)
        
        while True:
            
            self.bot.send_message(self.chat.id, self.loc.get("conversation_admin_select_user"), reply_markup=keyboard)
            
            reply = self.__wait_for_regex("user_([0-9]+)", cancellable=True)
            
            if isinstance(reply, CancelSignal):
                return reply
            
            user = self.session.query(db.User).filter_by(user_id=int(reply)).one_or_none()
            
            if not user:
                self.bot.send_message(self.chat.id, self.loc.get("error_user_does_not_exist"))
                continue
            return user

    def __user_menu(self):
        
        Normal bot actions should be placed here.
        log.debug("Displaying __user_menu")
        
        while True:
            
            keyboard = [[telegram.KeyboardButton(self.loc.get("menu_order"))],
                        [telegram.KeyboardButton(self.loc.get("menu_order_status"))],
                        [telegram.KeyboardButton(self.loc.get("menu_add_credit"))],
                        [telegram.KeyboardButton(self.loc.get("menu_language"))],
                        [telegram.KeyboardButton(self.loc.get("menu_help"))]]
            
            self.bot.send_message(self.chat.id,
                                  self.loc.get("conversation_open_user_menu",
                                               credit=self.Price(self.user.credit)),
                                  reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
            
            selection = self.__wait_for_specific_message([
                self.loc.get("menu_order"),
                self.loc.get("menu_order_status"),
                self.loc.get("menu_add_credit"),
                self.loc.get("menu_language"),
                self.loc.get("menu_help")])
            
            self.update_user()
            
            if selection == self.loc.get("menu_order"):
                
                self.__order_menu()
            
            elif selection == self.loc.get("menu_order_status"):
                
                self.__order_status()
            
            elif selection == self.loc.get("menu_add_credit"):
                
                self.__add_credit_menu()
            
            elif selection == self.loc.get("menu_language"):
                
                self.__language_menu()
            
            elif selection == self.loc.get("menu_help"):
                
                self.__help_menu()

    def __order_menu(self):
        
        log.debug("Displaying __order_menu")
        
        products = self.session.query(db.Product).filter_by(deleted=False).all()
        
        
        cart: Dict[List[db.Product, int]] = {}
        
        for product in products:
            
            if product.price is None:
                continue
            
            message = product.send_as_message(w=self, chat_id=self.chat.id)
            
            cart[message['result']['message_id']] = [product, 0]
            
            inline_keyboard = telegram.InlineKeyboardMarkup(
                [[telegram.InlineKeyboardButton(self.loc.get("menu_add_to_cart"), callback_data="cart_add")]]
            )
            
            if product.image is None:
                self.bot.edit_message_text(chat_id=self.chat.id,
                                           message_id=message['result']['message_id'],
                                           text=product.text(w=self),
                                           reply_markup=inline_keyboard)
            else:
                self.bot.edit_message_caption(chat_id=self.chat.id,
                                              message_id=message['result']['message_id'],
                                              caption=product.text(w=self),
                                              reply_markup=inline_keyboard)
        
        inline_keyboard = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_cancel"),
                                                                                        callback_data="cart_cancel")]])
        
        final_msg = self.bot.send_message(self.chat.id,
                                          self.loc.get("conversation_cart_actions"),
                                          reply_markup=inline_keyboard)
        
        while True:
            callback = self.__wait_for_inlinekeyboard_callback()
            
            
            if callback.data == "cart_cancel":
                
                return
            
            elif callback.data == "cart_add":
                
                p = cart.get(callback.message.message_id)
                if p is None:
                    continue
                product = p[0]
                
                cart[callback.message.message_id][1] += 1
                
                product_inline_keyboard = telegram.InlineKeyboardMarkup(
                    [
                        [telegram.InlineKeyboardButton(self.loc.get("menu_add_to_cart"),
                                                       callback_data="cart_add"),
                         telegram.InlineKeyboardButton(self.loc.get("menu_remove_from_cart"),
                                                       callback_data="cart_remove")]
                    ])
                
                final_inline_keyboard = telegram.InlineKeyboardMarkup(
                    [
                        [telegram.InlineKeyboardButton(self.loc.get("menu_cancel"), callback_data="cart_cancel")],
                        [telegram.InlineKeyboardButton(self.loc.get("menu_done"), callback_data="cart_done")]
                    ])
                
                if product.image is None:
                    self.bot.edit_message_text(chat_id=self.chat.id,
                                               message_id=callback.message.message_id,
                                               text=product.text(w=self,
                                                                 cart_qty=cart[callback.message.message_id][1]),
                                               reply_markup=product_inline_keyboard)
                else:
                    self.bot.edit_message_caption(chat_id=self.chat.id,
                                                  message_id=callback.message.message_id,
                                                  caption=product.text(w=self,
                                                                       cart_qty=cart[callback.message.message_id][1]),
                                                  reply_markup=product_inline_keyboard)

                self.bot.edit_message_text(
                    chat_id=self.chat.id,
                    message_id=final_msg.message_id,
                    text=self.loc.get("conversation_confirm_cart",
                                      product_list=self.__get_cart_summary(cart),
                                      total_cost=str(self.__get_cart_value(cart))),
                    reply_markup=final_inline_keyboard)
            
            elif callback.data == "cart_remove":
                
                p = cart.get(callback.message.message_id)
                if p is None:
                    continue
                product = p[0]
                
                if cart[callback.message.message_id][1] > 0:
                    cart[callback.message.message_id][1] -= 1
                else:
                    continue
                
                product_inline_list = [[telegram.InlineKeyboardButton(self.loc.get("menu_add_to_cart"),
                                                                      callback_data="cart_add")]]
                if cart[callback.message.message_id][1] > 0:
                    product_inline_list[0].append(telegram.InlineKeyboardButton(self.loc.get("menu_remove_from_cart"),
                                                                                callback_data="cart_remove"))
                product_inline_keyboard = telegram.InlineKeyboardMarkup(product_inline_list)
                
                final_inline_list = [[telegram.InlineKeyboardButton(self.loc.get("menu_cancel"),
                                                                    callback_data="cart_cancel")]]
                for product_id in cart:
                    if cart[product_id][1] > 0:
                        final_inline_list.append([telegram.InlineKeyboardButton(self.loc.get("menu_done"),
                                                                                callback_data="cart_done")])
                        break
                final_inline_keyboard = telegram.InlineKeyboardMarkup(final_inline_list)
                
                if product.image is None:
                    self.bot.edit_message_text(chat_id=self.chat.id, message_id=callback.message.message_id,
                                               text=product.text(w=self,
                                                                 cart_qty=cart[callback.message.message_id][1]),
                                               reply_markup=product_inline_keyboard)
                else:
                    self.bot.edit_message_caption(chat_id=self.chat.id,
                                                  message_id=callback.message.message_id,
                                                  caption=product.text(w=self,
                                                                       cart_qty=cart[callback.message.message_id][1]),
                                                  reply_markup=product_inline_keyboard)

                self.bot.edit_message_text(
                    chat_id=self.chat.id,
                    message_id=final_msg.message_id,
                    text=self.loc.get("conversation_confirm_cart",
                                      product_list=self.__get_cart_summary(cart),
                                      total_cost=str(self.__get_cart_value(cart))),
                    reply_markup=final_inline_keyboard)
            
            elif callback.data == "cart_done":
                
                break
        
        cancel = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_skip"),
                                                                               callback_data="cmd_cancel")]])
        
        self.bot.send_message(self.chat.id, self.loc.get("ask_order_notes"), reply_markup=cancel)
        
        notes = self.__wait_for_regex(r"(.*)", cancellable=True)
        
        order = db.Order(user=self.user,
                         creation_date=datetime.datetime.now(),
                         notes=notes if not isinstance(notes, CancelSignal) else "")
        
        self.session.add(order)
        self.session.flush()
        
        for product in cart:
            
            for i in range(0, cart[product][1]):
                order_item = db.OrderItem(product=cart[product][0],
                                          order_id=order.order_id)
                self.session.add(order_item)
        
        credit_required = self.__get_cart_value(cart) - self.user.credit
        
        if credit_required > 0:
            self.bot.send_message(self.chat.id, self.loc.get("error_not_enough_credit"))
            
            if self.cfg["Payments"]["CreditCard"]["credit_card_token"] != "" \
                    and self.cfg["Appearance"]["refill_on_checkout"] \
                    and self.Price(self.cfg["Payments"]["CreditCard"]["min_amount"]) <= \
                    credit_required <= \
                    self.Price(self.cfg["Payments"]["CreditCard"]["max_amount"]):
                self.__make_payment(self.Price(credit_required))
        
        if self.user.credit < self.__get_cart_value(cart):
            
            self.session.rollback()
        else:
            
            self.__order_transaction(order=order, value=-int(self.__get_cart_value(cart)))

    def __get_cart_value(self, cart):
        
        value = self.Price(0)
        for product in cart:
            value += cart[product][0].price * cart[product][1]
        return value

    def __get_cart_summary(self, cart):
        
        product_list = ""
        for product_id in cart:
            if cart[product_id][1] > 0:
                product_list += cart[product_id][0].text(w=self,
                                                         style="short",
                                                         cart_qty=cart[product_id][1]) + "\n"
        return product_list

    def __order_transaction(self, order, value):
        
        transaction = db.Transaction(user=self.user,
                                     value=value,
                                     order_id=order.order_id)
        self.session.add(transaction)
        
        self.session.commit()
        
        self.user.recalculate_credit()
        
        self.session.commit()
        
        self.__order_notify_admins(order=order)

    def __order_notify_admins(self, order):
        
        self.bot.send_message(self.chat.id, self.loc.get("success_order_created", order=order.text(w=self,
                                                                                                   session=self.session,
                                                                                                   user=True)))
        
        admins = self.session.query(db.Admin).filter_by(live_mode=True).all()
        
        order_keyboard = telegram.InlineKeyboardMarkup(
            [
                [telegram.InlineKeyboardButton(self.loc.get("menu_complete"), callback_data="order_complete")],
                [telegram.InlineKeyboardButton(self.loc.get("menu_refund"), callback_data="order_refund")]
            ])
        
        for admin in admins:
            self.bot.send_message(admin.user_id,
                                  self.loc.get('notification_order_placed',
                                               order=order.text(w=self, session=self.session)),
                                  reply_markup=order_keyboard)

    def __order_status(self):
        
        log.debug("Displaying __order_status")
        
        orders = self.session.query(db.Order) \
            .filter(db.Order.user == self.user) \
            .order_by(db.Order.creation_date.desc()) \
            .limit(20) \
            .all()
        
        if len(orders) == 0:
            self.bot.send_message(self.chat.id, self.loc.get("error_no_orders"))
        
        for order in orders:
            self.bot.send_message(self.chat.id, order.text(w=self, session=self.session, user=True))
        

    def __add_credit_menu(self):
        
        log.debug("Displaying __add_credit_menu")
        
        keyboard = list()
        
        
        keyboard.append([telegram.KeyboardButton(self.loc.get("menu_cash"))])
        
        if self.cfg["Payments"]["CreditCard"]["credit_card_token"] != "":
            keyboard.append([telegram.KeyboardButton(self.loc.get("menu_credit_card"))])
        
        keyboard.append([telegram.KeyboardButton(self.loc.get("menu_cancel"))])
        
        self.bot.send_message(self.chat.id, self.loc.get("conversation_payment_method"),
                              reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
        
        selection = self.__wait_for_specific_message(
            [self.loc.get("menu_cash"), self.loc.get("menu_credit_card"), self.loc.get("menu_cancel")],
            cancellable=True)
        
        if selection == self.loc.get("menu_cash"):
            
            self.bot.send_message(self.chat.id,
                                  self.loc.get("payment_cash", user_cash_id=self.user.identifiable_str()))
        
        elif selection == self.loc.get("menu_credit_card"):
            
            self.__add_credit_cc()
        
        elif isinstance(selection, CancelSignal):
            
            return

    def __add_credit_cc(self):
        
        log.debug("Displaying __add_credit_cc")
        
        presets = self.cfg["Payments"]["CreditCard"]["payment_presets"]
        keyboard = [[telegram.KeyboardButton(str(self.Price(preset)))] for preset in presets]
        keyboard.append([telegram.KeyboardButton(self.loc.get("menu_cancel"))])
        
        cancelled = False
        
        while not cancelled:
            
            self.bot.send_message(self.chat.id, self.loc.get("payment_cc_amount"),
                                  reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
            
            selection = self.__wait_for_regex(r"([0-9]+(?:[.,][0-9]+)?|" + self.loc.get("menu_cancel") + r")",
                                              cancellable=True)
            
            if isinstance(selection, CancelSignal):
                
                cancelled = True
                continue
            
            value = self.Price(selection)
            
            if value > self.Price(self.cfg["Payments"]["CreditCard"]["max_amount"]):
                self.bot.send_message(self.chat.id,
                                      self.loc.get("error_payment_amount_over_max",
                                                   max_amount=self.Price(self.cfg["Credit Card"]["max_amount"])))
                continue
            elif value < self.Price(self.cfg["Payments"]["CreditCard"]["min_amount"]):
                self.bot.send_message(self.chat.id,
                                      self.loc.get("error_payment_amount_under_min",
                                                   min_amount=self.Price(self.cfg["Credit Card"]["min_amount"])))
                continue
            break
        
        else:
            
            return
        
        self.__make_payment(amount=value)

    def __make_payment(self, amount):
        
        self.invoice_payload = str(uuid.uuid4())
        
        prices = [telegram.LabeledPrice(label=self.loc.get("payment_invoice_label"), amount=int(amount))]
        
        fee = int(self.__get_total_fee(amount))
        if fee > 0:
            prices.append(telegram.LabeledPrice(label=self.loc.get("payment_invoice_fee_label"),
                                                amount=fee))
        
        inline_keyboard = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_pay"),
                                                                                        pay=True)],
                                                         [telegram.InlineKeyboardButton(self.loc.get("menu_cancel"),
                                                                                        callback_data="cmd_cancel")]])
        
        self.bot.send_invoice(self.chat.id,
                              title=self.loc.get("payment_invoice_title"),
                              description=self.loc.get("payment_invoice_description", amount=str(amount)),
                              payload=self.invoice_payload,
                              provider_token=self.cfg["Payments"]["CreditCard"]["credit_card_token"],
                              start_parameter="tempdeeplink",
                              currency=self.cfg["Payments"]["currency"],
                              prices=prices,
                              need_name=self.cfg["Payments"]["CreditCard"]["name_required"],
                              need_email=self.cfg["Payments"]["CreditCard"]["email_required"],
                              need_phone_number=self.cfg["Payments"]["CreditCard"]["phone_required"],
                              reply_markup=inline_keyboard)
        
        precheckoutquery = self.__wait_for_precheckoutquery(cancellable=True)
        
        if isinstance(precheckoutquery, CancelSignal):
            
            return
        
        self.bot.answer_pre_checkout_query(precheckoutquery.id, ok=True)
        
        successfulpayment = self.__wait_for_successfulpayment(cancellable=False)
        
        transaction = db.Transaction(user=self.user,
                                     value=int(successfulpayment.total_amount) - fee,
                                     provider="Credit Card",
                                     telegram_charge_id=successfulpayment.telegram_payment_charge_id,
                                     provider_charge_id=successfulpayment.provider_payment_charge_id)

        if successfulpayment.order_info is not None:
            transaction.payment_name = successfulpayment.order_info.name
            transaction.payment_email = successfulpayment.order_info.email
            transaction.payment_phone = successfulpayment.order_info.phone_number
        
        self.user.recalculate_credit()
        
        self.session.commit()

    def __get_total_fee(self, amount):
        
        fee_percentage = self.cfg["Payments"]["CreditCard"]["fee_percentage"] / 100
        fee_fixed = self.cfg["Payments"]["CreditCard"]["fee_fixed"]
        total_fee = amount * fee_percentage + fee_fixed
        if total_fee > 0:
            return total_fee
        
        return 0

    def __admin_menu(self):
        
        Administrative bot actions should be placed here.
        log.debug("Displaying __admin_menu")
        
        while True:
            
            keyboard = []
            if self.admin.edit_products:
                keyboard.append([self.loc.get("menu_products")])
            if self.admin.receive_orders:
                keyboard.append([self.loc.get("menu_orders")])
            if self.admin.create_transactions:
                keyboard.append([self.loc.get("menu_edit_credit")])
                keyboard.append([self.loc.get("menu_transactions"), self.loc.get("menu_csv")])
            if self.admin.is_owner:
                keyboard.append([self.loc.get("menu_edit_admins")])
            keyboard.append([self.loc.get("menu_user_mode")])
            
            self.bot.send_message(self.chat.id, self.loc.get("conversation_open_admin_menu"),
                                  reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
            
            selection = self.__wait_for_specific_message([self.loc.get("menu_products"),
                                                          self.loc.get("menu_orders"),
                                                          self.loc.get("menu_user_mode"),
                                                          self.loc.get("menu_edit_credit"),
                                                          self.loc.get("menu_transactions"),
                                                          self.loc.get("menu_csv"),
                                                          self.loc.get("menu_edit_admins")])
            
            if selection == self.loc.get("menu_products"):
                
                self.__products_menu()
            
            elif selection == self.loc.get("menu_orders"):
                
                self.__orders_menu()
            
            elif selection == self.loc.get("menu_edit_credit"):
                
                self.__create_transaction()
            
            elif selection == self.loc.get("menu_user_mode"):
                
                self.bot.send_message(self.chat.id, self.loc.get("conversation_switch_to_user_mode"))
                
                self.__user_menu()
            
            elif selection == self.loc.get("menu_edit_admins"):
                
                self.__add_admin()
            
            elif selection == self.loc.get("menu_transactions"):
                
                self.__transaction_pages()
            
            elif selection == self.loc.get("menu_csv"):
                
                self.__transactions_file()

    def __products_menu(self):
        
        log.debug("Displaying __products_menu")
        
        products = self.session.query(db.Product).filter_by(deleted=False).all()
        
        product_names = [product.name for product in products]
        
        product_names.insert(0, self.loc.get("menu_cancel"))
        product_names.insert(1, self.loc.get("menu_add_product"))
        product_names.insert(2, self.loc.get("menu_delete_product"))
        
        keyboard = [[telegram.KeyboardButton(product_name)] for product_name in product_names]
        
        self.bot.send_message(self.chat.id, self.loc.get("conversation_admin_select_product"),
                              reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
        
        selection = self.__wait_for_specific_message(product_names, cancellable=True)
        
        if isinstance(selection, CancelSignal):
            
            return
        
        elif selection == self.loc.get("menu_add_product"):
            
            self.__edit_product_menu()
        
        elif selection == self.loc.get("menu_delete_product"):
            
            self.__delete_product_menu()
        
        else:
            
            product = self.session.query(db.Product).filter_by(name=selection, deleted=False).one()
            
            self.__edit_product_menu(product=product)

    def __edit_product_menu(self, product: Optional[db.Product] = None):
        
        log.debug("Displaying __edit_product_menu")
        
        cancel = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_skip"),
                                                                               callback_data="cmd_cancel")]])
        
        while True:
            
            self.bot.send_message(self.chat.id, self.loc.get("ask_product_name"))
            
            if product:
                self.bot.send_message(self.chat.id, self.loc.get("edit_current_value", value=escape(product.name)),
                                      reply_markup=cancel)
            
            name = self.__wait_for_regex(r"(.*)", cancellable=bool(product))
            
            if (product and isinstance(name, CancelSignal)) or \
                    self.session.query(db.Product).filter_by(name=name, deleted=False).one_or_none() in [None, product]:
                
                break
            self.bot.send_message(self.chat.id, self.loc.get("error_duplicate_name"))
        
        self.bot.send_message(self.chat.id, self.loc.get("ask_product_description"))
        
        if product:
            self.bot.send_message(self.chat.id,
                                  self.loc.get("edit_current_value", value=escape(product.description)),
                                  reply_markup=cancel)
        
        description = self.__wait_for_regex(r"(.*)", cancellable=bool(product))
        
        self.bot.send_message(self.chat.id,
                              self.loc.get("ask_product_price"))
        
        if product:
            self.bot.send_message(self.chat.id,
                                  self.loc.get("edit_current_value",
                                               value=(str(self.Price(product.price))
                                                      if product.price is not None else 'Non in vendita')),
                                  reply_markup=cancel)
        
        price = self.__wait_for_regex(r"([0-9]+(?:[.,][0-9]{1,2})?|[Xx])",
                                      cancellable=True)
        
        if isinstance(price, CancelSignal):
            pass
        elif price.lower() == "x":
            price = None
        else:
            price = self.Price(price)
        
        self.bot.send_message(self.chat.id, self.loc.get("ask_product_image"), reply_markup=cancel)
        
        photo_list = self.__wait_for_photo(cancellable=True)
        
        if not product:
            
            
            product = db.Product(name=name,
                                 description=description,
                                 price=int(price) if price is not None else None,
                                 deleted=False)
            
            self.session.add(product)
        
        else:
            
            product.name = name if not isinstance(name, CancelSignal) else product.name
            product.description = description if not isinstance(description, CancelSignal) else product.description
            product.price = int(price) if not isinstance(price, CancelSignal) else product.price
        
        if isinstance(photo_list, list):
            
            largest_photo = photo_list[0]
            for photo in photo_list[1:]:
                if photo.width > largest_photo.width:
                    largest_photo = photo
            
            photo_file = self.bot.get_file(largest_photo.file_id)
            
            self.bot.send_message(self.chat.id, self.loc.get("downloading_image"))
            self.bot.send_chat_action(self.chat.id, action="upload_photo")
            
            product.set_image(photo_file)
        
        self.session.commit()
        
        self.bot.send_message(self.chat.id, self.loc.get("success_product_edited"))

    def __delete_product_menu(self):
        log.debug("Displaying __delete_product_menu")
        
        products = self.session.query(db.Product).filter_by(deleted=False).all()
        
        product_names = [product.name for product in products]
        
        product_names.insert(0, self.loc.get("menu_cancel"))
        
        keyboard = [[telegram.KeyboardButton(product_name)] for product_name in product_names]
        
        self.bot.send_message(self.chat.id, self.loc.get("conversation_admin_select_product_to_delete"),
                              reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
        
        selection = self.__wait_for_specific_message(product_names, cancellable=True)
        if isinstance(selection, CancelSignal):
            
            return
        else:
            
            product = self.session.query(db.Product).filter_by(name=selection, deleted=False).one()
            
            product.deleted = True
            self.session.commit()
            
            self.bot.send_message(self.chat.id, self.loc.get("success_product_deleted"))

    def __orders_menu(self):
        
        log.debug("Displaying __orders_menu")
        
        stop_keyboard = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_stop"),
                                                                                      callback_data="cmd_cancel")]])
        cancel_keyboard = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_cancel"),
                                                                                        callback_data="cmd_cancel")]])
        
        
        self.bot.send_message(self.chat.id,
                              self.loc.get("conversation_live_orders_start"),
                              reply_markup=telegram.ReplyKeyboardRemove())
        
        self.bot.send_message(self.chat.id,
                              self.loc.get("conversation_live_orders_stop"),
                              reply_markup=stop_keyboard)
        
        order_keyboard = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_complete"),
                                                                                       callback_data="order_complete")],
                                                        [telegram.InlineKeyboardButton(self.loc.get("menu_refund"),
                                                                                       callback_data="order_refund")]])
        
        orders = self.session.query(db.Order) \
            .filter_by(delivery_date=None, refund_date=None) \
            .join(db.Transaction) \
            .join(db.User) \
            .all()
        
        for order in orders:
            
            self.bot.send_message(self.chat.id, order.text(w=self, session=self.session),
                                  reply_markup=order_keyboard)
        
        self.admin.live_mode = True
        
        self.session.commit()
        while True:
            
            update = self.__wait_for_inlinekeyboard_callback(cancellable=True)
            
            if isinstance(update, CancelSignal):
                
                self.admin.live_mode = False
                break
            
            order_id = re.search(self.loc.get("order_number").replace("{id}", "([0-9]+)"), update.message.text).group(1)
            order = self.session.query(db.Order).filter(db.Order.order_id == order_id).one()
            
            if order.delivery_date is not None or order.refund_date is not None:
                
                self.bot.edit_message_text(self.chat.id, self.loc.get("error_order_already_cleared"))
                break
            
            if update.data == "order_complete":
                
                order.delivery_date = datetime.datetime.now()
                
                self.session.commit()
                
                self.bot.edit_message_text(order.text(w=self, session=self.session), chat_id=self.chat.id,
                                           message_id=update.message.message_id)
                
                self.bot.send_message(order.user_id,
                                      self.loc.get("notification_order_completed",
                                                   order=order.text(w=self, session=self.session, user=True)))
            
            elif update.data == "order_refund":
                
                reason_msg = self.bot.send_message(self.chat.id, self.loc.get("ask_refund_reason"),
                                                   reply_markup=cancel_keyboard)
                
                reply = self.__wait_for_regex("(.*)", cancellable=True)
                
                if isinstance(reply, CancelSignal):
                    
                    self.bot.delete_message(self.chat.id, reason_msg.message_id)
                    continue
                
                order.refund_date = datetime.datetime.now()
                
                order.refund_reason = reply
                
                order.transaction.refunded = True
                
                order.user.recalculate_credit()
                
                self.session.commit()
                
                self.bot.edit_message_text(order.text(w=self, session=self.session),
                                           chat_id=self.chat.id,
                                           message_id=update.message.message_id)
                
                self.bot.send_message(order.user_id,
                                      self.loc.get("notification_order_refunded", order=order.text(w=self,
                                                                                                   session=self.session,
                                                                                                   user=True)))
                
                self.bot.send_message(self.chat.id, self.loc.get("success_order_refunded", order_id=order.order_id))

    def __create_transaction(self):
        
        log.debug("Displaying __create_transaction")
        
        user = self.__user_select()
        
        if isinstance(user, CancelSignal):
            return
        
        cancel = telegram.InlineKeyboardMarkup([[telegram.InlineKeyboardButton(self.loc.get("menu_cancel"),
                                                                               callback_data="cmd_cancel")]])
        
        self.bot.send_message(self.chat.id, self.loc.get("ask_credit"), reply_markup=cancel)
        
        reply = self.__wait_for_regex(r"(-? ?[0-9]{1,3}(?:[.,][0-9]{1,2})?)", cancellable=True)
        
        if isinstance(reply, CancelSignal):
            return
        
        price = self.Price(reply)
        
        self.bot.send_message(self.chat.id, self.loc.get("ask_transaction_notes"), reply_markup=cancel)
        
        reply = self.__wait_for_regex(r"(.*)", cancellable=True)
        
        if isinstance(reply, CancelSignal):
            return
        
        transaction = db.Transaction(user=user,
                                     value=int(price),
                                     provider="Manual",
                                     notes=reply)
        self.session.add(transaction)
        
        user.recalculate_credit()
        
        self.session.commit()
        
        self.bot.send_message(user.user_id,
                              self.loc.get("notification_transaction_created",
                                           transaction=transaction.text(w=self)))
        
        self.bot.send_message(self.chat.id, self.loc.get("success_transaction_created",
                                                         transaction=transaction.text(w=self)))

    def __help_menu(self):
        
        log.debug("Displaying __help_menu")
        
        keyboard = [[telegram.KeyboardButton(self.loc.get("menu_contact_shopkeeper"))],
                    [telegram.KeyboardButton(self.loc.get("menu_cancel"))]]
        
        self.bot.send_message(self.chat.id,
                              self.loc.get("conversation_open_help_menu"),
                              reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
        
        selection = self.__wait_for_specific_message(self.loc.get("menu_contact_shopkeeper"), cancellable=True)
        
        if selection == self.loc.get("menu_contact_shopkeeper"):
            
            shopkeepers = self.session.query(db.Admin).filter_by(display_on_help=True).join(db.User).all()
            
            shopkeepers_string = "\n".join([admin.user.mention() for admin in shopkeepers])
            
            self.bot.send_message(self.chat.id, self.loc.get("contact_shopkeeper", shopkeepers=shopkeepers_string))
        

    def __transaction_pages(self):
        
        log.debug("Displaying __transaction_pages")
        
        page = 0
        
        message = self.bot.send_message(self.chat.id, self.loc.get("loading_transactions"))
        
        while True:
            
            transactions = self.session.query(db.Transaction) \
                .order_by(db.Transaction.transaction_id.desc()) \
                .limit(10) \
                .offset(10 * page) \
                .all()
            
            inline_keyboard_list = [[]]
            
            if page != 0:
                
                inline_keyboard_list[0].append(
                    telegram.InlineKeyboardButton(self.loc.get("menu_previous"), callback_data="cmd_previous")
                )
            
            if len(transactions) == 10:
                
                inline_keyboard_list[0].append(
                    telegram.InlineKeyboardButton(self.loc.get("menu_next"), callback_data="cmd_next")
                )
            
            inline_keyboard_list.append(
                [telegram.InlineKeyboardButton(self.loc.get("menu_done"), callback_data="cmd_done")])
            
            inline_keyboard = telegram.InlineKeyboardMarkup(inline_keyboard_list)
            
            transactions_string = "\n".join([transaction.text(w=self) for transaction in transactions])
            text = self.loc.get("transactions_page", page=page + 1, transactions=transactions_string)
            
            self.bot.edit_message_text(chat_id=self.chat.id, message_id=message.message_id, text=text,
                                       reply_markup=inline_keyboard)
            
            selection = self.__wait_for_inlinekeyboard_callback()
            
            if selection.data == "cmd_previous" and page != 0:
                
                page -= 1
            
            elif selection.data == "cmd_next" and len(transactions) == 10:
                
                page += 1
            
            elif selection.data == "cmd_done":
                
                break

    def __transactions_file(self):
        
        log.debug("Generating __transaction_file")
        
        transactions = self.session.query(db.Transaction).order_by(db.Transaction.transaction_id.asc()).all()
        
        try:
            with open(f"transactions_{self.chat.id}.csv", "x"):
                pass
        except IOError:
            pass
        
        with open(f"transactions_{self.chat.id}.csv", "w") as file:
            
            file.write(f"UserID;"
                       f"TransactionValue;"
                       f"TransactionNotes;"
                       f"Provider;"
                       f"ChargeID;"
                       f"SpecifiedName;"
                       f"SpecifiedPhone;"
                       f"SpecifiedEmail;"
                       f"Refunded?\n")
            
            for transaction in transactions:
                file.write(f"{transaction.user_id if transaction.user_id is not None else ''};"
                           f"{transaction.value if transaction.value is not None else ''};"
                           f"{transaction.notes if transaction.notes is not None else ''};"
                           f"{transaction.provider if transaction.provider is not None else ''};"
                           f"{transaction.provider_charge_id if transaction.provider_charge_id is not None else ''};"
                           f"{transaction.payment_name if transaction.payment_name is not None else ''};"
                           f"{transaction.payment_phone if transaction.payment_phone is not None else ''};"
                           f"{transaction.payment_email if transaction.payment_email is not None else ''};"
                           f"{transaction.refunded if transaction.refunded is not None else ''}\n")
        
        self.bot.send_message(self.chat.id, self.loc.get("csv_caption"))
        
        with open(f"transactions_{self.chat.id}.csv") as file:
            
            requests.post(f"https://api.telegram.org/bot{self.cfg['Telegram']['token']}/sendDocument",
                          files={"document": file},
                          params={"chat_id": self.chat.id,
                                  "parse_mode": "HTML"})
        
        os.remove(f"transactions_{self.chat.id}.csv")

    def __add_admin(self):
        
        log.debug("Displaying __add_admin")
        
        user = self.__user_select()
        
        if isinstance(user, CancelSignal):
            return
        
        admin = self.session.query(db.Admin).filter_by(user_id=user.user_id).one_or_none()
        if admin is None:
            
            keyboard = telegram.ReplyKeyboardMarkup([[self.loc.get("emoji_yes"), self.loc.get("emoji_no")]],
                                                    one_time_keyboard=True)
            
            self.bot.send_message(self.chat.id, self.loc.get("conversation_confirm_admin_promotion"),
                                  reply_markup=keyboard)
            
            selection = self.__wait_for_specific_message([self.loc.get("emoji_yes"), self.loc.get("emoji_no")])
            
            if selection == self.loc.get("emoji_no"):
                return
            
            admin = db.Admin(user=user,
                             edit_products=False,
                             receive_orders=False,
                             create_transactions=False,
                             is_owner=False,
                             display_on_help=False)
            self.session.add(admin)
        
        message = self.bot.send_message(self.chat.id, self.loc.get("admin_properties", name=str(admin.user)))
        
        while True:
            
            inline_keyboard = telegram.InlineKeyboardMarkup([
                [telegram.InlineKeyboardButton(
                    f"{self.loc.boolmoji(admin.edit_products)} {self.loc.get('prop_edit_products')}",
                    callback_data="toggle_edit_products"
                )],
                [telegram.InlineKeyboardButton(
                    f"{self.loc.boolmoji(admin.receive_orders)} {self.loc.get('prop_receive_orders')}",
                    callback_data="toggle_receive_orders"
                )],
                [telegram.InlineKeyboardButton(
                    f"{self.loc.boolmoji(admin.create_transactions)} {self.loc.get('prop_create_transactions')}",
                    callback_data="toggle_create_transactions"
                )],
                [telegram.InlineKeyboardButton(
                    f"{self.loc.boolmoji(admin.display_on_help)} {self.loc.get('prop_display_on_help')}",
                    callback_data="toggle_display_on_help"
                )],
                [telegram.InlineKeyboardButton(
                    self.loc.get('menu_done'),
                    callback_data="cmd_done"
                )]
            ])
            
            self.bot.edit_message_reply_markup(message_id=message.message_id,
                                               chat_id=self.chat.id,
                                               reply_markup=inline_keyboard)
            
            callback = self.__wait_for_inlinekeyboard_callback()
            
            if callback.data == "toggle_edit_products":
                admin.edit_products = not admin.edit_products
            elif callback.data == "toggle_receive_orders":
                admin.receive_orders = not admin.receive_orders
            elif callback.data == "toggle_create_transactions":
                admin.create_transactions = not admin.create_transactions
            elif callback.data == "toggle_display_on_help":
                admin.display_on_help = not admin.display_on_help
            elif callback.data == "cmd_done":
                break
        self.session.commit()

    def __language_menu(self):
        
        log.debug("Displaying __language_menu")
        keyboard = []
        options: Dict[str, str] = {}
        
        if "it" in self.cfg["Language"]["enabled_languages"]:
            lang = "🇮🇹 Italiano"
            keyboard.append([telegram.KeyboardButton(lang)])
            options[lang] = "it"
        if "en" in self.cfg["Language"]["enabled_languages"]:
            lang = "🇬🇧 English"
            keyboard.append([telegram.KeyboardButton(lang)])
            options[lang] = "en"
        if "ru" in self.cfg["Language"]["enabled_languages"]:
            lang = "🇷🇺 Русский"
            keyboard.append([telegram.KeyboardButton(lang)])
            options[lang] = "ru"
        if "uk" in self.cfg["Language"]["enabled_languages"]:
            lang = "🇺🇦 Українська"
            keyboard.append([telegram.KeyboardButton(lang)])
            options[lang] = "uk"
        if "zh_cn" in self.cfg["Language"]["enabled_languages"]:
            lang = "🇨🇳 简体中文"
            keyboard.append([telegram.KeyboardButton(lang)])
            options[lang] = "zh_cn"
        if "he" in self.cfg["Language"]["enabled_languages"]:
            lang = "🇮🇱 עברית"
            keyboard.append([telegram.KeyboardButton(lang)])
            options[lang] = "he"
        if "es_mx" in self.cfg["Language"]["enabled_languages"]:
            lang = "🇲🇽 Español"
            keyboard.append([telegram.KeyboardButton(lang)])
            options[lang] = "es_mx"
        
        self.bot.send_message(self.chat.id,
                              self.loc.get("conversation_language_select"),
                              reply_markup=telegram.ReplyKeyboardMarkup(keyboard, one_time_keyboard=True))
        
        response = self.__wait_for_specific_message(list(options.keys()))
        
        self.user.language = options[response]
        
        self.session.commit()
        
        self.__create_localization()

    def __create_localization(self):
        
        if self.user.language not in self.cfg["Language"]["enabled_languages"]:
            log.debug(f"User's language '{self.user.language}' is not enabled, changing it to the default")
            self.user.language = self.cfg["Language"]["default_language"]
            self.session.commit()
        
        self.loc = localization.Localization(
            language=self.user.language,
            fallback=self.cfg["Language"]["fallback_language"],
            replacements={
                "user_string": str(self.user),
                "user_mention": self.user.mention(),
                "user_full_name": self.user.full_name,
                "user_first_name": self.user.first_name,
                "today": datetime.datetime.now().strftime("%a %d %b %Y"),
            }
        )

    def __graceful_stop(self, stop_trigger: StopSignal):
        
        log.debug("Gracefully stopping the conversation")
        
        if stop_trigger.reason == "timeout":
            
            self.bot.send_message(self.chat.id, self.loc.get('conversation_expired'),
                                  reply_markup=telegram.ReplyKeyboardRemove())
        
        
        
        self.session.close()
        
        sys.exit(0)
