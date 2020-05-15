import redis
import atexit
import datetime
import logging
logging.basicConfig(filename="logs.log", level=logging.INFO)


def register(conn, username):

    if conn.hget('users:', username):
        print(f"User {username} exists")
        return None

    user_id = conn.incr('user:id:')

    pipeline = conn.pipeline(True)

    pipeline.hset('users:', username, user_id)

    pipeline.hmset('user:%s' % user_id, {
        'login': username,
        'id': user_id,
        'queue': 0,
        'checking': 0,
        'blocked': 0,
        'sent': 0,
        'delivered': 0
    })
    pipeline.execute()
    logging.info(f"USER {username} registered at {datetime.datetime.now()} \n")
    return user_id


def sign_in(conn, username) -> int:
    user_id = conn.hget("users:", username)

    if not user_id:
        print("Current user does not exist %s" % username)
        return -1

    conn.sadd("online:", username)
    logging.info(f"USER {username} logged in at {datetime.datetime.now()} \n")

    return int(user_id)


def sign_out(conn, user_id) -> int:
    logging.info(f"USER {user_id} signed out at {datetime.datetime.now()} \n")
    return conn.srem("online:", conn.hmget("user:%s" % user_id, ["login"])[0])


def create_message(conn, message_text, sender_id, consumer) -> int:
    message_id = int(conn.incr('message:id:'))
    consumer_id = int(conn.hget("users:", consumer))

    if not consumer_id:
        print("Current user does not exist %s, unable to send message" % consumer)
        return

    pipeline = conn.pipeline(True)

    pipeline.hmset('message:%s' % message_id, {
        'text': message_text,
        'id': message_id,
        'sender_id': sender_id,
        'consumer_id': consumer_id,
        'status': "created"
    })
    pipeline.lpush("queue:", message_id)
    pipeline.hmset('message:%s' % message_id, {
        'status': 'queue'
    })
    pipeline.zincrby("sent:", 1, "user:%s" %
                     conn.hmget("user:%s" % sender_id, ["login"])[0])
    pipeline.hincrby("user:%s" % sender_id, "queue", 1)
    pipeline.execute()

    return message_id


def print_messages(connection, user_id):
    messages = connection.smembers("sentto:%s" % user_id)
    for message_id in messages:
        message = connection.hmget("message:%s" % message_id, [
                                   "sender_id", "text", "status"])
        sender_id = message[0]
        print("From: %s - %s" % (connection.hmget("user:%s" %
                                                  sender_id, ["login"])[0], message[1]))
        if message[2] != "delivered":
            pipeline = connection.pipeline(True)
            pipeline.hset("message:%s" % message_id, "status", "delivered")
            pipeline.hincrby("user:%s" % sender_id, "sent", -1)
            pipeline.hincrby("user:%s" % sender_id, "delivered", 1)
            pipeline.execute()


def main_menu() -> int:
    print(30 * ">", "MENU", 30 * "<")
    print("1. Register in the system")
    print("2. Login to the system")
    print("3. Exit from program")
    return int(input("Choose option [1-3]: "))


def user_menu() -> int:
    print(30 * ">", "MENU", 30 * "<")
    print("1. Sign out")
    print("2. Send a message")
    print("3. Inbox messages")
    print("4. My messages statistics")
    return int(input("Choose option [1-4]: "))


def registration_form(connection):
    login = input("Enter your username:")
    register(connection, login)


def message_form(connection, current_user_id):
    message = input("Enter message text:")
    recipient = input("Enter recipient username:")
    try:
        if create_message(connection, message, current_user_id, recipient):
            print("Sending message...")
    except ValueError:
        print("no user with login %s found!", recipient)


def print_messages_staticsics(connection, current_user_id):
    current_user = connection.hmget("user:%s" % current_user_id,
                                    ['queue', 'checking', 'blocked', 'sent', 'delivered'])
    print("In queue: %s\nChecking: %s\nBlocked: %s\nSent: %s\nDelivered: %s" %
          tuple(current_user))


def main():
    def exit_handler():
        sign_out(connection, current_user_id)

    atexit.register(exit_handler)
    signed_in = False
    current_user_id = -1
    connection = redis.Redis(charset="utf-8", decode_responses=True)
    menu = main_menu

    while True:
        option = menu()

        if option == 1:
            if not signed_in:
                registration_form(connection)
            else:
                sign_out(connection, current_user_id)
                connection.publish('users', "User %s signed out"
                                   % connection.hmget("user:%s" % current_user_id, ["login"])[0])
                signed_in = False
                current_user_id = -1
                menu = main_menu

        elif option == 2:
            if signed_in:
                message_form(connection, current_user_id)
            else:
                login = input("Enter your login:")
                current_user_id = sign_in(connection, login)
                signed_in = current_user_id != -1
                if signed_in:
                    connection.publish('users', "User %s signed in"
                                       % connection.hmget("user:%s" % current_user_id, ["login"])[0])
                    menu = user_menu

        elif option == 3:
            if signed_in:
                print_messages(connection, current_user_id)
            else:
                break

        elif option == 4:
            print_messages_staticsics(connection, current_user_id)
        else:
            print("Wrong option selected")


if __name__ == '__main__':
    main()
