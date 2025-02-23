import imaplib
import os
import sys
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

USER = os.getenv("USER")
PWD = os.getenv("USER_PASSWORD")
SMTP_SERVER = os.getenv("SMTP_SERVER", "imap.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 993))
CHECK_MAIL = os.getenv("CHECK_MAIL")


# Establish IMAP connection and login
def get_mail_connection():
    mail = imaplib.IMAP4_SSL(SMTP_SERVER, SMTP_PORT)
    mail.login(USER, PWD)
    mail.select("INBOX", readonly=True)
    return mail


# Fetch all emails or only those after the latest_date
def get_mail_ids(latest_date: datetime = None):
    with get_mail_connection() as mail:
        search_query = ["FROM", f'"{CHECK_MAIL}"']

        if latest_date:
            formatted_date = latest_date.strftime("%d-%b-%Y").upper()
            search_query.extend(["SINCE", formatted_date])

        # Unpack list into arguments
        data = mail.search(None, *search_query)

        # Decode mail IDs
        return (
            data[1][0].split()
            if data[1] and isinstance(data[1][0], bytes) and data[1][0]
            else []
        )
