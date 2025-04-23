import email
from email.utils import parsedate_to_datetime
import os
import re

import pandas as pd
from bs4 import BeautifulSoup
import pytz
from app.search_inbox import get_mail_connection
from dotenv import load_dotenv

load_dotenv()


def get_parsed_emails(mail_ids):
    try:
        # get mail connection
        mail_connection = get_mail_connection()

        # define what final data would look like
        parsed_mail_data = {
            "UPI Ref. No.": [],
            "To VPA": [],
            "From VPA": [],
            "Payee Name": [],
            "Amount": [],
            "Transaction Date": [],
        }

        # iterate through each mail_id
        for mail_id in mail_ids:
            # try to fetch the mail data for the current iteration's mail_id
            status, fetched_mail_data = mail_connection.fetch(mail_id, "(RFC822)")

            # continue if status not OK
            if status != "OK":
                continue

            # get mail message
            raw_mail = email.message_from_bytes(fetched_mail_data[0][1])

            # Walk through mail content
            for part in raw_mail.walk():
                # Get mail body
                body = part.get_payload(decode=True)

                # continue if body is None
                if body is None:
                    continue

                # Parse body content
                soup = BeautifulSoup(body, "html.parser")

                # Get spans having class = "gmailmsg"
                spans = soup.find_all("span", class_="gmailmsg")

                # iterate through all valid spans
                for span in spans:
                    # Skip spans which don't contain UPI Ref No or with FAILED status
                    if "UPI Ref. No. " not in span.text:
                        continue
                    if "Transaction Status: FAILED" in span.text:
                        continue

                    # Get key:value pairs by splitting
                    lines = str(span).split("<br/>")
                    for line in lines:
                        # Skip lines that contain ':' or start with '<'
                        if line.startswith("<") or ":" not in line:
                            continue

                        # split only once on ":" separator to get two items - key and value
                        # strip each element and get cleaned values
                        pay_key, pay_val = map(str.strip, line.split(":", 1))

                        # skip if key is not in desired data keys
                        if pay_key not in parsed_mail_data:
                            continue

                        if pay_key == "Transaction Date":
                            # parse mail's date to valid datetime
                            email_datetime = parsedate_to_datetime(raw_mail["Date"])

                            # convert datetime to UTC timezone
                            email_datetime_utc = email_datetime.astimezone(pytz.UTC)

                            # Add element to list
                            parsed_mail_data[pay_key].append(email_datetime_utc)
                        else:
                            # add element to list normally
                            parsed_mail_data[pay_key].append(pay_val)
        return parsed_mail_data
    except Exception as e:
        return None


def get_mail_dataframe(email_data):
    try:
        mail_df = pd.DataFrame(email_data)

        # Rename columns
        mail_df = mail_df.rename(
            columns={
                "UPI Ref. No.": "upi_ref_no",
                "Amount": "amount",
                "From VPA": "sender_upi_id",
                "To VPA": "payee_upi_id",
                "Payee Name": "payee_name",
                "Transaction Date": "transaction_date",
            }
        )

        ids = os.getenv("IDS").split(",")

        # Build regex pattern
        ids_pattern = "|".join(map(re.escape, ids))

        # Convert data types
        mail_df["amount"] = mail_df["amount"].astype(float)
        mail_df["upi_ref_no"] = mail_df["upi_ref_no"].astype(str)
        mail_df["transaction_date"] = pd.to_datetime(mail_df["transaction_date"])

        match_mask = mail_df["payee_upi_id"].str.contains(
            ids_pattern, regex=True, na=False
        )
        mail_df.loc[match_mask, "amount"] *= -1

        return mail_df
    except Exception as e:
        return None
