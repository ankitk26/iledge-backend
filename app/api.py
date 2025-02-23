import pandas as pd
from datetime import datetime
from os import getenv
from dotenv import load_dotenv
from fastapi import FastAPI, Response
from supabase import Client, create_client
from app.parse_email import get_mail_dataframe, get_parsed_emails
from app.search_inbox import get_mail_ids

# Load env variables
load_dotenv()

# Supabase credentials
SUPABASE_URL = getenv("SUPABASE_URL")
SUPABASE_KEY = getenv("SUPABASE_KEY")

# Create supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()


# Get all transactions from supabase
# Fetch all transactions from transactions table
# This endpoint will not be used in the client. This is just for testing purpose
@app.get("/transactions")
def get_transactions(response: Response):
    data = supabase.table("transactions").select("*").execute().data
    response.status_code = 200
    return {
        "status": "ok",
        "message": f"Transactions fetched! Transaction count - {len(data)}",
    }


# Generic function to process transactions from the mails
# This can be used for fetching latest transactions or doing a full refresh
def process_transactions(mail_df=None):
    if mail_df is not None and mail_df.empty:
        return 0

    # Select columns related to receiver
    receiver_df = mail_df[["receiver_upi_id", "receiver_name", "transaction_date"]]

    # Calculate rank for each receiver_upi_id based on recent transaction date. Recent transaction gets rank 1
    receiver_df["rank"] = receiver_df.groupby("receiver_upi_id")[
        "transaction_date"
    ].rank(method="first", ascending=False)

    # Filter most recent names
    eff_receiver_df = receiver_df[receiver_df["rank"] == 1]

    # Get list of new receivers to be added/updated
    receiver_upi_ids = eff_receiver_df["receiver_upi_id"].tolist()

    # Convert dataframe to list of tuples containing two values - receiver_id and name(receiver_name)
    receiver_data = [
        {
            "receiver_upi_id": row.receiver_upi_id,
            "name": row.receiver_name,
        }
        for row in eff_receiver_df.itertuples(index=False)
    ]

    # Insert receivers into "receiver" table
    # ON CONFLICT - this will update the name with the latest name if a receiver_upi is matched
    supabase.table("receivers").upsert(
        receiver_data, on_conflict="receiver_upi_id"
    ).execute()

    # Get list of all receivers upserted above
    db_receiver_data = (
        supabase.table("receivers")
        .select("id, receiver_upi_id")
        .in_("receiver_upi_id", receiver_upi_ids)
        .execute()
    )

    # Create dictionary mapping each receiver_upi_id to each database ID
    receiver_mapping = {
        row["receiver_upi_id"]: row["id"] for row in db_receiver_data.data
    }

    # Prepare transactions data
    transaction_records = [
        {
            "upi_ref_no": row.upi_ref_no,
            "amount": row.amount,
            "sender_upi_id": row.sender_upi_id,
            "receiver_id": receiver_mapping.get(row.receiver_upi_id),
            "transaction_date": str(row.transaction_date),
        }
        for _, row in mail_df.iterrows()
    ]

    # Insert all transactions from mail_df
    supabase.table("transactions").upsert(
        transaction_records, on_conflict="upi_ref_no"
    ).execute()

    return 1


# Do full refresh of receivers and transactions tables
@app.post("/all-transactions")
def populate_all_transactions(response: Response):
    try:
        mail_ids = get_mail_ids()
        mail_data = get_parsed_emails(mail_ids)
        mail_df = get_mail_dataframe(mail_data)

        # Truncate data and reset identity in receivers and transactions tables
        supabase.rpc("truncate_and_reset").execute()

        # Process all transactions
        is_inserts_done = process_transactions()

        response.status_code = 201
        return {
            "status": "ok",
            "message": "Full refresh done",
        }
    except Exception as e:
        response.status_code = 500
        return {
            "status": "error",
            "message": f"Something went wrong - Error message - {e}",
        }


# Populate new transactions not present in supabase
@app.post("/new-transactions")
def add_new_transactions(response: Response):
    try:
        # Get transaction_date of the last transaction
        last_transaction_timestamp_data = (
            supabase.table("transactions")
            .select("transaction_date")
            .order("transaction_date", desc=True)
            .limit(1)
            .execute()
            .data
        )

        # If no result is returned from above query, this means there are no transactions
        # Hence use the other endpoint to do full load of all transactions
        response.status_code = 404
        if len(last_transaction_timestamp_data) == 0:
            return {
                "status": "warning",
                "message": "No transactions found. Please do a full refresh first",
            }

        # Extract timestamp
        last_transaction_timestamp = last_transaction_timestamp_data[0][
            "transaction_date"
        ]

        # Extract date from timestamp
        last_transaction_date = datetime.fromisoformat(
            last_transaction_timestamp
        ).date()

        recent_mail_ids = get_mail_ids(last_transaction_date)
        recent_mail_data = get_parsed_emails(recent_mail_ids)
        mail_df = get_mail_dataframe(recent_mail_data)

        is_inserts_done = process_transactions(mail_df)

        response.status_code = 204
        if is_inserts_done == 1:
            response.status_code = 201

        return {
            "status": "ok",
            "message": (
                "Transactions upserted"
                if is_inserts_done == 1
                else "No transactions to add"
            ),
        }

    except Exception as e:
        response.status_code = 500
        return {
            "status": "error",
            "message": f"Something went wrong - Error message - {e}",
        }
