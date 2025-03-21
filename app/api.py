from datetime import datetime
from os import getenv
from dotenv import load_dotenv
from fastapi import FastAPI, Response, HTTPException, Depends, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from supabase import Client, create_client
from app.parse_email import get_mail_dataframe, get_parsed_emails
from app.search_inbox import get_mail_ids
from typing import Optional


# Load env variables
load_dotenv()

# Supabase credentials
SUPABASE_URL = getenv("SUPABASE_URL")
SUPABASE_KEY = getenv("SUPABASE_KEY")

# Create supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initiate API app
app = FastAPI()

# Get environment
app_env = getenv("APP_ENV", "dev")

# Configure CORS based on environment
origins = []
if app_env == "prd":
    # Only production URL
    origins = [getenv("FRONTEND_URL")]
else:
    # Development - include localhost URLs
    origins = [
        # NextJS dev server
        "http://localhost:3000",
        # FastAPI dev server
        "http://localhost:8000",
    ]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exception):
    """Catch any HTTPException and return JSONResponse"""
    return JSONResponse(
        status_code=exception.status_code,
        content={"status": "error", "message": exception.detail},
    )


def api_response(message, data=None, status="success"):
    """Create JSONResponse for success message passed"""
    response = {"status": status, "message": message}
    if data is not None:
        response["data"] = data
    return JSONResponse(content=response, status_code=200)


def get_proper_iso_format(date_str):
    """Fix the datetime to proper ISO format"""
    try:
        date_part, time_part = date_str.split("T")
        time_main, ms_part = time_part.split(".")

        # Ensure milliseconds are exactly 3 digits
        ms_part = ms_part.zfill(3)
        fixed_date_str = f"{date_part}T{time_main}.{ms_part}"

        # Convert to datetime
        return datetime.fromisoformat(fixed_date_str)
    except (ValueError, IndexError) as e:
        print(f"Error: {e}")
        return None


async def verify_session(session_token: Optional[str] = Cookie(None)):
    """Verify is session is passed. If yes, then check validity of the session"""

    # Raise exception if session token is not passed
    if not session_token:
        raise HTTPException(status_code=401, detail="Session Token not provided")

    # Query the session table to get the valid session
    session_data = (
        supabase.table("session")
        .select("id, user_id, expires_at")
        .eq("token", session_token)
        .limit(1)
        .execute()
    )

    # Check if session exists
    if not session_data.data:
        raise HTTPException(status_code=401, detail="Invalid session")

    session = session_data.data[0]
    expiry_date = get_proper_iso_format(session["expires_at"])

    # Check if session has expired
    if expiry_date < datetime.now():
        raise HTTPException(status_code=401, detail="Session expired")

    # Get the user associated with this session
    user_data = (
        supabase.table("user")
        .select("id, role")
        .eq("id", session["user_id"])
        .limit(1)
        .execute()
    )

    # Check if user exists
    if not user_data.data:
        raise HTTPException(status_code=401, detail="User not found")

    return user_data.data[0]


async def verify_admin_session(user: dict = Depends(verify_session)):
    """Verify that a session belongs to an admin user"""
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    return user


@app.get("/transactions")
def get_transactions(response: Response, valid_user=Depends(verify_session)):
    """
    - Get all transactions from supabase
    - Fetch all transactions from transactions table
    - This endpoint will not be used in the client. This is just for testing purpose
    """

    data = (
        supabase.table("transactions")
        .select("id", "user_id", count="exact")
        .eq("user_id", valid_user["id"])
        .limit(1)
        .execute()
    )

    return api_response(
        message=f"Transactions fetched! Transaction count - {data.count}",
    )


def process_transactions(user_id, mail_df=None):
    """
    - Generic function to process transactions from the mails
    - This can be used for fetching latest transactions or doing a full refresh
    """

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
            "user_id": user_id,
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
            "user_id": user_id,
        }
        for _, row in mail_df.iterrows()
    ]

    # Insert all transactions from mail_df
    supabase.table("transactions").upsert(
        transaction_records, on_conflict="upi_ref_no"
    ).execute()


@app.post("/all-transactions")
def populate_all_transactions(
    response: Response, admin_user: dict = Depends(verify_admin_session)
):
    """
    Do full refresh of receivers and transactions tables
    """
    try:
        mail_ids = get_mail_ids()
        mail_data = get_parsed_emails(mail_ids)
        mail_df = get_mail_dataframe(mail_data)

        # Truncate data and reset identity in receivers and transactions tables
        supabase.rpc("truncate_and_reset").execute()

        # Process all transactions
        process_transactions(
            mail_df=mail_df,
            user_id=admin_user["id"],
        )

        response.status_code = 201
        return api_response(message="Full refresh done")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post("/new-transactions")
def add_new_transactions(
    response: Response, valid_user: dict = Depends(verify_session)
):
    """
    Populate new transactions not present in supabase
    """
    try:
        # Get transaction_date of the last transaction
        user_id = valid_user["id"]

        last_transaction_timestamp_data = (
            supabase.table("transactions")
            .select("transaction_date", "user_id")
            .eq("user_id", user_id)
            .order("transaction_date", desc=True)
            .limit(1)
            .execute()
            .data
        )

        # If no result is returned from above query, this means there are no transactions
        # Hence use the other endpoint to do full load of all transactions
        if len(last_transaction_timestamp_data) == 0:
            return api_response(
                message="No transactions found. Please do a full refresh first"
            )

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

        process_transactions(mail_df=mail_df, user_id=user_id)

        return api_response(message="Transactions upserted")

    except Exception:
        raise HTTPException(status_code=500, detail="Something went wrong")
