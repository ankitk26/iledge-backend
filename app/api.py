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

    if expiry_date is None:
        raise HTTPException(
            status_code=500, detail="Could not parse session expiry date"
        )
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


@app.get("/expenses")
def get_expenses(response: Response, valid_user=Depends(verify_session)):
    """
    - Get all expenses from supabase
    - This endpoint will not be used in the client. This is just for testing purpose
    """

    data = (
        supabase.table("expense")
        .select("id", "user_id", count="exact")
        .eq("user_id", valid_user["id"])
        .limit(1)
        .execute()
    )

    return api_response(
        message=f"Expenses fetched! Row count - {data.count}",
    )


def process_expenses(user_id, mail_df=None):
    """
    - Generic function to process expenses from the mails
    - This can be used for fetching latest expenses or doing a full refresh
    """

    if mail_df is not None and mail_df.empty:
        return 0

    # Select columns related to payee
    payee_df = mail_df[["payee_upi_id", "payee_name", "transaction_date"]]
    # Calculate rank for each payee_upi_id based on recent transaction date. Recent transaction gets rank 1
    payee_df["rank"] = payee_df.groupby("payee_upi_id")["transaction_date"].rank(
        method="first", ascending=False
    )

    # Filter most recent names
    eff_payee_df = payee_df[payee_df["rank"] == 1]

    # Get list of new payees to be added/updated
    payee_upi_ids = eff_payee_df["payee_upi_id"].tolist()

    # Convert dataframe to list of tuples containing two values - payee_id and name(payee_name)
    payee_data = [
        {
            "payee_upi_id": row.payee_upi_id,
            "name": row.payee_name,
            "user_id": user_id,
        }
        for row in eff_payee_df.itertuples(index=False)
    ]

    # Insert payees into "payee" table
    # ON CONFLICT - this will update the name with the latest name if a payee_upi is matched
    supabase.table("payee").upsert(payee_data, on_conflict="payee_upi_id").execute()

    # Get list of all payees upserted above
    db_payee_data = (
        supabase.table("payee")
        .select("id, payee_upi_id")
        .in_("payee_upi_id", payee_upi_ids)
        .execute()
    )

    # Create dictionary mapping each payee_upi_id to each database ID
    payee_mapping = {row["payee_upi_id"]: row["id"] for row in db_payee_data.data}

    # Prepare expense data
    expense_records = [
        {
            "upi_ref_no": row.upi_ref_no,
            "amount": row.amount,
            "sender_upi_id": row.sender_upi_id,
            "payee_id": payee_mapping.get(row.payee_upi_id),
            "transaction_date": str(row.transaction_date),
            "user_id": user_id,
        }
        for _, row in mail_df.iterrows()
    ]

    # Insert all expenses from mail_df
    supabase.table("expense").upsert(
        expense_records, on_conflict="upi_ref_no"
    ).execute()


@app.post("/expenses")
def populate_all_expenses(
    response: Response, admin_user: dict = Depends(verify_admin_session)
):
    """
    Do full refresh of payee and expenses tables
    """
    try:
        mail_ids = get_mail_ids()

        mail_data = get_parsed_emails(mail_ids)
        if mail_data is None:
            raise HTTPException(
                status_code=400, detail="Error in processing emails. Please try again"
            )

        mail_df = get_mail_dataframe(mail_data)
        if mail_df is None:
            raise HTTPException(
                status_code=400, detail="Error in processing emails. Please try again"
            )

        # Truncate data and reset identity in receivers and transactions tables
        supabase.rpc("truncate_and_reset").execute()

        process_expenses(mail_df=mail_df, user_id=admin_user["id"])

        response.status_code = 201
        return api_response(message="Full refresh done")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post("/expenses/new")
def add_new_expenses(response: Response, valid_user: dict = Depends(verify_session)):
    """
    Populate new expenses not present in supabase
    """
    try:
        user_id = valid_user["id"]

        # Get transaction_date of the last expense
        last_transaction_timestamp_data = (
            supabase.table("expense")
            .select("transaction_date", "user_id")
            .eq("user_id", user_id)
            .order("transaction_date", desc=True)
            .limit(1)
            .execute()
            .data
        )

        # If no result is returned from above query, this means there are no expenses
        # Hence use the other endpoint to do full load of all expenses
        if len(last_transaction_timestamp_data) == 0:
            return api_response(
                message="No expenses found. Please do a full refresh first"
            )

        last_transaction_timestamp = last_transaction_timestamp_data[0][
            "transaction_date"
        ]

        last_transaction_date = datetime.fromisoformat(
            last_transaction_timestamp
        ).date()

        recent_mail_ids = get_mail_ids(last_transaction_date)
        recent_mail_data = get_parsed_emails(recent_mail_ids)
        mail_df = get_mail_dataframe(recent_mail_data)

        process_expenses(mail_df=mail_df, user_id=user_id)

        return api_response(message="expenses upserted")

    except Exception:
        raise HTTPException(status_code=500, detail="Something went wrong")
