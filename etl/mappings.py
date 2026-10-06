"""Canonical values, parsing formats and cleaning policy for the hotel bookings dataset."""

RAW_COLUMNS = [
    "booking_id",
    "hotel",
    "is_canceled",
    "lead_time",
    "arrival_date",
    "stays_in_weekend_nights",
    "stays_in_week_nights",
    "adults",
    "children",
    "babies",
    "meal",
    "country",
    "market_segment",
    "distribution_channel",
    "deposit_type",
    "customer_type",
    "adr",
    "total_of_special_requests",
    "reservation_status",
    "reservation_status_date",
]

# Compared after trimming and lower-casing.
NULL_TOKENS = {"", "na", "n/a", "null", "none", "-"}

CATEGORY_VALUES = {
    "hotel": ["City Hotel", "Resort Hotel"],
    "meal": ["BB", "HB", "FB", "SC", "Undefined"],
    "market_segment": [
        "Online TA",
        "Offline TA/TO",
        "Groups",
        "Direct",
        "Corporate",
        "Complementary",
        "Aviation",
        "Undefined",
    ],
    "distribution_channel": ["TA/TO", "Direct", "Corporate", "GDS", "Undefined"],
    "deposit_type": ["No Deposit", "Non Refund", "Refundable"],
    "customer_type": ["Transient", "Transient-Party", "Contract", "Group", "Undefined"],
    "reservation_status": ["Check-Out", "Canceled", "No-Show"],
}

# The dataset documentation defines "Undefined" and "SC" as the same thing: no meal package.
CATEGORY_ALIASES = {
    "meal": {"Undefined": "SC"},
}

BOOLEAN_VALUES = {
    "1": True, "true": True, "yes": True, "y": True,
    "0": False, "false": False, "no": False, "n": False,
}

INTEGER_COLUMNS = [
    "lead_time",
    "stays_in_weekend_nights",
    "stays_in_week_nights",
    "adults",
    "children",
    "babies",
    "total_of_special_requests",
]

# The source system is European, so slash dates are day-first.
ARRIVAL_DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y", "%Y/%m/%d", "%B %d, %Y"]
STATUS_DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d.%m.%Y"]

# Codes pycountry does not resolve to the current ISO 3166-1 alpha-3 code.
COUNTRY_OVERRIDES = {
    "CN": "CHN",   # ISO alpha-2 used for China in the source
    "TMP": "TLS",  # retired code for East Timor
}
UNKNOWN_COUNTRY_CODE = "UNK"
UNKNOWN_COUNTRY_NAME = "Unknown"

# Applied to missing values only; values that are present but unparseable are rejected instead.
MISSING_VALUE_DEFAULTS = {
    "children": 0,
    "meal": "SC",
    "market_segment": "Undefined",
    "customer_type": "Undefined",
    "country": UNKNOWN_COUNTRY_CODE,
}

# Missing values here cannot be inferred without inventing revenue, dates or guests.
REQUIRED_COLUMNS = [c for c in RAW_COLUMNS if c not in MISSING_VALUE_DEFAULTS]

BOOKING_ID_PATTERN = r"^BKG-\d{6}$"
ADR_MAX = 1000

HOTEL_ATTRIBUTES = {
    "City Hotel": {"hotel_type": "City", "location": "Lisbon"},
    "Resort Hotel": {"hotel_type": "Resort", "location": "Algarve"},
}
