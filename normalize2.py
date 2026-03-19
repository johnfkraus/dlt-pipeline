import polars as pl

# --- Dummy normalization functions (replace these with your real ones) ---

def normalize_phone(row):
    v = row["value"]
    return f"PHONE:{v}" if v is not None else None

def normalize_email(row):
    v = row["value"]
    return f"EMAIL:{v}" if v is not None else None

def normalize_wechat(row):
    v = row["value"]
    return f"WECHAT:{v}" if v is not None else None

def normalize_whatsapp(row):
    v = row["value"]
    return f"WHATSAPP:{v}" if v is not None else None

def normalize_imei(row):
    v = row["value"]
    return f"IMEI:{v}" if v is not None else None

def normalize_mixed(row):
    v = row["value"]
    t = row["selector_a_type"]
    return f"MIXED:{t or 'unknown'}:{v}" if v is not None else None


# --- Router called from Polars ---

def route_and_normalize(row, nominal_selector_type=None):
    """
    row is like a dict with keys:
      - selector_a
      - selector_a_type
      - selector_a_alt
    We’ll also add a 'value' key to pass to your normalizers.
    Optional nominal_selector_type overrides selector_a_type
    """
    selector = row.get("selector_a")
    if nominal_selector_type is not None:
        sel_type = nominal_selector_type
    else:
        sel_type = row.get("selector_a_type")
    selector_alt = row.get("selector_a_alt")

    # If selector_a_type is null, we still normalize, but we may use selector_a_alt
    # instead of selector_a when selector_a_alt is not null.
    if sel_type is None:
        # choose which value to normalize
        value = selector_alt if selector_alt is not None else selector
        row = dict(row)
        row["value"] = value
        return normalize_mixed(row)

    # selector_a_type is not null: route based on it
    value = selector
    if value is None and selector_alt is not None:
        # if main selector is null but alt is present, use alt
        value = selector_alt

    row = dict(row)
    row["value"] = value

    if value is None:
        return None

    if sel_type == "phone":
        return normalize_phone(row)
    elif sel_type == "email":
        return normalize_email(row)
    elif sel_type == "wechat":
        return normalize_wechat(row)
    elif sel_type == "whatsapp":
        return normalize_whatsapp(row)
    elif sel_type == "imei":
        return normalize_imei(row)
    else:
        # unknown type, fall back to mixed
        return normalize_mixed(row)


# --- Sample data to prove it works ---

df = pl.DataFrame(
    {
        "selector_a": [
            "user@example.com",   # email
            None,                 # phone
            None,                 # use alt with known type
            "some_wechat_id",     # wechat
            "123456789012345",    # imei
            None,                 # type null, use alt
            "raw_mystery_value",  # type null, no alt
        ],
        "selector_a_type": [
            "email",
            "phone",
            "phone",
            "wechat",
            "imei",
            None,
            None,
        ],
        "selector_a_alt": [
            None,
            "+1 212 555 0000",
            "+44 7700 900123",     # alt phone
            None,
            None,
            "alt@example.com",     # alt when type is null
            None,
        ],
    }
)

df2 = pl.DataFrame(
    {
        "selector_a": [
            "+1 212 555 0000",
            "+44 7700 900123",
            None,
        ],
    }
)

df = df.with_columns(
    pl.struct(["selector_a", "selector_a_type", "selector_a_alt"])
      .map_elements(route_and_normalize)
      .alias("selector_a_final")
)

print(df)

df2 = df2.with_columns(
    pl.struct(["selector_a"])
      .map_elements(route_and_normalize)
      .alias("selector_a_final")
)
