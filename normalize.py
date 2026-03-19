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

def route_and_normalize(row):
    """
    row is like a dict with keys:
      - selector_a
      - selector_a_type
      - selector_a_alt
    We’ll also add a 'value' key to pass to your normalizers.
    """
    dataset_name_to_selector_a_type = {
        "c01": "phone",
        "c02": "email",
        "c03": "wechat",
    }

    dataset_name = row.get("dataset_name")
    # print(dataset_name)
    dataset_sel_type = dataset_name_to_selector_a_type.get(dataset_name)
    print(dataset_sel_type, dataset_name)
    selector = row.get("selector_a")
    if dataset_sel_type:
        sel_type = dataset_sel_type
    else:
        sel_type = row.get("selector_a_type")
    print(f"{dataset_name}, {sel_type}")
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
            "+1 212 555 0000",    # phone
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
            None,
            "+44 7700 900123",     # alt phone
            None,
            None,
            "alt@example.com",     # alt when type is null
            None,
        ],
        "dataset_name": [
            "c01",
            "c02",
            "c03",
            "c04",
            "c05",
            "c06",
            "c07",
        ],
    }
)

df = df.with_columns(
    pl.struct(["selector_a", "selector_a_type", "selector_a_alt", "dataset_name"])
      .map_elements(route_and_normalize)
      .alias("selector_a_final")
)

print(df)
