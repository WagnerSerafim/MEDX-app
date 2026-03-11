from datetime import datetime

from utils.utils import is_valid_date, limpar_numero, verify_nan


def normalize_birthdate(raw_value):
    birth = verify_nan(raw_value)
    if birth is None:
        return "1900-01-01"

    if isinstance(birth, datetime):
        birth = birth.strftime("%Y-%m-%d")
    else:
        birth = str(birth)
        if " " in birth:
            birth = birth.split(" ")[0]
        if "/" in birth:
            birth = birth.replace("/", "-")

    if is_valid_date(birth, "%Y-%m-%d"):
        return birth
    return "1900-01-01"


def normalize_sex(raw_value):
    sex = verify_nan(raw_value)
    if sex is None:
        return "M"

    value = str(sex).strip().upper()
    if value in {"F", "FEMININO"}:
        return "F"
    return "M"


def mount_phone(ddd, phone):
    phone_value = limpar_numero(verify_nan(phone))
    if not phone_value:
        return None

    ddd_value = limpar_numero(verify_nan(ddd))
    if ddd_value:
        return f"({ddd_value}) {phone_value}"
    return phone_value
