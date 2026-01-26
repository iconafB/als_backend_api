from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple

# Dataclasses

@dataclass(slots=True)
class RawRecord:
    id: Optional[str]
    fore_name: Optional[str]
    last_name: Optional[str]
    cell: Optional[str]

@dataclass(slots=True)
class FeedRecord:
    vendor_lead_code: str
    first_name: str
    last_name: str
    phone_number: str

@dataclass(slots=True)
class CleaningRecord:
    name: str
    surname: str
    id_number: str
    cell_number: str


# Cleaning function


INVALID_VALUES = {"", "Null", "null", "NULL"}


def clean_and_process_results(new_result: List[Dict[str, Any]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    
    feeds: List[Dict[str, str]] = []

    feeds_cleaning: List[Dict[str, str]] = []

    for r in new_result:
        # Extract fields
        id_val = r.get("id")
        fore_name = r.get("fore_name")
        last_name = r.get("last_name")
        cell = r.get("cell")

        # Clean invalid values

        if not isinstance(id_val, str) or id_val in INVALID_VALUES or id_val.isspace():
            continue
        if not isinstance(cell, str) or cell in INVALID_VALUES or cell.isspace():
            continue
        if not isinstance(fore_name, str) or fore_name in INVALID_VALUES or fore_name.isspace():
            fore_name = None
        if not isinstance(last_name, str) or last_name in INVALID_VALUES or last_name.isspace():
            last_name = None

        # Fallback for missing fore_name

        first_name = fore_name or last_name
        
        if first_name is None:

            continue

        # Append directly as dictionaries

        feeds.append({
            "first_name": first_name,
            "last_name": last_name or "",
            "phone_number": cell,
            "vendor_lead_code": id_val
        })

        feeds_cleaning.append({
            "name": first_name,
            "surname": last_name or "",
            "id_number": id_val,
            "cell_number": cell
        })


        print("print all the feeds immediately after cleaning")
        print(feeds)
    return feeds, feeds_cleaning
