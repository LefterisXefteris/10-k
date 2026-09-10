"""
v2: read distinct companies from sec_ai_10k.parquet,
ask Hugging Face for models only when the author clearly matches,
and save everything into one Parquet file.

Unique orgs are fetched concurrently (HF_WORKERS, default 8).
Each org is requested once, then joined back to companies.

Example call: https://huggingface.co/api/models?author=Snowflake

v2 is stricter than v1: it never guesses from the first word
(that matched Allegro Microsystems to the unrelated org "allegro").
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json
import os
import re
import time
from urllib.parse import quote
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "DATA_DIR",
        Path(__file__).resolve().parent.parent / "data",
    )
)
PARQUET_IN = DATA_DIR / "sec_ai_10k.parquet"
PARQUET_OUT = DATA_DIR / "hf_models.parquet"
HF_URL = "https://huggingface.co/api/models?author="

# HF_MAX_COMPANIES=5 while testing. Unset = all companies.
_max_companies = os.environ.get("HF_MAX_COMPANIES", "").strip()
MAX_COMPANIES = int(_max_companies) if _max_companies else None
_max_workers = os.environ.get("HF_WORKERS", "8").strip()
MAX_WORKERS = max(1, int(_max_workers) if _max_workers else 8)

# Tickers whose Hugging Face org is not the company name.
# Alphabet != google, Meta Platforms != facebook, AMD != "advanced micro devices".
TICKER_TO_ORG = {
    "GOOG": "google",
    "GOOGL": "google",
    "META": "facebook",
    "AMD": "amd",
}

# Drop these from the end of a company name, one at a time.
# " COM" is only for names like AMAZON COM INC. QUALCOMM is safe because
# we require a space before the suffix.
LEGAL_SUFFIXES = [
    "Inc.",
    "Inc",
    "Corp.",
    "Corp",
    "Corporation",
    "Ltd.",
    "Ltd",
    "LLC",
    "Co.",
    "Co",
    "PLC",
    "L.P.",
    "LP",
    "Incorporated",
    "Company",
    "COM",
]

COLUMNS = [
    "company",
    "org",
    "model_id",
    "downloads",
    "likes",
    "pipeline_tag",
    "library_name",
    "created_at",
    "tags",
]
SCHEMA = pa.schema(
    [
        ("company", pa.string()),
        ("org", pa.string()),
        ("model_id", pa.string()),
        ("downloads", pa.int64()),
        ("likes", pa.int64()),
        ("pipeline_tag", pa.string()),
        ("library_name", pa.string()),
        ("created_at", pa.string()),
        ("tags", pa.string()),
    ]
)


def distinct_companies():
    """Return unique company names from the SEC Parquet file, sorted."""
    if not PARQUET_IN.exists():
        raise FileNotFoundError(f"Missing {PARQUET_IN}")
    names = set(pq.read_table(PARQUET_IN, columns=["company"]).column("company").to_pylist())
    return sorted(name for name in names if name)


def company_tickers(company):
    """Pick ticker symbols out of 'Name  (SNOW)  (CIK 0001640147)'."""
    tickers = []
    for group in re.findall(r"\(([^)]+)\)", company):
        if group.strip().upper().startswith("CIK"):
            continue
        for part in group.split(","):
            ticker = part.strip()
            if ticker:
                tickers.append(ticker)
    return tickers


def clean_company_name(company):
    """
    Keep the brand, drop ticker / CIK / Inc / Corp.

    Snowflake Inc.  (SNOW)  (CIK ...)  ->  Snowflake
    AMAZON COM INC  (AMZN)  (CIK ...)  ->  AMAZON
    """
    name = company.split("(")[0].strip()
    if " /" in name:
        name = name.split(" /")[0].strip()

    while True:
        stripped = False
        for suffix in LEGAL_SUFFIXES:
            token = " " + suffix
            if name.lower().endswith(token.lower()):
                name = name[: -len(token)].rstrip(" ,.")
                stripped = True
                break
        if not stripped:
            break

    return name.strip(" ,.")


def orgs_to_try(company):
    """
    Hugging Face author names we are willing to query for this company.

    We only use:
      1) a known ticker mapping, or
      2) the full cleaned company name (a couple of letter-casings)

    We never use the first word alone.
    """
    mapped = []
    for ticker in company_tickers(company):
        org = TICKER_TO_ORG.get(ticker)
        if org and org not in mapped:
            mapped.append(org)
    if mapped:
        return mapped

    name = clean_company_name(company)
    # Short names ("AI", "3M") and multi-word names ("Allegro Microsystems")
    # are too easy to match to the wrong Hugging Face user.
    if len(name) < 3 or " " in name:
        return []

    if name.isupper():
        # MICROSOFT -> try Microsoft, then microsoft
        return [name.title(), name.lower()]
    return [name]


def fetch_models(org):
    """Call Hugging Face and return models that really belong to this author."""
    url = HF_URL + quote(org)
    req = Request(url, headers={"User-Agent": "sec-ai-10k-hf-v2"})
    with urlopen(req, timeout=30) as response:
        models = json.loads(response.read().decode())

    prefix = org + "/"
    return [m for m in models if (m.get("id") or "").startswith(prefix)]


def fetch_org(org):
    """One Hub request. Sleep after the call so workers stay polite."""
    try:
        models = fetch_models(org)
        time.sleep(0.1)
        return org, models, None
    except Exception as exc:
        time.sleep(0.1)
        return org, [], exc


def unique_orgs(company_orgs):
    """Preserve first-seen order so later joins stay stable."""
    orgs = []
    seen = set()
    for _, candidates in company_orgs:
        for org in candidates:
            if org not in seen:
                seen.add(org)
                orgs.append(org)
    return orgs


def fetch_orgs(orgs):
    """Fetch each unique org once, concurrently."""
    models_by_org = {}
    if not orgs:
        return models_by_org

    workers = min(MAX_WORKERS, len(orgs))
    print(f"Fetching {len(orgs)} unique orgs with {workers} workers")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_org, org) for org in orgs]
        for future in as_completed(futures):
            org, models, error = future.result()
            models_by_org[org] = models
            done += 1
            if error:
                print(f"[{done}/{len(orgs)}] error {org}: {error}")
            elif models:
                print(f"[{done}/{len(orgs)}] {org}: {len(models)} models")
            elif done % 50 == 0:
                print(f"[{done}/{len(orgs)}] still fetching")
    return models_by_org


def models_to_rows(company, org, models):
    """Turn Hugging Face JSON into flat Parquet rows."""
    rows = []
    for model in models:
        tags = model.get("tags") or []
        rows.append(
            {
                "company": company,
                "org": org,
                "model_id": model.get("id") or model.get("modelId"),
                "downloads": model.get("downloads"),
                "likes": model.get("likes"),
                "pipeline_tag": model.get("pipeline_tag"),
                "library_name": model.get("library_name"),
                "created_at": model.get("createdAt"),
                "tags": "; ".join(tags),
            }
        )
    return rows


def write_rows(rows):
    PARQUET_OUT.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=SCHEMA) if rows else SCHEMA.empty_table()
    pq.write_table(table, PARQUET_OUT)


def main(**context):
    companies = distinct_companies()
    if MAX_COMPANIES is not None:
        companies = companies[:MAX_COMPANIES]
    print(f"Found {len(companies)} distinct companies")

    company_orgs = [(company, orgs_to_try(company)) for company in companies]
    models_by_org = fetch_orgs(unique_orgs(company_orgs))

    all_rows = []
    companies_with_models = 0
    for company, candidates in company_orgs:
        org_used = None
        models = []
        for org in candidates:
            models = models_by_org.get(org) or []
            if models:
                org_used = org
                break
        if not models:
            continue
        all_rows.extend(models_to_rows(company, org_used, models))
        companies_with_models += 1

    write_rows(all_rows)
    print(
        f"Saved {len(all_rows)} models from {companies_with_models} companies "
        f"to {PARQUET_OUT}"
    )


if __name__ == "__main__":
    main()
