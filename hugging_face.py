"""
v2: read distinct companies from sec_ai_10k.parquet,
ask Hugging Face for models only when the author clearly matches,
and save everything into one Parquet file.

Example call: https://huggingface.co/api/models?author=Snowflake

v2 is stricter than v1: it never guesses from the first word
(that matched Allegro Microsystems to the unrelated org "allegro").
"""

from pathlib import Path
import json
import re
import time
from urllib.parse import quote
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

PARQUET_IN = Path(__file__).resolve().parent / "sec_ai_10k.parquet"
PARQUET_OUT = Path(__file__).resolve().parent / "hf_models.parquet"
HF_URL = "https://huggingface.co/api/models?author="

# Change this to a small number while testing (e.g. 5). None = all companies.
MAX_COMPANIES = None

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
    table = pa.Table.from_pylist(rows, schema=SCHEMA) if rows else SCHEMA.empty_table()
    pq.write_table(table, PARQUET_OUT)


def main():
    companies = distinct_companies()
    if MAX_COMPANIES is not None:
        companies = companies[:MAX_COMPANIES]
    print(f"Found {len(companies)} distinct companies")

    all_rows = []
    total_models = 0
    companies_with_models = 0
    for i, company in enumerate(companies, start=1):
        models = []
        org_used = None
        try:
            for org in orgs_to_try(company):
                models = fetch_models(org)
                time.sleep(0.1)
                if models:
                    org_used = org
                    break
        except Exception as e:
            print(f"[{i}/{len(companies)}] error {e}")
            continue

        if not models:
            if i % 100 == 0:
                print(
                    f"[{i}/{len(companies)}] still scanning "
                    f"({companies_with_models} companies with models so far)"
                )
            continue

        rows = models_to_rows(company, org_used, models)
        all_rows.extend(rows)
        write_rows(all_rows)
        total_models += len(rows)
        companies_with_models += 1
        print(f"[{i}/{len(companies)}] {org_used}: {len(models)} models")

    write_rows(all_rows)
    print(
        f"Saved {total_models} models from {companies_with_models} companies "
        f"to {PARQUET_OUT}"
    )


if __name__ == "__main__":
    main()
