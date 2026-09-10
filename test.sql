-- Inspect the DuckDB warehouse after dbt run.
-- From the repo root:
--   cd transform && dbt run --profiles-dir .
--   duckdb transform/warehouse.duckdb -f test.sql

-- How many rows landed in each table.
SELECT 'companies' AS table_name, count(*) AS n FROM sec_ai.companies
UNION ALL SELECT 'company_tickers', count(*) FROM sec_ai.company_tickers
UNION ALL SELECT 'filings', count(*) FROM sec_ai.filings
UNION ALL SELECT 'filing_companies', count(*) FROM sec_ai.filing_companies
UNION ALL SELECT 'models', count(*) FROM sec_ai.models
UNION ALL SELECT 'model_tags', count(*) FROM sec_ai.model_tags
ORDER BY table_name;

-- Companies and their tickers.
SELECT
    companies.cik,
    companies.name,
    string_agg(company_tickers.ticker, ', ' ORDER BY company_tickers.ticker) AS tickers
FROM sec_ai.companies
LEFT JOIN sec_ai.company_tickers
    ON companies.cik = company_tickers.cik
GROUP BY companies.cik, companies.name
ORDER BY companies.name
LIMIT 20;

-- Recent 10-K filings with the companies named on them.
SELECT
    filings.file_date,
    filings.form,
    filings.accession,
    string_agg(companies.name, ' | ' ORDER BY companies.name) AS companies
FROM sec_ai.filings
INNER JOIN sec_ai.filing_companies
    ON filings.file_id = filing_companies.file_id
INNER JOIN sec_ai.companies
    ON filing_companies.cik = companies.cik
GROUP BY filings.file_id, filings.file_date, filings.form, filings.accession
ORDER BY filings.file_date DESC NULLS LAST
LIMIT 20;

-- Filings that name more than one legal entity (parent + subsidiary).
SELECT
    filings.accession,
    filings.form,
    count(*) AS company_count,
    string_agg(companies.name, ' | ' ORDER BY companies.name) AS companies
FROM sec_ai.filings
INNER JOIN sec_ai.filing_companies
    ON filings.file_id = filing_companies.file_id
INNER JOIN sec_ai.companies
    ON filing_companies.cik = companies.cik
GROUP BY filings.file_id, filings.accession, filings.form
HAVING count(*) > 1
ORDER BY company_count DESC, filings.accession
LIMIT 20;

-- Hugging Face models linked back to an SEC company.
SELECT
    companies.name,
    models.org,
    models.model_id,
    models.pipeline_tag,
    models.downloads,
    models.likes
FROM sec_ai.models
INNER JOIN sec_ai.companies
    ON models.cik = companies.cik
ORDER BY models.downloads DESC NULLS LAST
LIMIT 20;

-- Models that did not match a company CIK.
SELECT model_id, org, downloads
FROM sec_ai.models
WHERE cik IS NULL
ORDER BY downloads DESC NULLS LAST
LIMIT 20;

-- Tags on the most-downloaded models.
SELECT
    models.model_id,
    models.downloads,
    string_agg(model_tags.tag, ', ' ORDER BY model_tags.tag) AS tags
FROM sec_ai.models
INNER JOIN sec_ai.model_tags
    ON models.model_id = model_tags.model_id
GROUP BY models.model_id, models.downloads
ORDER BY models.downloads DESC NULLS LAST
LIMIT 10;
