"""
db_engine.py
------------
Reads the two JSON files produced by financials_engine.py and loads them
into a local SQLite database (financial_data.db).

Run this AFTER financials_engine.py has finished:
    python db_engine.py

No extra libraries needed — sqlite3 and json are both part of Python's
standard library (no pip install required).

─────────────────────────────────────────────────────────────────────────
WHY THE DATA HAS FLAT '::' KEYS
─────────────────────────────────────────────────────────────────────────
Because FILTER_STATIC and FILTER_TIMESERIES use the EODHD '::' filter
syntax to select specific fields, the API returns those fields as flat
keys using the full filter path as the key name. For example:

    "General::Name": "Altri SGPS SA"      ← flat key, read directly
    "Highlights": { "PERatio": 31.9 }     ← still a nested dict

This means:
  - General fields  → read with data.get("General::Name") etc.
  - Highlights, Valuation, SharesStats, Technicals, AnalystRatings
    → still nested dicts, read with highlights.get("PERatio") etc.
  - Earnings::History, Financials::*::quarterly
    → flat keys, read with data.get("Earnings::History") etc.
"""

import sqlite3
import json
import os
from datetime import datetime


# ── Paths ──────────────────────────────────────────────────────────────────────
STATIC_JSON   = "./Outputs/Raw_data/raw_data_static.json"
DYNAMIC_JSON  = "./Outputs/Raw_data/raw_data_dynamic.json"
DATABASE_FILE = "./Outputs/financial_data.db"


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — HELPER
# ══════════════════════════════════════════════════════════════════════════════

def to_float(value):
    """
    Safely converts a value to float, returning None on failure.

    The EODHD API returns financial figures as strings ("379297000000.00").
    We convert them to numbers for the database. Python None passes through
    as-is and is stored as NULL in SQLite. "NA" strings also return None.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_dict(value):
    """
    Returns value if it is a dict, otherwise returns an empty dict.
    Protects against the API returning "NA" or another string instead of
    a dict for blocks like AnalystRatings.
    """
    return value if isinstance(value, dict) else {}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — CREATE TABLES
# ══════════════════════════════════════════════════════════════════════════════

def create_tables(db):
    """
    Creates the 4 tables if they do not already exist.

    'db' is the open database connection, created once in main() and passed
    in here so all functions share the same connection.

    IF NOT EXISTS makes this safe to call on every run — it never touches
    data that is already there.

    PRIMARY KEY is the unique identifier per row. SQLite rejects any INSERT
    that would create a duplicate, which is how duplicates are prevented.
    FOREIGN KEY means a security_code in a child table must already exist
    in the securities table — a safety net against orphaned rows.
    """
    cursor = db.cursor()

    # --- Table 1: securities ---
    # One row per listed stock. Populated from FILTER_STATIC General:: fields.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS securities (
            security_code       TEXT PRIMARY KEY,   -- e.g. 'EDP.LS'
            name                TEXT,
            exchange            TEXT,
            currency_code       TEXT,
            country_name        TEXT,
            isin                TEXT,
            fiscal_year_end     TEXT,
            sector              TEXT,
            industry            TEXT,
            gic_sector          TEXT,
            gic_group           TEXT,
            gic_industry        TEXT,
            gic_sub_industry    TEXT,
            is_delisted         INTEGER,            -- 0 = listed, 1 = delisted
            description         TEXT,
            web_url             TEXT,
            scraped_at          TEXT                -- ISO timestamp of last refresh
        )
    """)

    # --- Table 2: highlights ---
    # One row per stock. Holds Highlights, Valuation, SharesStats,
    # Technicals and AnalystRatings — all flattened into columns.
    # FOREIGN KEY: security_code must exist in securities table.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS highlights (
            security_code                   TEXT PRIMARY KEY,
            market_cap                      REAL,
            ebitda                          REAL,
            pe_ratio                        REAL,
            peg_ratio                       REAL,
            wall_street_target_price        REAL,
            book_value                      REAL,
            dividend_share                  REAL,
            dividend_yield                  REAL,
            earnings_share                  REAL,
            eps_estimate_current_year       REAL,
            eps_estimate_next_year          REAL,
            profit_margin                   REAL,
            operating_margin_ttm            REAL,
            return_on_assets_ttm            REAL,
            return_on_equity_ttm            REAL,
            revenue_ttm                     REAL,
            quarterly_revenue_growth_yoy    REAL,
            quarterly_earnings_growth_yoy   REAL,
            trailing_pe                     REAL,
            forward_pe                      REAL,
            price_sales_ttm                 REAL,
            price_book_mrq                  REAL,
            enterprise_value                REAL,
            shares_outstanding              REAL,
            shares_float                    REAL,
            percent_insiders                REAL,
            percent_institutions            REAL,
            beta                            REAL,
            week_52_high                    REAL,
            week_52_low                     REAL,
            ma_50_day                       REAL,
            ma_200_day                      REAL,
            analyst_rating                  REAL,
            analyst_target_price            REAL,
            analyst_strong_buy              INTEGER,
            analyst_buy                     INTEGER,
            analyst_hold                    INTEGER,
            analyst_sell                    INTEGER,
            analyst_strong_sell             INTEGER,
            scraped_at                      TEXT,
            FOREIGN KEY (security_code) REFERENCES securities(security_code)
        )
    """)

    # --- Table 3: financials_quarterly ---
    # Multiple rows per stock. Each row = one stock × one statement type × one quarter.
    # The PRIMARY KEY is the combination of all three — none alone is unique.
    #
    # All three statement types (Balance_Sheet, Cash_Flow, Income_Statement) share
    # this table, distinguished by the statement_type column. A Balance Sheet row
    # will have NULL in income statement columns and vice versa — this is expected.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financials_quarterly (
            security_code               TEXT,
            statement_type              TEXT,   -- 'Balance_Sheet', 'Cash_Flow', 'Income_Statement'
            period_date                 TEXT,   -- e.g. '2025-09-30'
            filing_date                 TEXT,
            currency_symbol             TEXT,
            -- Balance Sheet
            total_assets                REAL,
            total_liabilities           REAL,
            total_stockholder_equity    REAL,
            cash                        REAL,
            cash_and_equivalents        REAL,
            short_term_investments      REAL,
            net_receivables             REAL,
            inventory                   REAL,
            total_current_assets        REAL,
            total_current_liabilities   REAL,
            long_term_debt              REAL,
            short_term_debt             REAL,
            accounts_payable            REAL,
            retained_earnings           REAL,
            common_stock                REAL,
            goodwill                    REAL,
            property_plant_equipment    REAL,
            net_working_capital         REAL,
            net_debt                    REAL,
            shares_outstanding          REAL,
            -- Cash Flow
            total_cashflows_from_ops    REAL,
            capital_expenditures        REAL,
            investments                 REAL,
            other_cashflows_from_ops    REAL,
            total_cashflows_financing   REAL,
            net_borrowings              REAL,
            free_cash_flow              REAL,
            change_in_cash              REAL,
            -- Income Statement
            total_revenue               REAL,
            cost_of_revenue             REAL,
            gross_profit                REAL,
            research_development        REAL,
            selling_general_admin       REAL,
            total_operating_expenses    REAL,
            operating_income            REAL,
            ebit                        REAL,
            ebitda                      REAL,
            net_income                  REAL,
            income_before_tax           REAL,
            income_tax_expense          REAL,
            interest_expense            REAL,
            depreciation_amortization   REAL,
            PRIMARY KEY (security_code, statement_type, period_date),
            FOREIGN KEY (security_code) REFERENCES securities(security_code)
        )
    """)

    # --- Table 4: earnings_history ---
    # Multiple rows per stock. Each row = one stock × one quarter-end date.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS earnings_history (
            security_code       TEXT,
            period_date         TEXT,   -- quarter end date, e.g. '2025-12-31'
            report_date         TEXT,   -- actual publication date
            before_after_market TEXT,
            currency            TEXT,
            eps_actual          REAL,
            eps_estimate        REAL,
            eps_difference      REAL,
            surprise_percent    REAL,
            PRIMARY KEY (security_code, period_date),
            FOREIGN KEY (security_code) REFERENCES securities(security_code)
        )
    """)

    db.commit()
    print("Tables ready.")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — LOAD STATIC DATA
# ══════════════════════════════════════════════════════════════════════════════

def load_static(db, raw_data_static):
    """
    Iterates over raw_data_static and upserts into 'securities' and 'highlights'.

    INSERT OR REPLACE is the upsert: if a row with the same PRIMARY KEY already
    exists it is replaced with the new values, so re-running the script monthly
    refreshes data without ever creating duplicates.

    The three-level structure we navigate:
        raw_data_static
        └── country  ("Portugal")
            └── country_venue_key  ("Portugal.LS")
                └── security_code  ("EDP.LS")
                    └── { API response for this stock }

    Inside that dict, General fields are FLAT keys ("General::Name") because
    FILTER_STATIC used the :: syntax to select them individually. The blocks
    Highlights, Valuation, SharesStats, Technicals, AnalystRatings were
    requested as whole blocks so they remain as normal nested dicts.
    """
    cursor = db.cursor()
    scraped_at = datetime.now().isoformat()
    securities_inserted = 0
    highlights_inserted = 0

    for country, venues in raw_data_static.items():
        for venue_key, securities in venues.items():
            for security_code, data in securities.items():

                # General fields: flat keys because they were filtered individually
                # with ::  e.g. "General::Name", "General::ISIN"
                # safe_str returns None if the value is "NA"
                def safe_str(val):
                    return None if val in (None, "NA") else val

                # Nested blocks: requested as whole blocks so still dicts
                highlights = safe_dict(data.get("Highlights"))
                valuation  = safe_dict(data.get("Valuation"))
                shares     = safe_dict(data.get("SharesStats"))
                technicals = safe_dict(data.get("Technicals"))
                ratings    = safe_dict(data.get("AnalystRatings"))

                # ── securities ────────────────────────────────────────────
                cursor.execute("""
                    INSERT OR REPLACE INTO securities (
                        security_code, name, exchange, currency_code,
                        country_name, isin, fiscal_year_end,
                        sector, industry,
                        gic_sector, gic_group, gic_industry, gic_sub_industry,
                        is_delisted, description, web_url,
                        scraped_at
                    ) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                """, (
                    security_code,
                    safe_str(data.get("General::Name")),
                    safe_str(data.get("General::Exchange")),
                    safe_str(data.get("General::CurrencyCode")),
                    safe_str(data.get("General::CountryName")),
                    safe_str(data.get("General::ISIN")),
                    safe_str(data.get("General::FiscalYearEnd")),
                    safe_str(data.get("General::Sector")),
                    safe_str(data.get("General::Industry")),
                    safe_str(data.get("General::GicSector")),
                    safe_str(data.get("General::GicGroup")),
                    safe_str(data.get("General::GicIndustry")),
                    safe_str(data.get("General::GicSubIndustry")),
                    None if data.get("General::IsDelisted") in (None, "NA") else (1 if data.get("General::IsDelisted") else 0),
                    safe_str(data.get("General::Description")),
                    safe_str(data.get("General::WebURL")),
                    scraped_at,
                ))
                securities_inserted += 1

                # ── highlights ────────────────────────────────────────────
                cursor.execute("""
                    INSERT OR REPLACE INTO highlights (
                        security_code,
                        market_cap, ebitda, pe_ratio, peg_ratio,
                        wall_street_target_price, book_value,
                        dividend_share, dividend_yield, earnings_share,
                        eps_estimate_current_year, eps_estimate_next_year,
                        profit_margin, operating_margin_ttm,
                        return_on_assets_ttm, return_on_equity_ttm,
                        revenue_ttm,
                        quarterly_revenue_growth_yoy,
                        quarterly_earnings_growth_yoy,
                        trailing_pe, forward_pe,
                        price_sales_ttm, price_book_mrq, enterprise_value,
                        shares_outstanding, shares_float,
                        percent_insiders, percent_institutions,
                        beta, week_52_high, week_52_low,
                        ma_50_day, ma_200_day,
                        analyst_rating, analyst_target_price,
                        analyst_strong_buy, analyst_buy, analyst_hold,
                        analyst_sell, analyst_strong_sell,
                        scraped_at
                    ) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                """, (
                    security_code,
                    to_float(highlights.get("MarketCapitalization")),
                    to_float(highlights.get("EBITDA")),
                    to_float(highlights.get("PERatio")),
                    to_float(highlights.get("PEGRatio")),
                    to_float(highlights.get("WallStreetTargetPrice")),
                    to_float(highlights.get("BookValue")),
                    to_float(highlights.get("DividendShare")),
                    to_float(highlights.get("DividendYield")),
                    to_float(highlights.get("EarningsShare")),
                    to_float(highlights.get("EPSEstimateCurrentYear")),
                    to_float(highlights.get("EPSEstimateNextYear")),
                    to_float(highlights.get("ProfitMargin")),
                    to_float(highlights.get("OperatingMarginTTM")),
                    to_float(highlights.get("ReturnOnAssetsTTM")),
                    to_float(highlights.get("ReturnOnEquityTTM")),
                    to_float(highlights.get("RevenueTTM")),
                    to_float(highlights.get("QuarterlyRevenueGrowthYOY")),
                    to_float(highlights.get("QuarterlyEarningsGrowthYOY")),
                    to_float(valuation.get("TrailingPE")),
                    to_float(valuation.get("ForwardPE")),
                    to_float(valuation.get("PriceSalesTTM")),
                    to_float(valuation.get("PriceBookMRQ")),
                    to_float(valuation.get("EnterpriseValue")),
                    to_float(shares.get("SharesOutstanding")),
                    to_float(shares.get("SharesFloat")),
                    to_float(shares.get("PercentInsiders")),
                    to_float(shares.get("PercentInstitutions")),
                    to_float(technicals.get("Beta")),
                    to_float(technicals.get("52WeekHigh")),
                    to_float(technicals.get("52WeekLow")),
                    to_float(technicals.get("50DayMA")),
                    to_float(technicals.get("200DayMA")),
                    to_float(ratings.get("Rating")),
                    to_float(ratings.get("TargetPrice")),
                    ratings.get("StrongBuy"),
                    ratings.get("Buy"),
                    ratings.get("Hold"),
                    ratings.get("Sell"),
                    ratings.get("StrongSell"),
                    scraped_at,
                ))
                highlights_inserted += 1

    db.commit()
    print(f"  securities  : {securities_inserted} rows upserted")
    print(f"  highlights  : {highlights_inserted} rows upserted")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — LOAD TIMESERIES DATA
# ══════════════════════════════════════════════════════════════════════════════

def load_timeseries(db, raw_data_dynamic):
    """
    Iterates over raw_data_dynamic and upserts into 'financials_quarterly'
    and 'earnings_history'.

    Because FILTER_TIMESERIES selected each block with :: syntax, the API
    returned them as flat keys:

        data
        ├── "Earnings::History"                      → dict of quarters
        ├── "Financials::Balance_Sheet::quarterly"   → dict of quarters
        ├── "Financials::Cash_Flow::quarterly"       → dict of quarters
        └── "Financials::Income_Statement::quarterly"→ dict of quarters

    We read these directly with data.get("Earnings::History") etc., then
    loop over the date keys inside each dict to write one row per quarter.
    That date-key loop is the 'unpivoting' step.
    """
    cursor = db.cursor()
    earnings_inserted   = 0
    financials_inserted = 0

    for country, venues in raw_data_dynamic.items():
        for venue_key, securities in venues.items():
            for security_code, data in securities.items():

                # ── earnings_history ──────────────────────────────────────
                # Flat key because Earnings::History was filtered with ::
                history = safe_dict(data.get("Earnings::History"))

                for period_date, row in history.items():
                    cursor.execute("""
                        INSERT OR REPLACE INTO earnings_history (
                            security_code, period_date, report_date,
                            before_after_market, currency,
                            eps_actual, eps_estimate,
                            eps_difference, surprise_percent
                        ) VALUES (?,?,?,?,?,?,?,?,?)
                    """, (
                        security_code,
                        period_date,
                        row.get("reportDate"),
                        row.get("beforeAfterMarket"),
                        row.get("currency"),
                        to_float(row.get("epsActual")),
                        to_float(row.get("epsEstimate")),
                        to_float(row.get("epsDifference")),
                        to_float(row.get("surprisePercent")),
                    ))
                    earnings_inserted += 1

                # ── financials_quarterly ──────────────────────────────────
                # Flat keys because each was filtered with :: syntax
                statement_map = {
                    "Balance_Sheet":    safe_dict(data.get("Financials::Balance_Sheet::quarterly")),
                    "Cash_Flow":        safe_dict(data.get("Financials::Cash_Flow::quarterly")),
                    "Income_Statement": safe_dict(data.get("Financials::Income_Statement::quarterly")),
                }

                for statement_type, quarters in statement_map.items():
                    for period_date, row in quarters.items():
                        cursor.execute("""
                            INSERT OR REPLACE INTO financials_quarterly (
                                security_code, statement_type, period_date,
                                filing_date, currency_symbol,
                                total_assets, total_liabilities,
                                total_stockholder_equity,
                                cash, cash_and_equivalents,
                                short_term_investments, net_receivables,
                                inventory,
                                total_current_assets, total_current_liabilities,
                                long_term_debt, short_term_debt,
                                accounts_payable, retained_earnings,
                                common_stock, goodwill,
                                property_plant_equipment,
                                net_working_capital, net_debt,
                                shares_outstanding,
                                total_cashflows_from_ops, capital_expenditures,
                                investments, other_cashflows_from_ops,
                                total_cashflows_financing, net_borrowings,
                                free_cash_flow, change_in_cash,
                                total_revenue, cost_of_revenue, gross_profit,
                                research_development, selling_general_admin,
                                total_operating_expenses, operating_income,
                                ebit, ebitda, net_income,
                                income_before_tax, income_tax_expense,
                                interest_expense, depreciation_amortization
                            ) VALUES (
                                ?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?
                            )
                        """, (
                            security_code,
                            statement_type,
                            period_date,
                            row.get("filing_date"),
                            row.get("currency_symbol"),
                            # Balance Sheet
                            to_float(row.get("totalAssets")),
                            to_float(row.get("totalLiab")),
                            to_float(row.get("totalStockholderEquity")),
                            to_float(row.get("cash")),
                            to_float(row.get("cashAndEquivalents")),
                            to_float(row.get("shortTermInvestments")),
                            to_float(row.get("netReceivables")),
                            to_float(row.get("inventory")),
                            to_float(row.get("totalCurrentAssets")),
                            to_float(row.get("totalCurrentLiabilities")),
                            to_float(row.get("longTermDebt")),
                            to_float(row.get("shortTermDebt")),
                            to_float(row.get("accountsPayable")),
                            to_float(row.get("retainedEarnings")),
                            to_float(row.get("commonStock")),
                            to_float(row.get("goodWill")),
                            to_float(row.get("propertyPlantEquipment")),
                            to_float(row.get("netWorkingCapital")),
                            to_float(row.get("netDebt")),
                            to_float(row.get("commonStockSharesOutstanding")),
                            # Cash Flow
                            to_float(row.get("totalCashFromOperatingActivities")),
                            to_float(row.get("capitalExpenditures")),
                            to_float(row.get("investments")),
                            to_float(row.get("otherCashflowsFromOperatingActivities")),
                            to_float(row.get("totalCashflowsFromFinancingActivities")),
                            to_float(row.get("netBorrowings")),
                            to_float(row.get("freeCashFlow")),
                            to_float(row.get("changeInCash")),
                            # Income Statement
                            to_float(row.get("totalRevenue")),
                            to_float(row.get("costOfRevenue")),
                            to_float(row.get("grossProfit")),
                            to_float(row.get("researchDevelopment")),
                            to_float(row.get("sellingGeneralAdministrative")),
                            to_float(row.get("totalOperatingExpenses")),
                            to_float(row.get("operatingIncome")),
                            to_float(row.get("ebit")),
                            to_float(row.get("ebitda")),
                            to_float(row.get("netIncome")),
                            to_float(row.get("incomeBeforeTax")),
                            to_float(row.get("incomeTaxExpense")),
                            to_float(row.get("interestExpense")),
                            to_float(row.get("depreciationAndAmortization")),
                        ))
                        financials_inserted += 1

    db.commit()
    print(f"  earnings_history    : {earnings_inserted} rows upserted")
    print(f"  financials_quarterly: {financials_inserted} rows upserted")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """
    Entry point. Opens the database, runs all load functions, then closes.

    sqlite3.connect() creates financial_data.db if it does not exist yet,
    or opens it quietly if it already does.

    db.rollback() inside the except block undoes any rows written in the
    current run but not yet committed, so the database is never left in a
    half-written state after an error.
    """
    os.makedirs(os.path.dirname(DATABASE_FILE), exist_ok=True)
    print(f"Opening database: {DATABASE_FILE}")
    db = sqlite3.connect(DATABASE_FILE)
    db.execute("PRAGMA foreign_keys = OFF")

    try:
        print("\n── Step 1: create tables ──")
        create_tables(db)

        print("\n── Step 2: load static data ──")
        if os.path.exists(STATIC_JSON):
            with open(STATIC_JSON, mode="r", encoding="utf-8") as f:
                raw_static = json.load(f)
            load_static(db, raw_static)
        else:
            print(f"  WARNING: {STATIC_JSON} not found — run financials_engine.py first.")

        print("\n── Step 3: load timeseries data ──")
        if os.path.exists(DYNAMIC_JSON):
            with open(DYNAMIC_JSON, mode="r", encoding="utf-8") as f:
                raw_dynamic = json.load(f)
            load_timeseries(db, raw_dynamic)
        else:
            print(f"  WARNING: {DYNAMIC_JSON} not found — run financials_engine.py first.")

        print(f"\nDone. Database at: {DATABASE_FILE}")

    except Exception as e:
        db.rollback()
        print(f"\nERROR — changes rolled back: {e}")
        raise

    finally:
        db.close()


if __name__ == "__main__":
    main()