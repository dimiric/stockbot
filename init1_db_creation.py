import psycopg2
import psycopg2.extras
import settings.config as conf

class NewTables:
    #Establish Postgresql connection based on settings in config.py
    conn = psycopg2.connect(
        dbname=conf.dbname,
        user=conf.dbuser,
        password=conf.dbpass,
        host=conf.dbhost,
        port=conf.dbport,
        sslmode=conf.dbsslmode
        )

    with conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Drop old tables if they exist in db from conf.dbname
        print(f"Deleting all tables.")
        cur.execute("DROP TABLE IF EXISTS fundamentals;")
        cur.execute("DROP TABLE IF EXISTS prices_daily;")
        cur.execute("DROP TABLE IF EXISTS prices_1min;")
        cur.execute("DROP TABLE IF EXISTS prices_5min;")
        cur.execute("DROP TABLE IF EXISTS prices_15min;")
        cur.execute("DROP TABLE IF EXISTS prices_30min;")
        cur.execute("DROP TABLE IF EXISTS prices_1hr;")
        cur.execute("DROP TABLE IF EXISTS prices_2hr;")
        cur.execute("DROP TABLE IF EXISTS prices_4hr;")
        # Delete "stocks" table last due to dependencies in other tables
        cur.execute("DROP TABLE IF EXISTS stocks;")

        # Create table "stocks" first
        print(f"Adding table: stocks")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                cusip TEXT NOT NULL,
                symbol TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                exchange TEXT NOT NULL,
                type TEXT NOT NULL,
                marginable TEXT NOT NULL,
                table_fundamentals INTEGER,
                table_daily INTEGER,
                table_1min INTEGER,
                table_5min INTEGER,
                table_15min INTEGER,
                table_30min INTEGER,
                table_1hr INTEGER,
                table_2hr INTEGER,
                table_4hr INTEGER,
                table_funda_milli INTEGER,
                table_daily_milli INTEGER,
                table_1min_milli INTEGER,
                table_5min_milli INTEGER,
                table_15min_milli INTEGER,
                table_30min_milli INTEGER,
                table_1hr_milli INTEGER,
                table_2hr_milli INTEGER,
                table_4hr_milli INTEGER,
                added_date DATE NOT NULL,
                updated_date DATE NOT NULL,
                status INTEGER
                );"""
            )

        # Create table fundamentals
        print(f"Adding table: fundamentals")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS fundamentals (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                high52 NUMERIC(16,6),
                low52 NUMERIC(16,6),
                dividendAmount NUMERIC(16,6),
                dividendYield NUMERIC(16,6),
                dividendDate DATE,
                peRatio NUMERIC(16,6),
                pegRatio NUMERIC(16,6),
                pbRatio NUMERIC(16,6),
                prRatio NUMERIC(16,6),
                pcfRatio NUMERIC(16,6),
                grossMarginTTM NUMERIC(16,6),
                grossMarginMRQ NUMERIC(16,6),
                netProfitMarginTTM NUMERIC(16,6),
                netProfitMarginMRQ NUMERIC(16,6),
                operatingMarginTTM NUMERIC(16,6),
                operatingMarginMRQ NUMERIC(16,6),
                returnOnEquity NUMERIC(16,6),
                returnOnAssets NUMERIC(16,6),
                returnOnInvestment NUMERIC(16,6),
                quickRatio NUMERIC(16,6),
                currentRatio NUMERIC(16,6),
                interestCoverage NUMERIC(16,6),
                totalDebtToCapital NUMERIC(16,6),
                ltDebtToEquity NUMERIC(16,6),
                totalDebtToEquity NUMERIC(16,6),
                epsTTM NUMERIC(16,6),
                epsChangePercentTTM NUMERIC(16,6),
                epsChangeYear NUMERIC(16,6),
                epsChange NUMERIC(16,6),
                revChangeYear NUMERIC(16,6),
                revChangeTTM NUMERIC(16,6),
                revChangeIn NUMERIC(16,6),
                sharesOutstanding NUMERIC(16,1),
                marketCapFloat NUMERIC(16,6),
                marketCap NUMERIC(16,6),
                bookValuePerShare NUMERIC(16,6),
                shortIntToFloat NUMERIC(16,6),
                shortIntDayToCover NUMERIC(16,6),
                divGrowthRate3Year NUMERIC(16,6),
                dividendPayAmount NUMERIC(16,6),
                dividendPayDate NUMERIC(16,6),
                beta NUMERIC(16,6),
                vol1DayAvg NUMERIC(16,6),
                vol10DayAvg NUMERIC(16,6),
                vol3MonthAvg NUMERIC(16,6),
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for daily prices  
        print(f"Adding table: prices_daily")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_daily (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for 1 minute prices
        print(f"Adding table: prices_1min")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_1min (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                tradingtime TIME,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for 5 minute prices
        print(f"Adding table: prices_5min")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_5min (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                tradingtime TIME,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for 15 minute prices
        print(f"Adding table: prices_15min")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_15min (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                tradingtime TIME,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for 30 minute prices
        print(f"Adding table: prices_30min")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_30min (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                tradingtime TIME,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for 1 hour prices
        print(f"Adding table: prices_1hr")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_1hr (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                tradingtime TIME,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for 2 hour prices
        print(f"Adding table: prices_2hr")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_2hr (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                tradingtime TIME,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        # Create table for 4 hour prices
        print(f"Adding table: prices_4hr")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS prices_4hr (
                id SERIAL PRIMARY KEY,
                stock_id INTEGER NOT NULL,
                datetime INTEGER,
                tradingday DATE,
                tradingtime TIME,
                open NUMERIC(16,6) NOT NULL,
                high NUMERIC(16,6) NOT NULL,
                low NUMERIC(16,6) NOT NULL,
                close NUMERIC(16,6) NOT NULL,
                volume NUMERIC(16,0) NOT NULL,
                hist_source INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
                );"""
            )

        #Commit changes to DB
        conn.commit()