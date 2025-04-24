import psycopg2
import json

# JSON data
commission_data = {
    "2016": {
        "1": {"commission_rate": 0.9204},
        "2": {"commission_rate": 2.4954},
        "3": {"commission_rate": 0.8856},
        "4": {"commission_rate": 0.7738},
        "5": {"commission_rate": 1.2777},
        "6": {"commission_rate": 1.0931},
        "7": {"commission_rate": 1.4103},
        "8": {"commission_rate": 1.3302},
        "9": {"commission_rate": 1.2715},
        "10": {"commission_rate": 1.5497},
        "11": {"commission_rate": 1.5374},
        "12": {"commission_rate": 0.9614}
    },
    "2017": {
        "1": {"commission_rate": 1.1853},
        "2": {"commission_rate": 0.7589},
        "3": {"commission_rate": 1.1466},
        "4": {"commission_rate": 1.3846},
        "5": {"commission_rate": 0.9099},
        "6": {"commission_rate": 0.9009},
        "7": {"commission_rate": 0.5917},
        "8": {"commission_rate": 0.7058},
        "9": {"commission_rate": 0.8452},
        "10": {"commission_rate": 1.1614},
        "11": {"commission_rate": 0.4232},
        "12": {"commission_rate": 0.5341}
    },
    "2018": {
        "1": {"commission_rate": 0.9324},
        "2": {"commission_rate": 1.0791},
        "3": {"commission_rate": 1.0487},
        "4": {"commission_rate": 0.4650},
        "5": {"commission_rate": 0.6963},
        "6": {"commission_rate": 0.3511},
        "7": {"commission_rate": 0.4808},
        "8": {"commission_rate": 0.7991},
        "9": {"commission_rate": 0.7773},
        "10": {"commission_rate": 0.6177},
        "11": {"commission_rate": 0.5104},
        "12": {"commission_rate": 0.9685}
    },
    "2019": {
        "1": {"commission_rate": 0.5519},
        "2": {"commission_rate": 0.7162},
        "3": {"commission_rate": 0.9182},
        "4": {"commission_rate": 0.4702},
        "5": {"commission_rate": 0.3104},
        "6": {"commission_rate": 0.2511},
        "7": {"commission_rate": 0.2675},
        "8": {"commission_rate": 0.2790},
        "9": {"commission_rate": 0.2186},
        "10": {"commission_rate": 0.1385},
        "11": {"commission_rate": 0.0000},
        "12": {"commission_rate": 0.1044}
    },
    "2020": {
        "1": {"commission_rate": 0.0030},
        "2": {"commission_rate": 2.8840},
        "3": {"commission_rate": 2.3923},
        "4": {"commission_rate": 13.7186},
        "5": {"commission_rate": 4.2993},
        "6": {"commission_rate": 1.5780},
        "7": {"commission_rate": 1.6775},
        "8": {"commission_rate": 2.8173}
    },
    "2024": {
        "1": {"commission_rate": 2.4057},
        "5": {"commission_rate": 2.5105},
        "6": {"commission_rate": 1.5097},
        "7": {"commission_rate": 1.0258},
        "8": {"commission_rate": 3.2261},
        "9": {"commission_rate": 2.4261},
        "10": {"commission_rate": 2.3970},
        "11": {"commission_rate": 2.4162},
        "12": {"commission_rate": 3.8136}
    },
    "2025": {
        "1": {"commission_rate": 2.3115},
        "2": {"commission_rate": 3.7200},
        "3": {"commission_rate": 2.3373}
    }
}

# Database connection details
db_params = {
    "host": "localhost",
    "dbname": "loan_tracker_multi_tenant",
    "user": "joshuaokello",
    "password": "Finafrica123"
    # "host": "prod-core-pg.cluster-clzcsbthrzqz.eu-central-1.rds.amazonaws.com",
    # "dbname": "loan_tracker_multi_tenant",
    # "user": "St6ye3_3e4T6",
    # "password": "TXRKjQNRJOq2WmUA"
}


def update_commission_rate(year, month, rate, cur):
    # Connect to the PostgreSQL database

    # SQL query to update the commission rate
    query = """
        UPDATE loans_loan
        SET agent_commission = loan_amount * %s
        WHERE 
            transaction_completed_date IS NOT NULL
            AND EXTRACT(MONTH FROM DATE(transaction_completed_date)) = %s
            AND EXTRACT(YEAR FROM DATE(transaction_completed_date)) = %s;
    """

    # Execute the query with the commission rate, month, and year
    cur.execute(query, (rate, month, year))

    # Commit changes and close the connection

    print(f"Commission rate updated for {year}-{month} with rate: {rate}")


def process_commission_data():
    try:
        conn = psycopg2.connect(**db_params)
        try:
            for year, months in commission_data.items():
                for month, data in months.items():
                    cur = conn.cursor()
                    cur.execute("SET search_path TO gb_firstcred;")
                    rate = data["commission_rate"]
                    update_commission_rate(int(year), int(month), (rate/100), cur)
                    conn.commit()
                    cur.close()
            conn.close()
        except Exception as e:
            print(f"Error: {e}")
            if conn:
                conn.rollback()
                conn.close()
    except Exception as e:
        print(f"Failed to connect: {e}")


# Start processing and updating commission rates
process_commission_data()
