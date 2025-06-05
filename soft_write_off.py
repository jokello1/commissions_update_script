from datetime import datetime

import psycopg2


ids = [
    12052
]

dates = [
    "25/3/2025"
]

# Database connection details
db_params = {
    "host": "10.10.1.9",
    "dbname": "loan_tracker_multi_tenant",
    "user": "postgres",
    "password": "Xskdc0wauicn2ucnaecasdsa7dnizucawencascdca@A"
    # "host": "prod-core-pg.cluster-clzcsbthrzqz.eu-central-1.rds.amazonaws.com",
    # "dbname": "loan_tracker_multi_tenant",
    # "user": "St6ye3_3e4T6",
    # "password": "TXRKjQNRJOq2WmUA"
}
def update_loan_sub_status(id,date,cur):
    # Connect to the PostgreSQL database

    date_obj = datetime.strptime(date, "%d/%m/%Y")

    # Format to desired output
    formatted_date = date_obj.strftime("%Y-%m-%d")

    # SQL query to update the sub_status and sub_status_date
    query = """
        UPDATE loans_loan
        SET
          sub_status = 'TWO',
          sub_status_date = %s
        WHERE id = %s;
    """

    # Execute the query with the id and date
    cur.execute(query, (id, formatted_date))

    # Commit changes and close the connection

    print(f"Status and date updated for {id}-{formatted_date}")


def process_loan_status_data():
    # Sanity check
    if len(ids) != len(dates):
        raise ValueError("IDs and dates lists must be the same length")

    table_name = "loans_loan"
    try:
        conn = psycopg2.connect(**db_params)
        try:
            for i, id_ in enumerate(ids):
                cur = conn.cursor()
                cur.execute("SET search_path TO fin_za;")
                update_loan_sub_status(ids[i], dates[i], cur)
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
process_loan_status_data()