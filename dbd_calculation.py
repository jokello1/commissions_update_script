# import pandas as pd
# import psycopg
#
#
# def fetch_data(report_date: str, threshold: float, conn_params: dict) -> pd.DataFrame:
#     """
#     Simplified SQL query - we'll do the complex FIFO logic in Python
#     """
#     sql = """
#     WITH schedule_data AS (
#         SELECT
#             ssfls.loan_id,
#             ssfls."ScheduleDate",
#             (COALESCE(ssfls.principal_amount, 0)
#               + COALESCE(ssfls.interest_amount, 0)
#               + COALESCE(ssfls.fee_charges_amount, 0)
#               + COALESCE(ssfls.penalty_charges_amount, 0)) AS expected_installment
#         FROM dbt_dom_za.fact_finconnect__simple_loan_schedule ssfls
#         WHERE ssfls."ScheduleDate" <= CAST(%(report_date)s AS date)
#     ),
#
#     repayment AS (
#         SELECT
#             sflt.loan_id,
#             date_trunc('month', sflt.transaction_date)::date AS txn_month,
#             SUM(COALESCE(sflt.amount, 0)) AS monthly_received
#         FROM dbt_dom_za.stg_fineract__loan_transaction sflt
#         WHERE sflt.is_reversed IS NOT TRUE
#           AND sflt.transaction_type_enum = '2'
#           AND sflt.transaction_date <= %(report_date)s
#         GROUP BY sflt.loan_id, date_trunc('month', sflt.transaction_date)
#     ),
#
#     arrears AS (
#         SELECT
#             laa.loan_id,
#             laa.overdue_since_date_derived::date AS overdue_date,
#             COALESCE(laa.total_overdue_derived, 0) AS arrears_snapshot,
#             GREATEST(0, ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date))::int AS dpd,
#             CASE
#                 WHEN laa.overdue_since_date_derived IS NULL THEN 'Paid'
#                 WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) BETWEEN 0 AND 31 THEN 'Current'
#                 WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) < 61 THEN '30 Days'
#                 WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) < 91 THEN '60 Days'
#                 ELSE '90 Days+'
#             END AS dpd_bucket
#         FROM dbt_dom_za.stg_fineract__loan_arrears_aging laa
#     )
#
#     SELECT
#         sd.loan_id,
#         sd."ScheduleDate" as schedule_date,
#         sd.expected_installment,
#         COALESCE(r.monthly_received, 0) AS installment_received,
#         COALESCE(a.dpd, 0) as dpd,
#         COALESCE(a.dpd_bucket, 'Paid') as dpd_bucket,
#         a.arrears_snapshot,
#         ROW_NUMBER() OVER (PARTITION BY sd.loan_id ORDER BY sd."ScheduleDate") as installment_number
#     FROM schedule_data sd
#     LEFT JOIN repayment r
#         ON r.loan_id = sd.loan_id
#         AND r.txn_month = date_trunc('month', sd."ScheduleDate")
#     LEFT JOIN arrears a
#         ON a.loan_id = sd.loan_id
#     ORDER BY sd.loan_id, sd."ScheduleDate"
#     """
#
#     with psycopg.connect(**conn_params) as conn:
#         return pd.read_sql(sql, conn, params={"report_date": report_date})
#
#
# def apply_fifo_allocation(df: pd.DataFrame, threshold: float, report_date: str) -> pd.DataFrame:
#     """
#     Apply FIFO payment allocation logic:
#     1. Use first installment's expected amount as standard for all installments
#     2. Allocate payments chronologically with carryover
#     """
#
#     def process_loan_group(group):
#         group = group.copy().sort_values('schedule_date')
#
#         # Step 1: Use first expected payment as standard for all installments
#         first_expected = group.iloc[0]['expected_installment']
#         group['standardized_expected'] = first_expected
#
#         # Step 2: Apply FIFO allocation - fully pay installments from beginning
#         payments = group['installment_received'].tolist()
#         total_payments_available = sum(payments)  # Total money available
#         allocated_payments = []
#         cumulative_expected = []
#         running_expected = 0
#         money_used = 0
#
#         for i, (_, row) in enumerate(group.iterrows()):
#             expected = first_expected
#             running_expected += expected
#             cumulative_expected.append(running_expected)
#
#             # Try to fully pay this installment if we have money available
#             money_remaining = total_payments_available - money_used
#
#             if money_remaining >= expected:
#                 # Can fully pay this installment
#                 allocated = expected
#                 money_used += expected
#             else:
#                 # Can only partially pay with remaining money
#                 allocated = money_remaining
#                 money_used += money_remaining
#
#             allocated_payments.append(allocated)
#
#         # Update the group with calculated values
#         group['allocated_payment'] = allocated_payments  # Individual installment allocation
#         group['cumulative_expected'] = cumulative_expected
#         # Total Received should show the individual allocated amount, not cumulative
#         group['total_received'] = allocated_payments
#         group['arrears_amount'] = group['standardized_expected'] - group['allocated_payment']
#         group['arrears_threshold'] = threshold * first_expected
#
#         # Payment status flags
#         group['paid'] = (group['total_received'] >= group['standardized_expected']).astype(int)
#         group['due'] = (group['arrears_amount'] > group['arrears_threshold']).astype(int)
#
#         # Calculate days past due based on schedule
#         report_date_dt = pd.to_datetime(report_date)
#         schedule_date_dt = pd.to_datetime(group['schedule_date'])
#         group['days_past_due'] = (report_date_dt - schedule_date_dt).dt.days
#         group['days_past_due'] = group['days_past_due'].clip(lower=0)
#
#         # Months past due (CD)
#         group['months_past_due'] = (group['days_past_due'] / 30).astype(int)
#
#         return group
#
#     # Process each loan separately
#     result = df.groupby('loan_id', group_keys=False).apply(process_loan_group)
#
#     # Format final output columns
#     result['Date'] = pd.to_datetime(report_date).strftime('%Y/%m/%d')
#     result['Loan Id'] = result['loan_id']
#     result['Due Date'] = pd.to_datetime(result['schedule_date']).dt.strftime('%Y/%m/%d')
#     result['Expected Payment'] = result['standardized_expected'].round(2)
#     result['Instalment Amount'] = result['installment_received'].round(2)  # Original received amount
#     result['Total Payments'] = result['cumulative_expected'].round(2)  # Cumulative expected
#     result['Total Received'] = result['total_received'].round(2)  # Individual allocated amount
#     result['Arrears Amount'] = result['arrears_amount'].round(2)
#     result['Arrears Threshold'] = result['arrears_threshold'].round(2)
#     result['DPD'] = result['days_past_due']
#     result['CD'] = result['months_past_due']
#     result['Paid'] = result['paid']
#     result['Due'] = result['due']
#     result['DPD Bucket'] = result['dpd_bucket']
#
#     # Aggregate arrears by bucket (simplified)
#     result['Current'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == 'Current' else 0, axis=1)
#     result['30 Days'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == '30 Days' else 0, axis=1)
#     result['60 Days'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == '60 Days' else 0, axis=1)
#     result['90 Days+'] = result.apply(lambda x: x['arrears_amount'] if '90' in str(x['dpd_bucket']) else 0, axis=1)
#
#     # Select and order final columns
#     output_columns = [
#         'Date', 'Loan Id', 'Total Payments', 'Arrears Threshold', 'Due Date',
#         'Expected Payment', 'Instalment Amount', 'Total Received', 'Due',
#         'Arrears Amount', 'Paid', 'DPD', 'DPD Bucket', 'CD',
#         'Current', '30 Days', '60 Days', '90 Days+'
#     ]
#
#     return result[output_columns]
#
#
# def create_example_data():
#     """Create example data to demonstrate the logic"""
#     example_data = {
#         'loan_id': [1, 1, 1, 1, 2, 2, 2],
#         'schedule_date': [
#             '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01',
#             '2024-01-01', '2024-02-01', '2024-03-01'
#         ],
#         'expected_installment': [1000, 1000, 1000, 1000, 500, 500, 500],
#         'installment_received': [800, 1200, 0, 500, 300, 400, 600],
#         'dpd': [0, 0, 30, 60, 0, 15, 45],
#         'dpd_bucket': ['Current', 'Current', '30 Days', '60 Days', 'Current', 'Current', '30 Days'],
#         'arrears_snapshot': [0, 0, 200, 700, 0, 100, 200],
#         'installment_number': [1, 2, 3, 4, 1, 2, 3]
#     }
#
#     return pd.DataFrame(example_data)
#
#
# def demonstrate_fifo_logic():
#     """Demonstrate the FIFO logic step by step"""
#     print("=== FIFO Logic Demonstration ===")
#     print("Loan 1:")
#     print("Expected: [1000, 1000, 1000, 1000]")
#     print("Received: [800, 1200, 0, 500]")
#     print("Total Available Money: 800 + 1200 + 0 + 500 = 2500")
#     print()
#
#     # Manual calculation to show logic
#     expected = [1000, 1000, 1000, 1000]
#     received = [800, 1200, 0, 500]
#     total_available = sum(received)  # 2500
#
#     money_used = 0
#     allocated = []
#
#     print("FIFO Allocation (pay installments fully from beginning):")
#     for i in range(len(expected)):
#         money_remaining = total_available - money_used
#
#         if money_remaining >= expected[i]:
#             # Can fully pay this installment
#             this_allocation = expected[i]
#             money_used += expected[i]
#         else:
#             # Can only partially pay
#             this_allocation = money_remaining
#             money_used += money_remaining
#
#         allocated.append(this_allocation)
#
#         print(f"Installment {i + 1}:")
#         print(f"  Expected: {expected[i]}")
#         print(f"  Money remaining in pool: {money_remaining}")
#         print(f"  Allocated to this installment: {this_allocation}")
#         print(f"  Arrears for this installment: {expected[i] - this_allocation}")
#         print(f"  Total money used so far: {money_used}")
#         print()
#
#     print("Final Total Received (individual amounts):", allocated)
#     print("Logic: Use all available money to pay installments from the beginning")
#     print()
#     return allocated
#
#
# def main():
#     # Database connection parameters
#     conn_params = {
#         "dbname": "warehouse",
#         "user": "St6ye3_3e4T6",
#         "password": "TXRKjQNRJOq2WmUA",
#         "host": "10.10.1.9",
#         "port": "5432"
#     }
#
#     report_date = "2024-01-31"
#     threshold = 0.1  # 10% threshold for arrears
#
#     print("=== FIFO Payment Allocation Demo ===\n")
#     """
#     # First show the manual logic
#     demonstrate_fifo_logic()
#
#     # For demonstration, let's use example data
#     print("1. Using example data to demonstrate logic:")
#     df_example = create_example_data()
#     print("Original Data:")
#     print(df_example[['loan_id', 'expected_installment', 'installment_received']])
#     print("\n")
#
#     result_example = apply_fifo_allocation(df_example, threshold, report_date)
#     print("After FIFO Allocation:")
#     print("Key columns explanation:")
#     print("- Expected Payment: Standardized amount (from first installment)")
#     print("- Instalment Amount: Original payment received")
#     print("- Total Payments: Cumulative expected payments")
#     print("- Total Received: FIFO allocated amount for this installment")
#     print("- Arrears Amount: Difference between expected and allocated for this installment")
#     print()
#     print(result_example[['Loan Id', 'Expected Payment', 'Instalment Amount',
#                           'Total Payments', 'Total Received', 'Arrears Amount']].head(10))
#     print("\n")
#     """
#     # Uncomment below to use real database data
#
#     print("2. Fetching real data from database...")
#     df_real = fetch_data(report_date, threshold, conn_params)
#     print(f"Fetched {len(df_real)} rows")
#
#     print("3. Applying FIFO allocation...")
#     result_real = apply_fifo_allocation(df_real, threshold, report_date)
#
#     print("4. Sample output:")
#     print(result_real.head(20))
#
#     # Save results
#     result_real.to_csv("fifo_loan_allocation_report.csv", index=False)
#     print("Results saved to fifo_loan_allocation_report.csv")
#
#
#
# if __name__ == "__main__":
#     main()
# import pandas as pd
# import psycopg
#
#
# def fetch_data(report_date: str, threshold: float, conn_params: dict) -> pd.DataFrame:
#     """
#     Simplified SQL query - we'll do the complex FIFO logic in Python
#     """
#     sql = """
#     WITH schedule_data AS (
#         SELECT
#             ssfls.loan_id,
#             ssfls."ScheduleDate",
#             (COALESCE(ssfls.principal_amount, 0)
#               + COALESCE(ssfls.interest_amount, 0)
#               + COALESCE(ssfls.fee_charges_amount, 0)
#               + COALESCE(ssfls.penalty_charges_amount, 0)) AS expected_installment
#         FROM dbt_dom_za.fact_finconnect__simple_loan_schedule ssfls
#         WHERE ssfls."ScheduleDate" <= CAST(%(report_date)s AS date)
#     ),
#
#     repayment AS (
#         SELECT
#             sflt.loan_id,
#             date_trunc('month', sflt.transaction_date)::date AS txn_month,
#             SUM(COALESCE(sflt.amount, 0)) AS monthly_received
#         FROM dbt_dom_za.stg_fineract__loan_transaction sflt
#         WHERE sflt.is_reversed IS NOT TRUE
#           AND sflt.transaction_type_enum = '2'
#           AND sflt.transaction_date <= %(report_date)s
#         GROUP BY sflt.loan_id, date_trunc('month', sflt.transaction_date)
#     ),
#
#     arrears AS (
#         SELECT
#             laa.loan_id,
#             laa.overdue_since_date_derived::date AS overdue_date,
#             COALESCE(laa.total_overdue_derived, 0) AS arrears_snapshot,
#             GREATEST(0, ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date))::int AS dpd,
#             CASE
#                 WHEN laa.overdue_since_date_derived IS NULL THEN 'Paid'
#                 WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) BETWEEN 0 AND 31 THEN 'Current'
#                 WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) < 61 THEN '30 Days'
#                 WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) < 91 THEN '60 Days'
#                 ELSE '90 Days+'
#             END AS dpd_bucket
#         FROM dbt_dom_za.stg_fineract__loan_arrears_aging laa
#     )
#
#     SELECT
#         sd.loan_id,
#         sd."ScheduleDate" as schedule_date,
#         sd.expected_installment,
#         COALESCE(r.monthly_received, 0) AS installment_received,
#         COALESCE(a.dpd, 0) as dpd,
#         COALESCE(a.dpd_bucket, 'Paid') as dpd_bucket,
#         a.arrears_snapshot,
#         ROW_NUMBER() OVER (PARTITION BY sd.loan_id ORDER BY sd."ScheduleDate") as installment_number
#     FROM schedule_data sd
#     LEFT JOIN repayment r
#         ON r.loan_id = sd.loan_id
#         AND r.txn_month = date_trunc('month', sd."ScheduleDate")
#     LEFT JOIN arrears a
#         ON a.loan_id = sd.loan_id
#     ORDER BY sd.loan_id, sd."ScheduleDate"
#     """
#
#     with psycopg.connect(**conn_params) as conn:
#         return pd.read_sql(sql, conn, params={"report_date": report_date})
#
#
# def apply_fifo_allocation(df: pd.DataFrame, threshold: float, report_date: str) -> pd.DataFrame:
#     """
#     Apply FIFO payment allocation logic:
#     1. Use first installment's expected amount as standard for all installments
#     2. Allocate payments chronologically with carryover
#     """
#
#     def process_loan_group(group):
#         group = group.copy().sort_values('schedule_date')
#
#         # Step 1: Handle zero expected installments and standardize expected payments
#         # If expected is 0 but installment received > 0, use the received amount as expected
#         group['adjusted_expected'] = group.apply(
#             lambda row: row['installment_received'] if row['expected_installment'] == 0 and row[
#                 'installment_received'] > 0
#             else row['expected_installment'], axis=1
#         )
#
#         # Use first non-zero expected payment as standard for all installments
#         first_expected = group[group['adjusted_expected'] > 0]['adjusted_expected'].iloc[0] if len(
#             group[group['adjusted_expected'] > 0]) > 0 else group.iloc[0]['adjusted_expected']
#         group['standardized_expected'] = first_expected
#
#         # Step 2: Apply FIFO allocation - fully pay installments from beginning
#         payments = group['installment_received'].tolist()
#         total_payments_available = sum(payments)  # Total money available
#         allocated_payments = []
#         cumulative_expected = []
#         running_expected = 0
#         money_used = 0
#
#         for i, (_, row) in enumerate(group.iterrows()):
#             expected = first_expected
#             running_expected += expected
#             cumulative_expected.append(running_expected)
#
#             # Try to fully pay this installment if we have money available
#             money_remaining = total_payments_available - money_used
#
#             if money_remaining >= expected:
#                 # Can fully pay this installment
#                 allocated = expected
#                 money_used += expected
#             else:
#                 # Can only partially pay with remaining money
#                 allocated = money_remaining
#                 money_used += money_remaining
#
#             allocated_payments.append(allocated)
#
#         # Update the group with calculated values
#         group['allocated_payment'] = allocated_payments  # Individual installment allocation
#         group['cumulative_expected'] = cumulative_expected
#         # Total Received should show the individual allocated amount, not cumulative
#         group['total_received'] = allocated_payments
#         group['arrears_amount'] = group['standardized_expected'] - group['allocated_payment']
#         group['arrears_threshold'] = threshold * first_expected
#
#         # Payment status flags
#         group['paid'] = (group['total_received'] >= group['standardized_expected']).astype(int)
#         group['due'] = (group['arrears_amount'] > group['arrears_threshold']).astype(int)
#
#         # Calculate days past due based on schedule
#         report_date_dt = pd.to_datetime(report_date)
#         schedule_date_dt = pd.to_datetime(group['schedule_date'])
#         group['days_past_due'] = (report_date_dt - schedule_date_dt).dt.days
#         group['days_past_due'] = group['days_past_due'].clip(lower=0)
#
#         # Months past due (CD)
#         group['months_past_due'] = (group['days_past_due'] / 30).astype(int)
#
#         return group
#
#     # Process each loan separately
#     result = df.groupby('loan_id', group_keys=False).apply(process_loan_group)
#
#     # Format final output columns
#     result['Date'] = pd.to_datetime(report_date).strftime('%Y/%m/%d')
#     result['Loan Id'] = result['loan_id']
#     result['Due Date'] = pd.to_datetime(result['schedule_date']).dt.strftime('%Y/%m/%d')
#     result['Expected Payment'] = result['standardized_expected'].round(2)
#     result['Instalment Amount'] = result['installment_received'].round(2)  # Original received amount
#     result['Total Payments'] = result['cumulative_expected'].round(2)  # Cumulative expected
#     result['Total Received'] = result['total_received'].round(2)  # Individual allocated amount
#     result['Arrears Amount'] = result['arrears_amount'].round(2)
#     result['Arrears Threshold'] = result['arrears_threshold'].round(2)
#     result['DPD'] = result['days_past_due']
#     result['CD'] = result['months_past_due']
#     result['Paid'] = result['paid']
#     result['Due'] = result['due']
#     result['DPD Bucket'] = result['dpd_bucket']
#
#     # Aggregate arrears by bucket (simplified)
#     result['Current'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == 'Current' else 0, axis=1)
#     result['30 Days'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == '30 Days' else 0, axis=1)
#     result['60 Days'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == '60 Days' else 0, axis=1)
#     result['90 Days+'] = result.apply(lambda x: x['arrears_amount'] if '90' in str(x['dpd_bucket']) else 0, axis=1)
#
#     # Select and order final columns
#     output_columns = [
#         'Date', 'Loan Id', 'Total Payments', 'Arrears Threshold', 'Due Date',
#         'Expected Payment', 'Instalment Amount', 'Total Received', 'Due',
#         'Arrears Amount', 'Paid', 'DPD', 'DPD Bucket', 'CD',
#         'Current', '30 Days', '60 Days', '90 Days+'
#     ]
#
#     return result[output_columns]
#
#
# def create_example_data():
#     """Create example data to demonstrate the logic"""
#     example_data = {
#         'loan_id': [1, 1, 1, 1, 2, 2, 2],
#         'schedule_date': [
#             '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01',
#             '2024-01-01', '2024-02-01', '2024-03-01'
#         ],
#         'expected_installment': [1000, 1000, 1000, 1000, 500, 500, 500],
#         'installment_received': [800, 1200, 0, 500, 300, 400, 600],
#         'dpd': [0, 0, 30, 60, 0, 15, 45],
#         'dpd_bucket': ['Current', 'Current', '30 Days', '60 Days', 'Current', 'Current', '30 Days'],
#         'arrears_snapshot': [0, 0, 200, 700, 0, 100, 200],
#         'installment_number': [1, 2, 3, 4, 1, 2, 3]
#     }
#
#     return pd.DataFrame(example_data)
#
#
# def demonstrate_fifo_logic():
#     """Demonstrate the FIFO logic step by step"""
#     print("=== FIFO Logic Demonstration ===")
#     print("Loan 1:")
#     print("Expected: [1000, 1000, 1000, 1000]")
#     print("Received: [800, 1200, 0, 500]")
#     print("Total Available Money: 800 + 1200 + 0 + 500 = 2500")
#     print()
#
#     # Manual calculation to show logic
#     expected = [1000, 1000, 1000, 1000]
#     received = [800, 1200, 0, 500]
#     total_available = sum(received)  # 2500
#
#     money_used = 0
#     allocated = []
#
#     print("FIFO Allocation (pay installments fully from beginning):")
#     for i in range(len(expected)):
#         money_remaining = total_available - money_used
#
#         if money_remaining >= expected[i]:
#             # Can fully pay this installment
#             this_allocation = expected[i]
#             money_used += expected[i]
#         else:
#             # Can only partially pay
#             this_allocation = money_remaining
#             money_used += money_remaining
#
#         allocated.append(this_allocation)
#
#         print(f"Installment {i + 1}:")
#         print(f"  Expected: {expected[i]}")
#         print(f"  Money remaining in pool: {money_remaining}")
#         print(f"  Allocated to this installment: {this_allocation}")
#         print(f"  Arrears for this installment: {expected[i] - this_allocation}")
#         print(f"  Total money used so far: {money_used}")
#         print()
#
#     print("Final Total Received (individual amounts):", allocated)
#     print("Logic: Use all available money to pay installments from the beginning")
#     print()
#     return allocated
#
#
# def main():
#     # Database connection parameters
#     conn_params = {
#         "dbname": "warehouse",
#         "user": "St6ye3_3e4T6",
#         "password": "TXRKjQNRJOq2WmUA",
#         "host": "10.10.1.9",
#         "port": "5432"
#     }
#
#     report_date = "2024-01-31"
#     threshold = 0.1  # 10% threshold for arrears
#
#     print("=== FIFO Payment Allocation Demo ===\n")
#     """
#     # First show the manual logic
#     demonstrate_fifo_logic()
#
#     # For demonstration, let's use example data
#     print("1. Using example data to demonstrate logic:")
#     df_example = create_example_data()
#     print("Original Data:")
#     print(df_example[['loan_id', 'expected_installment', 'installment_received']])
#     print("\n")
#
#     result_example = apply_fifo_allocation(df_example, threshold, report_date)
#     print("After FIFO Allocation:")
#     print("Key columns explanation:")
#     print("- Expected Payment: Standardized amount (from first installment)")
#     print("- Instalment Amount: Original payment received")
#     print("- Total Payments: Cumulative expected payments")
#     print("- Total Received: FIFO allocated amount for this installment")
#     print("- Arrears Amount: Difference between expected and allocated for this installment")
#     print()
#     print(result_example[['Loan Id', 'Expected Payment', 'Instalment Amount',
#                           'Total Payments', 'Total Received', 'Arrears Amount']].head(10))
#     print("\n")
#     """
#     # Uncomment below to use real database data
#
#     print("2. Fetching real data from database...")
#     df_real = fetch_data(report_date, threshold, conn_params)
#     print(f"Fetched {len(df_real)} rows")
#
#     print("3. Applying FIFO allocation...")
#     result_real = apply_fifo_allocation(df_real, threshold, report_date)
#
#     print("4. Sample output:")
#     print(result_real.head(20))
#
#     # Save results
#     result_real.to_csv("fifo_loan_allocation_report.csv", index=False)
#     print("Results saved to fifo_loan_allocation_report.csv")
#
#
#
# if __name__ == "__main__":
#     main()

import pandas as pd
import psycopg


def fetch_data(report_date: str, threshold: float, conn_params: dict) -> pd.DataFrame:
    """
    Simplified SQL query - we'll do the complex FIFO logic in Python
    """
    sql = """
    WITH schedule_data AS (
        SELECT 
            ssfls.loan_id,
            ssfls."ScheduleDate",
            (COALESCE(ssfls.principal_amount, 0)
              + COALESCE(ssfls.interest_amount, 0)
              + COALESCE(ssfls.fee_charges_amount, 0) 
              + COALESCE(ssfls.penalty_charges_amount, 0)) AS expected_installment
        FROM dbt_dom_za.fact_finconnect__simple_loan_schedule ssfls
    ),

    repayment AS (
        SELECT 
            sflt.loan_id,
            date_trunc('month', sflt.transaction_date)::date AS txn_month,
            SUM(COALESCE(sflt.amount, 0)) AS monthly_received
        FROM dbt_dom_za.stg_fineract__loan_transaction sflt
        WHERE sflt.is_reversed IS NOT TRUE 
          AND sflt.transaction_type_enum = '2'
          AND sflt.transaction_date <= %(report_date)s
        GROUP BY sflt.loan_id, date_trunc('month', sflt.transaction_date)
    ),

    arrears AS (
        SELECT
            laa.loan_id,
            laa.overdue_since_date_derived::date AS overdue_date,
            COALESCE(laa.total_overdue_derived, 0) AS arrears_snapshot,
            GREATEST(0, ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date))::int AS dpd,
            CASE
                WHEN laa.overdue_since_date_derived IS NULL THEN 'Paid'
                WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) BETWEEN 0 AND 31 THEN 'Current'
                WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) < 61 THEN '30 Days'
                WHEN ((CAST(%(report_date)s AS date)) - laa.overdue_since_date_derived::date) < 91 THEN '60 Days'
                ELSE '90 Days+'
            END AS dpd_bucket
        FROM dbt_dom_za.stg_fineract__loan_arrears_aging laa
    )

    SELECT 
        sd.loan_id,
        sd."ScheduleDate" as schedule_date,
        sd.expected_installment,
        COALESCE(r.monthly_received, 0) AS installment_received,
        COALESCE(a.dpd, 0) as dpd,
        COALESCE(a.dpd_bucket, 'Paid') as dpd_bucket,
        a.arrears_snapshot,
        ROW_NUMBER() OVER (PARTITION BY sd.loan_id ORDER BY sd."ScheduleDate") as installment_number
    FROM schedule_data sd
    LEFT JOIN repayment r 
        ON r.loan_id = sd.loan_id
        AND r.txn_month = date_trunc('month', sd."ScheduleDate")
    LEFT JOIN arrears a 
        ON a.loan_id = sd.loan_id
    ORDER BY sd.loan_id, sd."ScheduleDate"
    """

    with psycopg.connect(**conn_params) as conn:
        return pd.read_sql(sql, conn, params={"report_date": report_date})


def apply_fifo_allocation(df: pd.DataFrame, threshold: float, report_date: str) -> pd.DataFrame:
    """
    Apply FIFO payment allocation logic:
    1. Use first installment's expected amount as standard for all installments
    2. Allocate payments chronologically with carryover
    """

    def process_loan_group(group):
        group = group.copy().sort_values('schedule_date')

        # Step 1: Handle zero expected installments and standardize expected payments
        # If expected is 0 but installment received > 0, use the received amount as expected
        group['adjusted_expected'] = group.apply(
            lambda row: row['installment_received'] if row['expected_installment'] == 0 and row[
                'installment_received'] > 0
            else row['expected_installment'], axis=1
        )

        # Use first non-zero expected payment as standard for all installments
        first_expected = group[group['adjusted_expected'] > 0]['adjusted_expected'].iloc[0] if len(
            group[group['adjusted_expected'] > 0]) > 0 else group.iloc[0]['adjusted_expected']
        group['standardized_expected'] = first_expected

        # Step 2: Apply FIFO allocation - fully pay installments from beginning
        payments = group['installment_received'].tolist()
        total_payments_available = sum(payments)  # Total money available
        allocated_payments = []
        cumulative_expected = []
        running_expected = 0
        money_used = 0

        for i, (_, row) in enumerate(group.iterrows()):
            expected = first_expected
            running_expected += expected
            cumulative_expected.append(running_expected)

            # Try to fully pay this installment if we have money available
            money_remaining = total_payments_available - money_used

            if money_remaining >= expected:
                # Can fully pay this installment
                allocated = expected
                money_used += expected
            else:
                # Can only partially pay with remaining money
                allocated = money_remaining
                money_used += money_remaining

            allocated_payments.append(allocated)

        # Update the group with calculated values
        group['allocated_payment'] = allocated_payments  # Individual installment allocation
        group['cumulative_expected'] = cumulative_expected
        # Total Received should show the individual allocated amount, not cumulative
        group['total_received'] = allocated_payments
        group['arrears_amount'] = group['standardized_expected'] - group['total_received']
        group['arrears_threshold'] = threshold * first_expected

        # Payment status flags based on Total Received vs Expected
        group['paid'] = (group['total_received'] >= group['standardized_expected']).astype(int)

        # Calculate DPD: days between each installment's schedule date and first arrears date
        report_date_dt = pd.to_datetime(report_date)
        group['due'] = ((pd.to_datetime(group['schedule_date']) <= report_date_dt) &
                       (group['total_received'] < group['standardized_expected'])).astype(int)

        # Find first installment where total_received < expected (first arrears)
        unpaid_installments = group[group['total_received'] < group['standardized_expected']]

        if len(unpaid_installments) > 0:
            # Get the schedule date of the first unpaid installment (first arrears date)
            first_arrears_date = pd.to_datetime(unpaid_installments.iloc[0]['schedule_date'])

            # Calculate DPD for each installment: this_schedule_date - first_arrears_date if this installment is after first arrears
            group['days_past_due'] = group.apply(lambda row:
                                                 max(0,
                                                     (report_date_dt - pd.to_datetime(row['schedule_date'])).days)
                                                 if pd.to_datetime(row['schedule_date']) >= first_arrears_date
                                                 else 0, axis=1
                                                 )
        else:
            # All installments are fully paid
            group['days_past_due'] = 0

        # DPD Bucket based on each installment's DPD
        group['dpd_bucket'] = group.apply(lambda row:
                                         'Paid' if (pd.to_datetime(row['schedule_date']) <= report_date_dt and
                                                   row['total_received'] >= row['standardized_expected']) else
                                         'Current' if 0 < row['days_past_due'] <= 31 else
                                         '30 Days' if 31 < row['days_past_due'] <= 60 else
                                         '60 Days' if 60 < row['days_past_due'] <= 90 else
                                         '90 Days+' if row['days_past_due'] > 90 else
                                         'Current',  # Default for future dates or edge cases
                                         axis=1)

        # Months past due (CD) for each installment
        group['months_past_due'] = (group['days_past_due'] / 30).astype(int)

        return group

    # Process each loan separately
    result = df.groupby('loan_id', group_keys=False).apply(process_loan_group)

    # Format final output columns
    result['Date'] = pd.to_datetime(report_date).strftime('%Y/%m/%d')
    result['Loan Id'] = result['loan_id']
    result['Due Date'] = pd.to_datetime(result['schedule_date']).dt.strftime('%Y/%m/%d')
    result['Expected Payment'] = result['standardized_expected'].round(2)
    result['Instalment Amount'] = result['installment_received'].round(2)  # Original received amount
    result['Total Payments'] = result['cumulative_expected'].round(2)  # Cumulative expected
    result['Total Received'] = result['total_received'].round(2)  # Individual allocated amount
    result['Arrears Amount'] = result['arrears_amount'].round(2)
    result['Arrears Threshold'] = result['arrears_threshold'].round(2)
    result['DPD'] = result['days_past_due']
    result['CD'] = result['months_past_due']
    result['Paid'] = result['paid']
    result['Due'] = result['due']
    result['DPD Bucket'] = result['dpd_bucket']

    # Aggregate arrears by bucket (simplified)
    result['Current'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == 'Current' else 0, axis=1)
    result['30 Days'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == '30 Days' else 0, axis=1)
    result['60 Days'] = result.apply(lambda x: x['arrears_amount'] if x['dpd_bucket'] == '60 Days' else 0, axis=1)
    result['90 Days+'] = result.apply(lambda x: x['arrears_amount'] if '90' in str(x['dpd_bucket']) else 0, axis=1)

    # Select and order final columns
    output_columns = [
        'Date', 'Loan Id', 'Total Payments', 'Arrears Threshold', 'Due Date',
        'Expected Payment', 'Instalment Amount', 'Total Received', 'Due',
        'Arrears Amount', 'Paid', 'DPD', 'DPD Bucket', 'CD',
        'Current', '30 Days', '60 Days', '90 Days+'
    ]

    return result[output_columns]


def create_example_data():
    """Create example data to demonstrate the logic"""
    example_data = {
        'loan_id': [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
        'schedule_date': [
            '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01',
            '2024-01-01', '2024-02-01', '2024-03-01',
            '2024-01-01', '2024-02-01', '2024-03-01'
        ],
        'expected_installment': [1000, 1000, 1000, 1000, 500, 500, 500, 0, 0, 800],
        # Loan 3 has 0 expected but payments
        'installment_received': [800, 1200, 0, 500, 300, 400, 600, 750, 750, 800],
        # Loan 3 receives payments despite 0 expected
        'dpd': [0, 0, 30, 60, 0, 15, 45, 0, 0, 0],
        'dpd_bucket': ['Current', 'Current', '30 Days', '60 Days', 'Current', 'Current', '30 Days', 'Current',
                       'Current', 'Current'],
        'arrears_snapshot': [0, 0, 200, 700, 0, 100, 200, 0, 0, 0],
        'installment_number': [1, 2, 3, 4, 1, 2, 3, 1, 2, 3]
    }

    return pd.DataFrame(example_data)


def demonstrate_fifo_logic():
    """Demonstrate the FIFO logic step by step"""
    print("=== FIFO Logic Demonstration ===")
    print("Loan 1:")
    print("Expected: [1000, 1000, 1000, 1000]")
    print("Received: [800, 1200, 0, 500]")
    print("Schedule Dates: [2024-01-01, 2024-02-01, 2024-03-01, 2024-04-01]")
    print("Report Date: 2024-01-31")
    print("Total Available Money: 2500")
    print()

    # Manual calculation for Loan 1
    expected = [1000, 1000, 1000, 1000]
    received = [800, 1200, 0, 500]
    schedule_dates = ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01']
    total_available = sum(received)

    money_used = 0
    allocated = []

    print("FIFO Allocation for Loan 1:")
    for i in range(len(expected)):
        money_remaining = total_available - money_used

        if money_remaining >= expected[i]:
            this_allocation = expected[i]
            money_used += expected[i]
        else:
            this_allocation = money_remaining
            money_used += money_remaining

        allocated.append(this_allocation)
        is_paid = this_allocation >= expected[i]
        is_due = this_allocation < expected[i]
        arrears = expected[i] - this_allocation

        print(f"Installment {i + 1} ({schedule_dates[i]}):")
        print(f"  Expected: {expected[i]}")
        print(f"  Total Received: {this_allocation}")
        print(f"  Arrears Amount: {arrears}")
        print(f"  Paid: {1 if is_paid else 0}")
        print(f"  Due: {1 if is_due else 0}")
        print()

    # Find first unpaid installment for DPD calculation
    first_arrears_index = None
    for i, allocation in enumerate(allocated):
        if allocation < expected[i]:
            first_arrears_index = i
            break

    if first_arrears_index is not None:
        from datetime import datetime
        report_date = datetime(2024, 1, 31)
        first_arrears_date = datetime.strptime(schedule_dates[first_arrears_index], '%Y-%m-%d')

        print(f"DPD Calculation (First Arrears Date: {schedule_dates[first_arrears_index]}):")
        for i, schedule_date_str in enumerate(schedule_dates):
            schedule_date = datetime.strptime(schedule_date_str, '%Y-%m-%d')

            if schedule_date >= first_arrears_date:
                # This installment is at or after first arrears
                # DPD = this installment's schedule date - first arrears date
                dpd = (schedule_date - first_arrears_date).days
            else:
                # This installment is before first arrears - no DPD
                dpd = 0

            cd = dpd // 30
            dpd_bucket = ('Paid' if dpd == 0 else
                          'Current' if 0 < dpd <= 31 else
                          '30 Days' if dpd <= 60 else
                          '60 Days' if dpd <= 90 else
                          '90 Days+')

            print(f"  Installment {i + 1} ({schedule_date_str}): ")
            print(
                f"    Schedule Date - First Arrears Date = {schedule_date_str} - {schedule_dates[first_arrears_index]} = {dpd} days")
            print(f"    DPD={dpd}, CD={cd}, Bucket={dpd_bucket}")
            print()
    else:
        print("All installments are fully paid - DPD = 0 for all")

    print()
    return allocated


def main():
    # Database connection parameters
    conn_params = {
        "dbname": "warehouse",
        "user": "St6ye3_3e4T6",
        "password": "TXRKjQNRJOq2WmUA",
        "host": "10.10.1.9",
        "port": "5432"
    }

    report_date = "2024-01-31"
    threshold = 0.1  # 10% threshold for arrears

    print("=== FIFO Payment Allocation Demo ===\n")
    """
    # First show the manual logic
    demonstrate_fifo_logic()

    # For demonstration, let's use example data
    print("1. Using example data to demonstrate logic:")
    df_example = create_example_data()
    print("Original Data:")
    print(df_example[['loan_id', 'expected_installment', 'installment_received']])
    print("\nNote: Loan 3 has expected_installment=0 for first two rows but received payments")
    print("This will be adjusted so the payment amount becomes the expected amount")
    print("\n")

    result_example = apply_fifo_allocation(df_example, threshold, report_date)
    print("After FIFO Allocation:")
    print("Key columns explanation:")
    print("- Expected Payment: Standardized amount (from first installment)")
    print("- Instalment Amount: Original payment received")
    print("- Total Payments: Cumulative expected payments")
    print("- Total Received: FIFO allocated amount for this installment")
    print("- Arrears Amount: Expected - Total Received for this installment")
    print("- Due: 1 if Total Received < Expected, 0 otherwise")
    print("- Paid: 1 if Total Received >= Expected, 0 otherwise")
    print("- DPD: Days from first unpaid installment's schedule date to report date")
    print("- CD: DPD converted to months")
    print()
    print(result_example[['Loan Id', 'Expected Payment', 'Total Received',
                          'Arrears Amount', 'Due', 'Paid', 'DPD', 'CD']].head(10))
    print("\n")
    """
    # Uncomment below to use real database data

    print("2. Fetching real data from database...")
    df_real = fetch_data(report_date, threshold, conn_params)
    print(f"Fetched {len(df_real)} rows")

    print("3. Applying FIFO allocation...")
    result_real = apply_fifo_allocation(df_real, threshold, report_date)

    print("4. Sample output:")
    print(result_real.head(20))

    # Save results
    result_real.to_csv("fifo_loan_allocation_report.csv", index=False)
    print("Results saved to fifo_loan_allocation_report.csv")



if __name__ == "__main__":
    main()