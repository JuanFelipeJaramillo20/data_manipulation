import redshift_connector
import pandas as pd
import matplotlib.pyplot as plt
from calendar import monthrange


# Connect to the database
conn = redshift_connector.connect(
        user="user_test",
        password="Password123",
        host="redshift-cluster-datawarehouse.cew9a5azwld4.us-east-1.redshift.amazonaws.com",
        port="5439",
        database="test"
    )

    # Step 2: Execute SQL queries
cursor = conn.cursor()

id_lists_per_month = {
    "1": [],
    "2": [],
    "3": [],
    "4": [],
    "5": [],
    "6": [],
    "7": [],
    "8": []
}

number_of_months = {
    "1": 8,
    "2": 7,
    "3": 6,
    "4": 5,
    "5": 4,
    "6": 4,
    "7": 2,
    "8": 1
}

for i in range (1,9):
    last_day_of_month = monthrange(2023, i)[1]
    query = f"select id from company where close_date >= '{i}/01/2023' and close_date <='{i}/{last_day_of_month}/2023'"
    cursor.execute(query)
    results = cursor.fetchall()
    for value in results:
        id_lists_per_month[f"{i}"].append(value[0])
    #print(id_lists_per_month)
    #print(number_of_months)


values = {
    "1": [],
    "2": [],
    "3": [],
    "4": [],
    "5": [],
    "6": [],
    "7": [],
    "8": []
}

for month in range(1, 9):
    num_months = number_of_months[str(month)]
    values[month] = [0] * num_months  # Initialize with None values based on the number of months

    for i, company_id in enumerate(id_lists_per_month[str(month)]):
        query = f"SELECT SUM(i.amount) AS total_revenue FROM stripe_invoice i JOIN company_identifiers ci ON (',' || ci.stripe_company_ids || ',') ILIKE ('%,' || i.company_id || ',%') WHERE ci.company_id = {company_id} AND EXTRACT(MONTH FROM i.sent_date) = {month} AND EXTRACT(YEAR FROM i.sent_date) = 2023;"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            total_revenue = result[0]
        else:
            total_revenue = 0
        values[month][i] = total_revenue

print(values)


"""
query = "SELECT  SUM(i.amount) AS total_revenue FROM  stripe_invoice i JOIN  company_identifiers ci ON  (',' || ci.stripe_company_ids || ',') ILIKE ('%,' || i.company_id || ',%') WHERE  ci.company_id = 2  AND EXTRACT(MONTH FROM i.sent_date) > 1  AND EXTRACT(YEAR FROM i.sent_date) = 2023;"
cursor.execute(query)
results = cursor.fetchall()
print(results)
"""
