import redshift_connector
import pandas as pd
import matplotlib.pyplot as plt
from calendar import monthrange
import numpy as np

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

for i in range(1, 9):
    last_day_of_month = monthrange(2023, i)[1]
    query = f"SELECT id FROM company WHERE close_date >= '2023-{i:02d}-01' AND close_date <= '2023-{i:02d}-{last_day_of_month}'"
    cursor.execute(query)
    results = cursor.fetchall()
    for value in results:
        id_lists_per_month[str(i)].append(value[0])

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
    for i in range(month, 9):
        total_revenue = 0
        unique_ids = set()
        for company_id in id_lists_per_month[str(month)]:
            query = f"SELECT * FROM stripe_invoice i JOIN company_identifiers ci ON (',' || ci.stripe_company_ids || ',') ILIKE ('%,' || i.company_id || ',%') WHERE ci.company_id = {company_id} AND EXTRACT(MONTH FROM i.sent_date) = {i} AND EXTRACT(YEAR FROM i.sent_date) = 2023;"
            cursor.execute(query)
            results = cursor.fetchall()
            for item in results:
                item_id, amount = item[1], item[3]
                if item_id not in unique_ids:
                    unique_ids.add(item_id)
                    total_revenue += amount
        values[str(month)].append(total_revenue)

print(values)

max_len = max(len(arr) for arr in values.values())
values_array = np.array([arr + [0] * (max_len - len(arr)) for arr in values.values()])

def plot_2d_table(values_array):
    rows, cols = values_array.shape
    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(values_array, cmap="viridis")

    for i in range(rows):
        for j in range(cols):
            text = ax.text(j, i, str(values_array[i, j]), ha="center", va="center", color="w")

    ax.set_xticks(np.arange(cols))
    ax.set_yticks(np.arange(rows))
    ax.set_xticklabels(range(cols))
    ax.set_yticklabels(["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08"])
    ax.set_xlabel("Months After Close Date")
    ax.set_ylabel("Cohort Month")
    ax.set_title("Cohorts Chart of Revenue in USD Collected")

    plt.tight_layout()
    plt.show()

# Plot the 2D table from the `values` variable
plot_2d_table(values_array)