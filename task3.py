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
    "1": [0,0,0,0,0,0,0,0],
    "2": [0,0,0,0,0,0,0,0],
    "3": [0,0,0,0,0,0,0,0],
    "4": [0,0,0,0,0,0,0,0],
    "5": [0,0,0,0,0,0,0,0],
    "6": [0,0,0,0,0,0,0,0],
    "7": [0,0,0,0,0,0,0,0],
    "8": [0,0,0,0,0,0,0,0],
}


for month in range(1, 9):
    index = 0  # Initialize the index to the first position
    for i in range(0, number_of_months[str(month)]):
        total_revenue = 0  # Initialize total revenue for this month
        unique_ids = set()  # Initialize a set to track unique invoice IDs
        for company_id in id_lists_per_month[str(month)]:
            query = f"SELECT * FROM stripe_invoice i JOIN company_identifiers ci ON (',' || ci.stripe_company_ids || ',') ILIKE ('%,' || i.company_id || ',%') WHERE ci.company_id = {company_id} AND EXTRACT(MONTH FROM i.sent_date) = {i+1} AND EXTRACT(YEAR FROM i.sent_date) = 2023;"
            cursor.execute(query)
            results = cursor.fetchall()
            for item in results:
                item_id = item[1]
                if item_id not in unique_ids:
                    unique_ids.add(item_id)  # Add the ID to the set
                    total_revenue += item[3]  # Add the revenue to the total
        values[str(month)][index] = total_revenue  # Assign the total revenue for this month and position
        index += 1  # Increment the index for the next iteration
            

print(values)

values_array = np.array([
    values["1"],
    values["2"],
    values["3"],
    values["4"],
    values["5"],
    values["6"],
    values["7"],
    values["8"]
])

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