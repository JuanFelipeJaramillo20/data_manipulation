import redshift_connector
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

conn = redshift_connector.connect(
        user="user_test",
        password="Password123",
        host="redshift-cluster-datawarehouse.cew9a5azwld4.us-east-1.redshift.amazonaws.com",
        port="5439",
        database="test"
    )

cursor = conn.cursor()

query = "WITH ACTIVATION_DATES AS(SELECT * FROM(SELECT company_id,date,date_count,SUM ([date_count]) OVER (PARTITION BY [company_id] ORDER BY [date] ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last_three_days_count FROM(SELECT ci.company_id, con.date, SUM(con.total) AS date_count FROM company_identifiers ci INNER JOIN conversations con ON ci.account_identifier = con.account_id GROUP BY ci.company_id, con.date) As t) AS o WHERE last_three_days_count >= 350) SELECT c.id, c.close_date, c.associated_partner, MIN(ac.date) AS min_activation_date FROM ACTIVATION_DATES ac INNER JOIN COMPANY c ON ac.company_id = c.id WHERE c.associated_partner IS NOT NULL GROUP BY c.id, c.close_date, c.associated_partner;"
cursor.execute(query)
results = cursor.fetchall()
activated_companies = []
for result in   results:
    if result[2]:
        company = {
            "id": result[0],
            "activation_date": result[3]
        }
        activated_companies.append(company)


companies_to_remove = []
for company in activated_companies:
    id = company["id"]
    date = company["activation_date"]
    query = f"with total_convs as (select company_id,sum(total) as total, date from conversations join company_identifiers on conversations.account_id = company_identifiers.account_identifier join company on company.id = company_identifiers.company_id where company_id = {id} and successful = TRUE group by company_id, date) select * from total_convs where date >= '{date}' order by date"
    cursor.execute(query)
    results = cursor.fetchall()
    if results:
        total_successful = 0
        success_date = None
        for result in results:
            if (total_successful + result[1]) >= 500:
                success_date = result[2]
                break
            else:
                total_successful += result[1]
        if success_date is not None:
            company["successfull_date"] = success_date
        else:
            companies_to_remove.append(company)

# Remove companies after the loop
for company in companies_to_remove:
    activated_companies.remove(company)


print("activated companies:", activated_companies)

final_values=[]
for company in activated_companies:
    if "successfull_date" in company:
        limit = datetime(company["activation_date"].year, company["activation_date"].month + 2, company["activation_date"].day ).date()
        if (company["activation_date"] >= datetime(2023,1,1).date() and company["successfull_date"] <= limit):
            print("COMPANY: ", company)
            final_values.append(company)
start_date = datetime(2023, 2, 1).date()
end_date = datetime(2023, 8, 13).date()

week_start_dates = [start_date]
current_date = start_date
while current_date < end_date:
    current_date += timedelta(weeks=1)
    week_start_dates.append(current_date)

x_axis_values = [date.strftime("%b %d") for date in week_start_dates]
y_axis_values = []

cumulative_count = 0

for week_start in week_start_dates:
    week_end = week_start + timedelta(weeks=1)
    num_successful_companies = sum(
        1 for result in final_values if week_start <= result["successfull_date"] <= week_end
    )
    cumulative_count += num_successful_companies
    y_axis_values.append(cumulative_count)

plt.figure(figsize=(10, 6))
plt.plot(x_axis_values, y_axis_values)

plt.xlabel("Month and Week")

plt.ylabel("Cumulative Number of Companies")

plt.grid(False)

plt.xticks(rotation=45)

plt.show()
