import redshift_connector
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

conn = redshift_connector.connect(
    user="user_test",
    password="Password123",
    host="redshift-cluster-datawarehouse.cew9a5azwld4.us-east-1.redshift.amazonaws.com",
    port="5439",
    database="test"
)

cursor = conn.cursor()

cursor.execute("SELECT company_id, COUNT(account_id) AS num_accounts, MIN(date) AS activation_date, SUM(three_day_total) AS num_total_messages, SUM(CASE WHEN three_day_successful THEN total ELSE 0 END) AS num_successful_messages, MIN(CASE WHEN cumulative_successful >= 500 THEN date ELSE NULL END) AS successful_date FROM (SELECT account_id, date, total, (SELECT SUM(total) FROM conversations WHERE conversations.account_id = c.account_id AND conversations.date <= c.date AND conversations.date >= c.date - INTERVAL '2 days') AS three_day_total, (SELECT SUM(total) FROM conversations WHERE conversations.account_id = c.account_id AND conversations.date <= c.date AND conversations.date >= c.date - INTERVAL '3 days' AND conversations.successful = TRUE) AS three_day_successful, (SELECT SUM(total) FROM conversations WHERE conversations.account_id = c.account_id AND conversations.date <= c.date AND conversations.successful = TRUE) AS cumulative_successful FROM conversations AS c) AS conversations_with_three_day_total INNER JOIN company_identifiers ON account_id = account_identifier INNER JOIN company on id = company_id WHERE three_day_total >= 350 AND associated_partner IS NOT NULL GROUP BY company_id ORDER BY company_id;")

results: tuple = cursor.fetchall()
print(results)

start_date = datetime(2023, 3, 1).date()
end_date = datetime(2023, 8, 31).date()

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
        1 for result in results if week_start <= result[-1] <= week_end
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
