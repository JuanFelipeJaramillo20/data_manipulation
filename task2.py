import redshift_connector
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from calendar import monthrange

conn = redshift_connector.connect(
    user="user_test",
    password="Password123",
    host="redshift-cluster-datawarehouse.cew9a5azwld4.us-east-1.redshift.amazonaws.com",
    port="5439",
    database="test"
)

cursor = conn.cursor()

days=[31,28,31,30,31,30,31,31,30,31,30,31]


def define_query(month, year):

    if(month == 1):
        start_date = datetime(year-1, 12, 1)    
        #last_day_of_month = monthrange(year, 1)[1]
        last_day_of_month = days[0]
        finish_date = datetime(year, 1, last_day_of_month)
    else:
        start_date = datetime(year, month -1, 1)
        #last_day_of_month = monthrange(year, month)[1]
        last_day_of_month = days[month-1]
        finish_date = datetime(year, month, last_day_of_month)
    actual_date = datetime(year, month, monthrange(year, month)[1])
    actual_date_start = datetime(year, month, 1)

    query = f"SELECT c.id, c.close_date, SUM(co.total) AS successful_messages FROM(SELECT id, close_date FROM company WHERE close_date >= '{start_date}' AND close_date <= '{finish_date}') AS closed_companies INNER JOIN company_identifiers ci ON closed_companies.id = ci.company_id INNER JOIN conversations co ON co.account_id = ci.account_identifier INNER JOIN company c on c.id = ci.company_id WHERE co.date >= '{actual_date_start}' AND co.date <= '{actual_date}' AND co.successful = TRUE GROUP BY c.id, c.close_date HAVING SUM(co.total) >= 1500"

    """
    This query should retrieve the percentage by month:
    query= f""select 100 * (cast(sum(case when sucessful then 1 else 0 end) as float) / count(*)) as result from
        (select *, case 
            when c.id in (
            select company_id from company_identifiers ci inner join conversations conv on
                conv.account_id = ci.account_identifier and
                conv.successful and
                conv.date >= '{actual_date_start}' and 
                conv.date <= '{actual_date}'
                group by company_id
                having sum(total) >= 1500
            )
            then true 
            else false 
            end as sucessful
            from company c where
            c.close_date >= '{start_date}' and c.close_date <= '{finish_date}')""
    """

    query_total = f"select COUNT(*) from company WHERE close_date > '{start_date}' and close_date <= '{finish_date}'"

    cursor.execute(query)
    results = len(cursor.fetchall())
    cursor.execute(query_total)
    total = max(cursor.fetchall()[0][0],1)
    print("RESULTS", results, "TOTAL", total, "PERCENTAGE: ", ((results/total)*100))
    return ((results/total)*100)


percentages = []

for i in range(1,9):
    print("PARA MES ", i)
    percentages.append(define_query(i,2023))

month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug']

plt.figure(figsize=(10, 6))
plt.plot(month_labels, percentages, marker='o')

plt.xlabel("Month")
plt.ylabel("Percentage of Successful Companies")
plt.title("Percentage of Recently Closed Companies That Were Successful (2023)")
plt.ylim(0, 70)

plt.grid(True)

plt.show()