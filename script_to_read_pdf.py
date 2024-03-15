import pandas as pd
import tabula
from sqlalchemy import create_engine

#Part 1: Data Extraction  and Part 2: Data Storage

pdf = tabula.read_pdf('Test PDF.pdf', pages='all', stream=True)
print(type(pdf))

engine = create_engine("mysql+mysqlconnector://root:@localhost:3306/temp")


for table in pdf :
    try :
        print(type(table))
        table = table.fillna("")
        table.columns = ['App_ID', 'Xref', 'Settlement_Date', 'Broker', 'Sub_Broker', 'Borrower_Name', 'Description', 'Total_Loan_Amount', 'Commission_Rate', 'Upfront', 'Upfront_Incl_GST']
        table['Settlement_Date'] = pd.to_datetime(table['Settlement_Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        table.to_sql('taxInvoice', con=engine, if_exists='append', index=False)
    except Exception as e:
        print('skipped', e)
        print(table.columns)


# Part 4  SQL operations



# SELECT sum(Total_Loan_Amount) AS `Total Loan Amount` FROM taxInvoice BETWEEN NOW() AND NOW() - INTERVAL 1 day;

# SELECT max(Total_Loan_Amount), Broker FROM taxInvoice GROUP BY Broker;




    # Part 5 Reporting
""" 

1.
    To calculate the report specific to broker in specific timeframe we requet the database with query as follows : 

        For daily time frame :

            First we will fetch all the broker availabel in our database using  : 

                select distinct(broker) from taxInvoice;

            Now we will fetch all the data for that broker in that day using :

                SELECT * FROM taxInvoice where Broker = 'Broker Name' AND Settlement_Date = NOW();

        For monthly time frame :

            we will iterate through all the brokers and for each broker we will fetch all the data for that broker in that month using :

                SELECT * FROM taxInvoide where Broker = 'Broker Name' AND settlement_Date BETWEEN now() and now() - INTERVAL 7 day;

        For yearly time frame :

            we will iterate through all the brokers and for each broker we will fetch all the data for that broker in that year using :

                SELECT * FROM taxInvoide where Broker = 'Broker Name' AND settlement_Date BETWEEN now() and now() - INTERVAL 1 YEAR;
    

2.

    To generate total loan amount grouped by date we can use :

        SELECT sum(Total_Loan_Amount) AS `Total Loan Amount`, Settlement_Date FROM taxInvoice GROUP BY Settlement_Date

3. 

    For different tier data we can use :

        select sum(Total_Loan_Amount) AS `Total Loan Amount`, Broker, Settlement_Date FROM taxInvoice where Total_Loan_Amount > 100000 

        select sum(Total_Loan_Amount) AS `Total Loan Amount`, Broker, Settlement_Date FROM taxInvoice where Total_Loan_Amount > 50000

        select sum(Total_Loan_Amount) AS `Total Loan Amount`, Broker, Settlement_Date FROM taxInvoice where Total_Loan_Amount > 10000


4.

    For Generateing a report of the number of loans under each tier group by date.

        select count(*) from taxInvoice group by Total_Loan_Amount > 100000 
"""



