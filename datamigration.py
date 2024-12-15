import sqlalchemy as sal
import pandas as pd
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

logging.basicConfig(filename='server.log',level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')


def read_config():
    try:
        with open(r"configfile\\config.json") as fp:
            data = json.load(fp)

        mysql_engine =sal.create_engine(f"mysql+pymysql://{data['username']}:{data['password']}@{data['host']}:{data['port']}/{data['database']}")
        conn = mysql_engine.connect()
        logging.info("Successfully connect to MySql databse")


        with open(r"configfile\\ssms.json") as f:
            data1 = json.load(f)

        ssms_engine = sal.create_engine("mssql+pyodbc://DESKTOP-BN99CMD\\SQLEXPRESS/python?driver=ODBC+Driver+17+for+SQL+Server")
        conn1 =ssms_engine.connect()
        logging.info("Data dumped to Mssql database")


        query = "SELECT * FROM sbi_bank_customers;"

        df = pd.read_sql(query, conn)
        logging.info("Successfully connect to Mssql databse")

        # Write data to SSMS
        df.to_sql("sbi_customer", con=conn1, if_exists="replace", index=False)
        logging.info("Data added to Mssql databse successfully")

        conn.close()
        conn1.close()
        logging.info("Process completed!!!")
        return True
    except Exception as e:
        print(e)
        return False

##Send_Email
def send_email(status):
    with open(r'configfile\gmail.json') as femail:
        gmail_cred = json.load(femail)

    sender_email ="psneha010@gmail.com"
    reciever_email = "psneha010@gmail.com"
    password = f"{gmail_cred['apppass']}"

    subject = "Data Migration Status "

    mysql_db = "DB_connection.sbi_bank_customer"
    mssql_db = "Python.sbi_customer"

    if status:
        email_body = f"Hello team,\n\n{mysql_db} data successfully loaded to {mssql_db}"
        logging.info("Data migrated succesfully")
    else:
        email_body = f"Hello team, \n\n data migration from {mysql_db} to {mssql_db}"
        logging.info('Data migration failed')

    message =MIMEMultipart()
    message['From'] = sender_email
    message['To'] =  reciever_email
    message['Subject'] =subject

    message.attach(MIMEText(email_body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com',587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, reciever_email, message.as_string())
            logging.info("Email sent")

    except Exception as e:
        print(f"Email not sent",e)
def main():
    status=read_config()
    send_email(status)


if __name__ == '__main__':
    main()
