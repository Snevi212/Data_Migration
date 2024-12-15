import smtplib

email = input("SENDER EMAIL: ")
reciever_email = input("RECEIVER EMAIL: ")

subject = input("SUBJECT: ")
message = input("MESSAGE: ")

text = f"Subject: {subject}\n\n{message}"

server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()

server.login(email,"kzivlccfdnjbtcjy")

server.sendmail(email,reciever_email, text)

print("Email has been sent to "+ reciever_email)
