from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_mail(body):

    message = MIMEMultipart()
    message['Subject'] = 'Bets'
    message['From'] = 'douglasbcarrion@gmail.com'
    message['To'] = 'douglasbcarrion@gmail.com'

    body_content = body
    message.attach(MIMEText(body_content, "html"))
    msg_body = message.as_string()

    server = SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(message['From'], 'bxrzccwvwlqjfmax')
    server.sendmail(message['From'], message['To'], msg_body)
    server.quit()
