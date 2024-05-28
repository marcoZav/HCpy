# Import smtplib for the actual sending function
import smtplib

# Import the email modules we'll need
from email.message import EmailMessage

# Create a text/plain message
msg = EmailMessage()
msg.set_content('test')

# me == the sender's email address
# you == the recipient's email address
msg['Subject'] = 'test'
msg['From'] = 'marco.zavarini@sas.com'
msg['To'] = 'marco.zavarini@sas.com'

# Send the message via our own SMTP server.
s = smtplib.SMTP('localhost')
s.send_message(msg)
s.quit()