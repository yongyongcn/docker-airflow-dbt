import smtplib

try:
    # Use 587 for STARTTLS
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls() 
    server.login('yongyongcn@gmail.com', 'ivdg otps utfd ouzk')
    print("Connection Successful!")
    server.quit()
except Exception as e:
    print(f"Connection Failed: {e}")