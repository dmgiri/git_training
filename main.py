"""This is cron job"""
import schedule

def print_msg():
    print("print every 1 sec")
schedule.every(1).seconds.do(print_msg)
while True:
    schedule.run_pending()