from datetime import datetime, timedelta, date, time

# consider date in string format
start_date = "30-May-2020-15:59:02"
start_date = datetime.strptime(start_date, "%d-%b-%Y-%H:%M:%S")
print('start_date :', start_date)
print(type(start_date))

today_date = datetime.today().strftime('%Y-%m-%d')
print('today_date: ', today_date)
print(type(today_date))

time_start = datetime.now()
print('time_start :', time_start)
print(type(time_start))

old_date_time = datetime(2021, 10, 23, 0, 0, 0)
print('old_date_time :', old_date_time)
print(type(old_date_time))

old_date = datetime(2021, 10, 23)
print('old_date :', old_date)
print(type(old_date))

today = date.today()
print('today :', today)
print(type(today))
