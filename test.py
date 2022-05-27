from datetime import datetime, timedelta, date, time


def date_range(start_date: date, end_date: date):

    delta = end_date - start_date
    dates = [start_date + timedelta(days=i) for i in range(delta.days +1)]
    return dates


date_range(2022-11-11, 2022-10-10)

# current_date = datetime.now()
# print(current_date)

# date = current_date.strftime("%Y-%m-%d")
# print("current_date :", date)


# import datetime

# import datetime
# e = datetime.datetime.now()
# print("Current date and time = %s" % e)
# print("Today's date:  = %s/%s/%s" % (e.day, e.month, e.year))
# print("The time is now: = %s:%s:%s" % (e.hour, e.minute, e.second))


# a = e.strftime("%Y-%m-%d")
# print("Value of A:", a)

# old_date_time = datetime(2021, 10, 23, 0, 0, 0)
# print('old_date_time:', old_date_time)

# old_date = datetime.date(2021, 10, 23)
# print('old_date:', old_date)

# today = date.today()
# print('today:', today)

# y = old_date - today
# print('y:', y)

# time_start = datetime.now()
# print('time_start:', time_start)

# x = time_start - old_date_time

