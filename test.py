from datetime import datetime, timedelta, date, time

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


import datetime
e = datetime.datetime.now()
print ("Current date and time = %s" % e)
print ("Today's date:  = %s/%s/%s" % (e.day, e.month, e.year))
print ("The time is now: = %s:%s:%s" % (e.hour, e.minute, e.second))


