from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from __id_search import job2


# def my_job1():
#     main()
# #     print ('my_job1 is running, Now is %s' % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def my_job2():
    job2()
    print ('my_job2 is running, Now is %s' % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

sched = BlockingScheduler()
# 每隔5秒运行一次my_job1
# sched.add_job(my_job1, 'cron', week=5,hour=16,minute=33,id='my_job1')
# 每隔5秒运行一次my_job2
sched.add_job(my_job2,'cron',hour=13 ,minute=49,id='my_job2')
sched.start()




#
# cron参数如下：
#
#     year (int|str) – 年，4位数字
#     month (int|str) – 月 (范围1-12)
#     day (int|str) – 日 (范围1-31)
#     week (int|str) – 周 (范围1-53)
#     day_of_week (int|str) – 周内第几天或者星期几 (范围0-6 或者 mon,tue,wed,thu,fri,sat,sun)
#     hour (int|str) – 时 (范围0-23)
#     minute (int|str) – 分 (范围0-59)
#     second (int|str) – 秒 (范围0-59)
#     start_date (datetime|str) – 最早开始日期(包含)
#     end_date (datetime|str) – 最晚结束时间(包含)
#     timezone (datetime.tzinfo|str) – 指定时区
