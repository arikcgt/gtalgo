from os import listdir
from os.path import isfile, join
import numpy as np
import datetime
from numpy.matlib import empty, random, argsort, sort
from boost_lib import dixon3
__author__ = 'uri lipowezkie'


def week_day(year, month, day):
    offset = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    week = ['Sunday',
            'Monday',
            'Tuesday',
            'Wednesday',
            'Thursday',
            'Friday',
            'Saturday']
    afterFeb = 1
    if month > 2: afterFeb = 0
    aux = year - 1700 - afterFeb
    # dayOfWeek for 1700/1/1 = 5, Friday
    dayOfWeek  = 5
    # partial sum of days between current date and 1700/1/1
    dayOfWeek += (aux + afterFeb) * 365
    # leap year correction
    dayOfWeek += aux / 4 - aux / 100 + (aux + 100) / 400
    # sum monthly and day offsets
    dayOfWeek += offset[month - 1] + (day - 1)
    dayOfWeek %= 7
    return dayOfWeek, week[dayOfWeek]


print "CSV offers reading test"
root_dir = '/tmp/driver_offers_IL_2016_08_clean'
suffix = 'csv'
preffix = 'drivers'
only_files = [f for f in listdir(root_dir) if isfile(join(root_dir, f))]
cnt = 0
for item in only_files:
    t_ext = suffix in item
    if not t_ext:
        continue
    t_drv = preffix in item
    if t_drv:
        continue
    full_path = root_dir + '/' + item
    offers = [f.rstrip().split(',') for f in open(full_path, 'r').readlines()]
    n_offers = len(offers)-1
    print "There are ", n_offers, " offers in ", item
    if cnt == 0:
        title = offers[0]
        date_idx = title.index("offers.date_key")
        hour_idx = title.index("offers.hour_key")
        accept_idx = title.index("offers.is_accepted")
        status_idx = title.index("offers.offer_status_key")
        dist_idx = title.index("offers.distance_from_order_on_creation")
        ride_type_idx = title.index("orders.ride_type_key")
        class_idx = title.index("orders.class_type_gk")
        payment_idx = title.index("orders.payment_type_key")
        print title, " indices: ", date_idx, payment_idx
    hour = []
    accept = []
    status = []
    dist = []
    ride_type = []
    class_type = []
    payment = []
    day = []
    month = []
    year = []
    w_day = []
    # 1 - Numerical, 2 - Rank, 3 - Categorical
    fea_name_type = {'Hour': 2, 'Dist': 1, 'Ride': 3, 'Class': 3, 'Day': 2, 'Week_Day': 2, 'Payment': 3}
    for i in range(n_offers):
        t_offer = offers[i+1]
        date_stamp = t_offer[date_idx]
        t_date = date_stamp[1:-1]
        dt = datetime.datetime.strptime(t_date, "%Y-%m-%d")
        yy = dt.year
        year.append(yy)
        dd = dt.day
        day.append(dd)
        mm = dt.month
        month.append(mm)
        wd = week_day(yy,mm,dd)
        w_day.append(wd[0])
        t_hou = t_offer[hour_idx]
        t_hou = t_hou[1:-1]
        hour.append(int(t_hou))
        t_acc = t_offer[accept_idx]
        lt_acc = len(t_acc)
        if lt_acc == 0:
            acc = 0
        else:
            t_acc = t_acc[1:-1]
            acc = int(t_acc)
        accept.append(acc)
        t_sta = t_offer[status_idx]
        t_sta = t_sta[1:-1]
        t_sta = int(t_sta)
        status.append(t_sta)
        t_dist = t_offer[dist_idx]
        lt_dist = len(t_dist)
        if lt_dist == 0:
            dst = 0
        else:
            t_dist = t_dist[1:-1]
            dst = float(t_dist)
        dist.append(dst)
        t_ride = t_offer[ride_type_idx]
        t_ride = t_ride[1:-1]
        t_ride = int(t_ride)
        ride_type.append(t_ride)
        t_class = t_offer[class_idx]
        t_class = t_class[1:-1]
        t_class = int(t_class)
        class_type.append(t_class)
        t_pay = t_offer[payment_idx]
        t_pay = t_pay[1:-1]
        t_pay = int(t_pay)
        payment.append(t_pay)
    cnt += 1
    uni_day = set(day)
    print "day: Number = ", len(uni_day), ", Minimun = ", min(day), ", Maximum = ", max(day)
    uni_wd = set(w_day)
    print "week day: Number = ", len(uni_wd), ", Minimun = ", min(w_day), ", Maximum = ", max(w_day)
    uni_hou = set(hour)
    print "hour: Number = ", len(uni_hou), ", Minimun = ", min(hour), ", Maximum = ", max(hour)
    uni_sta = set(status)
    print "Status: Number = ", len(uni_sta), ", Minimun = ", min(status), ", Maximum = ", max(status)
    uni_dist = set(dist)
    nz = np.nonzero(dist)
    nz = nz[0]
    n_zeros = n_offers - len(nz)
    print "Distance: Number = ", len(uni_dist), ", Minimun = ", min(dist), ", Maximum = ", max(dist), ", Number of zeros = ", n_zeros
    dixon3(dist, "Dist")
    uri_ride = set(ride_type)
    print "Ride Type: Number = ", len(uri_ride), ", Minimun = ", min(ride_type), ", Maximum = ", max(ride_type)
    uni_class = set(class_type)
    print "Class Type: Number = ", len(uni_class), ", Minimun = ", min(class_type), ", Maximum = ", max(class_type)
    uni_pay = set(payment)
    print "Payment Type: Number = ", len(uni_pay), ", Minimun = ", min(payment), ", Maximum = ", max(payment)
    y_accept = np.array(accept)
    y_accept[y_accept == 0] = -1
    uni_acc = set(y_accept)
    print "accept: Number = ", len(uni_acc), ", Minimun = ", min(y_accept), ", Maximum = ", max(y_accept)
    n_fea = len(fea_name_type)
    fea = empty((n_fea,n_offers))
    fea[0, :] = np.array(hour)
    fea[1, :] = np.array(dist)
    fea[2, :] = np.array(ride_type)
    fea[3, :] = np.array(class_type)
    fea[4, :] = np.array(day)
    fea[5, :] = np.array(w_day)
    fea[6, :] = np.array(payment)
    rand_fea = random.rand(n_offers)
    uni_rand = set(rand_fea)
    print "Random: Number = ", len(uni_rand), ", Minimun = ", min(rand_fea), ", Maximum = ", max(rand_fea)
    rand_fea_idx = argsort(rand_fea)
    train_gr_idx = int(2*n_offers/3)
    cont_gr_idx = train_gr_idx + int(2*n_offers/9)
    train_gr = rand_fea_idx[0:train_gr_idx]
    cont_gr = rand_fea_idx[train_gr_idx+1:cont_gr_idx]
    val_gr = rand_fea_idx[cont_gr_idx+1:]
    print "Training of ", len(train_gr), " records, Control of ", len(cont_gr), "records and Validation of ", len(val_gr), " records"
    # if cnt > 10:
    break
print cnt, " files have been found"
