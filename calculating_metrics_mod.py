from collections import defaultdict
from datetime import  date, datetime, timedelta

# Расчет количества уникальных юзеров по дням;
def count_daily_uniq_users(data):
    date_users_info = defaultdict(set)

    for row in data:
      date_users_info[row[-1].date()].add(row[0])

    daily_uniq_users = []
    header = ['Дата', 'DAU']
    daily_uniq_users.append(header)

    for dt_key, value in sorted(date_users_info.items(), key=lambda elem: elem[0]):
      daily_uniq_users.append([dt_key.isoformat(), len(value)])

    return daily_uniq_users


# Расчет количества попыток решить задачу по дням (всего попыток и успешных решений):
def count_daily_submits(data):
    daily_submits = defaultdict(dict)

    for row in sorted(data, key=lambda elem: elem[-1]):
      if row[-2].lower() == 'submit':
        if row[-3] == 0:
          daily_submits[row[-1].date().isoformat()]['total_submits'] = daily_submits[row[-1].date().isoformat()].get('total_submits', 0) + 1
        else:
          daily_submits[row[-1].date().isoformat()]['correct_submits'] = daily_submits[row[-1].date().isoformat()].get('correct_submits', 0) + 1

    res_daily_submits = []
    header = ['Дата', 'Всего попыток решений', 'Успешных решений']
    res_daily_submits.append(header)

    for dt_key, sumbits_value in daily_submits.items():
      res_daily_submits.append([dt_key] + list(map(lambda sorted_elem: sorted_elem[1], sorted(sumbits_value.items(), key=lambda elem: elem[0], reverse=True))))

    return res_daily_submits