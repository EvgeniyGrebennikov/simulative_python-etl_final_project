import logging
import calculating_metrics_mod
import psycopg2
import requests
import re
import os, sys
import calculating_metrics_mod
import gspread
import smtplib
import ssl
from datetime import date, datetime, timedelta
from glob import glob
from oauth2client.service_account import ServiceAccountCredentials
from email.message import EmailMessage


# Настройка логирования и запись данных в файл
logger = logging.getLogger(__name__)
fileHandler = logging.FileHandler(filename=f"{date.today()}.txt", encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s | %(name)s | %(levelname)s: %(message)s",
    handlers=[fileHandler]
)


# Компилирование шаблона для поиска файлов логов
logs_compile = re.compile(r"\d{4}-\d{2}-\d{2}.txt")
# Поиск файлов логов по совпадению шаблона
txt_logs = [fl for fl in glob(r"*.txt") if logs_compile.match(fl)]

# Поиск txt-файлов логов, удаление более ранних логов (записанных более 3-х дней назад)
for fl in txt_logs:
  if date.fromisoformat(fl.split('.')[0]) <= date.today() - timedelta(days=3):
    os.remove(fl)


# Класс для извлечения данных из API
class Extract:
    api_url = ""
    client = ""
    client_key = ""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
           cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_response(cls, start_date="2023-05-01 00:00:00", end_date="2023-05-31 23:59:59"):
        cls.params = {
            "client": cls.client,
            "client_key": cls.client_key,
            "start": start_date,
            "end": end_date
        }

        try:
            logger.info("Скачивание данных началось")
            print("Скачивание данных началось")
            cls.res = requests.get(cls.api_url, params=cls.params)
            cls.res.raise_for_status()

        except requests.exceptions.HTTPError as err:
            logger.exception(err)
            print(f"Ошибка {err}")
            return (f"HTTP Error: {err}")

        except requests.exceptions.RequestException as err:
            logger.error(err, exc_info=False)
            print(f"Ошибка {err}")
            return (f"Request Error: {err}")

        else:
            if cls.res.status_code == 200:
                logger.info("Скачивание данных завершилось")
                print("Скачивание данных завершилось")
                return [UserActive(row['lti_user_id'], row['passback_params'], row['is_correct'], row['attempt_type'], row['created_at']) for row in cls.res.json()]
            else:
                logger.error(f"Ошибка доступа к API со статус-кодом {cls.res.status_code}")
                print(f"Ошибка доступа к API со статус-кодом {cls.res.status_code}")


# Класс для представления выгруженных строк в виде объектов
class UserActive:
    def __init__(self, lti_user_id, passback_params, is_correct, attempt_type, created_at):
        self.__lti_user_id = lti_user_id
        self.__passback_params = passback_params
        self.__is_correct = is_correct
        self.__attempt_type = attempt_type
        self.__created_at = created_at

    def get_lti_user_id(self):
        return self.__lti_user_id

    def set_lti_user_id(self, lti_user_id):
        self.__lti_user_id = lti_user_id

    def get_passback_params(self):
        return self.__passback_params

    def set_passback_params(self, passback_params):
        self.__passback_params = passback_params

    def get_is_correct(self):
        return self.__is_correct

    def set_is_correct(self, is_correct):
        self.__is_correct = is_correct

    def get_attempt_type(self):
        return self.__attempt_type

    def set_attempt_type(self, attempt_type):
        self.__attempt_type = attempt_type

    def get_created_at(self):
        return self.__created_at

    def set_created_at(self, created_at):
        self.__created_at = created_at


# Класс для преобразования выгруженных данных
class Transform:
    RESULT_ROWS = []

    # Компиляция шаблонов регулярных выражений
    oauth_con_key_compiled = re.compile(r"\'oauth_consumer_key\'\: \'([A-Za-z1-9\_]*)\'")
    lis_sourcedid_compiled = re.compile(r"\bcourse-v\d{1,}\:SkillFactory.+\:lms.skillfactory.ru\-\w+\:\w+\b")
    service_url_compiled = re.compile(r"\bhttps?\:\/{2}lms\.skillfactory\.ru\/.+\/grade_handler\b")
    date_format = "%Y-%m-%d %H:%M:%S.%f"

    @classmethod
    def get_transformed_data(cls, data):
        logger.info("Начало валидации и преобразования данных")
        print("Начало валидации и преобразования данных")
        for obj in data:
            try:

                if obj.get_lti_user_id() is not None:
                    user_id = obj.get_lti_user_id()
                    oauth_consumer_key = cls.oauth_con_key_compiled.search(obj.get_passback_params()).group() if cls.oauth_con_key_compiled.search(obj.get_passback_params()).group() == '' else None
                    lis_result_sourcedid = cls.lis_sourcedid_compiled.search(obj.get_passback_params()).group() if cls.lis_sourcedid_compiled.search(obj.get_passback_params()) else None
                    lis_outcome_service_url = cls.service_url_compiled.search(obj.get_passback_params()).group() if cls.service_url_compiled.search(obj.get_passback_params()) else None
                    is_correct = obj.get_is_correct()
                    attempt_type = obj.get_attempt_type()
                    created_at = datetime.strptime(obj.get_created_at(), cls.date_format)

                    # Добавление спискового включения итоговых данных в атрибут класса RESULT_ROWS
                    cls.RESULT_ROWS.append(tuple([user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at]))

                else:
                    logger.error("Пропущена строка данных: user_id отсутствует")
                    print("Пропущена строка данных: user_id отсутствует")

            except Exception as err:
                logger.error(f"Ошибка при преобразовании данных: {err}", exc_info=False)
                print(f"Ошибка при преобразовании данных: {err}")

        logger.info("Конец валидации и преобразования данных")
        print("Конец валидации и преобразования данных")
        return cls.RESULT_ROWS


# Класс коннектор для загрузки данных в БД
class Load:
    def __init__(self, host='localhost', port=5432, database='students_grades', user='postgres', password='12345678', autocommit=False):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.__count_rows = 0

        if autocommit:
            self.connection.autocommit = True

    def create_table(self):
        with self.connection.cursor() as cursor:
            self.query = """
            create table  if not exists users_info (
                id serial primary key,
                user_id varchar(32) not null,
                oauth_consumer_key varchar(30),
                lis_result_sourcedid varchar(250),
                lis_outcome_service_url varchar(270),
                is_correct smallint,
                attempt_type varchar(6),
                created_at timestamp
            );
            """
            cursor.execute(self.query)

            if not self.connection.autocommit:
                self.connection.commit()

    def insert(self, data):
        logger.info("Заполнение базы данных началось")
        print("Заполнение базы данных началось")

        with self.connection.cursor() as cursor:
            self.query = """insert into users_info (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at)
                            values (%s, %s, %s, %s, %s, %s, %s)"""

            for rows in data:
                try:
                    # Выполнение SQL-запроса
                    cursor.execute(self.query, rows)

                    if not self.connection.autocommit:
                        self.connection.commit()

                    self.__count_rows += 1

                except Exception as err:
                    logger.error(f"Ошибка при вставке значений в БД: {err}", exc_info=False)

            logger.info(f"Заполнение базы данных закончилось. Записано {self.__count_rows} строк.")
            print(f"Заполнение базы данных закончилось. Записано {self.__count_rows} строк.")

    def truncate(self):
        with self.connection.cursor() as cursor:
            query = """truncate users_info"""
            cursor.execute(query)

            if not self.connection.autocommit:
                self.connection.commit()

        logger.info("Удаление данных из таблицы выполнено")
        print("Удаление данных из таблицы выполнено")

    def close_connection(self):
        self.connection.close()


# Класс для работы с GoogleSheets (создание/удаление/очистка страниц, загрузка расчитанных метрик)
class ImportSheets:
    __instance = None
    SCOPE = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file']

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self, url="https://docs.google.com/spreadsheets/d/1B_IT9Ia6zD2GTAwZFkoJd4LbMmUcUfnpzycYzuyjimY/edit?gid=0#gid=0", key_file="creds.json"):
        self.url = url
        self.key_file = key_file
        # Загружаем ключи аутентификации из файла json
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(key_file, self.SCOPE)
        # Авторизуемся в Google Sheets API
        self.client = gspread.authorize(self.creds)
        self.table = self.client.open_by_url(url)

    # Предоставление доступа по email
    def share(self, email, perm_type='user', role='writer'):
        self.table.share(email, perm_type=perm_type, role=role)

    # Добавить рабочий лист по названию
    def add_worksheet(self, title, rows=100, cols=15):
        if title not in [sheet.title for sheet in self.table.worksheets()]:
            self.table.add_worksheet(title, rows=rows, cols=cols)

    # Удалить рабочий лист по названию
    def del_worksheet(self, title):
        if title in [sheet.title for sheet in self.table.worksheets()]:
            self.table.del_worksheet(self.table.worksheet(title))

    # Очистить рабочий лист по названию
    def worksheet_clear(self, title):
        if title in [sheet.title for sheet in self.table.worksheets()]:
            self.worksheet = self.table.worksheet(title)
            self.worksheet.clear()

    def insert_rows(self, title, rows, start_row=1):
        try:
            self.worksheet = self.table.worksheet(title)
            print(f"Данные добавлены в Google Sheets: {title}")
        except gspread.WorksheetNotFound:
            print(f"Указанный лист {title} не найден!")

        self.worksheet.insert_rows(rows, row=start_row)


# Извлекаем данные из API
data = Extract.get_response()
# Преобразуем извлеченные данные в корректный формат (проверка данных)
correct_data = Transform.get_transformed_data(data)


# Создаем объект класса для подключения к БД и загружаем данные
db = Load()

# Создаем таблицу в БД, если она не существует
db.create_table()
# Загружаем исправленные данные в БД
db.insert(correct_data)
# Закрываем соединение
db.close_connection()


# Расчет агрегированных данных для Google Sheets
# Расчет данных по ежедневным уникальным юзерам 
daily_dau = calculating_metrics_mod.count_daily_uniq_users(correct_data)
# Расчет данных по ежедневным попыткам решений (submit) и правильным решениям (submit=1)
daily_attempts = calculating_metrics_mod.count_daily_submits(correct_data)


# Создаем объект класса для работы с Google Sheets
gs_object = ImportSheets()

# Очищаем листы таблицы, в которые загружаем данные
gs_object.worksheet_clear("Daily DAU")
gs_object.worksheet_clear("Daily Attempts")

# Добавление агрегированных данных по юзерам и решениям
gs_object.insert_rows("Daily DAU", daily_dau)
gs_object.insert_rows("Daily Attempts", daily_attempts)


# Отправляем email с информированием о результате и ссылкой на гугл-таблицу
try:
    # Указать почту отправителя и пароль
    sender_email = 'Указать_почту'
    password = 'Указать_пароль'

    # Создание объекта письма
    msg = EmailMessage()

    # Заполнение содержимого письма
    subject = "Данные загружены в БД и Google Sheets"
    message = f"Теперь вы можете ознакомиться с данными.\nДля Google Sheets данные доступны для просмотра по ссылке {gs_object.url}"

    msg.set_content(message)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = 'Указать_почту_получателя'

    # Создание защищенного соединения SSL
    smtp_server = "smtp.mail.ru"
    port = 465
    context = ssl.create_default_context()

    # Создание защищенного SSL-соединение с SMTP-сервером, который обрабатывает отправку электронной почты.
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.send_message(msg=msg)

except Exception as err:
    print(f"Ошибка при отправке письма: {err}. Укажите почту, пароль!")