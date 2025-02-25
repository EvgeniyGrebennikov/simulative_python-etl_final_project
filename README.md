### Итоговым проектом является скрипт, в котором:
- Будет происходить обращение к нашему API для получения данных по активности студентам онлайн-платформы;
- Данные будут обрабатываться и готовиться к загрузке в базу данных;
- Обработанные данные будут загружаться в локальную базу PostgreSQL, которая предварительно развернута на своем компьютере;
- Одновременно с загрузкой данных в БД будет проводиться их агрегация и рассчет показателей активности студентов (количество попыток решить задачу, количество успешных попыток и уникальных юзеров за день). Все расчеты выгружаются в таблицу Google Sheets;
- По окончанию работы скрпита происходит email-оповещение на почту.
<br>
Во время обработки будет сохраняться лог работы скрипта с отлавливанием всех ошибок и выводом промежуточных стадий (например, скачивание началось / скачивание завершилось / заполнение базы началось и т.д., с трекингом времени). Лог нужно сохранять в текстовый файл. Файл нужно именовать в соответствии с текущей датой. Если в папке с логами уже есть другие логи - их необходимо удалять, оставляем только логи за последние 3 дня.
<br>
#### Работа с API
Для взаимодействия с API и получения данных, используется библиотеку requests.
Пример ответа от API:

	[
    {
        "lti_user_id": "3583bf109f8b458e13ae1ac9d85c396a",
        "passback_params": "{'oauth_consumer_key': '', 'lis_result_sourcedid': 'course-v1:SkillFactory+DST-3.0+28FEB2021:lms.skillfactory.ru-ca3ecf8e5f284c329eb7bd529e1a9f7e:3583bf109f8b458e13ae1ac9d85c396a', 'lis_outcome_service_url': 'https://lms.skillfactory.ru/courses/course-v1:SkillFactory+DST-3.0+28FEB2021/xblock/block-v1:SkillFactory+DST-3.0+28FEB2021+type@lti+block@ca3ecf8e5f284c329eb7bd529e1a9f7e/handler_noauth/grade_handler'}",
        "is_correct": null,
        "attempt_type": "run",
        "created_at": "2023-05-31 09:16:11.313646"
    },
    {
        "lti_user_id": "ab6ddeb7654ab35d44434d8db629bd01",
        "passback_params": "{'oauth_consumer_key': '', 'lis_result_sourcedid': 'course-v1:SkillFactory+DSPR-2.0+14JULY2021:lms.skillfactory.ru-0cf38fe58c764865bae254da886e119d:ab6ddeb7654ab35d44434d8db629bd01', 'lis_outcome_service_url': 'https://lms.skillfactory.ru/courses/course-v1:SkillFactory+DSPR-2.0+14JULY2021/xblock/block-v1:SkillFactory+DSPR-2.0+14JULY2021+type@lti+block@0cf38fe58c764865bae254da886e119d/handler_noauth/grade_handler'}",
        "is_correct": null,
        "attempt_type": "run",
        "created_at": "2023-05-31 09:16:30.117858"
    }
	]
<br>
#### Структура таблицы
Выгруженные данные сохраняются локально в базу данных (таблица users_info):
- user_id - строковый айди пользователя;
- oauth_consumer_key - уникальный токен клиента;
- lis_result_sourcedid - ссылка на блок, в котором находится задача в ЛМС;
- lis_outcome_service_url - URL адрес в ЛМС, куда мы шлем оценку;
- is_correct - была ли попытка верной (null, если это run);
- attempt_type - ран или сабмит;
- created_at - дата и время попытки.
<b><i>oauth_consumer_key, lis_result_sourcedid</i></b> и <b><i>lis_outcome_service_url</i></b> находятся в <b><i>passback_params</i></b>. Предварительно необходимо провалидировать данные (чтобы не пропустить не тот тип данных или не подходящее значение). Если что-то не так - запись можно пропустить и занести информацию в лог.
<br>
Проект представлен в файлах <b><i>ETL.py</i></b> и <b><i>calculating_metrics_mod.py</i></b>, где:
- ETL.py - основной ETL-скрипт, позволяющий произвести выгрузку данных, их обработку и загрузку в БД и Google Sheets;
-  calculating_metrics_mod.py - скрипт с агрегацией данных по активности студентов (для Google Sheets). Данный файл импортируется в виде библиотеки в основной скрипт ETL.py.

