from discord_webhook import DiscordWebhook
from src import config
import requests
from bs4 import BeautifulSoup
from loguru import logger
import sqlite3
import json
import time
from datetime import datetime


db_path = 'src/db.sqlite'
global_params = {'topics_dict': {}, 'updates_hrefs': set}
logger.add(f'log/{__name__}.log', format='{time} {level} {message}', level='DEBUG', rotation='10 MB',
           compression='zip')

"""
Все ок работает. Чутка намудил с параметрами и с типами данных.
Сейчас скрипт видит что есть одна новая новость. Надо чекнуть как себя поведет когда их будет несколько
Хочу чтоб он смотрел не по последней записи, а по всей странице
Это позволит проверять редко и по этому даже если появится несколько новстей, он их всех сдетектит и отправит
Кажется работает
"""


def check_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    tables = cursor.execute(
        'SELECT name from sqlite_master WHERE type = "table"').fetchall()  # Смотрим какие есть таблицы
    valid_tables = []
    for t in tables:
        valid_tables.append(t[0])

    if 'actual_topics' not in valid_tables:
        cursor.execute('CREATE TABLE "actual_topics" ("id"	INTEGER NOT NULL, "title" TEXT NOT NULL, "href" TEXT NOT '
                       'NULL, "update_time" TEXT, PRIMARY KEY("id" AUTOINCREMENT))')
    conn.commit()


def parse_titles_hrefs_from_site():
    s = requests.Session()
    url = 'https://forum.crossout.ru/index.php?/forum/20-novosti/'
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0"}
    response = s.get(url, headers=headers)
    response_soup = BeautifulSoup(response.text, 'lxml')  # Передаем в суп полученную страницу
    topics = response_soup.findAll('h4', class_='ipsDataItem_title ipsContained_container')
    #logger.info(f'topics: {topics}')
    topics_dict = {}
    for topic in topics:  # По итерации по всем элементам таблицы
        title = topic.a.get('title')  # Находим все заголовки
        #logger.debug(f'title: {title}') 25
        href = topic.a.get('href')  # И вложенные в них ссылки
        #logger.debug(f'href: {href}') 25
        # Превращаем данные в словарь (заголовок: ссылка)
        topics_dict[f'{href}'] = title
        global_params['topics_dict'] = {}  # Зачищаем то что осталось в глобальной переменной
        global_params['topics_dict'] = topics_dict  # Кладем в глобальную переменную
    #logger.info(f'global_params["topics_dict"]: {global_params["topics_dict"]}')
        #Кладем словарь. Ключ - ссылка, значение - заголовок
    #logger.info(f'topics_dict: {topics_dict}')
    #for key in topics_dict.keys():
    #    print(key)
    #logger.info(f'len(topics_dict): {len(topics_dict)}')


def posting_updates():
    updates = global_params["updates_hrefs"]
    if updates:
        for i in updates:
            logger.info(f'Updates href for posting: {i}')
            do_discord_webhook(config.odin_webhook_url, i)
    else:
        logger.info(f'No updates for posting')
        pass


def do_alarm(t_alarmtext):
    headers = {"Content-type": "application/json"}
    payload = {"text": f"{t_alarmtext}", "chat_id": f"{config.admin_id}"}
    requests.post(url=config.webhook_url, data=json.dumps(payload), headers=headers)


def check_updates():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    new_topics = global_params["topics_dict"]
    new_hrefs = []  # Собираем ссылки на темы с сайта в множество
    for topic in new_topics:
        new_hrefs.append(topic)
    #logger.info(f'new_hrefs: {new_hrefs}')
    old_hrefs = []
    old_hrefs_from_db = cursor.execute("SELECT href FROM actual_topics").fetchall()  # Собираем из базы сохраненные ссылки
    for old_href in old_hrefs_from_db:  # Превращаем их в множество
        #logger.info(f'old_hrefs_from_db: {old_hrefs_from_db}')
        old_hrefs.append(old_href[0])
    conn.commit()
    #logger.info(f'old_hrefs: {old_hrefs}')
    #logger.info(f'new_hrefs: {len(new_hrefs)}')
    #logger.info(f'old_hrefs: {len(old_hrefs)}')

    # Преобразуем в множесто, чтоб можно было один список вычесть из другого
    new_hrefs = set(new_hrefs)
    old_hrefs = set(old_hrefs)
    updates_hrefs = new_hrefs - old_hrefs
    #logger.info(f'updates_hrefs: {updates_hrefs}')
    # Пишем в глобальную переменную
    global_params['updates_hrefs'] = set  # зачищая предварительно то что там было
    global_params['updates_hrefs'] = updates_hrefs
    #logger.info(f'global_params["updates_hrefs"]: {global_params["updates_hrefs"]}')


def save_updates():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM actual_topics')  # Удаляем текущие данные
    now = datetime.now()
    time = now.strftime('%d-%m-%Y %H:%M:%S')
    for key in global_params["topics_dict"].keys():  # Перезаписываем актуальные темы
        #logger.info(f'topik: {global_params["topics_dict"][f"{key}"]}, href: {key} ')
        cursor.execute(f'INSERT INTO actual_topics VALUES (Null, "{global_params["topics_dict"][f"{key}"]}", "{key}", "{time}")')
    conn.commit()


def do_discord_webhook(webhook_url, content):
    webhook = DiscordWebhook(url=f'{webhook_url}', content=f'{content}')
    response = webhook.execute()
    return response


@logger.catch()
def main():
    while True:
        parse_titles_hrefs_from_site()
        check_updates()
        posting_updates()
        check_db()
        save_updates()
        time.sleep(3600)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Вы завершили работу программы collector')
        #logger.info('Program has been stop manually')
    except IndexError:
        logger.error(f'IndexError Exception')
    except Exception as e:
        t_alarmtext = f'Crossout_helper (app_collector.py):\n {str(e)}'
        do_alarm(t_alarmtext)
        logger.error(f'Other except error Exception')