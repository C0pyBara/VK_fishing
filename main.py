from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import requests
import csv
import pandas as pd
import matplotlib.pyplot as plt

# Параметры
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Функция для выгрузки данных из VK
def fetch_vk_data():
    access_token = 'YOUR_ACCESS_TOKEN'
    group_id = 'vk_fishing'
    version = '5.131'

    def get_group_members(group_id, offset=0, count=1000):
        url = f'https://api.vk.com/method/groups.getMembers'
        params = {
            'group_id': group_id,
            'offset': offset,
            'count': count,
            'fields': 'id,first_name,last_name,last_seen,contacts,city,counters',
            'access_token': access_token,
            'v': version
        }
        response = requests.get(url, params=params)
        return response.json()

    members = []
    offset = 0
    while True:
        data = get_group_members(group_id, offset=offset)
        if 'response' in data and data['response']['items']:
            members.extend(data['response']['items'])
            offset += 1000
        else:
            break

    with open('/tmp/vk_group_members.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['user_id_vk', 'fullname', 'last_seen', 'contacts', 'friends_count', 'town']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for member in members:
            writer.writerow({
                'user_id_vk': member['id'],
                'fullname': f"{member['first_name']} {member['last_name']}",
                'last_seen': member.get('last_seen', {}).get('time', 'N/A'),
                'contacts': member.get('mobile_phone', 'N/A'),
                'friends_count': member.get('counters', {}).get('friends', 'N/A'),
                'town': member.get('city', {}).get('title', 'N/A')
            })

# Функция для загрузки данных в Clickhouse
def load_data_to_clickhouse():
    client = Client('localhost')

    client.execute('''
    CREATE TABLE IF NOT EXISTS vk_group_members (
        user_id_vk UInt64,
        fullname String,
        last_seen UInt64,
        contacts String,
        friends_count UInt32,
        town String
    ) ENGINE = MergeTree()
    ORDER BY user_id_vk
    ''')

    data = []
    with open('/tmp/vk_group_members.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append((
                int(row['user_id_vk']),
                row['fullname'],
                int(row['last_seen']) if row['last_seen'] != 'N/A' else 0,
                row['contacts'],
                int(row['friends_count']) if row['friends_count'] != 'N/A' else 0,
                row['town']
            ))

    client.execute('INSERT INTO vk_group_members VALUES', data)

# Функция для анализа данных и построения графиков
def analyze_and_plot_data():
    client = Client('localhost')

    # Топ-5 самых популярных имен
    query_top_names = '''
    SELECT
        fullname,
        COUNT(*) as count
    FROM vk_group_members
    GROUP BY fullname
    ORDER BY count DESC
    LIMIT 5
    '''
    top_names = client.execute(query_top_names)

    names_df = pd.DataFrame(top_names, columns=['fullname', 'count'])
    names_df.plot(kind='bar', x='fullname', y='count', title='Top 5 Most Popular Names')
    plt.savefig('/tmp/top_5_names.png')

    # Диаграмма рассеяния: Количество друзей в зависимости от возраста
    query_friends_age = '''
    SELECT
        contacts as age,
        friends_count
    FROM vk_group_members
    WHERE contacts != 'N/A'
    '''
    friends_age = client.execute(query_friends_age)

    friends_age_df = pd.DataFrame(friends_age, columns=['age', 'friends_count'])
    friends_age_df.plot(kind='scatter', x='age', y='friends_count', title='Friends Count vs Age')
    plt.savefig('/tmp/friends_vs_age.png')

    # Топ-3 города по среднему количеству друзей участников
    query_top_cities = '''
    SELECT
        town,
        AVG(friends_count) as avg_friends
    FROM vk_group_members
    GROUP BY town
    ORDER BY avg_friends DESC
    LIMIT 3
    '''
    top_cities = client.execute(query_top_cities)
    print("Top 3 cities with highest average number of friends:", top_cities)

    # Самый часто встречаемый город
    query_most_common_city = '''
    SELECT
        town,
        COUNT(*) as count
    FROM vk_group_members
    GROUP BY town
    ORDER BY count DESC
    LIMIT 1
    '''
    most_common_city = client.execute(query_most_common_city)
    print("Most common city among group members:", most_common_city)

# Определение DAG
with DAG(
    'vk_group_data_dag',
    default_args=default_args,
    description='DAG для выгрузки данных из группы VK, загрузки в Clickhouse и анализа',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_vk_data',
        python_callable=fetch_vk_data
    )

    load_data = PythonOperator(
        task_id='load_data_to_clickhouse',
        python_callable=load_data_to_clickhouse
    )

    analyze_data = PythonOperator(
        task_id='analyze_and_plot_data',
        python_callable=analyze_and_plot_data
    )

    fetch_data >> load_data >> analyze_data
