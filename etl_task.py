from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Подключение к ClickHouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
}

# Дефолтные параметры, которые прокидываются в таски

default_args = {
    'owner': 'dm-gribanov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes =  5),
    'start_date': datetime(2023, 3, 23)
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_gribanov():
    # Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. 
    # В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
    @task
    def extract_feed():
        query = """
        select
          toDate(time) as event_date,
          user_id,
          gender,
          age,
          os,
          countIf(action = 'view') as views,
          countIf(action = 'like') as likes
        from
          simulator_20230220.feed_actions
        group by
          event_date,
          user_id,
          gender,
          age,
          os
        order by
          event_date"""
        df_feed = ph.read_clickhouse(query, connection = connection)
        return df_feed
    
    @task
    def extract_message():
        query = """
            select
              event_date,
              user_id,
              gender,
              age,
              os,
              messages_received,
              messages_sent,
              users_received,
              users_sent
            from
              (
                select
                  toDate(time) as event_date,
                  user_id,
                  gender,
                  age,
                  os,
                  count(reciever_id) as messages_sent,
                  count(distinct reciever_id) as users_sent
                from
                  simulator_20230220.message_actions
                group by
                  event_date,
                  user_id,
                  gender,
                  age,
                  os
              ) s1
              left join (
                select
                  reciever_id,
                  count(user_id) as messages_received,
                  count(distinct user_id) as users_received
                from
                  simulator_20230220.message_actions
                group by
                  reciever_id
              ) s2 on s1.user_id = s2.reciever_id
            order by
              event_date"""
        df_message = ph.read_clickhouse(query, connection = connection)
        return df_message
    # Далее объединяем две таблицы в одну. 
    @task
    def merge(df_feed, df_message):
        df = df_feed.merge(df_message, how = 'left', on = ['user_id','os','gender','age','event_date'])
        return df
    
    #Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
    @task
    def gender(df):
        df_gender = df.groupby(['event_date','gender'], as_index=False).agg\
           ({'likes': 'sum',
             'views': 'sum',
             'messages_received': 'sum',
             'messages_sent': 'sum',
             'users_received': 'sum',
             'users_sent': 'sum'})
        
        df_gender.insert(1, 'dimension', 'gender')
        df_gender.loc[df_gender.gender == 0, 'gender'] = 'female'
        df_gender.loc[df_gender.gender == 1, 'gender'] = 'male'
        df_gender = df_gender.rename(columns={'gender': 'dimension_value'})
        return df_gender
        
    @task
    def os(df):
        df_os = df.groupby(['event_date','os'], as_index=False).agg\
           ({'likes': 'sum',
             'views': 'sum',
             'messages_received': 'sum',
             'messages_sent': 'sum',
             'users_received': 'sum',
             'users_sent': 'sum'})
        
        df_os.insert(1, 'dimension', 'os')
        df_os = df_os.rename(columns={'os': 'dimension_value'})
        return df_os
        
    @task
    def age(df):
        df_age = df.groupby(['event_date','age'], as_index=False).agg\
           ({'likes': 'sum',
             'views': 'sum',
             'messages_received': 'sum',
             'messages_sent': 'sum',
             'users_received': 'sum',
             'users_sent': 'sum'})
        df_age.insert(1, 'dimension', 'age')
        df_age = df_age.rename(columns = {'age': 'dimension_value'})
        return df_age

    @task
    def concat(df_age, df_os, df_gender):
        df = pd.concat([df_age, df_os, df_gender])
        df = df.astype({'event_date': 'object',
                        'dimension': 'object',
                        'dimension_value': 'object',
                        'views': 'int64', 
                        'likes': 'int64', 
                        'messages_sent': 'int64',
                        'users_sent': 'int64', 
                        'users_received': 'int64', 
                        'messages_received': 'int64'})
        return df
        
    # И финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
    @task
    def to_clickhouse(df, table_name):
        query = """
            create table if not exists test.{} (
            event_date String, 
            dimension String, 
            dimension_value String, 
            views Int64, 
            likes Int64, 
            messages_sent Int64,
            users_sent Int64, 
            users_received Int64, 
            messages_received Int64
            )
            engine = MergeTree()
            order by event_date""".format(table_name)
        ph.execute(query = query, connection=connection_test)
        ph.to_clickhouse(df = df, table = table_name, connection = connection_test, index = False)
    
    df_feed = extract_feed()
    df_message = extract_message()
    df = merge(df_feed, df_message)
    df_gender = gender(df)
    df_os = os(df)
    df_age = age(df)
    df = concat(df_age, df_os, df_gender)
    to_clickhouse(df, 'dm_gribanov')
    
dag_gribanov = dag_gribanov()
