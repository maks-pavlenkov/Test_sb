from sqlalchemy import MetaData
from sqlalchemy_schemadisplay import create_schema_graph
import psycopg2
from psycopg2 import Error
from collections import defaultdict
import os

os.environ["PATH"] += os.pathsep + r'C:\Program Files (x86)\Mozilla Firefox\Graphviz/bin'  # Тут надо вставить путь от


# папки, где расположена папка bin библиотеки graphviz


class GetErText:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)

    """This creates a diagram"""
    def creating(self):
        graph = create_schema_graph(
            metadata=MetaData(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'),
            show_datatypes=True,
            show_indexes=False,
            rankdir='BT',
            concentrate=False,
        )
        graph.write_png('dbschema.jpg')

    """Getting tables and columns for use in the next func"""
    def get_data(self):
        prepared_data = defaultdict(list)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "select table_name, column_name from information_schema.columns where table_schema not in ("
                    "'information_schema','pg_catalog');")
                for char in cursor.fetchall():
                    if not prepared_data:
                        prepared_data[char[0]] = [char[1]]
                    else:
                        prepared_data[char[0]] += [char[1]]
            return prepared_data
        except (Exception, Error) as error:
            print('Ошибка подлкючения к БД - ', error)

    """Using prev data and getting all foreign keys and creating a txt file"""
    def get_text(self):
        try:
            with self.connection.cursor() as cursor:
                result = {}
                cursor.execute(
                    "select table_name, constraint_name from information_schema.constraint_column_usage where "
                    "constraint_name "
                    "like 'fk_%';")
                data = self.get_data()
                for char in cursor.fetchall():
                    edited = char[1].split('_')
                    if not result and char[0] in data:
                        result[char[0]] = [edited[1]]
                    else:
                        result[char[0]] += [edited[1]]
        except (Exception, Error) as error:
            print('Ошибка подлкючения к БД - ', error)

        with open('data.txt', 'w') as file:
            for key, value in data.items():
                for key1 in result.keys():
                    if key1 in key:
                        changed = [", ".join(m) for m in result.values()]
                        file.write(f'{key}: {", ".join(value)}\n    linked objects: {", ".join(changed)}\n')
                    else:
                        file.write(f'{key}: {", ".join(value)}\n')


obj = GetErText(user='postgres', password='Oracle', host='localhost', port='5432', database='postgres')
obj.creating()
obj.get_text()
