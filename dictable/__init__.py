import datetime
import json

from .filter import FilterToQuery

def json_encode(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    raise TypeError (f'Object of type {type(value)} is not JSON not serializable')

class Dictsqlite:
    def __init__(self, sqlite):
        self.sqlite = sqlite
        self._dictable_cache = {}

    def __getitem__(self, table_name):
        return Dictable(self, table_name)
    
    def execute(self, *args, **kwargs):
        return self.sqlite.execute(*args, **kwargs)


class Dictable:
    def __init__(self, dictsqlite, table_name, json_column='_json'):
        self.sqlite = dictsqlite.sqlite
        self.json_column = None
        self.table_name = table_name
        dictable_cache = dictsqlite._dictable_cache.get(self.table_name)
        if dictable_cache is None:
            self.primary_columns = set()
            self.other_columns = set()
            sql = f'pragma table_info({self.table_name})'
            bad_table = True
            for row in self.sqlite.execute(sql):
                bad_table = False
                if row[1] == json_column:
                    self.json_column = json_column
                    continue
                if row[5]:
                    self.primary_columns.add(row[1])
                else:
                    self.other_columns.add(row[1])
            if bad_table:
                raise ValueError(f'No such table: {table_name}')
            if not self.json_column and json_column:
                raise ValueError(f'table {table_name} must have a column {json_column}')
            self.all_columns = tuple(self.primary_columns) + tuple(self.other_columns) + (self.json_column,)
                
            dictsqlite._dictable_cache[self.table_name] = \
                (self.primary_columns, self.other_columns, self.all_columns, self.json_column)
        else:
            self.primary_columns, self.other_columns, self.all_columns, self.json_column = dictable_cache

    def __setitem__(self, key, document):
        if isinstance(key, tuple):
            values = key
        else:
            values = (key,)
        if len(values) != len(self.primary_columns):
            raise KeyError(f'key for table {self.table_name} requires {len(self.primary_columns)} value(s), {len(values)} given')
        values += tuple(document.get(i) for i in self.other_columns)
        if self.json_column:
            json_value = dict((k,v) for k, v in document.items() if k not in self.other_columns and k not in self.primary_columns)
            values += (json.dumps(json_value, default=json_encode),)
        sql = f'insert into {self.table_name} ({",".join(self.all_columns)}) values ({",".join("?" for i in self.all_columns)})'
        print('!sql!', sql, values)
        self.sqlite.execute(sql, values)

    def add(self, document):
        key = tuple(document.get(i) for i in self.primary_columns)
        self[key] = document

    def _row_to_dict(self, row):
        result = dict(zip(self.all_columns,row))
        if self.json_column:
            result.update(json.loads(result.pop(self.json_column)))
        return row
    
    def __getitem__(self, key):
        if isinstance(key, tuple):
            values = key
        else:
            values = (key,)
        if len(values) != len(self.primary_columns):
            raise KeyError(f'key for table {self.table_name} requires {len(self.primary_columns)} value(s), {len(values)} given')
        sql = f'select {",".join(self.all_columns)} from {self.table_name} WHERE {" AND ".join(f"{k} = ?" for k in self.primary_columns)}'
        print('!sql!', sql, values)
        return self._row_to_dict(next(self.sqlite.execute(sql, values)))
    
    def __iter__(self):
        sql = f'select {",".join(self.all_columns)} from {self.table_name}'
        print('!sql!', sql)
        for row in self.sqlite.execute(sql):
            yield self._row_to_dict(row)

    def count(self):
        sql = f'select count(*) from {self.table_name}'
        print('!sql!', sql)
        return next(self.sqlite.execute(sql))[0]

    def filter(self, query):
        if False:
            yield None
