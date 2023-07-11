

from pyhive import hive

class Connect:

    def __init__(self, 
                 host: str = "35.208.0.141",
                 port: int = 10000,
                 database: str = "default",
                 username: str = "admin",
                 password: str = None):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self._connect = self.connect()

    def connect(self):
        return hive.Connection(host=self.host,
                               port=self.port,
                               database=self.database,
                               username=self.username,
                               password=self.password)
    
    def get_fetchone(self, query: str):
        cursor = self._connect.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        if result is not None:
            return result[0]
        else:
            return None 

    
    def execute(self, query: str):
        return self._connect.cursor().execute(query)

    def fetchall(self, query: str):
        return self.execute(query).fetchall()
    
    def fetchone(self, query: str):
        return self.execute(query).fetchone()
    
    def close(self):
        self.connect().close()
    
