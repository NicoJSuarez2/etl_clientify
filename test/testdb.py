import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=72.61.70.104,1433;"
    "DATABASE=master;"
    "UID=sa;"
    "PWD=TuPasswordFuerte123!;"
    "Encrypt=no;"
)

print("Conectado correctamente")
