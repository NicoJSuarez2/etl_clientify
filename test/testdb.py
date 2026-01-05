import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=master;"
    "UID=x;"
    "PWD=x;"
    "Encrypt=no;"
)

print("Conectado correctamente")
