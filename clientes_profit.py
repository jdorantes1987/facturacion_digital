import sys
from pandas import DataFrame

sys.path.append("../profit")
from data.mod.ventas.clientes import Clientes


class ClientesProfit:
    def __init__(self, conexion):
        self.clientes = Clientes(conexion)

    def get_clientes(self) -> DataFrame:
        return self.clientes.get_clientes_profit()


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    db = DatabaseConnector(sqlserver_connector)
    oClientesProfit = ClientesProfit(db)
    clientes = oClientesProfit.get_clientes()
    print(clientes)
