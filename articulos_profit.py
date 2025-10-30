import sys

sys.path.append("../profit")
from data.mod.inventario.articulos import Articulos


class ArticulosProfit:
    def __init__(self, conexion):
        self.articulos = Articulos(conexion)

    def get_articulos(self):
        return self.articulos.get_articulos()

    def get_articulos_precio(self):
        """
        Obtiene los artículos con su precio.
        :return: DataFrame con los artículos y sus precios.
        """
        return self.articulos.get_articulos_precio()


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
    oArticulosProfit = ArticulosProfit(db)
    articulos = oArticulosProfit.get_articulos()
    print(articulos)
