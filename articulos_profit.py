import sys

sys.path.append("..\\profit")
from data.mod.inventario.articulos import Articulos


class ArticulosProfit:
    def __init__(self, conexion):
        self.articulos = Articulos(conexion)

    def get_articulos(self):
        return self.articulos.get_articulos()


if __name__ == "__main__":
    import os

    from conn.conexion import DatabaseConnector
    from dotenv import load_dotenv

    load_dotenv(override=True)
    # Para SQL Server
    datos_conexion = dict(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        base_de_datos=os.environ["DB_NAME_DERECHA_PROFIT"],
    )
    try:
        oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
        oArticulosProfit = ArticulosProfit(oConexion)
        articulos = oArticulosProfit.get_articulos()
        print(articulos)
    except Exception as e:
        print(f"Error al obtener articulos: {e}")
