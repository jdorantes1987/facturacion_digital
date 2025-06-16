import sys
from pandas import DataFrame

sys.path.append("..\\profit")
from data.mod.ventas.clientes import Clientes


class ClientesProfit:
    def __init__(self, conexion):
        self.clientes = Clientes(conexion)

    def get_clientes(self) -> DataFrame:
        return self.clientes.get_clientes_profit()


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
        oClientesProfit = ClientesProfit(oConexion)
        clientes = oClientesProfit.get_clientes()
        print(clientes)
    except Exception as e:
        print(f"Error al obtener clientes: {e}")
