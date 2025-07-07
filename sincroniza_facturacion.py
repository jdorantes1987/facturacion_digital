import sys

from numpy import nan
from pandas import DataFrame

from get_api_invoices import GetInvoices

sys.path.append("..\\profit")
from data.mod.ventas import documentos
from data.mod.ventas.clientes import Clientes
from data.mod.ventas.facturas_ventas import FacturasVentas


class SincronizaFacturacion:
    def __init__(self, conexion, api_gateway_client):
        self.oDocumentos = documentos.Documentos(conexion)
        self.oVentas = FacturasVentas(conexion)
        self.oClientes = Clientes(conexion)
        self.client = api_gateway_client

    def __set_facturas_profit(self, params=None):
        documentos = self.oDocumentos.get_documentos(tipo_doc="FACT")
        # fecha sin hora, minutos y segundos
        documentos["fec_emis"] = documentos["fec_emis"].dt.normalize()
        # filtrar entre fechas por el campo "fec_emis"
        documentos = documentos[
            documentos["fec_emis"].between(params["fechaInicio"], params["fechaFin"])
        ]
        return set(documentos["nro_doc"].str.strip())

    def __set_facturas_imprenta(self, params=None):
        oGetInvoices = GetInvoices(self.client)
        facturas_imprenta = oGetInvoices.get_data_invoices(params=params)
        if facturas_imprenta.empty:
            return set()
        facturas_imprenta = facturas_imprenta[
            facturas_imprenta["document"] == "FACTURA"
        ]
        return set(facturas_imprenta["invoice_number"].str.strip())

    def data_a_validar_en_sheet(self, params=None) -> DataFrame:
        # Campos a validar en Sheet
        campos_a_validar = [
            "doc_num_encab",
            "rif",
            "fec_emis",
            "co_art",
            "total_art",
            "prec_vta",
            "comentario",
        ]
        # Conjuntos de facturas de Profit e Imprenta
        set_profit = self.__set_facturas_profit(params=params)
        set_imprenta = self.__set_facturas_imprenta(params=params)
        # Obtener las diferencias entre Profit e Imprenta
        diferencias = set_profit.difference(set_imprenta)
        # Si no hay diferencias, retornar un DataFrame vacío
        if not diferencias:
            return DataFrame()

        # Obtener los clientes de Profit
        clientes = self.oClientes.get_clientes_profit()[["co_cli", "rif"]]
        # Obtener las facturas de Profit entre las fechas especificadas
        data = self.oVentas.get_facturas(
            fecha_d=params["fechaInicio"], fecha_h=params["fechaFin"]
        )
        data["co_cli"] = data["co_cli"].str.strip()
        data["doc_num_encab"] = data["doc_num_encab"].str.strip()
        data["co_art"] = data["co_art"].str.strip()
        data.sort_values(by=["doc_num_encab", "reng_num"], inplace=True)
        # Combinar datos de facturas con clientes de Profit
        data = data.merge(clientes, left_on="co_cli", right_on="co_cli", how="left")
        # Filtrar las facturas que no están en la imprenta
        data = data[data["doc_num_encab"].isin(diferencias)]
        # Reemplazar valores NaN por cadenas vacías
        return data.replace(nan, "", regex=True)[campos_a_validar]


if __name__ == "__main__":

    import os
    from datetime import date

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector
    from dotenv import load_dotenv

    from api_gateway_client import ApiGatewayClient
    from api_key_manager import ApiKeyManager
    from token_generator import TokenGenerator

    load_dotenv(override=True)
    TokenGenerator().update_token()

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    db = DatabaseConnector(sqlserver_connector)

    api_url = os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES")
    api_key_manager = ApiKeyManager()
    client = ApiGatewayClient(api_url, api_key_manager)

    oSincronizaFacturacion = SincronizaFacturacion(db, client)
    hoy = date.today().strftime("%Y-%m-%d")

    # Puedes pasar parámetros de consulta si la API los soporta, por ejemplo: {"numeroFactura": "12345"}
    params = {
        "fechaInicio": "2025-06-20",  # Fecha de inicio del rango
        "fechaFin": hoy,  # Fecha de fin del rango
    }
    print(
        oSincronizaFacturacion.data_a_validar_en_sheet(params=params).to_string(
            index=False
        )
    )
    # print(oSincronizaFacturacion.set_facturas_profit(params=params))
