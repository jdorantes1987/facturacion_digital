import sys

from get_api_invoices import GetInvoices

sys.path.append("..\\profit")
from data.mod.ventas import documentos


class SincronizaFacturacion:
    def __init__(self, conexion, api_gateway_client):
        self.oDocumentos = documentos.Documentos(conexion)
        self.client = api_gateway_client

    def set_facturas_profit(self, params=None):
        documentos = self.oDocumentos.get_documentos(tipo_doc="FACT")
        # fecha sin hora, minutos y segundos
        documentos["fec_emis"] = documentos["fec_emis"].dt.normalize()
        # filtrar entre fechas por el campo "fec_emis"
        documentos = documentos[
            documentos["fec_emis"].between(params["fechaInicio"], params["fechaFin"])
        ]
        return set(documentos["nro_doc"].str.strip())

    def set_facturas_imprenta(self, params=None):
        oGetInvoices = GetInvoices(self.client)
        facturas_imprenta = oGetInvoices.get_data_invoices(params=params)
        if facturas_imprenta.empty:
            return set()
        return set(facturas_imprenta["invoice_number"].str.strip())

    def diferencias_profit_vs_imprenta(self, params=None):
        set_profit = self.set_facturas_profit(params=params)
        set_imprenta = self.set_facturas_imprenta(params=params)
        return set_profit.difference(set_imprenta)

    def diferencias_imprenta_vs_profit(self, params=None):
        set_profit = self.set_facturas_profit(params=params)
        set_imprenta = self.set_facturas_imprenta(params=params)
        return set_imprenta.difference(set_profit)


if __name__ == "__main__":

    import os
    from datetime import date

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector
    from dotenv import load_dotenv

    from api_gateway_client import ApiGatewayClient
    from api_key_manager import ApiKeyManager

    load_dotenv(override=True)

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
    # print(oSincronizaFacturacion.diferencias_imprenta_vs_profit(params=params))
    print(oSincronizaFacturacion.set_facturas_profit(params=params))
