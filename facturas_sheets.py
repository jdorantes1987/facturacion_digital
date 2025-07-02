from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pandas import DataFrame


class FacturasSheetManager:
    def __init__(self, spreadsheet_id, sheet_name, credentials_file):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.credentials_file = credentials_file
        self.service = self._get_service()

    def _get_service(self):
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(
            self.credentials_file, scopes=scopes
        )
        return build("sheets", "v4", credentials=creds)

    def clear_facturas_data(self):
        # Define los rangos a limpiar
        ranges_to_clear = [
            f"{self.sheet_name}!B2:B1000",  # Factura Number
            f"{self.sheet_name}!C2:C1000",  # Rif
            f"{self.sheet_name}!H2:H1000",  # Codigo Producto
            f"{self.sheet_name}!J2:J1000",  # Comentario
            f"{self.sheet_name}!M2:M1000",  # Monto
            f"{self.sheet_name}!L2:L1000",  # Total Articulo
        ]
        # Usa batchClear para limpiar todos los rangos en una sola llamada
        self.service.spreadsheets().values().batchClear(
            spreadsheetId=self.spreadsheet_id, body={"ranges": ranges_to_clear}
        ).execute()
        # Escribir False en toda la columna U2:U1000
        values = [[False] for _ in range(999)]
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.sheet_name}!U2:U1000",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
        return True

    def update_facturas_sheet(self, data_facturas_a_validar: DataFrame):
        data = data_facturas_a_validar.copy()
        self.clear_facturas_data()
        if not data.empty:
            # Selecciona y ordena las columnas a actualizar
            # Asegúrate de que las columnas existan en el DataFrame
            columnas = [
                "doc_num_encab",
                "rif",
                "co_art",
                "comentario",
                "total_art",
                "reng_neto",
            ]
            values = data[columnas].values.tolist()

            # Actualiza las columnas B, C y H en una sola llamada usando batchUpdate
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {
                        "range": f"{self.sheet_name}!B2:B{len(values)+1}",
                        "values": [[row[0]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!C2:C{len(values)+1}",
                        "values": [[row[1]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!H2:H{len(values)+1}",
                        "values": [[row[2]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!J2:J{len(values)+1}",
                        "values": [[row[3]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!L2:L{len(values)+1}",
                        "values": [[row[4]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!M2:M{len(values)+1}",
                        "values": [[row[5]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!U2:U{len(values)+1}",
                        "values": [[True] for _ in values],
                    },
                ],
            }
            self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=body
            ).execute()


if __name__ == "__main__":
    import os
    import sys
    from datetime import date

    from dotenv import load_dotenv

    from api_gateway_client import ApiGatewayClient
    from api_key_manager import ApiKeyManager
    from sincroniza_facturacion import SincronizaFacturacion
    from token_generator import TokenGenerator

    sys.path.append("..\\profit")
    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    load_dotenv(override=True)
    TokenGenerator().update_token()
    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.getenv("HOST_PRODUCCION_PROFIT"),
        database=os.getenv("DB_NAME_DERECHA_PROFIT"),
        user=os.getenv("DB_USER_PROFIT"),
        password=os.getenv("DB_PASSWORD_PROFIT"),
    )

    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_FACTURACION_NAME", "facturacion")
    CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # Inicializar el administrador de hojas de cálculo
    oFacturasManager = FacturasSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )

    # Inicializar el cliente de la API
    api_url = os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES")
    api_key_manager = ApiKeyManager()
    client = ApiGatewayClient(api_url, api_key_manager)

    try:
        db = DatabaseConnector(sqlserver_connector)
        oFacturasManager.clear_facturas_data()
        oSincronizaFacturacion = SincronizaFacturacion(db, client)
        hoy = date.today().strftime("%Y-%m-%d")

        # Puedes pasar parámetros de consulta si la API los soporta, por ejemplo: {"numeroFactura": "12345"}
        params = {
            "fechaInicio": "2025-06-20",  # Fecha de inicio del rango
            "fechaFin": hoy,  # Fecha de fin del rango
        }

        data = oSincronizaFacturacion.data_a_validar_en_sheet(params=params)
        oFacturasManager.update_facturas_sheet(data_facturas_a_validar=data)
        print("Hoja de facturas actualizada correctamente.")
    except Exception as e:
        print(f"Error al actualizar la hoja de facturas: {e}")
