from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pandas import DataFrame, merge_asof


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
            f"{self.sheet_name}!K2:K1000",  # Descripción Factura
            f"{self.sheet_name}!M2:M1000",  # Total Articulo
            f"{self.sheet_name}!N2:N1000",  # Monto
            f"{self.sheet_name}!O2:O1000",  # Tasa BCV
        ]
        # Usa batchClear para limpiar todos los rangos en una sola llamada
        self.service.spreadsheets().values().batchClear(
            spreadsheetId=self.spreadsheet_id, body={"ranges": ranges_to_clear}
        ).execute()
        # Escribir False en toda la columna U2:U1000
        values = [[False] for _ in range(999)]
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.sheet_name}!V2:V1000",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
        return True

    def update_facturas_sheet(
        self, data_facturas_a_validar: DataFrame, historico_tasas: DataFrame
    ):
        data = data_facturas_a_validar.copy()
        historico_tasas.sort_values(by="fecha", inplace=True, ascending=True)
        # Asegúrate de que el DataFrame no esté vacío antes de proceder
        if data.empty:
            print("No hay datos para actualizar en la hoja de facturas.")
            return None

        data.sort_values(by="fec_emis", inplace=True, ascending=True)
        data["fec_emis"] = data["fec_emis"].dt.normalize()  # Normalizar fechas
        data = merge_asof(
            data,
            historico_tasas,
            left_on="fec_emis",
            right_on="fecha",
            direction="nearest",
        )  # Combinar por aproximación
        data.sort_values(by="doc_num_encab", inplace=True, ascending=True)
        self.clear_facturas_data()
        if not data.empty:
            # Selecciona y ordena las columnas a actualizar
            # Asegúrate de que las columnas existan en el DataFrame
            columnas = [
                "doc_num_encab",
                "rif",
                "co_art",
                "comentario",
                "descrip",
                "total_art",
                "prec_vta",
                "venta_ask2",
            ]
            values = data[columnas].values.tolist()

            # Actualiza las columnas B, C y H en una sola llamada usando batchUpdate
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {
                        "range": f"{self.sheet_name}!B2:B{len(values) + 1}",
                        "values": [[row[0]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!C2:C{len(values) + 1}",
                        "values": [[row[1]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!H2:H{len(values) + 1}",
                        "values": [[row[2]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!J2:J{len(values) + 1}",
                        "values": [[row[3]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!K2:K{len(values) + 1}",
                        "values": [[row[4]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!M2:M{len(values) + 1}",
                        "values": [[row[5]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!N2:N{len(values) + 1}",
                        "values": [[row[6]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!O2:O{len(values) + 1}",
                        "values": [[row[7]] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!P2:P{len(values) + 1}",
                        "values": [[""] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!Q2:Q{len(values) + 1}",
                        "values": [[0.0] for _ in values],
                    },
                    {
                        "range": f"{self.sheet_name}!R2:R{len(values) + 1}",
                        "values": [[0.0] for _ in values],
                    },
                    {
                        "range": f"{self.sheet_name}!S2:S{len(values) + 1}",
                        "values": [[""] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!T2:T{len(values) + 1}",
                        "values": [[""] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!U2:U{len(values) + 1}",
                        "values": [[""] for row in values],
                    },
                    {
                        "range": f"{self.sheet_name}!V2:V{len(values) + 1}",
                        "values": [[True] for _ in values],
                    },
                ],
            }
            return (
                self.service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
                .execute()
            )


if __name__ == "__main__":
    import os
    import sys
    from datetime import date

    from dotenv import load_dotenv

    from api_gateway_client import ApiGatewayClient
    from api_key_manager import ApiKeyManager
    from data_sheets import ManagerSheets
    from datos_bcv import DatosBCV
    from sincroniza_facturacion import SincronizaFacturacion
    from token_generator import TokenGenerator

    sys.path.append("..\\profit")
    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("..\\profit", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

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
    CREDENTIALS_FILE = os.getenv("CGIMPRENTA_CREDENTIALS")

    # Inicializar el administrador de hojas de cálculo
    oFacturasManager = FacturasSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )

    # Inicializar el cliente de la API
    api_url = os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES")
    api_key_manager = ApiKeyManager()
    client = ApiGatewayClient(api_url, api_key_manager)

    SHEET_NAME = os.getenv("FILE_HISTORICO_TASAS_BCV_NAME")
    SPREADSHEET_ID = os.getenv("HISTORICO_TASAS_BCV_ID")
    CREDENTIALS_FILE = os.getenv("HISTORICO_TASAS_BCV_CREDENTIALS")

    # Inicializar el administrador de hojas de cálculo
    manager_sheets = ManagerSheets(SHEET_NAME, SPREADSHEET_ID, CREDENTIALS_FILE)
    # Crear instancia de DatosBCV
    datos_bcv = DatosBCV(manager_sheets)

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

        # Obtener datos históricos de tasas
        historico_tasas = datos_bcv.get_historico_tasas_google_sh()[
            ["fecha", "venta_ask2"]
        ]

        data = oSincronizaFacturacion.data_a_validar_en_sheet(params=params)
        if (
            oFacturasManager.update_facturas_sheet(
                data_facturas_a_validar=data, historico_tasas=historico_tasas
            )
            is not None
        ):
            print("Hoja de facturas actualizada correctamente.")
    except Exception as e:
        print(f"Error al actualizar la hoja de facturas: {e}")
