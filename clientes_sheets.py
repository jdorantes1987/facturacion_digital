from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from clientes_profit import ClientesProfit


class ClientesSheetManager:
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

    def clear_clientes_data(self):
        range_to_clear = f"{self.sheet_name}!2:1000"  # Ajusta 1000 si esperas más filas
        request = (
            self.service.spreadsheets()
            .values()
            .clear(spreadsheetId=self.spreadsheet_id, range=range_to_clear, body={})
        )
        response = request.execute()
        return response

    def update_clientes_sheet(self, conexion):
        self.clear_clientes_data()
        oClientesProfit = ClientesProfit(conexion=conexion)
        data = oClientesProfit.get_clientes()
        data = data[
            (data["inactivo"] == 0) & (data["tipo_adi"] <= 2)
        ]  # Filtrar clientes no anulados y los que no son sucursales
        data = data[["rif", "cli_des", "email", "telefonos", "direc1"]]
        if not data.empty:
            # actualizar la hoja de Google Sheets con los datos de clientes desde la fila 2
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A2",
                valueInputOption="RAW",
                body={"values": data.values.tolist()},
            ).execute()


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
        host=os.getenv("HOST_PRODUCCION_PROFIT"),
        database=os.getenv("DB_NAME_DERECHA_PROFIT"),
        user=os.getenv("DB_USER_PROFIT"),
        password=os.getenv("DB_PASSWORD_PROFIT"),
    )

    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_CLIENTES_NAME", "clientes")
    CREDENTIALS_FILE = os.getenv("CGIMPRENTA_CREDENTIALS")

    oClientesManager = ClientesSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )
    try:
        db = DatabaseConnector(sqlserver_connector)
        oClientesManager.update_clientes_sheet(db)
        print("Hoja de clientes actualizada correctamente.")
    except Exception as e:
        print(f"Error al actualizar la hoja de clientes: {e}")
