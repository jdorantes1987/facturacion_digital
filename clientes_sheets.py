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
        data = oClientesProfit.get_clientes()[
            ["rif", "cli_des", "email", "telefonos", "direc1"]
        ]
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

    from conn.conexion import DatabaseConnector
    from dotenv import load_dotenv

    load_dotenv(override=True)
    # Para SQL Server
    datos_conexion = dict(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        base_de_datos=os.environ["DB_NAME_DERECHA_PROFIT"],
    )

    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_CLIENTES_NAME", "clientes")
    CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    oClientesManager = ClientesSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )
    try:
        oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
        oClientesManager.update_clientes_sheet(oConexion)
        print("Hoja de clientes actualizada correctamente.")
    except Exception as e:
        print(f"Error al actualizar la hoja de clientes: {e}")
