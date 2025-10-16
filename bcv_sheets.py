from datetime import datetime

from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from pandas import DataFrame


class BCVSheetManager:
    def __init__(self, spreadsheet_id, sheet_name, credentials_file):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.credentials_file = credentials_file
        self.service = self._get_service()

    def _get_service(self):
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = ServiceAccountCredentials.from_service_account_file(
            self.credentials_file, scopes=scopes
        )
        return build("sheets", "v4", credentials=creds)

    def clear_data_historico_tasas(self):
        range_to_clear = f"{self.sheet_name}!2:5000"  # Ajusta 1000 si esperas más filas
        request = (
            self.service.spreadsheets()
            .values()
            .clear(spreadsheetId=self.spreadsheet_id, range=range_to_clear, body={})
        )
        response = request.execute()
        return response

    def update_historico_tasas_sheet(self, data_historico_tasas: DataFrame):
        if not data_historico_tasas.empty:
            data_historico_tasas["fecha"] = data_historico_tasas["fecha"].dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            # # Optimiza el formateo usando DataFrame.round y applymap para múltiples columnas
            # cols_to_format = ["compra_bid2", "venta_ask2", "var_tasas"]
            # data_historico_tasas[cols_to_format] = (
            #     data_historico_tasas[cols_to_format].round(7)
            #     # Aplicar formato a múltiples columnas coma decimal
            #     .map(lambda x: f"{x:.7f}".replace(".", ","))
            # )
            # Convierte fechas y limpia el rango antes de actualizar
            self.clear_data_historico_tasas()

            # actualizar la hoja de Google Sheets con los datos del histórico de tasas desde la fila 2
            return (
                self.service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.sheet_name}!A2",
                    valueInputOption="RAW",
                    body={"values": data_historico_tasas.values.tolist()},
                )
                .execute()
            )
        return None

    def get_last_updated_date(self) -> str:
        creds = None
        if os.path.exists("token.json"):
            creds = UserCredentials.from_authorized_user_file(
                "token.json",
                ["https://www.googleapis.com/auth/drive.metadata.readonly"],
            )

        if not creds or not creds.valid:
            from google_auth_oauthlib.flow import InstalledAppFlow

            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json",
                ["https://www.googleapis.com/auth/drive.metadata.readonly"],
            )
            creds = flow.run_local_server(port=0)
            with open("token.json", "w") as token:
                token.write(creds.to_json())

        service = build("drive", "v3", credentials=creds)
        file_id = "1MUaDhCM_4sGWTboQgXk1_Ge_4VLy5QRsGe2omH08Lbk"
        file = service.files().get(fileId=file_id, fields="modifiedTime").execute()
        # Convierte la fecha de Google Drive (RFC3339) a formato normalizado
        modified_time = file.get("modifiedTime")
        if modified_time:
            # Ejemplo: '2024-06-10T14:23:45.123Z'
            dt = datetime.strptime(modified_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            return dt.strftime("%Y-%m-%d")
        return None


if __name__ == "__main__":
    import os
    import sys
    from datetime import date

    from dotenv import load_dotenv

    from data_sheets import ManagerSheets
    from datos_bcv import DatosBCV

    sys.path.append("..\\conexiones")

    env_path = os.path.join("..\\conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Usa variables de entorno o reemplaza por tus valores
    FILE_NAME = os.getenv("FILE_HISTORICO_TASAS_BCV_NAME")
    SPREADSHEET_ID = os.getenv("HISTORICO_TASAS_BCV_ID")
    CREDENTIALS_FILE = os.getenv("HISTORICO_TASAS_BCV_CREDENTIALS")
    HISTORICO_TASAS_SHEET_DATA = os.getenv("HISTORICO_TASAS_SHEET_DATA")

    # Inicializar el administrador de hojas de cálculo
    manager_sheets = ManagerSheets(FILE_NAME, SPREADSHEET_ID, CREDENTIALS_FILE)
    # Crear instancia de DatosBCV
    oDatosBCV = DatosBCV(manager_sheets)
    # Actualizar histórico de tasas en Google Sheets
    # data_historico_tasas = oDatosBCV.get_historico_tasas_actualizado()
    oBCV_sheet_manager = BCVSheetManager(
        SPREADSHEET_ID, HISTORICO_TASAS_SHEET_DATA, CREDENTIALS_FILE
    )
    ultima_fecha_actualizacion = oBCV_sheet_manager.get_last_updated_date()
    if ultima_fecha_actualizacion is not None:
        hoy = date.today().strftime("%Y-%m-%d")
        if ultima_fecha_actualizacion != hoy:
            print("Actualizando histórico de tasas...")
            data_historico_tasas = oDatosBCV.get_historico_tasas_actualizado()
            if not data_historico_tasas.empty:
                oBCV_sheet_manager.update_historico_tasas_sheet(data_historico_tasas)
                print(
                    "Actualizando hoja de Google Sheets con datos históricos de tasas..."
                )
            else:
                print("No hay datos históricos de tasas para actualizar.")
