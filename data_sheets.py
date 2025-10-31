import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pandas import DataFrame


class ManagerSheets:
    def __init__(self, file_sheet_name, spreadsheet_id, credentials_file):
        self.file_sheet_name = file_sheet_name
        self.spreadsheet_id = spreadsheet_id
        self.credentials_file = credentials_file
        self.spreadsheet = self._get_spreadsheet()

    def _get_spreadsheet(self):
        # Autenticación y acceso a Google Sheets usando google-auth
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        # Crea credenciales dependiendo del tipo de credentials_file
        if isinstance(self.credentials_file, dict):
            creds = ServiceAccountCredentials.from_service_account_info(
                self.credentials_file, scopes=scopes
            )
        else:
            creds = ServiceAccountCredentials.from_service_account_file(
                self.credentials_file, scopes=scopes
            )

        client = gspread.authorize(creds)
        return client.open(self.file_sheet_name)

    def get_data_hoja(self, sheet_name) -> DataFrame:
        # Selecciona la hoja de Google Sheets
        worksheet = self.spreadsheet.worksheet(sheet_name)
        # Obtiene todos los valores de la hoja de cálculo
        data = DataFrame(
            worksheet.get_all_values()[1:],  # ignora la primera fila de encabezados
            columns=worksheet.get_all_values()[
                0
            ],  # obtiene la primera fila como encabezados
        )
        return data
