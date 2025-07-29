import logging


class DatosBCV:
    def __init__(self, manager_sheets):
        self.manager_sheets = manager_sheets

    def get_historico_tasas(self):
        self.data = self.manager_sheets.get_data_hoja(sheet_name="data")
        if self.data.empty:
            logging.error("No hay datos en la hoja 'data'.")
            return None
        return self.data


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    from data_sheets import ManagerSheets

    sys.path.append("..\\profit")

    env_path = os.path.join("..\\profit", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Usa variables de entorno o reemplaza por tus valores
    SHEET_NAME = os.getenv("FILE_HISTORICO_TASAS_BCV_NAME")
    SPREADSHEET_ID = os.getenv("HISTORICO_TASAS_BCV_ID")
    CREDENTIALS_FILE = os.getenv("HISTORICO_TASAS_BCV_CREDENTIALS")

    # Inicializar el administrador de hojas de cálculo
    manager_sheets = ManagerSheets(SHEET_NAME, SPREADSHEET_ID, CREDENTIALS_FILE)
    # Crear instancia de DatosBCV
    datos_bcv = DatosBCV(manager_sheets)
    # Obtener datos históricos de tasas
    historico_tasas = datos_bcv.get_historico_tasas()
    if historico_tasas is not None:
        print(historico_tasas)
