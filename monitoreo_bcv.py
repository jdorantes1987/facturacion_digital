import os
import sys
import datetime
from dotenv import load_dotenv
import time

from data_sheets import ManagerSheets
from datos_bcv import DatosBCV
from bcv_sheets import BCVSheetManager


sys.path.append("..\\profit")

env_path = os.path.join("..\\profit", ".env")
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

oBCV_sheet_manager = BCVSheetManager(
    SPREADSHEET_ID, HISTORICO_TASAS_SHEET_DATA, CREDENTIALS_FILE
)


# Actualizar histórico de tasas en Google Sheets
def actualizar_historico_tasas():
    data_historico_tasas = oDatosBCV.get_historico_tasas_actualizado()
    if not data_historico_tasas.empty:
        oBCV_sheet_manager.update_historico_tasas_sheet(data_historico_tasas)
        print("Actualizando hoja de Google Sheets con datos históricos de tasas...")
    else:
        print("No hay datos históricos de tasas para actualizar.")


def ejecutar_diariamente_a_hora(hora=0, minuto=0):
    while True:
        ahora = datetime.datetime.now()
        proxima_ejecucion = ahora.replace(
            hour=hora, minute=minuto, second=0, microsecond=0
        )
        if proxima_ejecucion <= ahora:
            proxima_ejecucion += datetime.timedelta(days=1)
        segundos_espera = (proxima_ejecucion - ahora).total_seconds()
        print(
            f"Esperando {segundos_espera:.0f} segundos para la próxima ejecución a las {proxima_ejecucion.time()}"
        )
        time.sleep(segundos_espera)
        actualizar_historico_tasas()


if __name__ == "__main__":
    ejecutar_diariamente_a_hora(hora=0, minuto=1)
