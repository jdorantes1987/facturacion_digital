import locale
import socket
import ssl
from datetime import datetime
from urllib.request import build_opener, urlcleanup, urlretrieve

from numpy import where
from pandas import DataFrame, concat, to_datetime
from xlrd import open_workbook

ssl._create_default_https_context = ssl._create_unverified_context
url_base = "https://www.bcv.org.ve/sites/default/files/EstadisticasGeneral"
dic_f_usd_year = {
    "2025": [
        "2_1_2d25_smc.xls",
        "2_1_2c25_smc.xls",
        "2_1_2b25_smc.xls",
        "2_1_2a25_smc.xls",
    ],
    "2024": [
        "2_1_2d24_smc.xls",
        "2_1_2c24_smc.xls",
        "2_1_2b24_smc.xls",
        "2_1_2a24_smc.xls",
    ],
    "2023": [
        "2_1_2d23_smc.xls",
        "2_1_2c23_smc.xls",
        "2_1_2c23_smc_60.xls",
        "2_1_2a23_smc.xls",
    ],
    "2022": [
        "2_1_2d22_smc.xls",
        "2_1_2c22_smc.xls",
        "2_1_2b22_smc.xls",
        "2_1_2a22_smc.xls",
    ],
    "2021": [
        "2_1_2d21_smc.xls",
        "2_1_2c21_smc.xls",
        "2_1_2b21_smc.xls",
        "2_1_2a21_smc_58.xls",
    ],
    "2020": [
        "2_1_2d20_smc.xls",
        "2_1_2c20_smc.xls",
        "2_1_2b20_smc.xls",
        "2_1_2a20_smc.xls",
    ],
}


class DatosBCV:
    def __init__(self, manager_sheets):
        self.manager_sheets = manager_sheets

    # Actualiza el archivo tasas_BCV.xlsx

    def get_historico_tasas_actualizado(self) -> DataFrame:
        locale.setlocale(locale.LC_ALL, "es_ES")
        df_file_tasa_new = self.get_data_usd_bcv_web_last_qt()
        if df_file_tasa_new.empty:
            print("No se pudo descargar el archivo de tasas del BCV.")
            return DataFrame()

        df_file_tasa = self.get_historico_tasas_google_sh()
        df_file_tasa_filtred = df_file_tasa[
            df_file_tasa["archivo"] != self.get_name_file_tasa_download()
        ]
        new_file_tasa = [df_file_tasa_new, df_file_tasa_filtred]
        df = concat(new_file_tasa).reset_index(drop=True)
        df["año"] = df["fecha"].dt.year
        df["mes"] = df["fecha"].dt.month
        df["dia"] = df["fecha"].dt.day
        df["mes_"] = df["fecha"].dt.month_name(locale="es_ES").str[:3]
        locale.setlocale(locale.LC_ALL, "")
        df["var_tasas"] = df["venta_ask2"].diff(
            periods=-1
        )  # Permite calcular la diferencia que existe entre el valor de la celda actual con respecto a la anterior
        df["var_tasas"] = df["var_tasas"].fillna(0)
        return df

    def get_data_usd_bcv_web_last_qt(self) -> DataFrame:
        data = DataFrame()
        name_file_bcv = self.get_name_file_tasa_download()
        url = url_base + f"/{name_file_bcv}"
        socket.setdefaulttimeout(7)  # 3 seconds
        #  cambiar el encabezado del agente de usuario
        opener = build_opener()
        #  agregar el encabezado de solicitud de agente de usuario
        opener.addheaders = [
            (
                "User-Agent",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/7.0.4 Mobile/16B91 Safari/605.1.15",
            )
        ]
        try:
            # urlretrieve Descarga archivos de la red al equipo local
            file_name = urlretrieve(url)
            print("Se descargó archivo de la ruta:", url)
            # Como el resultado de la descarga es una tuple con la información del archivo y el recurso de la web se coloca el indice [0] que es la del archivo
            wb = open_workbook(
                file_name[0], on_demand=True
            )  # Abre el libro de trabajo con el indicador "on_demand=True", para que las hojas no se carguen automáticamente.
            sheets = wb.sheet_names()
            cols = [
                "cod_mon",
                "mon_pais",
                "compra_bid",
                "venta_ask",
                "compra_bid2",
                "venta_ask2",
            ]
            df_arr_sh = []
            for i in range(len(sheets)):
                sh = wb.sheet_by_index(i)
                # Creamos las listas
                filas = []
                for fila in range(11, 33):
                    columnas = [
                        sh.cell_value(fila, columna + 1) for columna in range(0, 6)
                    ]
                    filas.append(columnas)
                df_base = DataFrame(filas, columns=cols)
                # extrae la fecha valor de la celda D5, hay que remover los espacios
                df_base["fecha"] = (
                    str(sh.cell_value(4, 3))[13:].replace("/", "").strip()
                )
                # df_base['fecha'] = sh.name  # Anteriormente extraía la fecha del nombre de la hoja lo cual es incorrecto, ya que se debe tomar la fecha valor.
                df_base["fecha"] = to_datetime(df_base["fecha"], format="%d%m%Y")
                df_base["archivo"] = name_file_bcv
                df_arr_sh.append(df_base)
            data = concat(df_arr_sh, axis=0, ignore_index=True)
            data = data[data["cod_mon"] == "USD"]
            data["compra_bid2"] = data["compra_bid2"].astype(float)
            data["venta_ask2"] = data["venta_ask2"].astype(float)
            urlcleanup()
            # wb.release_resources()
            # del wb
        except socket.timeout:
            print("socket.timeout")
        return data

    def get_name_file_tasa_download(self) -> str:
        year = str(datetime.now().year)
        quaeter = -self.get_current_quarter_number()
        name_file_tasa_download = dic_f_usd_year[year][quaeter]
        return name_file_tasa_download

    def get_current_quarter_number(self) -> int:
        quarter_number = (int(datetime.now().strftime("%m")) + 2) // 3
        return quarter_number

    def get_historico_tasas_google_sh(self) -> DataFrame:
        self.data = self.manager_sheets.get_data_hoja(sheet_name="data")
        if self.data.empty:
            print("No hay datos en la hoja 'data'.")
            return DataFrame()  # Retorna un DataFrame vacío en lugar de None

        data = self.data.copy()
        # Reemplaza valores vacíos por 0
        data["var_tasas"] = where(data["var_tasas"] == "", "0,00", data["var_tasas"])

        # Optimización: usar una función para limpiar y convertir columnas
        def limpiar_columna(col):
            return (
                col.str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .astype(float)
            )

        data["compra_bid"] = limpiar_columna(data["compra_bid"])
        data["venta_ask"] = limpiar_columna(data["venta_ask"])
        data["compra_bid2"] = limpiar_columna(data["compra_bid2"])
        data["venta_ask2"] = limpiar_columna(data["venta_ask2"])
        data["var_tasas"] = limpiar_columna(data["var_tasas"])
        data["fecha"] = data["fecha"].astype("datetime64[ns]")
        data.sort_values(by="fecha", inplace=True, ascending=False)
        return data


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    from data_sheets import ManagerSheets

    sys.path.append("../conexiones")

    env_path = os.path.join("../conexiones", ".env")
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
    oDatosBcv = DatosBCV(manager_sheets)
    # Obtener datos históricos de tasas
    historico_tasas = oDatosBcv.get_historico_tasas_actualizado()
    if not historico_tasas.empty:
        print(historico_tasas.dtypes)
