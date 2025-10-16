import logging
import logging.config
import time

logging.config.fileConfig("logging.ini")


class MonitoreoFacturacion:
    def __init__(
        self,
        conexion,
        oTokenGenerator,
        oFacturasManager,
        oSincronizaFacturacion,
        oDatosBCV,
    ):
        self.db = conexion
        self.logger = logging.getLogger(__class__.__name__)
        self.oFacturasManager = oFacturasManager
        self.oSincronizaFacturacion = oSincronizaFacturacion
        self.oDatosBCV = oDatosBCV
        self.oTokenGenerator = oTokenGenerator

    def __get_cursor(self):
        return self.db.get_cursor()

    def monitorear_cambios(self, params={}):
        cursor = self.__get_cursor()
        # Establece el valor inicial de versión
        cursor.execute("SELECT MAX(validador) FROM saFacturaVenta")
        ultima_version = cursor.fetchone()[0] or b"\x00" * 8  # valor binario inicial

        self.logger.info("Monitoreando cambios...")

        while True:
            time.sleep(5)

            cursor.execute(
                "SELECT * FROM saFacturaVenta WHERE validador > ?", ultima_version
            )
            cambios = cursor.fetchall()

            if cambios:
                self.logger.info(f"Se detectaron {len(cambios)} cambios:")
                self.logger.info("Generando nuevo token de autenticación...")
                self.oTokenGenerator.update_token()
                self.logger.info(
                    "Esperando 30 segundos antes de procesar los cambios..."
                )
                time.sleep(30)
                for fila in cambios:
                    self.logger.info(
                        fila[0] + " " + fila[2] + " " + fila[51] + " " + str(fila[53])
                    )
                # Actualiza la fecha de fin a hoy
                params["fechaFin"] = date.today().strftime("%Y-%m-%d")
                procesado = self.verificar_y_procesar_cambios(params=params)
                if procesado:
                    self.logger.info("Cambios enviados a Google Sheets.")

                cursor.execute("SELECT MAX(validador) FROM saFacturaVenta")
                ultima_version = cursor.fetchone()[0]

    def verificar_y_procesar_cambios(self, params={}):
        # Obtener datos históricos de tasas
        historico_tasas = self.oDatosBCV.get_historico_tasas_google_sh()[
            ["fecha", "venta_ask2"]
        ]
        data = self.oSincronizaFacturacion.data_a_validar_en_sheet(params=params)
        if data is None or data.empty:
            self.logger.info("No hay datos nuevos para procesar.")
            return False
        return self.oFacturasManager.update_facturas_sheet(
            data_facturas_a_validar=data, historico_tasas=historico_tasas
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
    from facturas_sheets import FacturasSheetManager

    sys.path.append("..\\conexiones")
    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("..\\conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    sqlserver_connector.connect()
    db = DatabaseConnector(sqlserver_connector)
    oTokenGenerator = TokenGenerator()
    # Inicializar el cliente de la API
    api_url = os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES")
    api_key_manager = ApiKeyManager()
    client = ApiGatewayClient(api_url, api_key_manager)

    oSincronizaFacturacion = SincronizaFacturacion(db, client)

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

    oMonitoreoFacturacion = MonitoreoFacturacion(
        db, oTokenGenerator, oFacturasManager, oSincronizaFacturacion, datos_bcv
    )
    hoy = date.today().strftime("%Y-%m-%d")
    params = {
        "fechaInicio": "2025-06-20",  # Fecha de inicio del rango
        "fechaFin": hoy,  # Fecha de fin del rango
    }
    oMonitoreoFacturacion.monitorear_cambios(params=params)
    sqlserver_connector.close_connection()
