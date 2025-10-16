import logging
import logging.config
import time

logging.config.fileConfig("logging.ini")


class MonitoreoProductos:
    def __init__(self, conexion, productos_sheet_manager=None):
        self.db = conexion

        self.oProductosManager = productos_sheet_manager
        self.logger = logging.getLogger(__class__.__name__)

    def __get_cursor(self):
        return self.db.get_cursor()

    def monitorear_cambios(self):
        cursor = self.__get_cursor()
        # Establece el valor inicial de versión
        cursor.execute("SELECT MAX(validador) FROM saArticulo")
        ultima_version = cursor.fetchone()[0] or b"\x00" * 8  # valor binario inicial
        self.logger.info("Monitoreando cambios...")

        while True:
            time.sleep(5)

            cursor.execute(
                "SELECT * FROM saArticulo WHERE validador > ?", ultima_version
            )
            cambios = cursor.fetchall()

            if cambios:
                self.logger.info(f"Se detectaron {len(cambios)} cambios:")
                for fila in cambios:
                    self.logger.info(
                        fila[0] + " " + fila[2] + " " + fila[60] + " " + str(fila[61])
                    )

                self.enviar_cambios_a_google_sheets()
                # Actualiza la última versión conocida
                cursor.execute("SELECT MAX(validador) FROM saArticulo")
                ultima_version = cursor.fetchone()[0]

    def enviar_cambios_a_google_sheets(self):
        try:
            db = self.db
            self.oProductosManager.update_productos_sheet(db)
            self.logger.info("Sheet de clientes actualizada correctamente.")
        except Exception as e:
            self.logger.error(
                f"Error al actualizar sheet de clientes: {e}", exc_info=True
            )


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv
    from productos_sheets import ProductosSheetManager

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
    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_PRODUCTOS_NAME", "productos")
    CREDENTIALS_FILE = os.getenv("CGIMPRENTA_CREDENTIALS")

    # Para Google Sheets
    productos_sheet_manager = ProductosSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )

    db = DatabaseConnector(sqlserver_connector)
    oMonitoreoProductos = MonitoreoProductos(db, productos_sheet_manager)
    oMonitoreoProductos.monitorear_cambios()
    sqlserver_connector.close_connection()
