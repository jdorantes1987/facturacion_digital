import logging
import logging.config
import time

logging.config.fileConfig("logging.ini")


class MonitoreoClientes:
    def __init__(self, conexion, clientes_sheet_manager):
        self.db = conexion
        self.logger = logging.getLogger(__class__.__name__)
        self.oClientesManager = clientes_sheet_manager

    def __get_cursor(self):
        return self.db.get_cursor()

    def monitorear_cambios(self):
        cursor = self.__get_cursor()
        # Establece el valor inicial de versión
        cursor.execute("SELECT MAX(validador) FROM saCliente")
        ultima_version = cursor.fetchone()[0] or b"\x00" * 8  # valor binario inicial

        self.logger.info("Monitoreando cambios...")

        while True:
            time.sleep(5)

            cursor.execute(
                "SELECT * FROM saCliente WHERE validador > ?", ultima_version
            )
            cambios = cursor.fetchall()

            if cambios:
                self.logger.info(f"Se detectaron {len(cambios)} cambios:")
                for fila in cambios:
                    self.logger.info(
                        fila[0] + " " + fila[2] + " " + fila[70] + " " + str(fila[71])
                    )

                self.enviar_cambios_a_google_sheets()
                # Actualiza la última versión conocida
                cursor.execute("SELECT MAX(validador) FROM saCliente")
                ultima_version = cursor.fetchone()[0]

    def enviar_cambios_a_google_sheets(self):
        try:
            db = self.db
            self.oClientesManager.update_clientes_sheet(db)
            self.logger.info("Sheet de clientes actualizada correctamente.")
        except Exception as e:
            self.logger.error(
                f"Error al actualizar sheet de clientes: {e}", exc_info=True
            )


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv
    from clientes_sheets import ClientesSheetManager

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
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    sqlserver_connector.connect()
    # Para Google Sheets
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_CLIENTES_NAME", "clientes")
    CREDENTIALS_FILE = os.getenv("CGIMPRENTA_CREDENTIALS")
    clientes_sheet_manager = ClientesSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )
    db = DatabaseConnector(sqlserver_connector)
    oMonitoreoClientes = MonitoreoClientes(db, clientes_sheet_manager)
    oMonitoreoClientes.monitorear_cambios()
    sqlserver_connector.close_connection()
