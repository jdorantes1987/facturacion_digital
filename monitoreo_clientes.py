import logging
import logging.config
import time

from clientes_sheets import ClientesSheetManager

logging.config.fileConfig("logging.ini")


class MonitoreoClientes:
    def __init__(self, conexion):
        self.db = conexion
        # Usa variables de entorno o reemplaza por tus valores
        SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
        SHEET_NAME = os.getenv("GOOGLE_SHEET_CLIENTES_NAME", "clientes")
        CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        self.oClientesManager = ClientesSheetManager(
            SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
        )

    def __get_cursor(self):
        return self.db.get_cursor()

    def monitorear_cambios(self):
        cursor = self.__get_cursor()
        # Establece el valor inicial de versión
        cursor.execute("SELECT MAX(validador) FROM saCliente")
        ultima_version = cursor.fetchone()[0] or b"\x00" * 8  # valor binario inicial

        logging.info("Monitoreando cambios...")

        while True:
            time.sleep(5)

            cursor.execute(
                "SELECT * FROM saCliente WHERE validador > ?", ultima_version
            )
            cambios = cursor.fetchall()

            if cambios:
                logging.info(f"Se detectaron {len(cambios)} cambios:")
                for fila in cambios:
                    logging.info(
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
            logging.info("Sheet de clientes actualizada correctamente.")
        except Exception as e:
            logging.error(f"Error al actualizar sheet de clientes: {e}", exc_info=True)


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("..\\profit")
    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    load_dotenv(override=True)

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    db = DatabaseConnector(sqlserver_connector)
    oMonitoreoClientes = MonitoreoClientes(db)
    oMonitoreoClientes.monitorear_cambios()
