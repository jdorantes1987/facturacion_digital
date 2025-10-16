from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from articulos_profit import ArticulosProfit


class ProductosSheetManager:
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

    def clear_productos_data(self):
        range_to_clear = f"{self.sheet_name}!2:1000"  # Ajusta 1000 si esperas más filas
        request = (
            self.service.spreadsheets()
            .values()
            .clear(spreadsheetId=self.spreadsheet_id, range=range_to_clear, body={})
        )
        response = request.execute()
        return response

    def update_productos_sheet(self, conexion):
        column_dev_data = ["co_art", "art_des", "art_des_coment", "precio", "tipo_imp"]
        self.clear_productos_data()
        oArticulosProfit = ArticulosProfit(conexion=conexion)
        art_profit = oArticulosProfit.get_articulos()[
            ["co_art", "art_des", "tipo_imp", "anulado"]
        ]
        art_profit = art_profit[(~art_profit["anulado"])]
        art_profit["art_des_coment"] = art_profit["art_des"]
        # Tipo de impuesto aplicable. Campo obligatorio. Valores permitidos: "E" (Exento), "R" (Reducido), "G" (General), "A" (Adicional), "P" (Percibido).
        art_profit["tipo_imp"] = art_profit["tipo_imp"].replace(
            {
                "5": "E",
                "Reducido": "R",
                "1": "G",
                "6": "E",
                "Adicional": "A",
                "Percibido": "P",
                "7": "E",
            }
        )
        if not art_profit.empty:
            precios = oArticulosProfit.get_articulos_precio()
            art_profit = art_profit.merge(precios, on="co_art", how="left")
            # reemplazar los valores NaN en la columna 'precio' con 1
            art_profit["precio"] = art_profit["precio"].fillna(1)
            art_profit = art_profit[column_dev_data]
            # actualizar la hoja de Google Sheets con los datos de clientes desde la fila 2
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A2",
                valueInputOption="RAW",
                body={"values": art_profit.values.tolist()},
            ).execute()


if __name__ == "__main__":
    import os
    import sys

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector
    from dotenv import load_dotenv

    sys.path.append("..\\conexiones")

    env_path = os.path.join("..\\conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.getenv("HOST_PRODUCCION_PROFIT"),
        database=os.getenv("DB_NAME_DERECHA_PROFIT"),
        user=os.getenv("DB_USER_PROFIT"),
        password=os.getenv("DB_PASSWORD_PROFIT"),
    )

    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_PRODUCTOS_NAME", "productos")
    CREDENTIALS_FILE = os.getenv("CGIMPRENTA_CREDENTIALS")

    oArticulosManager = ProductosSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )
    try:
        db = DatabaseConnector(sqlserver_connector)
        oArticulosManager.update_productos_sheet(db)
        print("Hoja de productos actualizada correctamente.")
    except Exception as e:
        print(f"Error al actualizar la hoja de productos: {e}")
