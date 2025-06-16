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
        data = oArticulosProfit.get_articulos()[
            ["co_art", "art_des", "tipo_imp", "anulado"]
        ]
        data = data[(~data["anulado"])]
        data["art_des_coment"] = data["art_des"]
        # Tipo de impuesto aplicable. Campo obligatorio. Valores permitidos: "E" (Exento), "R" (Reducido), "G" (General), "A" (Adicional), "P" (Percibido).
        data["tipo_imp"] = data["tipo_imp"].replace(
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
        if not data.empty:
            data["precio"] = 1.0
            data = data[column_dev_data]

            # actualizar la hoja de Google Sheets con los datos de clientes desde la fila 2
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A2",
                valueInputOption="RAW",
                body={"values": data.values.tolist()},
            ).execute()


if __name__ == "__main__":
    import os

    from conn.conexion import DatabaseConnector
    from dotenv import load_dotenv

    load_dotenv()
    # Para SQL Server
    datos_conexion = dict(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        base_de_datos=os.environ["DB_NAME_DERECHA_PROFIT"],
    )

    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_PRODUCTOS_NAME", "productos")
    CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    oArticulosManager = ProductosSheetManager(
        SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )
    try:
        oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
        oArticulosManager.update_productos_sheet(oConexion)
        print("Hoja de productos actualizada correctamente.")
    except Exception as e:
        print(f"Error al actualizar la hoja de productos: {e}")
