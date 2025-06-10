import gspread
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from pandas import DataFrame, to_numeric


class DataFacturacion:
    def __init__(self):
        # Autenticación y acceso a Google Sheets
        self.scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(
            "key.json", self.scope
        )
        client = gspread.authorize(self.creds)
        self.spreadsheet = client.open("facturacion_cgimprenta")

        # Construir el servicio de la API de Google Sheets
        self.sheet_service = build("sheets", "v4", credentials=self.creds)

    def get_data_a_facturar(self) -> DataFrame:
        # Selecciona la hoja de Google Sheets
        worksheet = self.spreadsheet.worksheet("facturacion")
        # Obtiene todos los valores de la hoja de cálculo
        data = DataFrame(
            worksheet.get_all_values()[1:],  # ignora la primera fila de encabezados
            columns=worksheet.get_all_values()[
                0
            ],  # obtiene la primera fila como encabezados
        )
        cols_montos = [
            "precioProducto",
            "tasa_del_dia",
            "monto",
            "igtf",
        ]  # Lista de columnas que contienen montos
        # Eliminar separadores de miles y reemplazar coma decimal por punto
        for col in cols_montos:
            data[col] = (
                data[col]
                .str.replace(".", "", regex=False)  # Remove thousand separator
                .str.replace(",", ".", regex=False)  # Replace decimal comma with dot
            )
        try:
            # Convertir a float, forzando errores a NaN
            data[cols_montos] = data[cols_montos].apply(
                to_numeric, errors="raise"  # Convertir a float errores a NaN
            )
            data["cantidadAdquirida"] = data["cantidadAdquirida"].astype(int)
            data = data[
                (data["facturar"].str.upper() == "SI")
                & (data["nombreRazonSocialCliente"] != "NO EXISTE")
                & (data["nombreProducto"] != "NO EXISTE")
            ]
        except Exception as e:
            print(f"Error en get_data_a_facturar: {e}")
            data = DataFrame()  # Si hay un error, devuelve un DataFrame vacío

        return data

    def get_data_clientes(self) -> DataFrame:
        # Selecciona la hoja de Google Sheets
        worksheet = self.spreadsheet.worksheet("clientes")
        # Obtiene todos los valores de la hoja de cálculo
        data = DataFrame(
            worksheet.get_all_values()[1:],  # ignora la primera fila de encabezados
            columns=worksheet.get_all_values()[
                0
            ],  # obtiene la primera fila como encabezados
        )
        return data

    def get_data_productos(self) -> DataFrame:
        # Selecciona la hoja de Google Sheets
        worksheet = self.spreadsheet.worksheet("productos")
        # Obtiene todos los valores de la hoja de cálculo
        data = DataFrame(
            worksheet.get_all_values()[1:],  # ignora la primera fila de encabezados
            columns=worksheet.get_all_values()[
                0
            ],  # obtiene la primera fila como encabezados
        )
        cols_montos = [
            "precio",
        ]  # Lista de columnas que contienen montos
        # Eliminar separadores de miles y reemplazar coma decimal por punto
        for col in cols_montos:
            data[col] = (
                data[col]
                .str.replace(".", "", regex=False)  # Remove thousand separator
                .str.replace(",", ".", regex=False)  # Replace decimal comma with dot
            )
        try:
            # Convertir a float, forzando errores a NaN
            data[cols_montos] = data[cols_montos].apply(
                to_numeric, errors="raise"  # Convertir a float errores a NaN
            )
        except Exception as e:
            print(f"Error al convertir columnas a numéricas: {e}")
            data = DataFrame()  # Si hay un error, devuelve un DataFrame vacío

        return data


if __name__ == "__main__":
    oDataFacturacion = DataFacturacion()
    # Puedes pasar parámetros de consulta si la API los soporta, por ejemplo: {"numeroFactura": "12345"}
    params = None
    result = oDataFacturacion.get_list_invoices(params)
    print("Facturas consultadas:", result)
