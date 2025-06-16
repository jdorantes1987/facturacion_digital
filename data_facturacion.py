import logging
import gspread


from oauth2client.service_account import ServiceAccountCredentials
from pandas import DataFrame, to_numeric

required_fields = {
    "enum": int,
    "numeroFactura": str,
    "documentoIdentidadCliente": str,
    "nombreRazonSocialCliente": str,
    "correoCliente": str,
    "direccionCliente": str,
    "telefonoCliente": str,
    "codigoProducto": str,
    "nombreProducto": str,
    "descripcionProducto": str,
    "tipoImpuesto": str,
    "cantidadAdquirida": (float, int),
    "precioProducto": float,
    "tasa_del_dia": float,
    "order_payment_method": str,
    "monto": float,
    "igtf": float,
    "banco": str,
    "telefono_pago_movil": str,
    "numero_de_referencia_de_operacion": str,
    "facturar": str,
    "incluir": str,
}


class DataFacturacion:

    def __init__(self, file_sheet_name, spreadsheet_id, sheet_name, credentials_file):

        self.file_sheet_name = file_sheet_name
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.credentials_file = credentials_file
        self.spreadsheet = self._get_spreadsheet()
        self.data = DataFrame()

    def _get_spreadsheet(self):
        # Autenticación y acceso a Google Sheets
        self.scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file, self.scope
        )
        client = gspread.authorize(self.creds)
        return client.open(self.file_sheet_name)

    def __data_a_facturar(self):
        try:
            # Selecciona la hoja de Google Sheets usando gspread
            worksheet = self.spreadsheet.worksheet(self.sheet_name)
            # Obtiene todos los valores de la hoja de cálculo
            all_values = worksheet.get_all_values()
            data = DataFrame(
                all_values[1:],  # ignora la primera fila de encabezados
                columns=all_values[0],  # obtiene la primera fila como encabezados
            )
            self.data = data
        except Exception as e:
            logging.error(f"Error al obtener datos de facturación: {e}")
            self.data = DataFrame()

    def __validar_campos_requeridos(self, data: list) -> bool:
        # Validar campos requeridos
        for field in required_fields.keys():
            if field not in data:
                logging.error(
                    f"Campo requerido '{field}' no encontrado en la data a facturar."
                )
                return False

        return True

    def __campos_validados_data_a_facturar(self):
        self.__data_a_facturar()
        if self.data.empty:
            return False
        return self.__validar_campos_requeridos(self.data.columns.to_list())

    def __tipos_datos_validados_data_a_facturar(self, data: dict) -> list[dict]:
        is_valid_data = True
        for field, field_type in required_fields.items():
            if not isinstance(data[field], field_type):
                logging.error(
                    f"Tipo de dato incorrecto para el campo '{field}': "
                    f"esperado {field_type}, recibido {type(data[field])}."
                )
                is_valid_data = False
        return is_valid_data

    def __get_facturas_validadas(self) -> DataFrame:
        data = DataFrame()
        if self.__campos_validados_data_a_facturar():
            data = self.data.copy()
            if not data.empty:
                # Lista de columnas que contienen montos
                cols_montos = [
                    "cantidadAdquirida",
                    "precioProducto",
                    "tasa_del_dia",
                    "monto",
                    "igtf",
                ]

                # Eliminar separadores de miles y reemplazar coma decimal por punto si las columnas son de tipo string
                for col in cols_montos:
                    if data[col].dtype == "object":
                        data[col] = (
                            data[col]
                            .str.replace(
                                ".", "", regex=False
                            )  # Remove thousand separator
                            .str.replace(
                                ",", ".", regex=False
                            )  # Replace decimal comma with dot
                        )

                try:
                    # Convertir a float, forzando errores a NaN
                    data[cols_montos] = data[cols_montos].apply(
                        to_numeric, errors="raise"  # Convertir a float errores a NaN
                    )
                    data = data[(data["facturar"].str.upper() == "SI")]
                    data["enum"] = data["enum"].astype(int)
                except Exception as e:
                    print(f"Error en obtener_facturas_validadas: {e}")
                    data = DataFrame()  # Si hay un error, devuelve un DataFrame vacío
        return data

    def __get_facturas_validadas_tipos_datos(self) -> DataFrame:
        data = self.__get_facturas_validadas()
        is_valid_data = True
        if not data.empty:
            for item_factura in data.to_dict(orient="records"):
                if not self.__tipos_datos_validados_data_a_facturar(item_factura):
                    # Obtener el primer item del dict
                    item_factura = next(iter(item_factura.items()))
                    logging.error(f"Campo: {item_factura}")
                    is_valid_data = False
                    continue  # O manejar el error según sea necesario
        if is_valid_data:
            return data
        else:
            logging.error("Datos de facturación no válidos.")
            return DataFrame()

    def get_data_facturacion(self) -> DataFrame:
        data = self.__get_facturas_validadas_tipos_datos()
        if not data.empty:
            columnas_a_eliminar = [
                "enum",
                "incluir",
                "facturar",
            ]
            data.drop(columnas_a_eliminar, axis=1, inplace=True)
            cols_montos = [
                "cantidadAdquirida",
                "precioProducto",
            ]  # Lista de columnas que contienen montos
            # Reemplazar punto decimal por coma
            for col in cols_montos:
                data[col] = data[col].astype(str).str.replace(".", ",", regex=False)

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
    import os

    from dotenv import load_dotenv

    load_dotenv(override=True)
    FILE_FACTURACION_NAME = os.getenv("GOOGLE_SHEET_FILE_FACTURACION_NAME")
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_FACTURACION_NAME")
    CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    oDataFacturacion = DataFacturacion(
        FILE_FACTURACION_NAME, SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )
    result = oDataFacturacion.get_data_productos()
    print("Datos consultados:", result)
