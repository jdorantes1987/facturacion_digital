import logging

from pandas import DataFrame, to_numeric

from data_sheets import ManagerSheets

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

    def __init__(self, spreadsheet_id, sheet_name, credentials_file):
        self.manager_sheets = ManagerSheets(
            file_sheet_name=sheet_name,
            spreadsheet_id=spreadsheet_id,
            credentials_file=credentials_file,
        )
        self.data = DataFrame()

    def __data_a_facturar(self):
        self.data = self.manager_sheets.get_data_hoja(sheet_name="facturacion")

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
            # Une las columnas "nombreProducto" y "descripcionProducto" según solicitud
            data["descripcionProducto"] = (
                data["nombreProducto"] + " - " + data["descripcionProducto"]
            )
            cols_montos = [
                "precioProducto",
            ]  # Lista de columnas que contienen montos
            # Reemplazar punto decimal por coma
            for col in cols_montos:
                data[col] = data[col].astype(str).str.replace(".", ",", regex=False)

        return data

    def get_data_clientes(self) -> DataFrame:
        # Selecciona la hoja de Google Sheets
        return self.manager_sheets.get_data_hoja(sheet_name="clientes")

    def get_data_productos(self) -> DataFrame:
        # Selecciona la hoja de Google Sheets
        data = self.manager_sheets.get_data_hoja(sheet_name="productos")
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
    import sys

    from dotenv import load_dotenv

    sys.path.append("..\\profit")

    env_path = os.path.join("..\\profit", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    FILE_FACTURACION_NAME = os.getenv("GOOGLE_SHEET_FILE_FACTURACION_NAME")
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    CREDENTIALS_FILE = os.getenv("CGIMPRENTA_CREDENTIALS")

    oDataFacturacion = DataFacturacion(
        SPREADSHEET_ID, FILE_FACTURACION_NAME, CREDENTIALS_FILE
    )
    result = oDataFacturacion.get_data_facturacion()
    print("Datos consultados:", result)
