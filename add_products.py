import logging
import logging.config

logging.config.fileConfig("logging.ini")


class AddProducts:
    def __init__(self, api_gateway_client):
        self.client = api_gateway_client
        self.logger = logging.getLogger(__class__.__name__)

    def add_product(self, product_data) -> dict:
        try:
            if (
                not isinstance(product_data, dict)
                or "productos" not in product_data
                or not isinstance(product_data["productos"], list)
            ):
                raise ValueError(
                    "Invalid product_data format. Expected a dictionary with a 'productos' key containing a list."
                )
            response = self.client.post_data(product_data)
            self.logger.info("Respuesta POST: %s", response)
            return response
        except Exception as e:
            self.logger.error(f"Error al agregar producto: %s {e}", exc_info=True)
            return {"error": str(e)}


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    from api_gateway_client import ApiGatewayClient
    from api_key_manager import ApiKeyManager
    from data_facturacion import DataFacturacion

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

    oProducts = AddProducts(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_PRODUCTS"), ApiKeyManager())
    )

    # Ejemplo de POST agregando un producto
    try:
        # Obtiene los primeros 3 productos de DataFacturacion y los convierte a dict
        productos = oDataFacturacion.get_data_productos().to_dict(orient="records")
        # Asignar la clave "productos" al dict
        payload = {"productos": productos}
        result = oProducts.add_product(payload)  # Envía el dict, no el string
        # print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
