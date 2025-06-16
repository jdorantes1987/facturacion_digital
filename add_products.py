import logging

from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager


class AddProducts:
    def __init__(self, api_gateway_client):
        self.client = api_gateway_client

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
            return response
        except Exception as e:
            logging.error("Error al agregar producto: %s", e)
            return {"error": str(e)}


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    from data_facturacion import DataFacturacion

    load_dotenv(override=True)
    logging.basicConfig(
        level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    FILE_FACTURACION_NAME = os.getenv("GOOGLE_SHEET_FILE_FACTURACION_NAME")
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_FACTURACION_NAME")
    CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    oProducts = AddProducts(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_PRODUCTS"), ApiKeyManager())
    )

    oDataFacturacion = DataFacturacion(
        FILE_FACTURACION_NAME, SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )

    # Ejemplo de POST agregando un producto
    try:
        # Obtiene los primeros 3 productos de DataFacturacion y los convierte a dict
        productos = oDataFacturacion.get_data_productos().to_dict(orient="records")
        # Asignar la clave "productos" al dict
        payload = {"productos": productos}
        result = oProducts.add_product(payload)  # Envía el dict, no el string
        print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
