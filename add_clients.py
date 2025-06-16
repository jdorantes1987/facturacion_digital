from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager
import logging


class AddClients:
    def __init__(self, api_gateway_client):
        self.client = api_gateway_client

    def add_client(self, client_data) -> dict:
        try:
            if (
                not isinstance(client_data, dict)
                or "clientes" not in client_data
                or not isinstance(client_data["clientes"], list)
            ):
                raise ValueError(
                    "Invalid client_data format. Expected a dictionary with a 'clientes' key containing a list."
                )
            response = self.client.post_data(client_data)
            return response
        except Exception as e:
            logging.error("Error al agregar cliente: %s", e)
            return {"error": str(e)}


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    from data_facturacion import DataFacturacion

    load_dotenv()
    logging.basicConfig(
        level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    FILE_FACTURACION_NAME = os.getenv("GOOGLE_SHEET_FILE_FACTURACION_NAME")
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    SHEET_NAME = os.getenv("GOOGLE_SHEET_FACTURACION_NAME")
    CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    oClients = AddClients(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_CLIENTES"), ApiKeyManager())
    )
    oDataFacturacion = DataFacturacion(
        FILE_FACTURACION_NAME, SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE
    )
    # Ejemplo de POST agregando un cliente
    try:
        clientes = oDataFacturacion.get_data_clientes().to_dict(orient="records")
        # Asignar la clave "clientes" al dict
        payload = {"clientes": clientes}
        result = oClients.add_client(payload)  # Envía el dict, no el string
        print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
