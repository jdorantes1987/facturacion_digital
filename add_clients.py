from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager
import logging


class AddClients:
    def __init__(self, api_gateway_client):
        self.client = api_gateway_client

    def add_client(self, client_data):
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


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    clients = AddClients(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_CLIENTES"), ApiKeyManager())
    )
    # Ejemplo de POST agregando un cliente
    try:
        payload = {
            "clientes": [
                {
                    "documentoIdentidadCliente": "J405722177",
                    "nombreRazonSocialCliente": "SISTEMAS ADMINISTRATIVOS PRADO, C.A",
                    "correoCliente": "sprado@prado.com",
                    "telefonoCliente": "04142094290",
                    "direccionCliente": "Caracas, Venezuela",
                }
            ]
        }
        result = clients.add_client(payload)
        print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
