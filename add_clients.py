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

    oClients = AddClients(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_CLIENTES"), ApiKeyManager())
    )
    # Ejemplo de POST agregando un cliente
    try:
        # Obtiene los primeros 3 clientes de DataFacturacion y los convierte a dict
        clientes = (
            DataFacturacion().get_data_clientes().head(1).to_dict(orient="records")
        )
        # Asignar la clave "clientes" al dict
        payload = {"clientes": clientes}
        result = oClients.add_client(payload)  # Envía el dict, no el string
        print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
