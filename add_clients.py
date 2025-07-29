import logging
import logging.config

logging.config.fileConfig("logging.ini")


class AddClients:
    def __init__(self, api_gateway_client):
        self.client = api_gateway_client
        self.logger = logging.getLogger(__class__.__name__)

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
            self.logger.info("Respuesta POST: %s", response)
            return response
        except Exception as e:
            self.logger.error(f"Error al agregar cliente: %s {e}", exc_info=True)
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

    oClients = AddClients(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_CLIENTES"), ApiKeyManager())
    )

    # Ejemplo de POST agregando un cliente
    try:
        clientes = oDataFacturacion.get_data_clientes().to_dict(orient="records")
        # Asignar la clave "clientes" al dict
        payload = {"clientes": clientes}
        result = oClients.add_client(payload)  # Envía el dict, no el string
        # print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
