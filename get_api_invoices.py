import os
from dotenv import load_dotenv
from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager


class GetInvoices:
    def __init__(self, api_gateway_client):
        self.client = api_gateway_client

    def get_list_invoices(self, params=None):
        try:
            response = self.client.get_data(params)
            return response
        except Exception as e:
            print(f"Error al consultar facturas: {e}")
            return {"error": str(e)}


if __name__ == "__main__":
    load_dotenv(override=True)  # Recarga las variables de entorno desde el archivo
    api_url = os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES")
    api_key_manager = ApiKeyManager()
    client = ApiGatewayClient(api_url, api_key_manager)
    invoice_consultas = GetInvoices(client)

    # Puedes pasar parámetros de consulta si la API los soporta, por ejemplo: {"numeroFactura": "12345"}
    params = None
    result = invoice_consultas.get_list_invoices(params)
    print("Facturas consultadas:", result)
