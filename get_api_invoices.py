import os

from dotenv import load_dotenv
from pandas import DataFrame

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

    def get_data_invoices(self, params=None) -> DataFrame:
        data_json = self.get_list_invoices(params=params)
        # Manejo de error
        if isinstance(data_json, dict) and "error" in data_json:
            return DataFrame()
        # Si la respuesta es un dict con clave 'invoices', extrae la lista
        if isinstance(data_json, dict) and "invoices" in data_json:
            data_json = data_json["invoices"]
        # Si la respuesta es una lista de listas, la "aplana"
        if (
            isinstance(data_json, list)
            and len(data_json) > 0
            and isinstance(data_json[0], list)
        ):
            data_json = [item for sublist in data_json for item in sublist]
        # Si la respuesta es un solo dict, lo convierte en lista
        if isinstance(data_json, dict):
            data_json = [data_json]
        if not isinstance(data_json, list):
            return DataFrame()
        return DataFrame(data_json)


if __name__ == "__main__":
    load_dotenv(override=True)  # Recarga las variables de entorno desde el archivo
    api_url = os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES")
    api_key_manager = ApiKeyManager()
    client = ApiGatewayClient(api_url, api_key_manager)
    invoice_consultas = GetInvoices(client)

    # Puedes pasar parámetros de consulta si la API los soporta, por ejemplo: {"numeroFactura": "12345"}
    params = None
    result = invoice_consultas.get_data_invoices(params).to_string(index=False)
    print("Facturas consultadas:", result)
