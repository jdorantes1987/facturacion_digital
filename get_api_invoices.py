from pandas import DataFrame


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
        data = DataFrame(data_json)

        # Cambia la URL del PDF de la factura para que apunte a la vista previa, regex=True permite usar expresiones regulares
        data["invoice_pdf"] = data["invoice_pdf"].replace(
            r"readonly/export_pdf", "readonly/invoice/preview", regex=True
        )
        return data

    def get_last_invoice(self, params=None) -> str:
        """
        Obtiene el máximo número de factura.
        """
        data = self.get_data_invoices(params=params)
        if not data.empty:
            data = data[data["document"] == "FACTURA"]  # Filtra por tipo de documento
            return data["invoice_number"].max()
        return ""


if __name__ == "__main__":
    import os
    import sys
    from datetime import date

    from dotenv import load_dotenv

    from api_gateway_client import ApiGatewayClient
    from api_key_manager import ApiKeyManager
    from token_generator import TokenGenerator

    sys.path.append("../conexiones")

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Actualiza el token de autenticación
    TokenGenerator().update_token()

    api_url = os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES")
    api_key_manager = ApiKeyManager()
    client = ApiGatewayClient(api_url, api_key_manager)
    invoice_consultas = GetInvoices(client)

    hoy = date.today().strftime("%Y-%m-%d")

    # Puedes pasar parámetros de consulta si la API los soporta, por ejemplo: {"numeroFactura": "12345"}
    params = {
        "fechaInicio": "2025-01-01",  # Fecha de inicio del rango
        "fechaFin": hoy,  # Fecha de fin del rango
    }
    result = invoice_consultas.get_data_invoices(params=params)

    print("Resultado:", result.to_excel("result.xlsx", index=False))
