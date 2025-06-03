from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager
import logging


class AddInvoice:
    def __init__(self, api_gateway_client):
        self.client = api_gateway_client

    def add_invoice(self, invoice_data) -> dict:
        try:
            if (
                not isinstance(invoice_data, dict)
                or "facturas" not in invoice_data
                or not isinstance(invoice_data["facturas"], list)
            ):
                raise ValueError(
                    "Invalid invoice_data format. Expected a dictionary with a 'facturas' key containing a list."
                )
            response = self.client.post_data(invoice_data)
            return response
        except Exception as e:
            logging.error("Error al agregar factura: %s", e)
            return {"error": str(e)}


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    oInvoice = AddInvoice(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_INVOICES"), ApiKeyManager())
    )
    # Ejemplo de POST agregando una factura
    try:
        payload = {
            "numeroSerie": "A",
            "cantidadFactura": 1,
            "facturas": [
                {
                    "numeroFactura": "00001",
                    "documentoIdentidadCliente": "J405722177",
                    "nombreRazonSocialCliente": "SISTEMAS ADMINISTRATIVOS PRADO, C.A",
                    "correoCliente": "sprado@prado.com",
                    "direccionCliente": "Caracas, Venezuela",
                    "telefonoCliente": "04142094290",
                    "productos": [
                        {
                            "codigoProducto": "ACI-H_01-1-2-1-1-2-1-015",
                            "nombreProducto": "ACI 15M SM F.O.",
                            "descripcionProducto": "Servicio Junio 2024 hasta Mayo 2025",
                            "tipoImpuesto": "G",
                            "cantidadAdquirida": 15,
                            "precioProducto": "1,55",
                        }
                    ],
                    "tasa_del_dia": "56.65",
                    "order_payment_methods": [
                        {
                            "order_payment_method": "pago_movil",
                            "monto": "56.65",
                            "igtf": "",
                            "banco": "Banco de Venezuela",
                            "telefono_pago_movil": "00000000000",
                            "numero_de_referencia_de_operacion": "123456",
                        }
                    ],
                }
            ],
        }
        result = oInvoice.add_invoice(payload)
        print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
