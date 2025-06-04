import logging

from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager


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


def agrupar_facturas(data_a_facturar):
    facturas_dict = {}
    for item in data_a_facturar:
        numero_factura = item["numeroFactura"]
        producto = {
            "codigoProducto": item["codigoProducto"],
            "nombreProducto": item["nombreProducto"],
            "descripcionProducto": item["descripcionProducto"],
            "tipoImpuesto": item["tipoImpuesto"],
            "cantidadAdquirida": item["cantidadAdquirida"],
            "precioProducto": item["precioProducto"],
        }
        # Campos de factura (sin productos)
        factura_keys = [
            "numeroFactura",
            "documentoIdentidadCliente",
            "nombreRazonSocialCliente",
            "correoCliente",
            "direccionCliente",
            "telefonoCliente",
            "tasa_del_dia",
            "order_payment_methods",
        ]
        if numero_factura not in facturas_dict:
            factura = {k: item[k] for k in factura_keys if k in item}
            factura["productos"] = [producto]
            facturas_dict[numero_factura] = factura
        else:
            facturas_dict[numero_factura]["productos"].append(producto)
    return list(facturas_dict.values())


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    from data_facturacion import DataFacturacion

    load_dotenv()
    logging.basicConfig(
        level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    oInvoice = AddInvoice(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_INVOICES"), ApiKeyManager())
    )
    data_a_facturar = DataFacturacion().get_data_a_facturar()
    data_a_facturar = data_a_facturar.drop(["enum"], axis=1)
    data_a_facturar = data_a_facturar.to_dict(orient="records")

    # Agrupar por numeroFactura y productos
    facturas_agrupadas = agrupar_facturas(data_a_facturar)

    # Ejemplo de POST agregando una factura
    try:
        payload = {
            "numeroSerie": "A",
            "cantidadFactura": len(facturas_agrupadas),
            "facturas": facturas_agrupadas,
        }
        print("Payload a enviar:", payload)
        result = oInvoice.add_invoice(payload)
        print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
