import logging
import logging.config
from collections import OrderedDict

from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager

logging.config.fileConfig("logging.ini")


class AddInvoice:

    def __init__(self, api_gateway_client):
        self.client = api_gateway_client
        self.logger = logging.getLogger(__class__.__name__)

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
            self.logger.error(f"Error al agregar factura: %s {e}", exc_info=True)
            return {"error": str(e)}

    def agrupar_facturas(self, data_a_facturar: list[dict]):
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
            if numero_factura not in facturas_dict:
                factura = OrderedDict()
                factura["numeroFactura"] = item.get("numeroFactura", "")
                factura["documentoIdentidadCliente"] = item.get(
                    "documentoIdentidadCliente", ""
                )
                factura["nombreRazonSocialCliente"] = item.get(
                    "nombreRazonSocialCliente", ""
                )
                factura["correoCliente"] = item.get("correoCliente", "")
                factura["direccionCliente"] = item.get("direccionCliente", "")
                factura["telefonoCliente"] = item.get("telefonoCliente", "")
                factura["productos"] = [producto]
                factura["tasa_del_dia"] = item.get("tasa_del_dia", "")
                factura["order_payment_methods"] = [
                    {
                        "order_payment_method": item.get("order_payment_method", ""),
                        "monto": item.get("monto", ""),
                        "igtf": item.get("igtf", ""),
                        "banco": item.get("banco", ""),
                        "telefono_pago_movil": item.get("telefono_pago_movil", ""),
                        "numero_de_referencia_de_operacion": item.get(
                            "numero_de_referencia_de_operacion", ""
                        ),
                    }
                ]
                # Si el campo "order_payment_method" esta vacio dejar el campo order_payment_methods vacio
                if not factura["order_payment_methods"][0]["order_payment_method"]:
                    factura["order_payment_methods"] = []
                    ## eliminar clave "tasa_del_dia"
                    # factura.pop("tasa_del_dia", None)

                facturas_dict[numero_factura] = factura
            else:
                facturas_dict[numero_factura]["productos"].append(producto)
        # Convertir cada OrderedDict a dict antes de regresar
        return [dict(factura) for factura in facturas_dict.values()]


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    from data_facturacion import DataFacturacion

    load_dotenv(override=True)
    logging.basicConfig(
        level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    oInvoice = AddInvoice(
        ApiGatewayClient(os.getenv("API_GATEWAY_URL_INVOICES"), ApiKeyManager())
    )
    print(os.getenv("API_GATEWAY_URL_INVOICES"))
    # Obtiene los datos a facturar
    FILE_FACTURACION_NAME = os.getenv("GOOGLE_SHEET_FILE_FACTURACION_NAME")
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_FACTURACION_ID")
    CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    data_a_facturar = (
        DataFacturacion(SPREADSHEET_ID, FILE_FACTURACION_NAME, CREDENTIALS_FILE)
        .get_data_facturacion()
        .to_dict(orient="records")
    )
    facturas_agrupadas = oInvoice.agrupar_facturas(data_a_facturar)

    # Ejemplo de POST agregando una factura
    try:
        payload = {
            "numeroSerie": "A",
            "cantidadFactura": len(facturas_agrupadas),
            "facturas": facturas_agrupadas,
        }
        # result = oInvoice.add_invoice(payload)
        # oInvoice.logger.info(f"Respuesta POST: {result}")
        print("Payload a enviar:", payload)
    except Exception as e:
        oInvoice.logger.error(f"Error en POST: %s {e}", exc_info=True)
