import logging
from collections import OrderedDict

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
                    # eliminar clave "tasa_del_dia"
                    factura.pop("tasa_del_dia", None)

                facturas_dict[numero_factura] = factura
            else:
                facturas_dict[numero_factura]["productos"].append(producto)
        # Convertir cada OrderedDict a dict antes de regresar
        return [dict(factura) for factura in facturas_dict.values()]

    # Función que permite validar que la función agregar factura tenga los campos requeridos con los tipos de datos correctos
    def validar_factura(self, factura: dict) -> bool:
        required_fields = {
            "numeroFactura": str,
            "documentoIdentidadCliente": str,
            "nombreRazonSocialCliente": str,
            "correoCliente": str,
            "direccionCliente": str,
            "telefonoCliente": str,
            "codigoProducto": str,
            "nombreProducto": str,
            "descripcionProducto": str,
            "tipoImpuesto": str,
            "cantidadAdquirida": (str, int),
            "precioProducto": float,
            "tasa_del_dia": float,
            "order_payment_method": str,
            "monto": float,
            "igtf": float,
            "banco": str,
            "telefono_pago_movil": str,
            "numero_de_referencia_de_operacion": str,
        }

        # Validar campos requeridos y sus tipos de datos
        for field, field_type in required_fields.items():
            if field not in factura:
                logging.error(f"Campo requerido '{field}' no encontrado en la factura.")
                return False
            if not isinstance(factura[field], field_type):
                logging.error(
                    f"Tipo de dato incorrecto para el campo '{field}': "
                    f"esperado {field_type}, recibido {type(factura[field])}."
                )
                return False

        return True


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
    # Obtiene los datos a facturar
    data_a_facturar = DataFacturacion().get_data_a_facturar()

    # Si no hay datos a facturar, salir
    if not data_a_facturar.empty:
        columnas_a_eliminar = [
            "enum",
            "facturar",
        ]
        data_a_facturar = data_a_facturar.drop(columnas_a_eliminar, axis=1)
        data_a_facturar = data_a_facturar.to_dict(orient="records")

        # Validar cada factura antes de agrupar
        for factura in data_a_facturar:
            if not oInvoice.validar_factura(factura):
                logging.error(f"Factura inválida: {factura}")
                continue  # O manejar el error según sea necesario

        # Agrupar por numeroFactura y productos
        facturas_agrupadas = oInvoice.agrupar_facturas(data_a_facturar)

        # Ejemplo de POST agregando una factura
        try:
            payload = {
                "numeroSerie": "A",
                "cantidadFactura": len(facturas_agrupadas),
                "facturas": facturas_agrupadas,
            }
            print("Payload a enviar:", payload)
            # result = oInvoice.add_invoice(payload)
            # print("Respuesta POST:", result)
        except Exception as e:
            print("Error en POST:", e)
