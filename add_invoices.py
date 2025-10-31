import json
import logging
import logging.config
from collections import OrderedDict
from pandas import to_numeric
from data_facturacion import DataFacturacion

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
                factura["descripcionFactura"] = item.get("descripcionFactura", "")
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

    def facturacion_manual(self, data_a_facturar) -> dict:
        """
        Realizar la facturación según la fuente de datos en formato diccionario.
        """
        facturas_agrupadas = self.agrupar_facturas(data_a_facturar)
        payload = {
            "numeroSerie": "A",
            "cantidadFactura": len(facturas_agrupadas),
            "facturas": facturas_agrupadas,
        }
        try:
            result = self.add_invoice(payload)
            if isinstance(result, str):
                result = json.loads(result)
            if "invoice_list_success" in result:
                for invoice in result["invoice_list_success"]:
                    invoice["invoice_pdf"] = invoice["invoice_pdf"].replace(
                        r"readonly/export_pdf", "readonly/invoice/preview"
                    )
            self.logger.info(f"Respuesta POST: {result}")
            # Asegura que el resultado tenga las claves esperadas
            return {
                "success": result.get("success", False),
                "invoice_errors": result.get("invoice_errors", []),
                "message": result.get("message", ""),
                **result,
            }
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Error: La cadena no es un JSON válido. {e}", exc_info=True
            )
            self.logger.error(
                f"Cadena problemática: {self.add_invoice(payload)}", exc_info=True
            )
            return {
                "success": False,
                "invoice_errors": ["JSONDecodeError"],
                "message": str(e),
            }
        except Exception as e:
            self.logger.error(f"Error en POST: %s {e}", exc_info=True)
            return {"success": False, "invoice_errors": [str(e)], "message": str(e)}

    def es_correlativo_diff(self, serie):
        """
        Verifica si una serie es correlativa usando diferencias.
        """
        # Convertir a numérico
        serie_numerica = to_numeric(serie, errors="coerce")

        # Ordenar la serie
        serie_numerica = serie_numerica.sort_values()

        # Calcular la diferencia entre elementos consecutivos
        diferencias = serie_numerica.diff()

        # Los valores de diferencia deben ser 1, excepto el primero que es NaN
        # Verificar si todos los valores (excepto el primero) son 1
        return (diferencias.iloc[1:] == 1).all()


if __name__ == "__main__":
    import os
    import sys
    from time import sleep

    from dotenv import load_dotenv
    from pandas import Series

    from api_gateway_client import ApiGatewayClient
    from api_key_manager import ApiKeyManager
    from token_generator import TokenGenerator
    from get_api_invoices import GetInvoices

    sys.path.append("../conexiones")

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Actualiza el token de autenticación
    TokenGenerator().update_token()

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
    CREDENTIALS_FILE = os.getenv("CGIMPRENTA_CREDENTIALS")

    # data_a_facturar = (
    #     DataFacturacion(SPREADSHEET_ID, FILE_FACTURACION_NAME, CREDENTIALS_FILE)
    #     .get_data_facturacion()
    #     .to_dict(orient="records")
    # )
    # facturas_agrupadas = oInvoice.agrupar_facturas(data_a_facturar)

    # # Ejemplo de POST agregando una factura
    # try:
    #     payload = {
    #         "numeroSerie": "A",
    #         "cantidadFactura": len(facturas_agrupadas),
    #         "facturas": facturas_agrupadas,
    #     }
    #     result = oInvoice.add_invoice(payload)
    #     oInvoice.logger.info(f"Respuesta POST: {result}")
    #     # print("Payload a enviar:", payload)
    # except Exception as e:
    #     oInvoice.logger.error(f"Error en POST: %s {e}", exc_info=True)

    # ------------------------------------------------------------------------------
    # REALIZA LA FACTURACIÓN MANUAL
    # _______________________________________________________________________________
    # Hacer un loop while por el campo "numeroFactura" de data_a_facturar la idea es
    # que reconozca cada factura de manera única y valide de la respuesta de la API
    # sea satisfactoria

    data_a_facturar = DataFacturacion(
        SPREADSHEET_ID, FILE_FACTURACION_NAME, CREDENTIALS_FILE
    ).get_data_facturacion()

    last_invoice = GetInvoices(
        ApiGatewayClient(
            os.getenv("API_GATEWAY_URL_GET_LIST_INVOICES"), ApiKeyManager()
        )
    ).get_last_invoice()

    # Extrae la columna "numeroFactura" y la convierte en una lista
    numeros_factura = list(data_a_facturar["numeroFactura"].unique())

    # Agrega el último número de factura obtenido de la API al inicio de la lista de números de factura
    if not last_invoice != "":
        numeros_factura.append(last_invoice)

    # Convierte la lista a una Serie de pandas
    numeros_factura = Series(numeros_factura)

    # Verifica si los números de factura son correlativos
    es_correlativo = oInvoice.es_correlativo_diff(numeros_factura)

    # Si no son correlativos, imprime un mensaje y termina el programa
    if not es_correlativo:
        print(
            f"Los números de factura {sorted(numeros_factura.to_list())} no son correlativos, por favor revisar la data a enviar."
        )
        # Termina la ejecución del programa con un código de salida 1 (error)
        exit(1)

    i = 0
    # Filtra los datos de la factura actual a traves de un set del campo "numeroFactura"
    facturas = set(data_a_facturar["numeroFactura"])
    # Ordenar las facturas para procesarlas en orden
    facturas = sorted(facturas)
    # Convierte el set a una lista para poder acceder a los elementos por índice
    facturas = list(facturas)
    # Inicializa la variable result como un diccionario con clave "success" en True
    result = {"success": True, "invoice_errors": [], "message": ""}

    while (
        i < len(facturas)
        # Asegura que result tenga la clave "success" y sea True
        and isinstance(result, dict)
        and result.get("success", False) is not False
        # Asegura que no haya errores en la factura
        and len(result.get("invoice_errors", [])) == 0
    ):
        # Filtra los datos de la factura actual
        data_factura_actual = data_a_facturar[
            data_a_facturar["numeroFactura"] == facturas[i]
        ]
        result = oInvoice.facturacion_manual(
            data_factura_actual.to_dict(orient="records")
        )
        # Validación extra por si result no es dict
        if (
            isinstance(result, dict)
            and result.get("success", False)
            and len(result.get("invoice_errors", [])) == 0
        ):
            print(f"Factura {facturas[i]} guardada satisfactoriamente.")
        else:
            mensaje = (
                result["message"]
                if isinstance(result, dict) and "message" in result
                else str(result)
            )
            print(f"Se detuvo la facturación de {facturas[i]}: {mensaje}")
        i += 1
        sleep(2)

    print("Proceso de facturación finalizado.")
