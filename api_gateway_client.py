# api_gateway_client.py

import requests

API_GATEWAY_URL = "https://temp.cgimprenta.com/api/Invoice/add_clients"


def get_api_key():
    with open("api_key.txt", "r") as f:
        return f.read().strip()


API_KEY = get_api_key()


def get_data(params=None):
    headers = {"Authorization": API_KEY, "Content-Type": "application/json"}
    response = requests.get(API_GATEWAY_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def post_data(payload):
    headers = {"Authorization": API_KEY, "Content-Type": "application/json"}
    response = requests.post(API_GATEWAY_URL, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    # Ejemplo de GET
    # try:
    #     data = get_data({"": ""})
    #     print("Respuesta GET:", data)
    # except Exception as e:
    #     print("Error en GET:", e)

    # Ejemplo de POST
    try:
        payload = {
            "clientes": [
                {
                    "documentoIdentidadCliente": "J297124020",
                    "nombreRazonSocialCliente": "CORPORACIÓN VOXTEL, C.A.",
                    "correoCliente": "jarodriguez@voxtel.com.ve",
                    "telefonoCliente": "02122748250",
                    "direccionCliente": "Caracas, Venezuela",
                }
            ]
        }
        result = post_data(payload)
        print("Respuesta POST:", result)
    except Exception as e:
        print("Error en POST:", e)
