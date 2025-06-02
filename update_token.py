from api_key_manager import ApiKeyManager
from api_gateway_client import ApiGatewayClient


class UpdateToken:
    def __init__(self):
        self.client_api_gateway = ApiGatewayClient(
            url=os.getenv("API_GATEWAY_URL_AUTHENTICATOR"),
            api_key_manager=ApiKeyManager(),
        )

    def update(self):
        payload = {
            "userName": os.getenv("USER_API"),
            "userPassword": os.getenv("PW_API"),
        }
        try:
            result = self.client_api_gateway.post_data(payload)
            print("Respuesta POST:", result)
            if "token" in result:
                self.client_api_gateway.update_token(result["token"])
                print("Nuevo token guardado en 'api_key.txt'")
        except Exception as e:
            print("Error al actualizar el token:", e)


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    oUpdateToken = UpdateToken()
    oUpdateToken.update()
