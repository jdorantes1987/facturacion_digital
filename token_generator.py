from api_key_manager import ApiKeyManager
from api_gateway_client import ApiGatewayClient


class TokenGenerator:
    def __init__(self):
        self.client_api_gateway = ApiGatewayClient(
            url=os.getenv("API_GATEWAY_URL_AUTHENTICATOR"),
            api_key_manager=ApiKeyManager(),
        )

    def update_token(self) -> dict:
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
                return dict(success=True, token=result["token"])
        except Exception as e:
            print("Error al actualizar el token:", e)
            return dict(success=False, error=str(e))


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv(override=True)
    oTokenGenerator = TokenGenerator()
    oTokenGenerator.update_token()
