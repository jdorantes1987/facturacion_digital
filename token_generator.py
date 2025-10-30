import os

from api_gateway_client import ApiGatewayClient
from api_key_manager import ApiKeyManager


class TokenGenerator:
    def __init__(self, path_api_key="api_key.txt"):
        self.client_api_gateway = ApiGatewayClient(
            url=os.getenv("API_GATEWAY_URL_AUTHENTICATOR"),
            api_key_manager=ApiKeyManager(filepath=path_api_key),
        )

    def update_token(self) -> dict:
        payload = {
            "userName": os.getenv("USER_API"),
            "userPassword": os.getenv("API_TOKEN"),
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
    import sys
    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    oTokenGenerator = TokenGenerator()
    oTokenGenerator.update_token()
