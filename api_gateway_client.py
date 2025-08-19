import requests


class ApiGatewayClient:
    def __init__(self, url, api_key_manager):
        self.url = url
        self.api_key_manager = api_key_manager
        self.api_key = self.api_key_manager.load()

    def get_headers(self):
        return {"Authorization": self.api_key, "Content-Type": "application/json"}

    def get_data(self, params=None):
        self.api_key = self.api_key_manager.load()
        headers = self.get_headers()
        response = requests.get(self.url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def post_data(self, payload):
        headers = self.get_headers()
        response = requests.post(self.url, headers=headers, json=payload)
        if response.status_code == 400:
            return response.content.decode("utf-8")
        response.raise_for_status()
        return response.json()

    def update_token(self, token):
        self.api_key_manager.save(token)
        self.api_key = self.api_key_manager.load()
