class ApiKeyManager:
    def __init__(self, filepath="api_key.txt"):
        self.filepath = filepath

    def save(self, token):
        with open(self.filepath, "w") as f:
            f.write(f"Bearer {token}")

    def load(self):
        with open(self.filepath, "r") as f:
            return f.read().strip()
