import requests
import threading


class HttpClient:
    def __init__(self):
        self.session = None

    def get_client(self) -> requests.Session:
        if not self.session:
            self.session = requests.sessions.Session()
        return self.session
