import subprocess
import logging

from azure.identity import AzureCliCredential


class AzureManager:
    """Handles Azure authentication and storage operations"""

    def __init__(self):
        self.credential = None
        self.storage_clients = {}
        self.is_authenticated = False

    def authenticate(self) -> bool:
        """Authenticate using Azure CLI"""
        try:
            # Check if az login is valid
            result = subprocess.run(['az', 'account', 'show'],
                                    capture_output=True, text=True)
            if result.returncode != 0:
                return False

            self.credential = AzureCliCredential()
            self.is_authenticated = True
            return True
        except Exception as e:
            logging.error(f"Authentication failed: {e}")
            return False