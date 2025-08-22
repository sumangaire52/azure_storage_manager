import subprocess
import logging
import json
from typing import List, Dict

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
            result = subprocess.run(
                ["az", "account", "show"], capture_output=True, text=True
            )
            if result.returncode != 0:
                return False

            self.credential = AzureCliCredential()
            self.is_authenticated = True
            return True
        except Exception as e:
            logging.error(f"Authentication failed: {e}")
            return False

    def get_storage_accounts(self) -> List[Dict]:
        """Get list of all storage accounts"""
        if not self.is_authenticated:
            return []

        try:
            result = subprocess.run(
                ["az", "storage", "account", "list"],
                capture_output=True,
                text=True,
                check=True,
            )
            accounts = json.loads(result.stdout)
            return accounts
        except Exception as e:
            logging.error(f"Failed to get storage accounts: {e}")
            return []
