import subprocess
import logging
import json
from typing import List, Dict, Optional

from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient


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

    def get_containers(self, account_name: str) -> List[str]:
        """Get containers for storage account"""
        client = self.get_blob_service_client(account_name)
        if not client:
            return []

        try:
            containers = client.list_containers()
            return [c.name for c in containers]
        except Exception as e:
            logging.error(f"Failed to list containers: {e}")
            return []

    def get_blob_service_client(self, account_name: str) -> Optional[BlobServiceClient]:
        """Get blob service client for account"""
        if account_name not in self.storage_clients:
            try:
                account_url = f"https://{account_name}.blob.core.windows.net"
                client = BlobServiceClient(
                    account_url=account_url, credential=self.credential
                )
                self.storage_clients[account_name] = client
            except Exception as e:
                logging.error(f"Failed to create client for {account_name}: {e}")
                return None
        return self.storage_clients[account_name]
