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

    def get_blobs_in_container(
        self, account_name: str, container_name: str, prefix: str = ""
    ) -> List[Dict]:
        """Get blobs in container with hierarchy"""
        client = self.get_blob_service_client(account_name)
        if not client:
            return []

        try:
            container_client = client.get_container_client(container_name)
            blobs = container_client.walk_blobs(name_starts_with=prefix)

            blob_list = []
            for blob in blobs:
                is_dir = hasattr(blob, "prefix") and blob.prefix is not None

                if is_dir:
                    blob_dict = {
                        "name": blob.prefix,  # Directory path with trailing slash
                        "size": 0,
                        "last_modified": "",
                        "tier": "",
                        "is_directory": True,
                    }
                else:
                    # This is a file (blob)
                    blob_dict = {
                        "name": blob.name,
                        "size": getattr(blob, "size", 0),
                        "last_modified": getattr(
                            blob, "last_modified", None
                        ).isoformat()
                        if getattr(blob, "last_modified", None)
                        else "",
                        "tier": getattr(blob, "blob_tier", "")
                        if hasattr(blob, "blob_tier")
                        else "",
                        "is_directory": False,
                    }

                blob_list.append(blob_dict)

            return blob_list

        except Exception as e:
            logging.error(
                f"Failed to list blobs in {account_name}/{container_name}: {e}"
            )
            return []
