import subprocess
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from azure.identity import AzureCliCredential
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions


class AzureManager:
    """Handles Azure authentication and storage operations"""

    def __init__(self):
        self.credential = None
        self.storage_clients = {}
        self.account_keys = {}
        self.resource_groups = {}
        self.storage_mgmt_client = None
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

            # Get subscription ID for storage management
            import json

            account_info = json.loads(result.stdout)
            subscription_id = account_info["id"]

            # Create storage management client for getting account keys
            self.storage_mgmt_client = StorageManagementClient(
                self.credential, subscription_id
            )

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

    def get_resource_group_for_account(self, account_name: str) -> Optional[str]:
        """Find the resource group for a given storage account using Azure CLI"""
        if account_name in self.resource_groups:
            return self.resource_groups[account_name]

        try:
            # Use Azure CLI to find the storage account
            result = subprocess.run(
                [
                    "az",
                    "storage",
                    "account",
                    "show",
                    "--name",
                    account_name,
                    "--query",
                    "resourceGroup",
                    "--output",
                    "tsv",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                resource_group = result.stdout.strip()
                self.resource_groups[account_name] = resource_group
                logging.info(
                    f"Found resource group '{resource_group}' for account '{account_name}'"
                )
                return resource_group
            else:
                logging.error(
                    f"Failed to find storage account {account_name}: {result.stderr}"
                )
                return None
        except Exception as e:
            logging.error(f"Failed to get resource group for {account_name}: {e}")
            return None

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

    def get_account_key(self, account_name: str) -> Optional[str]:
        """Get storage account key using Azure CLI"""
        if account_name in self.account_keys:
            return self.account_keys[account_name]

        try:
            # Get resource group first
            resource_group = self.get_resource_group_for_account(account_name)
            if not resource_group:
                return None

            # Use Azure CLI to get account keys
            result = subprocess.run(
                [
                    "az",
                    "storage",
                    "account",
                    "keys",
                    "list",
                    "--account-name",
                    account_name,
                    "--resource-group",
                    resource_group,
                    "--query",
                    "[0].value",
                    "--output",
                    "tsv",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                account_key = result.stdout.strip()
                self.account_keys[account_name] = account_key
                return account_key
            else:
                logging.error(f"Failed to get keys for {account_name}: {result.stderr}")
                return None

        except Exception as e:
            logging.error(f"Failed to get account key for {account_name}: {e}")
            return None

    def generate_blob_sas_url(
        self,
        account_name: str,
        container_name: str,
        blob_name: str,
        expiry_hours: int = 1,
    ) -> Optional[str]:
        """Generate SAS URL for a blob"""
        try:
            account_key = self.get_account_key(account_name)
            if not account_key:
                return None

            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
            )

            blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"
            return f"{blob_url}?{sas_token}"

        except Exception as e:
            logging.error(f"Failed to generate SAS URL: {e}")
            return None
