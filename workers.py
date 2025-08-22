from PyQt6.QtCore import QObject, pyqtSignal, QThread


class AuthWorker(QObject):
    finished = pyqtSignal(bool)

    def __init__(self, azure_manager):
        super().__init__()
        self.azure_manager = azure_manager

    def run(self):
        success = self.azure_manager.authenticate()
        self.finished.emit(success)


class BlobFetchWorker(QThread):
    blobs_fetched = pyqtSignal(list)  # Emits list of blob dicts

    def __init__(
        self, azure_manager, account_name: str, container_name: str, prefix: str = ""
    ):
        super().__init__()
        self.azure_manager = azure_manager
        self.account_name = account_name
        self.container_name = container_name
        self.prefix = prefix

    def run(self):
        try:
            blobs = self.get_blobs_hierarchy()
            self.blobs_fetched.emit(blobs)
        except Exception as e:
            import logging

            logging.error(f"BlobFetchWorker failed: {e}")
            self.blobs_fetched.emit([])

    def get_blobs_hierarchy(self):
        """
        Fetch blobs and folders under the given prefix.
        Returns a list of dicts:
        [
            {"name": "folder1/", "is_directory": True},
            {"name": "folder1/file1.txt", "is_directory": False, "size": 1234, "last_modified": "..."},
            ...
        ]
        """
        client = self.azure_manager.get_blob_service_client(self.account_name)
        if not client:
            return []

        container_client = client.get_container_client(self.container_name)
        # Walk blobs using delimiter '/' to get hierarchy
        blobs = container_client.walk_blobs(name_starts_with=self.prefix, delimiter="/")

        blob_list = []
        for blob in blobs:
            # If it is a folder (BlobPrefix)
            if hasattr(blob, "name") and blob.name.endswith("/"):
                blob_list.append({"name": blob.name, "is_directory": True})
            else:
                blob_list.append(
                    {
                        "name": blob.name,
                        "is_directory": False,
                        "size": getattr(blob, "size", 0),
                        "last_modified": getattr(
                            blob, "last_modified", None
                        ).isoformat()
                        if getattr(blob, "last_modified", None)
                        else "",
                        "tier": getattr(blob, "blob_tier", "Unknown"),
                    }
                )
        return blob_list
