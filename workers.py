import logging
from pathlib import Path

from PyQt6.QtCore import QObject, pyqtSignal, QThread


class AuthWorker(QObject):
    finished = pyqtSignal(bool)

    def __init__(self, azure_manager):
        super().__init__()
        self.azure_manager = azure_manager

    def run(self):
        success = self.azure_manager.authenticate()
        self.finished.emit(success)


class DownloadWorker(QThread):
    """Worker thread for downloading blobs"""

    progress_updated = pyqtSignal(int)  # Progress percentage
    status_updated = pyqtSignal(str)  # Status message
    file_completed = pyqtSignal(str)  # File path completed
    download_completed = pyqtSignal(bool, str)  # Success, message

    def __init__(
        self, azure_manager, account_name, container_name, items_to_download, local_path
    ):
        super().__init__()
        self.azure_manager = azure_manager
        self.account_name = account_name
        self.container_name = container_name
        self.items_to_download = items_to_download
        self.local_path = Path(local_path)
        self.cancelled = False

    def cancel(self):
        """Cancel the download operation"""
        self.cancelled = True

    def run(self):
        """Main download logic"""
        try:
            total_files = 0
            completed_files = 0

            # First, count total files to download
            self.status_updated.emit("Calculating files to download...")

            files_to_download = []
            for item in self.items_to_download:
                if item.get("is_directory", False):
                    # Get all files in directory recursively
                    dir_files = self._get_all_files_in_directory(item["name"])
                    files_to_download.extend(dir_files)
                else:
                    # Single file
                    files_to_download.append(item)

            total_files = len(files_to_download)

            if total_files == 0:
                self.download_completed.emit(True, "No files to download")
                return

            self.status_updated.emit(f"Downloading {total_files} files...")

            # Download each file
            for file_blob in files_to_download:
                if self.cancelled:
                    self.download_completed.emit(False, "Download cancelled")
                    return

                # Skip if this is somehow a directory (shouldn't happen but safety check)
                if file_blob.get("is_directory", False):
                    logging.warning(
                        f"Skipping directory in file list: {file_blob['name']}"
                    )
                    continue

                success = self._download_single_file(file_blob)

                if success:
                    completed_files += 1
                    progress = int((completed_files / total_files) * 100)
                    self.progress_updated.emit(progress)
                    self.file_completed.emit(file_blob["name"])
                else:
                    logging.error(f"Failed to download: {file_blob['name']}")

            # Complete
            message = f"Successfully downloaded {completed_files}/{total_files} files"
            self.download_completed.emit(completed_files > 0, message)

        except Exception as e:
            logging.error(f"Download error: {e}")
            self.download_completed.emit(False, f"Download failed: {str(e)}")

    def _get_all_files_in_directory(self, directory_prefix, all_files=None):
        """Recursively get all files in a directory and all its subdirectories"""
        if all_files is None:
            all_files = []
        try:
            logging.info(
                f"Getting all files recursively for directory: {directory_prefix}"
            )

            blobs = self.azure_manager.get_blobs_in_container(
                account_name=self.account_name,
                container_name=self.container_name,
                prefix=directory_prefix,
            )

            for blob in blobs:
                if blob.get("is_directory", False):
                    self._get_all_files_in_directory(
                        directory_prefix=blob["name"], all_files=all_files
                    )
                else:
                    all_files.append(blob)
                    logging.debug(f"Added file for download: {blob['name']}")

            logging.info(
                f"Found {len(all_files)} files recursively in {directory_prefix}"
            )
            return all_files

        except Exception as e:
            logging.error(
                f"Failed to recursively list directory {directory_prefix}: {e}"
            )
            return []

    def _download_single_file(self, blob_info):
        """Download a single blob file"""
        try:
            blob_name = blob_info["name"]

            # Create local file path
            relative_path = blob_name
            local_file_path = self.local_path / relative_path

            # Create directories if they don't exist
            local_file_path.parent.mkdir(parents=True, exist_ok=True)

            self.status_updated.emit(f"Downloading: {blob_name}")

            # Get blob service client
            client = self.azure_manager.get_blob_service_client(self.account_name)
            if not client:
                return False

            # Download the blob
            blob_client = client.get_blob_client(
                container=self.container_name, blob=blob_name
            )

            with open(local_file_path, "wb") as download_file:
                download_stream = blob_client.download_blob()
                download_file.write(download_stream.readall())

            return True

        except Exception as e:
            logging.error(f"Failed to download {blob_info['name']}: {e}")
            return False


class TransferWorker(QThread):
    """Worker thread for transferring blobs between storage accounts"""

    progress_updated = pyqtSignal(int)  # Progress percentage
    status_updated = pyqtSignal(str)  # Status message
    file_completed = pyqtSignal(str)  # File path completed
    transfer_completed = pyqtSignal(bool, str)  # Success, message

    def __init__(
        self,
        azure_manager,
        source_account,
        source_container,
        dest_account,
        dest_container,
        items_to_transfer,
        options,
    ):
        super().__init__()
        self.azure_manager = azure_manager
        self.source_account = source_account
        self.source_container = source_container
        self.dest_account = dest_account
        self.dest_container = dest_container
        self.items_to_transfer = items_to_transfer
        self.options = options
        self.cancelled = False

    def cancel(self):
        """Cancel the transfer operation"""
        self.cancelled = True

    def run(self):
        """Main transfer logic"""
        try:
            completed_files = 0

            # First, get all files to transfer
            self.status_updated.emit("Calculating files to transfer...")

            files_to_transfer = []
            for item in self.items_to_transfer:
                if item.get("is_directory", False):
                    # Get all files in directory recursively
                    dir_files = self._get_all_files_in_directory(item["name"])
                    files_to_transfer.extend(dir_files)
                else:
                    # Single file
                    files_to_transfer.append(item)

            total_files = len(files_to_transfer)

            if total_files == 0:
                self.transfer_completed.emit(True, "No files to transfer")
                return

            self.status_updated.emit(f"Transferring {total_files} files...")

            # Transfer each file
            for file_blob in files_to_transfer:
                if self.cancelled:
                    self.transfer_completed.emit(False, "Transfer cancelled")
                    return

                # Skip directories
                if file_blob.get("is_directory", False):
                    continue

                success = self._transfer_single_file(file_blob)

                if success:
                    completed_files += 1
                    progress = int((completed_files / total_files) * 100)
                    self.progress_updated.emit(progress)
                    self.file_completed.emit(file_blob["name"])
                else:
                    logging.error(f"Failed to transfer: {file_blob['name']}")

            # Complete
            message = f"Successfully transferred {completed_files}/{total_files} files"
            self.transfer_completed.emit(completed_files > 0, message)

        except Exception as e:
            logging.error(f"Transfer error: {e}")
            self.transfer_completed.emit(False, f"Transfer failed: {str(e)}")

    def _get_all_files_in_directory(self, directory_prefix, all_files=None):
        """Recursively get all files in a directory (reuse the logic from download)"""
        if all_files is None:
            all_files = []
        try:
            blobs = self.azure_manager.get_blobs_in_container(
                account_name=self.source_account,
                container_name=self.source_container,
                prefix=directory_prefix,
            )

            for blob in blobs:
                if blob.get("is_directory", False):
                    self._get_all_files_in_directory(
                        directory_prefix=blob["name"], all_files=all_files
                    )
                else:
                    all_files.append(blob)

            return all_files

        except Exception as e:
            logging.error(
                f"Failed to recursively list directory {directory_prefix}: {e}"
            )
            return []

    def _transfer_single_file(self, blob_info):
        """Transfer a single blob between storage accounts"""
        try:
            source_blob_name = blob_info["name"]

            # Determine destination blob name
            if self.options.get("preserve_structure", True):
                dest_blob_name = source_blob_name
            else:
                dest_blob_name = source_blob_name.split("/")[-1]

            self.status_updated.emit(f"Transferring: {source_blob_name}")

            # Get destination client
            dest_client = self.azure_manager.get_blob_service_client(self.dest_account)
            if not dest_client:
                return False

            dest_blob_client = dest_client.get_blob_client(
                container=self.dest_container, blob=dest_blob_name
            )

            # Check if destination exists and handle overwrite
            if not self.options.get("overwrite", False):
                try:
                    dest_blob_client.get_blob_properties()
                    logging.warning(f"Skipping existing blob: {dest_blob_name}")
                    return True
                except Exception:
                    pass

            # Generate SAS URL for source blob
            source_sas_url = self.azure_manager.generate_blob_sas_url(
                account_name=self.source_account,
                container_name=self.source_container,
                blob_name=source_blob_name,
                expiry_hours=1,
            )

            if not source_sas_url:
                logging.error(f"Failed to generate SAS URL for {source_blob_name}")
                return False

            # Start copy operation
            dest_blob_client.start_copy_from_url(source_sas_url)

            # Wait for copy to complete
            import time

            max_wait_time = 300  # 5 minutes max
            wait_time = 0

            while wait_time < max_wait_time:
                if self.cancelled:
                    return False

                properties = dest_blob_client.get_blob_properties()
                copy_status = properties.copy.status

                if copy_status == "success":
                    logging.info(f"Successfully transferred: {source_blob_name}")
                    return True
                elif copy_status == "failed":
                    logging.error(f"Copy failed for {source_blob_name}")
                    return False
                elif copy_status in ["pending", "copying"]:
                    time.sleep(2)
                    wait_time += 2
                else:
                    logging.error(f"Unknown copy status: {copy_status}")
                    return False

            logging.error(f"Copy operation timed out for {source_blob_name}")
            return False

        except Exception as e:
            logging.error(f"Failed to transfer {blob_info['name']}: {e}")
            return False
