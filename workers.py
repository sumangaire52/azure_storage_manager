import logging
import time
from pathlib import Path

from PyQt6.QtCore import QObject, pyqtSignal, QThread

from utils import format_bytes, format_time


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
    speed_eta_updated = pyqtSignal(
        str, str, int, int, bool
    )  # Speed, ETA, bytes_transferred, total_bytes, size_calculation_complete

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

        # Speed and ETA tracking
        self.start_time = None
        self.bytes_transferred = 0
        self.total_bytes = 0
        self.completed_files = 0
        self.total_files = 0
        self.total_size_calculated = False

    def cancel(self):
        """Cancel the transfer operation"""
        self.cancelled = True

    def _calculate_speed_and_eta(self):
        """Calculate transfer speed and ETA with gradual size discovery"""
        if not self.start_time or self.bytes_transferred == 0:
            return (
                "0 B/s",
                "Calculating...",
                0,
                self.total_bytes,
                self.total_size_calculated,
            )

        elapsed_time = time.time() - self.start_time
        if elapsed_time == 0:
            return (
                "0 B/s",
                "Calculating...",
                self.bytes_transferred,
                self.total_bytes,
                self.total_size_calculated,
            )

        # Calculate speed
        speed_bps = self.bytes_transferred / elapsed_time
        speed_str = f"{format_bytes(speed_bps)}/s"

        # Calculate ETA based on file completion rate (more reliable than bytes during gradual discovery)
        if self.completed_files > 0 and self.total_files > 0:
            files_per_second = self.completed_files / elapsed_time
            remaining_files = self.total_files - self.completed_files

            if files_per_second > 0:
                eta_seconds = remaining_files / files_per_second
                eta_str = format_time(eta_seconds)
            else:
                eta_str = "Calculating..."
        else:
            eta_str = "Calculating..."

        return (
            speed_str,
            eta_str,
            self.bytes_transferred,
            self.total_bytes,
            self.total_size_calculated,
        )

    def run(self):
        """Main transfer logic with NO upfront size calculation"""
        try:
            self.start_time = time.time()
            self.completed_files = 0
            self.bytes_transferred = 0
            self.total_bytes = 0  # Will be calculated as we go
            self.total_size_calculated = (
                False  # Track when we're done calculating total
            )

            # First, get all files to transfer (NO size calculation)
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

            self.total_files = len(files_to_transfer)

            if self.total_files == 0:
                self.transfer_completed.emit(True, "No files to transfer")
                return

            self.status_updated.emit(
                f"Starting transfer of {self.total_files} files..."
            )

            # Transfer each file - get size ONLY when we're about to transfer it
            for i, file_blob in enumerate(files_to_transfer):
                if self.cancelled:
                    self.transfer_completed.emit(False, "Transfer cancelled")
                    return

                # Skip directories
                if file_blob.get("is_directory", False):
                    continue

                # Get file size ONLY for the current file being transferred
                file_size = 0
                try:
                    source_client = self.azure_manager.get_blob_service_client(
                        self.source_account
                    )
                    if source_client:
                        blob_client = source_client.get_blob_client(
                            container=self.source_container, blob=file_blob["name"]
                        )
                        properties = blob_client.get_blob_properties()
                        file_size = properties.size
                        self.total_bytes += file_size  # Add to running total
                except Exception as e:
                    logging.warning(f"Failed to get size for {file_blob['name']}: {e}")
                    file_size = 0

                success = self._transfer_single_file(file_blob)

                if success:
                    self.completed_files += 1
                    self.bytes_transferred += file_size

                    # Update progress based on file count
                    progress = int((self.completed_files / self.total_files) * 100)
                    self.progress_updated.emit(progress)
                    self.file_completed.emit(file_blob["name"])

                    # Check if we've calculated all sizes
                    if self.completed_files == self.total_files:
                        self.total_size_calculated = True

                    # Update speed and ETA
                    (
                        speed,
                        eta,
                        bytes_done,
                        total_bytes,
                        size_calc_done,
                    ) = self._calculate_speed_and_eta()
                    self.speed_eta_updated.emit(
                        speed, eta, bytes_done, total_bytes, size_calc_done
                    )
                else:
                    logging.error(f"Failed to transfer: {file_blob['name']}")

            # Mark total size calculation as complete
            self.total_size_calculated = True

            # Complete
            message = f"Successfully transferred {self.completed_files}/{self.total_files} files"
            self.transfer_completed.emit(self.completed_files > 0, message)

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
                    pass  # Blob doesn't exist, continue

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


class SizeCalculatorWorker(QThread):
    """Worker thread for calculating file sizes in batches"""

    size_batch_calculated = pyqtSignal(int)  # Total bytes calculated so far
    calculation_completed = pyqtSignal(int)  # Final total bytes

    def __init__(
        self, azure_manager, source_account, source_container, files_to_calculate
    ):
        super().__init__()
        self.azure_manager = azure_manager
        self.source_account = source_account
        self.source_container = source_container
        self.files_to_calculate = files_to_calculate
        self.cancelled = False
        self.batch_size = 10  # Calculate 10 files at a time
        self.batch_delay = 3.0  # 3 seconds delay between batches

    def cancel(self):
        """Cancel the size calculation"""
        self.cancelled = True

    def run(self):
        """Calculate file sizes in batches"""
        try:
            total_calculated = 0
            source_client = self.azure_manager.get_blob_service_client(
                self.source_account
            )

            if not source_client:
                logging.error("Failed to get source client for size calculation")
                self.calculation_completed.emit(0)
                return

            # Process files in batches
            for i in range(0, len(self.files_to_calculate), self.batch_size):
                if self.cancelled:
                    break

                batch = self.files_to_calculate[i : i + self.batch_size]
                batch_total = 0

                # Calculate sizes for this batch
                for file_blob in batch:
                    if self.cancelled:
                        break

                    if file_blob.get("is_directory", False):
                        continue

                    try:
                        blob_client = source_client.get_blob_client(
                            container=self.source_container, blob=file_blob["name"]
                        )
                        properties = blob_client.get_blob_properties()
                        file_size = properties.size
                        batch_total += file_size

                        # Small delay between individual requests within batch
                        time.sleep(0.1)  # 100ms between individual requests

                    except Exception as e:
                        logging.warning(
                            f"Failed to get size for {file_blob['name']}: {e}"
                        )
                        continue

                # Update total and emit progress
                total_calculated += batch_total
                self.size_batch_calculated.emit(total_calculated)

                # Delay between batches to avoid throttling
                if i + self.batch_size < len(
                    self.files_to_calculate
                ):  # Don't delay after last batch
                    time.sleep(self.batch_delay)

            # Emit final total
            self.calculation_completed.emit(total_calculated)

        except Exception as e:
            logging.error(f"Size calculation error: {e}")
            self.calculation_completed.emit(0)
