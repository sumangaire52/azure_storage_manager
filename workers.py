import logging
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyQt6.QtCore import QObject, pyqtSignal, QThread, QMutexLocker, QMutex

from utils import format_size, format_time


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

                # Skip if this is a directory
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

    progress_updated = pyqtSignal(int)
    status_updated = pyqtSignal(str)
    file_completed = pyqtSignal(str)
    transfer_completed = pyqtSignal(bool, str)  # Success, message
    speed_eta_updated = pyqtSignal(
        str, str, int, int, bool
    )  # Speed, estimated time, bytes_transferred, total_bytes, size_calculation_complete
    size_calculation_started = pyqtSignal(int)  # Total files to calculate
    size_calculation_progress = pyqtSignal(int)  # Running total bytes calculated

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

        # Size calculation worker
        self.size_calculator = None
        self.size_calculation_complete = False

    def cancel(self):
        """Cancel the transfer operation"""
        self.cancelled = True
        if self.size_calculator:
            self.size_calculator.cancel()

    def _calculate_speed_and_eta(self):
        """Calculate transfer speed and ETA with progressive total size updates"""
        if not self.start_time or self.bytes_transferred == 0:
            return (
                "0 B/s",
                "Calculating...",
                0,
                self.total_bytes,
                self.size_calculation_complete,
            )

        elapsed_time = time.time() - self.start_time
        if elapsed_time == 0:
            return (
                "0 B/s",
                "Calculating...",
                self.bytes_transferred,
                self.total_bytes,
                self.size_calculation_complete,
            )

        # Calculate speed
        speed_bps = self.bytes_transferred / elapsed_time
        speed_str = f"{format_size(speed_bps)}/s"

        # Calculate ETA based on what we know
        if self.total_bytes > 0 and speed_bps > 0:
            # We have some total size info, use byte-based ETA
            remaining_bytes = max(0, self.total_bytes - self.bytes_transferred)
            eta_seconds = remaining_bytes / speed_bps
            eta_str = format_time(eta_seconds)
        elif self.completed_files > 0 and self.total_files > 0:
            # Fall back to file-based ETA
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
            self.size_calculation_complete,
        )

    def run(self):
        """Main transfer logic with parallel transfers and size calculation"""
        try:
            self.start_time = time.time()
            self.completed_files = 0
            self.bytes_transferred = 0
            self.total_bytes = 0

            # First, get all files to transfer
            self.status_updated.emit("Calculating files to transfer...")

            files_to_transfer = []
            for item in self.items_to_transfer:
                if item.get("is_directory", False):
                    dir_files = self._get_all_files_in_directory(item["name"])
                    files_to_transfer.extend(dir_files)
                else:
                    files_to_transfer.append(item)

            self.total_files = len(files_to_transfer)

            if self.total_files == 0:
                self.transfer_completed.emit(True, "No files to transfer")
                return

            # Start size calculation in parallel
            self.status_updated.emit(
                f"Starting transfer of {self.total_files} files..."
            )
            self.size_calculation_started.emit(self.total_files)

            max_workers = self.options.get("concurrency", 8)

            self.size_calculator = SizeCalculatorWorker(
                self.azure_manager,
                self.source_account,
                self.source_container,
                files_to_transfer,
                max_workers=max_workers + 2,  # Use 2 more threads for size calculation
            )

            self.size_calculator.size_batch_calculated.connect(
                self._on_size_batch_calculated
            )
            self.size_calculator.calculation_completed.connect(
                self._on_size_calculation_completed
            )
            self.size_calculator.start()

            # Start concurrent transfers

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all transfer jobs
                future_to_file = {}

                for file_blob in files_to_transfer:
                    if self.cancelled:
                        break

                    if file_blob.get("is_directory", False):
                        continue

                    future = executor.submit(self._transfer_single_file, file_blob)
                    future_to_file[future] = file_blob

                # Process completed transfers as they finish
                for future in as_completed(future_to_file):
                    if self.cancelled:
                        break

                    file_blob = future_to_file[future]

                    try:
                        success = future.result()

                        # Get file size from cache if available, otherwise get it now
                        file_size = file_blob.get("size")
                        if file_size is None:
                            file_size = self._get_single_file_size(file_blob)
                            file_blob["size"] = file_size

                        if success:
                            self.completed_files += 1
                            self.bytes_transferred += file_size

                            # Update progress
                            if self.total_bytes > 0:
                                progress = min(
                                    int(
                                        (self.bytes_transferred / self.total_bytes)
                                        * 100
                                    ),
                                    99,
                                )
                            else:
                                progress = int(
                                    (self.completed_files / self.total_files) * 100
                                )

                            self.progress_updated.emit(progress)
                            self.file_completed.emit(file_blob["name"])

                            # Update speed and ETA
                            (
                                speed,
                                eta,
                                bytes_done,
                                total_bytes,
                                size_calc_complete,
                            ) = self._calculate_speed_and_eta()
                            self.speed_eta_updated.emit(
                                speed, eta, bytes_done, total_bytes, size_calc_complete
                            )
                        else:
                            logging.error(f"Failed to transfer: {file_blob['name']}")

                    except Exception as e:
                        logging.error(
                            f"Transfer future failed for {file_blob['name']}: {e}"
                        )

            # Wait for size calculation to complete (if still running)
            if self.size_calculator and self.size_calculator.isRunning():
                self.size_calculator.wait()

            # Complete
            message = f"Successfully transferred {self.completed_files}/{self.total_files} files ({format_size(self.bytes_transferred)})"
            self.transfer_completed.emit(self.completed_files > 0, message)

        except Exception as e:
            logging.error(f"Transfer error: {e}")
            self.transfer_completed.emit(False, f"Transfer failed: {str(e)}")

    def _on_size_batch_calculated(self, running_total):
        """Handle size calculation progress updates"""
        self.total_bytes = running_total  # Update total as we learn more
        self.size_calculation_progress.emit(running_total)

    def _on_size_calculation_completed(self, total_size):
        """Handle size calculation completion"""
        self.total_bytes = total_size
        self.size_calculation_complete = True
        logging.info(f"Total size calculation completed: {format_size(total_size)}")

    def _get_single_file_size(self, file_blob):
        """Get size for a single file if not already cached"""
        try:
            source_client = self.azure_manager.get_blob_service_client(
                self.source_account
            )
            if not source_client:
                return 0

            blob_client = source_client.get_blob_client(
                container=self.source_container, blob=file_blob["name"]
            )
            properties = blob_client.get_blob_properties()
            return properties.size
        except Exception as e:
            logging.warning(f"Failed to get size for {file_blob['name']}: {e}")
            return 0

    def _get_all_files_in_directory(self, directory_prefix, all_files=None):
        """Recursively get all files in a directory"""
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

            # Wait for copy to complete with optimized polling
            while True:
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
                    time.sleep(0.1)  # Fast polling
                else:
                    logging.error(f"Unknown copy status: {copy_status}")
                    return False

        except Exception as e:
            logging.error(f"Failed to transfer {blob_info['name']}: {e}")
            return False


class SizeCalculatorWorker(QThread):
    """Worker thread for calculating file sizes in batches"""

    size_batch_calculated = pyqtSignal(int)
    calculation_completed = pyqtSignal(int)
    progress_updated = pyqtSignal(int, int)

    def __init__(
        self,
        azure_manager,
        source_account,
        source_container,
        files_to_calculate,
        max_workers,
    ):
        super().__init__()
        self.azure_manager = azure_manager
        self.source_account = source_account
        self.source_container = source_container
        self.files_to_calculate = files_to_calculate
        self.cancelled = False
        self.max_workers = max_workers

        self.total_calculated = 0
        self.completed_files = 0
        self.mutex = QMutex()

    def cancel(self):
        """Cancel the size calculation"""
        self.cancelled = True

    def _worker_function(self, files_chunk, source_client, worker_id):
        """Worker function that processes a chunk of files"""
        chunk_total = 0

        for file_blob in files_chunk:
            if self.cancelled:
                break

            if file_blob.get("is_directory", False):
                file_blob["size"] = 0
                size = 0
            else:
                try:
                    blob_client = source_client.get_blob_client(
                        container=self.source_container, blob=file_blob["name"]
                    )
                    properties = blob_client.get_blob_properties()
                    size = properties.size
                    file_blob["size"] = size

                    # Stagger requests to avoid overwhelming API
                    time.sleep(0.02 * (worker_id + 1))  # Different delays per worker

                except Exception as e:
                    logging.warning(
                        f"Worker {worker_id} failed to get size for {file_blob['name']}: {e}"
                    )
                    file_blob["size"] = 0
                    size = 0

            chunk_total += size

            # Thread-safe update
            with QMutexLocker(self.mutex):
                self.total_calculated += size
                self.completed_files += 1
                current_total = self.total_calculated
                current_completed = self.completed_files

            # Emit progress (every 10 files to avoid too many signals)
            if current_completed % 10 == 0:
                self.size_batch_calculated.emit(current_total)
                self.progress_updated.emit(
                    current_completed, len(self.files_to_calculate)
                )

        return chunk_total

    def run(self):
        """Run calculation using work-stealing thread pool"""
        try:
            with QMutexLocker(self.mutex):
                self.total_calculated = 0
                self.completed_files = 0

            source_client = self.azure_manager.get_blob_service_client(
                self.source_account
            )

            if not source_client:
                logging.error("Failed to get source client for size calculation")
                self.calculation_completed.emit(0)
                return

            # Divide files among workers
            chunk_size = max(1, len(self.files_to_calculate) // self.max_workers)
            file_chunks = [
                self.files_to_calculate[i : i + chunk_size]
                for i in range(0, len(self.files_to_calculate), chunk_size)
            ]

            # Use ThreadPoolExecutor with work stealing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._worker_function, chunk, source_client, i)
                    for i, chunk in enumerate(file_chunks)
                ]

                # Wait for all workers to complete
                for future in as_completed(futures):
                    if self.cancelled:
                        break
                    try:
                        chunk_result = future.result()
                        logging.debug(f"Chunk completed with {chunk_result} bytes")
                    except Exception as e:
                        logging.error(f"Worker thread error: {e}")

            # Final update
            with QMutexLocker(self.mutex):
                final_total = self.total_calculated

            self.size_batch_calculated.emit(final_total)
            self.calculation_completed.emit(final_total)

            logging.info(f"Optimized calculation completed. Total: {final_total} bytes")

        except Exception as e:
            logging.error(f"Optimized size calculation error: {e}")
            self.calculation_completed.emit(0)


class UploadWorker(QThread):
    """Worker thread for uploading files to Azure Blob Storage"""

    progress_updated = pyqtSignal(int)
    status_updated = pyqtSignal(str)
    file_uploaded = pyqtSignal(str)
    upload_completed = pyqtSignal(bool, str)

    def __init__(
        self,
        azure_manager,
        account_name,
        container_name,
        file_paths,
        target_directory,
        is_folder=False,
        base_folder=None,
    ):
        super().__init__()
        self.azure_manager = azure_manager
        self.account_name = account_name
        self.container_name = container_name
        self.file_paths = file_paths
        self.target_directory = target_directory
        self.is_folder = is_folder
        self.base_folder = base_folder
        self._cancelled = False

    def cancel(self):
        """Cancel the upload operation"""
        self._cancelled = True

    def run(self):
        """Execute the upload operation"""
        try:
            # Get blob service client
            client = self.azure_manager.get_blob_service_client(self.account_name)
            if not client:
                self.upload_completed.emit(False, "Failed to get Azure client")
                return

            total_files = len(self.file_paths)
            completed_files = 0

            for i, file_path in enumerate(self.file_paths):
                if self._cancelled:
                    self.upload_completed.emit(False, "Upload cancelled by user")
                    return

                try:
                    # Calculate blob name based on upload type
                    blob_name = self._calculate_blob_name(file_path)

                    # Update status
                    self.status_updated.emit(f"Uploading {Path(file_path).name}...")

                    # Get blob client
                    blob_client = client.get_blob_client(
                        container=self.container_name, blob=blob_name
                    )

                    # Upload file
                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)

                    completed_files += 1
                    self.file_uploaded.emit(file_path)

                    # Update progress
                    progress = int((completed_files / total_files) * 100)
                    self.progress_updated.emit(progress)

                except Exception as e:
                    logging.error(f"Failed to upload {file_path}: {e}")
                    # Continue with other files
                    continue

            # Complete
            if completed_files == total_files:
                self.upload_completed.emit(
                    True, f"Successfully uploaded {completed_files} files"
                )
            else:
                self.upload_completed.emit(
                    False,
                    f"Uploaded {completed_files} of {total_files} files. Check logs for errors.",
                )

        except Exception as e:
            self.upload_completed.emit(False, f"Upload failed: {str(e)}")

    def _calculate_blob_name(self, file_path):
        """Calculate the blob name based on upload type and target directory"""
        file_path = Path(file_path)

        if self.is_folder and self.base_folder:
            # For folder upload, preserve directory structure INCLUDING the base folder name
            base_path = Path(self.base_folder)

            # Get relative path from the parent of base folder
            relative_path = file_path.relative_to(base_path.parent)
            blob_name = str(relative_path).replace("\\", "/")
        else:
            # For individual files, just use filename
            blob_name = file_path.name

        # Add target directory prefix if specified
        if self.target_directory:
            target_dir = self.target_directory.rstrip("/") + "/"
            blob_name = target_dir + blob_name

        return blob_name


class DeleteWorker(QThread):
    """Worker thread for deleting items from Azure Blob Storage"""

    progress_updated = pyqtSignal(int)
    status_updated = pyqtSignal(str)
    item_deleted = pyqtSignal(str)
    delete_completed = pyqtSignal(bool, str)

    def __init__(self, azure_manager, account_name, container_name, items_to_delete):
        super().__init__()
        self.azure_manager = azure_manager
        self.account_name = account_name
        self.container_name = container_name
        self.items_to_delete = items_to_delete
        self._cancelled = False

    def cancel(self):
        """Cancel the deletion operation"""
        self._cancelled = True

    def run(self):
        """Execute the deletion operation"""
        try:
            # Get blob service client
            client = self.azure_manager.get_blob_service_client(self.account_name)
            if not client:
                self.delete_completed.emit(False, "Failed to get Azure client")
                return

            total_items_to_delete = 0
            completed_deletions = 0
            failed_deletions = 0

            # First, collect all items to delete (including directory contents)
            all_blobs_to_delete = []

            for item in self.items_to_delete:
                if self._cancelled:
                    self.delete_completed.emit(False, "Deletion cancelled by user")
                    return

                if item.get("is_directory", False):
                    # For directories, get all blobs with this prefix
                    try:
                        self.status_updated.emit(f"Scanning directory: {item['name']}")
                        directory_blobs = self.azure_manager.get_blobs_in_container(
                            self.account_name, self.container_name, item["name"]
                        )

                        # Add all non-directory blobs from this directory
                        for blob in directory_blobs:
                            if not blob.get("is_directory", False):
                                all_blobs_to_delete.append(blob["name"])

                    except Exception as e:
                        logging.error(f"Failed to scan directory {item['name']}: {e}")
                        failed_deletions += 1
                        continue
                else:
                    # For files, add directly
                    all_blobs_to_delete.append(item["name"])

            total_items_to_delete = len(all_blobs_to_delete)

            if total_items_to_delete == 0:
                self.delete_completed.emit(True, "No items to delete")
                return

            # Now delete all collected blobs
            for i, blob_name in enumerate(all_blobs_to_delete):
                if self._cancelled:
                    self.delete_completed.emit(False, "Deletion cancelled by user")
                    return

                try:
                    # Update status
                    display_name = blob_name.split("/")[-1]  # Show just filename
                    self.status_updated.emit(f"Deleting: {display_name}")

                    # Get blob client and delete
                    blob_client = client.get_blob_client(
                        container=self.container_name, blob=blob_name
                    )

                    blob_client.delete_blob()

                    completed_deletions += 1
                    self.item_deleted.emit(blob_name)

                    # Update progress
                    progress = int(((i + 1) / total_items_to_delete) * 100)
                    self.progress_updated.emit(progress)

                except Exception as e:
                    logging.error(f"Failed to delete {blob_name}: {e}")
                    failed_deletions += 1
                    continue

            # Complete
            if failed_deletions == 0:
                self.delete_completed.emit(
                    True, f"Successfully deleted {completed_deletions} items"
                )
            else:
                self.delete_completed.emit(
                    False if completed_deletions == 0 else True,
                    f"Deleted {completed_deletions} items. {failed_deletions} failed. Check logs for details.",
                )

        except Exception as e:
            self.delete_completed.emit(False, f"Deletion failed: {str(e)}")
