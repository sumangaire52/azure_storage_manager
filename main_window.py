import json
import logging
import threading
from datetime import datetime
from pathlib import Path

from PyQt6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTabWidget,
    QTreeWidget,
    QTableWidget,
    QPushButton,
    QLabel,
    QComboBox,
    QTextEdit,
    QSplitter,
    QGroupBox,
    QSpinBox,
    QFormLayout,
    QListWidget,
    QDateTimeEdit,
    QFrame,
    QMessageBox,
    QListWidgetItem,
    QTreeWidgetItem,
    QFileDialog,
    QProgressDialog,
)
from PyQt6.QtCore import Qt, pyqtSignal, QDateTime, pyqtSlot
from PyQt6.QtGui import QFont

from log_handler import LogHandler
from managers import AzureManager
from utils import populate_signals, format_size
from workers import AuthWorker, DownloadWorker


class MainWindow(QMainWindow):
    """Main application window"""

    containers_loaded = pyqtSignal(list)
    blobs_loaded = pyqtSignal(list)
    directory_contents_loaded = pyqtSignal(object, list)

    def __init__(self):
        super().__init__()
        self.azure_manager = AzureManager()
        self.log_handler = LogHandler()
        self.auth_status_label = QLabel("Not Authenticated")
        self.auth_btn = QPushButton("Authenticate with Azure CLI")
        self.refresh_btn = QPushButton("Refresh Accounts")
        self.new_transfer_btn = QPushButton("New Transfer Job")
        self.download_btn = QPushButton("Download Selected")
        self.refresh_blobs_btn = QPushButton("Refresh")
        self.pause_btn = QPushButton("Pause Selected")
        self.resume_btn = QPushButton("Resume Selected")
        self.cancel_btn = QPushButton("Cancel Selected")
        self.clear_completed_btn = QPushButton("Clear Completed")
        self.schedule_job_btn = QPushButton("Schedule Transfer Job")
        self.clear_logs_btn = QPushButton("Clear Logs")
        self.export_logs_btn = QPushButton("Export Logs")
        self.tab_widget = QTabWidget()
        self.accounts_list = QListWidget()
        self.containers_list = QListWidget()
        self.log_level_combo = QComboBox()
        self.blobs_tree = QTreeWidget()
        self.transfers_table = QTableWidget()
        self.schedule_type_combo = QComboBox()
        self.schedule_datetime = QDateTimeEdit()
        self.interval_spin = QSpinBox()
        self.scheduled_jobs_table = QTableWidget()
        self.log_display = QTextEdit()
        self.log_handler = LogHandler()
        self.setup_logging()
        self.setup_ui()

    def setup_ui(self):
        """Set up the main user interface"""
        self.setWindowTitle("Azure Storage Manager")
        self.setGeometry(100, 100, 1400, 900)

        # Central widget with tabs
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        # Authentication section
        auth_frame = QFrame()
        auth_frame.setFrameStyle(QFrame.Shape.StyledPanel)
        auth_layout = QHBoxLayout()

        self.refresh_btn.setEnabled(False)

        auth_layout.addWidget(QLabel("Status:"))
        auth_layout.addWidget(self.auth_status_label)
        auth_layout.addStretch()
        auth_layout.addWidget(self.auth_btn)
        auth_layout.addWidget(self.refresh_btn)

        auth_frame.setLayout(auth_layout)
        main_layout.addWidget(auth_frame)

        # Tab widget
        main_layout.addWidget(self.tab_widget)

        # Setup tabs
        self.setup_storage_tab()
        self.setup_transfers_tab()
        self.setup_scheduler_tab()
        self.setup_logs_tab()

        # Enable signals
        populate_signals(self)

    def setup_storage_tab(self):
        """Setup storage accounts and containers tab"""
        storage_widget = QWidget()
        layout = QHBoxLayout()

        # Left side - Storage accounts and containers
        left_splitter = QSplitter(Qt.Orientation.Vertical)

        # Storage accounts
        accounts_group = QGroupBox("Storage Accounts")
        accounts_layout = QVBoxLayout()

        accounts_layout.addWidget(self.accounts_list)
        accounts_group.setLayout(accounts_layout)

        # Containers
        containers_group = QGroupBox("Containers")
        containers_layout = QVBoxLayout()

        containers_layout.addWidget(self.containers_list)
        containers_group.setLayout(containers_layout)

        left_splitter.addWidget(accounts_group)
        left_splitter.addWidget(containers_group)
        left_splitter.setSizes([300, 300])

        # Right side - Blob explorer
        right_group = QGroupBox("Blob Explorer")
        right_layout = QVBoxLayout()

        # Toolbar for blob operations
        blob_toolbar = QHBoxLayout()

        blob_toolbar.addWidget(self.new_transfer_btn)
        blob_toolbar.addWidget(self.download_btn)
        blob_toolbar.addWidget(self.refresh_blobs_btn)
        blob_toolbar.addStretch()

        # Blob tree
        self.blobs_tree.setHeaderLabels(["Name", "Size", "Modified", "Tier"])
        self.blobs_tree.setSelectionMode(QTreeWidget.SelectionMode.ExtendedSelection)

        right_layout.addLayout(blob_toolbar)
        right_layout.addWidget(self.blobs_tree)
        right_group.setLayout(right_layout)

        # Add to main layout
        layout.addWidget(left_splitter, 1)
        layout.addWidget(right_group, 2)

        storage_widget.setLayout(layout)
        self.tab_widget.addTab(storage_widget, "Storage Explorer")

    def setup_transfers_tab(self):
        """Setup transfers monitoring tab"""
        transfers_widget = QWidget()
        layout = QVBoxLayout()

        # Toolbar
        toolbar = QHBoxLayout()

        toolbar.addWidget(self.pause_btn)
        toolbar.addWidget(self.resume_btn)
        toolbar.addWidget(self.cancel_btn)
        toolbar.addStretch()
        toolbar.addWidget(self.clear_completed_btn)

        # Transfers table
        self.transfers_table.setColumnCount(8)
        self.transfers_table.setHorizontalHeaderLabels(
            [
                "Job ID",
                "Source",
                "Destination",
                "Progress",
                "Speed",
                "ETA",
                "Status",
                "Created",
            ]
        )
        self.transfers_table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows
        )

        layout.addLayout(toolbar)
        layout.addWidget(self.transfers_table)

        transfers_widget.setLayout(layout)
        self.tab_widget.addTab(transfers_widget, "Transfer Jobs")

    def setup_scheduler_tab(self):
        """Setup scheduler tab for batch operations"""
        scheduler_widget = QWidget()
        layout = QVBoxLayout()

        # Scheduling controls
        schedule_group = QGroupBox("Schedule New Job")
        schedule_layout = QFormLayout()

        self.schedule_type_combo.addItems(["One Time", "Recurring"])

        self.schedule_datetime.setDateTime(QDateTime.currentDateTime().addSecs(3600))

        self.interval_spin.setRange(1, 24)
        self.interval_spin.setValue(1)
        self.interval_spin.setSuffix(" hours")

        schedule_layout.addRow("Type:", self.schedule_type_combo)
        schedule_layout.addRow("Start Time:", self.schedule_datetime)
        schedule_layout.addRow("Interval:", self.interval_spin)
        schedule_layout.addRow("", self.schedule_job_btn)

        schedule_group.setLayout(schedule_layout)

        # Scheduled jobs list
        jobs_group = QGroupBox("Scheduled Jobs")
        jobs_layout = QVBoxLayout()

        self.scheduled_jobs_table.setColumnCount(5)
        self.scheduled_jobs_table.setHorizontalHeaderLabels(
            ["Job ID", "Type", "Next Run", "Status", "Actions"]
        )

        jobs_layout.addWidget(self.scheduled_jobs_table)
        jobs_group.setLayout(jobs_layout)

        layout.addWidget(schedule_group)
        layout.addWidget(jobs_group)

        scheduler_widget.setLayout(layout)
        self.tab_widget.addTab(scheduler_widget, "Scheduler")

    # Logging
    def setup_logs_tab(self):
        """Setup logs and monitoring tab"""
        logs_widget = QWidget()
        layout = QVBoxLayout()

        # Log controls
        log_controls = QHBoxLayout()
        self.log_level_combo.addItems(["DEBUG", "INFO", "WARNING", "ERROR"])
        self.log_level_combo.setCurrentText("INFO")

        log_controls.addWidget(QLabel("Log Level:"))
        log_controls.addWidget(self.log_level_combo)
        log_controls.addStretch()
        log_controls.addWidget(self.clear_logs_btn)
        log_controls.addWidget(self.export_logs_btn)

        # Log display
        self.log_display.setReadOnly(True)
        self.log_display.setFont(QFont("Consolas", 9))

        layout.addLayout(log_controls)
        layout.addWidget(self.log_display)

        logs_widget.setLayout(layout)
        self.tab_widget.addTab(logs_widget, "Logs")

    def setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory
        logs_dir = Path.home() / ".azure_storage_manager" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        log_file = logs_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )

        # Custom handler to display logs in UI
        self.log_handler.log_message.connect(self.append_log_message)
        logging.getLogger().addHandler(self.log_handler)

    @pyqtSlot(str)
    def append_log_message(self, message):
        """Append log message to the display"""
        self.log_display.append(message)

        # Auto-scroll to bottom
        cursor = self.log_display.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_display.setTextCursor(cursor)

        # Limit log display to last 1000 lines
        text = self.log_display.toPlainText()
        lines = text.split("\n")

        if len(lines) > 1000:
            self.log_display.setPlainText("\n".join(lines[-1000:]))

    def clear_logs(self):
        """Clear the log display"""
        self.log_display.clear()

    def export_logs(self):
        """Export logs to file"""
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Export Logs",
            f"azure_storage_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            "Text Files (*.txt);;All Files (*)",
        )

        if filename:
            try:
                with open(filename, "w") as f:
                    f.write(self.log_display.toPlainText())
                QMessageBox.information(self, "Success", f"Logs exported to {filename}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to export logs: {e}")

    def load_settings(self):
        """Load application settings"""
        settings_file = Path.home() / ".azure_storage_manager" / "settings.json"

        if settings_file.exists():
            try:
                with open(settings_file) as f:
                    settings = json.load(f)

                # Apply settings
                if "window_geometry" in settings:
                    self.restoreGeometry(bytes.fromhex(settings["window_geometry"]))

            except Exception as e:
                logging.warning(f"Failed to load settings: {e}")

    def save_settings(self):
        """Save application settings"""
        settings_dir = Path.home() / ".azure_storage_manager"
        settings_dir.mkdir(parents=True, exist_ok=True)

        settings = {
            "window_geometry": self.saveGeometry().toHex().data().decode(),
            "last_used_accounts": [
                self.accounts_list.item(i).text()
                for i in range(self.accounts_list.count())
            ],
        }

        try:
            with open(settings_dir / "settings.json", "w") as f:
                json.dump(settings, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save settings: {e}")

    def authenticate(self):
        """Authenticate with Azure CLI"""
        self.auth_btn.setEnabled(False)
        self.auth_btn.setText("Authenticating...")

        # Create worker
        self.worker = AuthWorker(self.azure_manager)  # noqa
        self.worker.finished.connect(self.on_authentication_complete)

        # Run in background
        threading.Thread(target=self.worker.run, daemon=True).start()

    def on_authentication_complete(self, success):
        """Handle authentication completion"""
        if success:
            self.auth_status_label.setText("✓ Authenticated")
            self.auth_status_label.setStyleSheet("color: green")
            self.auth_btn.setText("Re-authenticate")
            self.refresh_btn.setEnabled(True)
            self.refresh_storage_accounts()
            logging.info("Successfully authenticated with Azure")
        else:
            self.auth_status_label.setText("✗ Authentication Failed")
            self.auth_status_label.setStyleSheet("color: red")
            self.auth_btn.setText("Authenticate with Azure CLI")
            QMessageBox.critical(
                self,
                "Authentication Error",
                "Failed to authenticate. Please run 'az login' first.",
            )

        self.auth_btn.setEnabled(True)

    def refresh_storage_accounts(self):
        """Refresh the list of storage accounts"""
        self.accounts_list.clear()
        self.containers_list.clear()
        self.blobs_tree.clear()

        accounts = self.azure_manager.get_storage_accounts()

        for account in accounts:
            self.accounts_list.addItem(account["name"])

        logging.info(f"Loaded {len(accounts)} storage accounts")

    def on_account_selected(self, item):
        """Handle storage account selection and load containers"""
        account_name = item.text()

        # Clear current containers and blobs
        self.containers_list.clear()
        self.blobs_tree.clear()

        # Show a loading placeholder
        loading_item = QListWidgetItem("Loading...")
        loading_item.setFlags(Qt.ItemFlag.NoItemFlags)
        self.containers_list.addItem(loading_item)
        self.containers_list.setEnabled(False)
        self.blobs_tree.setEnabled(False)

        # Fetch containers in a background thread
        threading.Thread(
            target=self._fetch_containers, args=(account_name,), daemon=True
        ).start()

    def _fetch_containers(self, account_name):
        """Worker function to fetch containers in background"""
        try:
            containers = self.azure_manager.get_containers(account_name)
        except Exception as e:
            containers = []
            logging.error(f"Failed to load containers for {account_name}: {e}")

        # Emit signal to update UI in main thread
        self.containers_loaded.emit(containers)

    def populate_containers_list(self, containers):
        """Populate the containers list in the UI thread"""
        self.containers_list.clear()

        if not containers:
            no_item = QListWidgetItem("No containers found")
            no_item.setFlags(Qt.ItemFlag.NoItemFlags)
            self.containers_list.addItem(no_item)
        else:
            for container in containers:
                self.containers_list.addItem(container)

        self.containers_list.setEnabled(True)
        self.blobs_tree.setEnabled(True)
        logging.info(f"Loaded {len(containers)} containers")

    def on_container_selected(self, item):
        if not self.accounts_list.currentItem():
            return

        account_name = self.accounts_list.currentItem().text()
        container_name = item.text()

        self.blobs_tree.clear()
        # Show temporary loading node
        loading_item = QTreeWidgetItem(["Loading..."])
        self.blobs_tree.addTopLevelItem(loading_item)

        # Fetch blobs on background
        threading.Thread(
            target=self._fetch_blobs, args=(account_name, container_name), daemon=True
        ).start()

    def _fetch_blobs(self, account_name, container_name):
        """Worker function to fetch blobs in background"""
        try:
            blobs = self.azure_manager.get_blobs_in_container(
                account_name, container_name
            )
        except Exception as e:
            blobs = []
            logging.error(
                f"Failed to load blobs for {account_name}/{container_name}: {e}"
            )

        # Emit signal to update UI in main thread
        self.blobs_loaded.emit(blobs)

    def populate_blobs_tree(self, blobs: list):
        """Populate the blobs tree with the provided blob data"""
        self.blobs_tree.clear()

        if not blobs:
            # Show empty state
            empty_item = QTreeWidgetItem(["No blobs found", "", "", ""])
            empty_item.setFlags(Qt.ItemFlag.NoItemFlags)
            self.blobs_tree.addTopLevelItem(empty_item)
            return

        for blob in blobs:
            # Extract blob information
            name = blob.get("name", "")
            size = blob.get("size", 0)
            last_modified = blob.get("last_modified", "")
            tier = blob.get("tier", "")
            is_directory = blob.get("is_directory", False)

            # Format display values
            if is_directory:
                # For directories
                display_name = (
                    name.rstrip("/").split("/")[-1] + "/"
                    if name
                    else "Unknown Directory"
                )
                size_display = ""
                modified_display = ""
                tier_display = ""
            else:
                # For files
                display_name = name.split("/")[-1] if name else "Unknown File"
                size_display = format_size(size) if size > 0 else ""
                modified_display = last_modified[:19] if last_modified else ""
                tier_display = tier if tier else ""

            # Create tree widget item with all four columns
            item = QTreeWidgetItem(
                [display_name, size_display, modified_display, tier_display]
            )

            # Store the full blob data for later use
            item.setData(0, Qt.ItemDataRole.UserRole, blob)

            # If it's a directory, add a placeholder child for expansion
            if is_directory:
                placeholder = QTreeWidgetItem(["Loading..."])
                placeholder.setFlags(Qt.ItemFlag.NoItemFlags)
                item.addChild(placeholder)
                item.setChildIndicatorPolicy(
                    QTreeWidgetItem.ChildIndicatorPolicy.ShowIndicator
                )

            # Add item to tree
            self.blobs_tree.addTopLevelItem(item)

    def on_directory_expanded(self, item):
        """Handle directory expansion - lazy load subdirectories and files"""
        # Get the stored blob data from the item
        blob_data = item.data(0, Qt.ItemDataRole.UserRole)

        if not blob_data or not blob_data.get("is_directory", False):
            return

        # Check if we've already loaded this directory
        if item.childCount() == 1:
            first_child = item.child(0)
            if first_child.text(0) != "Loading...":
                # Already loaded, don't reload
                return

        # Get current account and container
        if (
            not self.accounts_list.currentItem()
            or not self.containers_list.currentItem()
        ):
            return

        account_name = self.accounts_list.currentItem().text()
        container_name = self.containers_list.currentItem().text()

        # Get the directory prefix (path)
        prefix = blob_data["name"]

        # Remove the placeholder "Loading..." item
        item.takeChildren()

        # Add a loading indicator
        loading_item = QTreeWidgetItem(["Loading..."])
        loading_item.setFlags(Qt.ItemFlag.NoItemFlags)
        item.addChild(loading_item)

        # Fetch subdirectories and files in background thread
        threading.Thread(
            target=self._fetch_directory_contents,
            args=(item, account_name, container_name, prefix),
            daemon=True,
        ).start()

    def _fetch_directory_contents(
        self, parent_item, account_name, container_name, prefix
    ):
        """Worker function to fetch directory contents in background"""
        try:
            # Fetch blobs with the directory prefix
            blobs = self.azure_manager.get_blobs_in_container(
                account_name, container_name, prefix
            )

            # Filter to get only direct children (not nested subdirectories)
            direct_children = []

            for blob in blobs:
                blob_name = blob["name"]

                # Skip the parent directory itself
                if blob_name == prefix:
                    continue

                # Remove the parent prefix to get the relative path
                if blob_name.startswith(prefix):
                    relative_path = blob_name[len(prefix) :]

                    # For directories: check if it's a direct child (only one more level)
                    if blob.get("is_directory", False):
                        # Count slashes in relative path - should be 0 for direct child directories
                        if relative_path.count("/") <= 1:
                            direct_children.append(blob)
                    else:
                        # For files: check if it's directly in this directory (no subdirectories)
                        if "/" not in relative_path:
                            direct_children.append(blob)

            # Emit signal to update UI in main thread
            self.directory_contents_loaded.emit(parent_item, direct_children)

        except Exception as e:
            logging.error(f"Failed to load directory contents for {prefix}: {e}")
            # Emit empty list on error
            self.directory_contents_loaded.emit(parent_item, [])

    def on_directory_contents_loaded(self, parent_item, blobs):
        """Handle directory contents loaded - update the tree in main thread"""
        # Remove the loading indicator
        parent_item.takeChildren()

        if not blobs:
            # No items in this directory
            empty_item = QTreeWidgetItem(["No items"])
            empty_item.setFlags(Qt.ItemFlag.NoItemFlags)
            parent_item.addChild(empty_item)
            return

        # Add each blob as a child item
        for blob in blobs:
            # Extract blob information
            name = blob.get("name", "")
            size = blob.get("size", 0)
            last_modified = blob.get("last_modified", "")
            tier = blob.get("tier", "")
            is_directory = blob.get("is_directory", False)

            # Format display values
            if is_directory:
                # For subdirectories - show just the folder name
                display_name = name.rstrip("/").split("/")[-1] + "/"
                size_display = ""
                modified_display = ""
                tier_display = ""
            else:
                # For files - show just the filename
                display_name = name.split("/")[-1]
                size_display = format_size(size) if size > 0 else ""
                modified_display = last_modified[:19] if last_modified else ""
                tier_display = tier if tier else ""

            # Create child item
            child_item = QTreeWidgetItem(
                [display_name, size_display, modified_display, tier_display]
            )

            # Store the full blob data
            child_item.setData(0, Qt.ItemDataRole.UserRole, blob)

            # If it's a directory, add placeholder for further expansion
            if is_directory:
                placeholder = QTreeWidgetItem(["Loading..."])
                placeholder.setFlags(Qt.ItemFlag.NoItemFlags)
                child_item.addChild(placeholder)
                child_item.setChildIndicatorPolicy(
                    QTreeWidgetItem.ChildIndicatorPolicy.ShowIndicator
                )

            # Add to parent
            parent_item.addChild(child_item)

        # Sort children: directories first, then files, both alphabetically
        parent_item.sortChildren(0, Qt.SortOrder.AscendingOrder)

    # Download logic for files and folder
    def download_selected_items(self):
        """Download selected items (files or folders) from the blob tree"""
        selected_items = self.blobs_tree.selectedItems()

        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select items to download.")
            return

        # Get current account and container
        if (
            not self.accounts_list.currentItem()
            or not self.containers_list.currentItem()
        ):
            QMessageBox.warning(
                self, "Warning", "Please select an account and container."
            )
            return

        account_name = self.accounts_list.currentItem().text()
        container_name = self.containers_list.currentItem().text()

        # Get blob data from selected items - be more careful about what we select
        items_to_download = []
        for item in selected_items:
            blob_data = item.data(0, Qt.ItemDataRole.UserRole)
            if blob_data and blob_data.get("name"):
                # Debug: Log what we're selecting
                item_type = (
                    "directory" if blob_data.get("is_directory", False) else "file"
                )
                logging.info(
                    f"Selected for download: {blob_data['name']} (type: {item_type})"
                )
                items_to_download.append(blob_data)

        if not items_to_download:
            QMessageBox.warning(
                self, "Warning", "No valid items selected for download."
            )
            return

        # Show selection summary
        dirs = sum(1 for item in items_to_download if item.get("is_directory", False))
        files = len(items_to_download) - dirs

        result = QMessageBox.question(
            self,
            "Confirm Download",
            f"Download {files} files and {dirs} folders?\n\nThis will download all contents recursively.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )

        if result != QMessageBox.StandardButton.Yes:
            return

        # Choose download directory
        download_path = QFileDialog.getExistingDirectory(
            self, "Select Download Location", str(Path.home() / "Downloads")
        )

        if not download_path:
            return  # User cancelled

        # Start download
        self._start_download(
            account_name, container_name, items_to_download, download_path
        )

    def _start_download(
        self, account_name, container_name, items_to_download, local_path
    ):
        """Start the download process with progress dialog"""

        # Create progress dialog
        self.download_progress = QProgressDialog(
            "Preparing download...", "Cancel", 0, 100, self
        )
        self.download_progress.setWindowTitle("Downloading Files")
        self.download_progress.setWindowModality(Qt.WindowModality.WindowModal)
        self.download_progress.setMinimumDuration(0)
        self.download_progress.setValue(0)

        # Create download worker
        self.download_worker = DownloadWorker(
            self.azure_manager,
            account_name,
            container_name,
            items_to_download,
            local_path,
        )

        # Connect signals
        self.download_worker.progress_updated.connect(self.download_progress.setValue)
        self.download_worker.status_updated.connect(self.download_progress.setLabelText)
        self.download_worker.file_completed.connect(self._on_file_downloaded)
        self.download_worker.download_completed.connect(self._on_download_completed)

        # Connect cancel button
        self.download_progress.canceled.connect(self.download_worker.cancel)

        # Start download
        self.download_worker.start()

        logging.info(
            f"Started download of {len(items_to_download)} items to {local_path}"
        )

    def _on_file_downloaded(self, file_path):
        """Handle individual file download completion"""
        logging.info(f"Downloaded: {file_path}")

    def _on_download_completed(self, success, message):
        """Handle download completion"""
        self.download_progress.close()

        if success:
            QMessageBox.information(self, "Download Complete", message)
            logging.info(f"Download completed: {message}")
        else:
            QMessageBox.critical(self, "Download Failed", message)
            logging.error(f"Download failed: {message}")

        # Clean up
        if hasattr(self, "download_worker"):
            self.download_worker.deleteLater()

    def download_single_blob(self, blob_name, local_file_path):
        """Download a single blob to a specific local file path"""
        if (
            not self.accounts_list.currentItem()
            or not self.containers_list.currentItem()
        ):
            return False

        account_name = self.accounts_list.currentItem().text()
        container_name = self.containers_list.currentItem().text()

        try:
            # Get blob service client
            client = self.azure_manager.get_blob_service_client(account_name)
            if not client:
                return False

            # Create directories if they don't exist
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            # Download the blob
            blob_client = client.get_blob_client(
                container=container_name, blob=blob_name
            )

            with open(local_file_path, "wb") as download_file:
                download_stream = blob_client.download_blob()
                download_file.write(download_stream.readall())

            logging.info(f"Successfully downloaded {blob_name} to {local_file_path}")
            return True

        except Exception as e:
            logging.error(f"Failed to download {blob_name}: {e}")
            return False

    def get_selected_items_info(self):
        """Get information about selected items for download preview"""
        selected_items = self.blobs_tree.selectedItems()

        if not selected_items:
            return {"files": 0, "directories": 0, "total_size": 0}

        info = {"files": 0, "directories": 0, "total_size": 0}

        for item in selected_items:
            blob_data = item.data(0, Qt.ItemDataRole.UserRole)
            if not blob_data:
                continue

            if blob_data.get("is_directory", False):
                info["directories"] += 1
                # Note: Would need to calculate directory size by listing all files
            else:
                info["files"] += 1
                info["total_size"] += blob_data.get("size", 0)

        return info
