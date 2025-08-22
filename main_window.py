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
)
from PyQt6.QtCore import Qt, pyqtSignal, QDateTime
from PyQt6.QtGui import QFont

from log_handler import LogHandler
from managers import AzureManager
from utils import populate_signals
from workers import AuthWorker


class MainWindow(QMainWindow):
    """Main application window"""

    containers_loaded = pyqtSignal(list)
    blobs_loaded = pyqtSignal(list)

    def __init__(self):
        super().__init__()
        self.azure_manager = AzureManager()
        self.worker = None
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
        self.worker = AuthWorker(self.azure_manager)
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
