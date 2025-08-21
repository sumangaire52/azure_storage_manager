import json
import logging
from datetime import datetime
from pathlib import Path

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTreeWidget,QTableWidget,
    QPushButton, QLabel, QComboBox, QTextEdit,
    QSplitter, QGroupBox, QSpinBox, QFormLayout,
    QListWidget, QDateTimeEdit, QFrame
)
from PyQt6.QtCore import Qt, pyqtSignal, QDateTime
from PyQt6.QtGui import QFont

from log_handler import LogHandler

try:
    from apscheduler.schedulers.qt import QtScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.date import DateTrigger
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False
    print("APScheduler not available. Install with: pip install apscheduler")

class MainWindow(QMainWindow):
    """Main application window"""
    containers_loaded = pyqtSignal(list)
    blobs_loaded = pyqtSignal(list)

    def __init__(self):
        super().__init__()
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
        """Setup the main user interface"""
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
        self.transfers_table.setHorizontalHeaderLabels([
            "Job ID", "Source", "Destination", "Progress", "Speed", "ETA", "Status", "Created"
        ])
        self.transfers_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

        layout.addLayout(toolbar)
        layout.addWidget(self.transfers_table)

        transfers_widget.setLayout(layout)
        self.tab_widget.addTab(transfers_widget, "Transfer Jobs")

    def setup_scheduler_tab(self):
        """Setup scheduler tab for batch operations"""
        scheduler_widget = QWidget()
        layout = QVBoxLayout()

        if not SCHEDULER_AVAILABLE:
            layout.addWidget(QLabel("Scheduler not available. Install APScheduler to enable scheduling features."))
            scheduler_widget.setLayout(layout)
            self.tab_widget.addTab(scheduler_widget, "Scheduler")
            return

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
        self.scheduled_jobs_table.setHorizontalHeaderLabels([
            "Job ID", "Type", "Next Run", "Status", "Actions"
        ])

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
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
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
                if 'window_geometry' in settings:
                    self.restoreGeometry(bytes.fromhex(settings['window_geometry']))

            except Exception as e:
                logging.warning(f"Failed to load settings: {e}")

    def save_settings(self):
        """Save application settings"""
        settings_dir = Path.home() / ".azure_storage_manager"
        settings_dir.mkdir(parents=True, exist_ok=True)

        settings = {
            'window_geometry': self.saveGeometry().toHex().data().decode(),
            'last_used_accounts': [self.accounts_list.item(i).text()
                                   for i in range(self.accounts_list.count())]
        }

        try:
            with open(settings_dir / "settings.json", 'w') as f:
                json.dump(settings, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save settings: {e}")