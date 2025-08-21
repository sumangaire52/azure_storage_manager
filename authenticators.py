import logging
import threading

from PyQt6.QtWidgets import QMessageBox

from managers import AzureManager
from workers import AuthWorker


class AzureAuthenticator:
    def __init__(self, window):
        super().__init__()
        self.window = window
        self.manager = AzureManager()
        self.worker = AuthWorker(self.manager)

    def authenticate(self):
        """Authenticate with Azure CLI"""
        self.window.auth_btn.setEnabled(False)
        self.window.auth_btn.setText("Authenticating...")

        self.worker.finished.connect(self.on_authentication_complete)

        # Run in background
        threading.Thread(target=self.worker.run, daemon=True).start()


    def on_authentication_complete(self, success):
        """Handle authentication completion"""
        if success:
            self.window.auth_status_label.setText("✓ Authenticated")
            self.window.auth_status_label.setStyleSheet("color: green")
            self.window.auth_btn.setText("Re-authenticate")
            self.window.refresh_btn.setEnabled(True)
            # self.refresh_storage_accounts()
            logging.info("Successfully authenticated with Azure")
        else:
            self.window.auth_status_label.setText("✗ Authentication Failed")
            self.window.auth_status_label.setStyleSheet("color: red")
            self.window.auth_btn.setText("Authenticate with Azure CLI")
            QMessageBox.critical(self, "Authentication Error Failed to authenticate. Please run 'az login' first.")

        self.window.auth_btn.setEnabled(True)