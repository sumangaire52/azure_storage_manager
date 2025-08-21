from PyQt6.QtCore import QObject, pyqtSignal


class AuthWorker(QObject):
    finished = pyqtSignal(bool)

    def __init__(self, azure_manager):
        super().__init__()
        self.azure_manager = azure_manager

    def run(self):
        success = self.azure_manager.authenticate()
        self.finished.emit(success)