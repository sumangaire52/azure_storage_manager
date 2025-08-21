import logging
from PyQt6.QtCore import QObject, pyqtSignal

class LogHandler(logging.Handler, QObject):
    """Custom log handler to emit signals for UI updates"""

    log_message = pyqtSignal(str)

    def __init__(self):
        logging.Handler.__init__(self)
        QObject.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        self.log_message.emit(msg)