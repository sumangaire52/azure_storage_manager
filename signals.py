class Signals:
    def __init__(self, window):
        window.auth_btn.clicked.connect(window.authenticate)
        # window.refresh_btn.clicked.connect(window.refresh_storage_accounts)
