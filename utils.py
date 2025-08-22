def populate_signals(window):
    window.auth_btn.clicked.connect(window.authenticate)
    window.accounts_list.itemClicked.connect(window.on_account_selected)
    window.containers_loaded.connect(window.populate_containers_list)
