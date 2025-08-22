SIZE_NAMES = ["B", "KB", "MB", "GB", "TB"]


def populate_signals(window):
    window.auth_btn.clicked.connect(window.authenticate)
    window.accounts_list.itemClicked.connect(window.on_account_selected)
    window.containers_loaded.connect(window.populate_containers_list)
    window.containers_list.itemClicked.connect(window.on_container_selected)
    window.blobs_loaded.connect(window.populate_blobs_tree)


def format_size(size_bytes):
    """Format file size in human-readable format"""
    if size_bytes == 0:
        return "0 B"

    i = 0
    size = float(size_bytes)

    while size >= 1024.0 and i < len(SIZE_NAMES) - 1:
        size /= 1024.0
        i += 1

    return f"{size:.1f} {SIZE_NAMES[i]}"
