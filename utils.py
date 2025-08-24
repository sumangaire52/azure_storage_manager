SIZE_NAMES = ["B", "KB", "MB", "GB", "TB"]


def populate_signals(window):
    # Authentication
    window.auth_btn.clicked.connect(window.authenticate)
    window.accounts_list.itemClicked.connect(window.on_account_selected)

    # Containers and blobs
    window.containers_loaded.connect(window.populate_containers_list)
    window.containers_list.itemClicked.connect(window.on_container_selected)
    window.blobs_loaded.connect(window.populate_blobs_tree)
    window.blobs_tree.itemExpanded.connect(window.on_directory_expanded)
    window.directory_contents_loaded.connect(window.on_directory_contents_loaded)
    window.new_transfer_btn.clicked.connect(window.create_new_transfer)

    # Files and folder download/transfer
    window.download_btn.clicked.connect(window.download_selected_items)

    # Files and folder upload
    window.upload_files_btn.clicked.connect(window.upload_files)
    window.upload_folder_btn.clicked.connect(window.upload_folder)
    window.upload_completed.connect(window.on_upload_completed)
    window.file_uploaded.connect(window.on_file_uploaded)

    # Files and folder delete
    window.delete_btn.clicked.connect(window.delete_selected_items)
    window.delete_completed.connect(window.on_delete_completed)
    window.item_deleted.connect(window.on_item_deleted)

    # Logging
    window.clear_logs_btn.clicked.connect(window.clear_logs)
    window.export_logs_btn.clicked.connect(window.export_logs)


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


def format_time(seconds):
    """Format seconds to human-readable time"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
