"""Job task class."""


class Task:
    """Task class."""

    def __init__(self, task_id, task_type, files):
        """Task constructor."""
        self.file_paths = files
        self.task_id = task_id
        self.task_type = task_type

    def get_task_id(self):
        """Return task ID."""
        return self.task_id
