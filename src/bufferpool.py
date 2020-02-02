class BufferPool:
    def __init__(self):
        self.page_directory = {}
        self.pid_counter = 1

    def new_base_page(self):
        page = BasePage()
        page_directory[pid_counter] = page
        pid_counter += 1

    def get(self, pid):
        return page_directory[pid]
