from page import BasePage, TailPage

class BufferPool:
    def __init__(self):
        self.page_directory = {}
        self.pid_counter = 1

    def new_base_page(self):
        page = BasePage()
        self.page_directory[self.pid_counter] = page
        self.pid_counter += 1
        return self.pid_counter - 1

    def get(self, pid):
        return self.page_directory[pid]
