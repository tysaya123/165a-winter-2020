import mmap
from multiprocessing import Lock

from config import PAGE_SIZE, BUFFERPOOL_SIZE
from page import BasePage, TailPage


class BufferPool:
    def __init__(self):
        self.page_directory = {}
        self.page_rep_directory = {}
        self.pid_counter = 1

        self.memory_file_name = "memory_file.txt"
        self.mem_file = open(self.memory_file_name, "w+b")
        # When next_open_memory > BUFFERPOOL_SIZE then begin evicting
        self.next_open_page = 0

    def new_base_page(self):
        page = BasePage()
        page_rep = PageRep()

        if self.next_open_page < BUFFERPOOL_SIZE:
            pass
            #TODO vacate a page

        self.page_directory[self.pid_counter] = page
        self.page_rep_directory[self.pid_counter] = page_rep
        self.pid_counter += 1
        return self.pid_counter - 1

    def new_tail_page(self, num_cols):
        tail_page = TailPage(num_cols)
        page_rep = PageRep()

        if self.next_open_page >= BUFFERPOOL_SIZE:
            pass
            #TODO vacate a page

        self.page_directory[self.pid_counter] = tail_page
        self.page_rep_directory[self.pid_counter] = page_rep
        self.pid_counter += 1
        return self.pid_counter - 1

    def get_tail_page(self, pid, num_cols):
        page = TailPage(num_cols)
        page = self.get_page(pid, page)
        return page

    def get_base_page(self, pid):
        page = BasePage()
        page = self.get_page(pid, page)
        return page

    def get_page(self, pid, page):
        page_rep = self.page_rep_directory[pid]
        page_rep.place_pin()

        if page_rep.get_in_memory():
            return self.page_directory[pid]

        # Otherwise check if there is space in the bufferpool
        if self.next_open_page >= BUFFERPOOL_SIZE:
            pass
            # TODO vacate a page

        # Get the page from memory
        page_data = self.read_page_from_memory(page_rep)
        page.unpack(page_data)
        self.page_directory[pid] = page
        page_rep.set_in_memory(True)

        return page

    def read_page_from_memory(self, page_rep):
        offset = page_rep.get_memory_offset()
        self.mem_file.seek(offset)
        return self.mem_file.read(PAGE_SIZE)

    def vacate(self):
        pass

    def get_open_memory_location(self):
        pass


    def page_in_memory(self, pid):
        return

    def create_memory_map(self):
        return mmap.mmap(self.mem_file.fileno(), 0)

    def close_file(self):
        self.mem_file.close()


class PageRep:
    def __init__(self):
        self.in_memory = True
        self.memory_offset = -1
        self.pins = 0
        self.pin_lock = Lock()

    def set_in_memory(self, is_in_memory):
        self.in_memory = is_in_memory

    def get_in_memory(self):
        return self.in_memory

    def set_memory_offset(self, offset):
        self.memory_offset = offset

    def get_memory_offset(self):
        return self.memory_offset

    def place_pin(self):
        self.pin_lock.acquire()
        self.pins += 1
        self.pin_lock.release()

    def remove_pin(self):
        #TODO check if 0 pins already
        self.pin_lock.acquire()
        self.pins -= 1
        self.pin_lock.release()
