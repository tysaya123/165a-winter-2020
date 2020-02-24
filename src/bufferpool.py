import mmap
from multiprocessing import Lock
from random import choice
import logging

from config import PAGE_SIZE, BUFFERPOOL_SIZE
from page import BasePage, TailPage


class BufferPool:
    def __init__(self):
        self.page_rep_directory = {}
        self.pid_counter = 1

        self.memory_file_name = "memory_file.txt"
        self.mem_file = open(self.memory_file_name, "w+b")
        # When num_open_memory > BUFFERPOOL_SIZE then begin evicting
        self.num_open_page = 0

    def new_base_page(self):
        page_rep = PageRep()
        page_rep.set_page(BasePage())
        page_rep.get_page().dirty = True

        if self.num_open_page >= BUFFERPOOL_SIZE:
            # logging.debug("not enough space")
            self.vacate()

        self.num_open_page += 1
        self.page_rep_directory[self.pid_counter] = page_rep
        self.pid_counter += 1
        return self.pid_counter - 1

    def new_tail_page(self, num_cols):
        page_rep = PageRep()
        page_rep.set_page(TailPage(num_cols))
        page_rep.get_page().dirty = True

        if self.num_open_page >= BUFFERPOOL_SIZE:
            # logging.debug("not enough space")
            self.vacate()

        self.num_open_page += 1
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
        # page_rep.place_pin()

        # logging.debug(pid)

        if page_rep.get_in_memory():
            return self.page_rep_directory[pid].get_page()

        # Otherwise check if there is space in the bufferpool
        # logging.debug(num_open_page + ':' + BUFFERPOOL_SIZE)
        if self.num_open_page >= BUFFERPOOL_SIZE:
            # logging.debug("not enough space")
            self.vacate()

        # Increment the number of pages in the bufferpool
        self.num_open_page += 1

        # Get the page from memory
        page_data = self.read_page_from_memory(page_rep)
        page.unpack(page_data)
        self.page_rep_directory[pid].set_page(page)
        page_rep.set_in_memory(True)

        return page

    def read_page_from_memory(self, page_rep):
        offset = page_rep.get_memory_offset()
        self.mem_file.seek(offset)
        return self.mem_file.read(PAGE_SIZE)

    def vacate(self):
        flushed = False
        while not flushed:
            # Choose a random pid from the page directory
            pids = list(self.page_rep_directory.keys())
            pid_to_flush = choice(pids)

            if not self.page_rep_directory[pid_to_flush].get_in_memory(): continue

            flushed = self.flush(pid_to_flush)

        self.num_open_page -= 1
        return

    def flush(self, pid):
        # TODO remove from directory
        # TODO add check for pins and return false if being used
        page_rep = self.page_rep_directory[pid]
        page = page_rep.get_page()
        if page.dirty:
            # We only need to write to disk if the page is dirty
            if page_rep.get_memory_offset() == -1:
                self.mem_file.seek(0, 2)
                # Set the offset to be the end of the file
                page_rep.set_memory_offset(self.mem_file.tell())
            else:
                offset = page_rep.get_memory_offset()
                # logging.debug("off" + str(offset))
                self.mem_file.seek(offset)

            data = page.pack()

            self.mem_file.write(data)
            self.mem_file.flush()

        # logging.debug("flushed:")
        # logging.debug(page_rep.get_page)

        page_rep.set_page(None)

        # logging.debug(page_rep.get_page)

        page_rep.set_in_memory(False)

        return True

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
        self.page = None

    def set_page(self, page):
        self.page = page

    def get_page(self):
        return self.page

    def set_in_memory(self, is_in_memory):
        self.in_memory = is_in_memory

    def get_in_memory(self):
        return self.in_memory

    def set_memory_offset(self, offset):
        self.memory_offset = offset

    def get_memory_offset(self):
        return self.memory_offset

    def place_pin(self):
        pass
        self.pin_lock.acquire()
        self.pins += 1
        self.pin_lock.release()

    def remove_pin(self):
        pass
        #TODO check if 0 pins already
        self.pin_lock.acquire()
        self.pins -= 1
        self.pin_lock.release()
