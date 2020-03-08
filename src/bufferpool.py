# TODO: Remember to remove all checks, asserts, etc. for optimization
# TODO: Remember to remove all calls to check_all_pins()
import logging
import mmap
import pickle

from multiprocessing import Lock
from random import choice
from os import path

from config import PAGE_SIZE, BUFFERPOOL_SIZE
from page import BasePage, TailPage

class BufferPool:
    def __init__(self, folder):
        self.vacate_count = 0
        self.global_lock = Lock()
        self.buff_lock = Lock()


        self.page_rep_directory = {}

        # Holds the pids for those pages that are in memory.
        self.page_pid_in_mem = set()

        self.pid_counter = 1

        self.memory_file_name = "memory_file.txt"
        file_path = path.join(folder, self.memory_file_name)
        if not path.isfile(file_path):
            self.mem_file = open(file_path, "w+b")
        else:
            self.mem_file = open(file_path, "r+b")
        # When num_open_memory > BUFFERPOOL_SIZE then begin evicting
        self.num_open_page = 0

    def new_base_page(self):
        self.global_lock.acquire()

        page_rep = PageRep()
        page_rep.set_page(BasePage())
        page_rep.get_page().dirty = True

        if self.num_open_page >= BUFFERPOOL_SIZE:
            # logging.debug("not enough space")
            # self.buff_lock.acquire()
            self.vacate()
            # self.buff_lock.release()


        self.num_open_page += 1
        self.page_rep_directory[self.pid_counter] = page_rep
        self.page_pid_in_mem.add(self.pid_counter)
        self.pid_counter += 1

        self.global_lock.release()

        return self.pid_counter - 1

    def new_tail_page(self, num_cols):
        self.global_lock.acquire()

        if num_cols <= 0:
            raise ValueError('Number of columns cannot be <= 0')

        page_rep = PageRep()
        page_rep.set_page(TailPage(num_cols))
        page_rep.get_page().dirty = True

        if self.num_open_page >= BUFFERPOOL_SIZE:
            # logging.debug("not enough space")
            # self.buff_lock.acquire()
            self.vacate()
            # self.buff_lock.release()


        self.num_open_page += 1
        self.page_rep_directory[self.pid_counter] = page_rep
        self.page_pid_in_mem.add(self.pid_counter)
        self.pid_counter += 1

        self.global_lock.release()

        return self.pid_counter - 1

    def get_tail_page(self, pid, num_cols):
        page = self.get_page(pid, False, num_cols)
        return page

    def get_base_page(self, pid):
        page = self.get_page(pid, True, None)
        return page

    def get_page(self, pid, isBase, num_cols):
        # TODO optimize by removed init calls above just pass a bool

        self.global_lock.acquire()

        # Removed
        #self.buff_lock.acquire()
        page_rep = self.page_rep_directory[pid]
        # Removed
        #self.buff_lock.release()

        page_rep.place_pin()
        if page_rep.get_in_memory():
            self.global_lock.release()
            return self.page_rep_directory[pid].get_page()

        # Otherwise check if there is space in the bufferpool
        # logging.debug(num_open_page + ':' + BUFFERPOOL_SIZE)
        if self.num_open_page >= BUFFERPOOL_SIZE:
            # logging.debug("not enough space")
            self.vacate()

        # self.buff_lock.release()
        # Increment the number of pages in the bufferpool
        self.num_open_page += 1

        # Get the page from memory
        if isBase: page = BasePage()
        else: page = TailPage(num_cols)
        page_data = self.read_page_from_memory(page_rep)
        page.unpack(page_data)
        self.page_rep_directory[pid].set_page(page)
        page_rep.set_in_memory(True)

        page = self.page_rep_directory[pid].get_page()
        self.page_pid_in_mem.add(pid)

        self.global_lock.release()

        return page

    def close_page(self, pid):
        self.global_lock.acquire()

        # self.buff_lock.acquire()
        if self.page_rep_directory[pid] is None:
            raise KeyError('Page with pid {} does not exist'.format(pid))
        self.page_rep_directory[pid].remove_pin()
        # self.buff_lock.release()

        self.global_lock.release()

    def read_page_from_memory(self, page_rep):
        offset = page_rep.get_memory_offset()
        self.mem_file.seek(offset)
        return self.mem_file.read(PAGE_SIZE)

    def vacate(self):
        # num_to_vacate = self.vacate_count + 1
        flushed = False
        while not flushed:
            #print(num_to_vacate, self.vacate_count)
            # Choose a random pid from the page directory

            # Removed
            #self.buff_lock.acquire()

            pid_to_flush = choice(tuple(self.page_pid_in_mem))

            page_rep = self.page_rep_directory[pid_to_flush]
            # Removed
            #self.buff_lock.release()

            # page_rep.pin_lock.acquire()
            # page_rep.pin_lock.release()
            if page_rep.pins > 0: continue

            flushed = self.flush(pid_to_flush)
            if flushed:
                self.vacate_count += 1
                self.num_open_page -= 1

    def flush_all(self):
        self.global_lock.acquire()

        for pid in self.page_rep_directory:
            page_rep = self.page_rep_directory[pid]
            if page_rep.get_in_memory():
                self.flush(pid)

        self.global_lock.release()

    def flush(self, pid):
        # TODO remove from directory
        # TODO add check for pins and return false if being used
        # Removed
        #self.buff_lock.acquire()
        page_rep = self.page_rep_directory[pid]
        self.page_pid_in_mem.remove(pid)
        # page_rep.pin_lock.acquire()

        if page_rep.pins > 0:
            raise ValueError('Cannot flush a page that is pinned')

        # TODO should change get to not add a pin
        page = page_rep.page
        if page is None:
            raise TypeError("Expected page in flush but got none")
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

        # Removed
        #self.buff_lock.release()
        # logging.debug("flushed:")
        # logging.debug(page_rep.get_page)

        page_rep.set_page(None)

        # logging.debug(page_rep.get_page)

        page_rep.set_in_memory(False)
        # page_rep.pin_lock.release()

        return True

    def close_file(self):
        self.mem_file.close()

    def dump(self):
        self.global_lock.acquire()

        self.close_file()
        #for pid, page_rep in self.page_rep_directory.items():
        #    # page_rep.pin_lock.acquire()
        #    pins = page_rep.pins
        #    # page_rep.pin_lock.release()
        #    if page_rep.pins != 0:
        #        raise ValueError("Page Rep had non-zero pin count while dumping")
        #    #page_rep.pin_lock = None

        data = [self.page_rep_directory, self.pid_counter]
        pickle_data = pickle.dumps(data)

        #self.initialize_locks()

        self.global_lock.release()

        return pickle_data

    def initialize_locks(self):
        for _, page_rep in self.page_rep_directory.items():
            pass
            #page_rep.pin_lock = Lock()

    def load(self, data):
        self.global_lock.acquire()

        [self.page_rep_directory, self.pid_counter] = pickle.loads(data)

        self.global_lock.release()
        #self.initialize_locks()

    def check_all_pins(self):
        for pid, page_rep in self.page_rep_directory.items():
            if page_rep.pins != 0:
                raise ValueError('Number of pins on page should be 0')

    def get_num_pins(self):
        total = 0
        for page_rep in list(self.page_rep_directory.values()):
            total += page_rep.get_num_pins()


class PageRep:
    def __init__(self):
        self.in_memory = True
        self.memory_offset = -1
        self.pins = 0
        # self.pin_lock = Lock()
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
        # self.pin_lock.acquire()
        self.pins += 1
        # self.pin_lock.release()

    def remove_pin(self):
        if self.pins == 0:
            raise ValueError('Number of pins cannot be less than 0')
        # self.pin_lock.acquire()
        self.pins -= 1
        # self.pin_lock.release()
