from multiprocessing import Queue

from os import path

from config import BufferpoolCalls, BUFFERPOOL_SIZE
from page import BasePage, TailPage

# Should worry if read and all being read
class BufferPool:
    def __init__(self, queues, folder):
        self.queues = queues
        self.pool = [PageRep() for _ in range(BUFFERPOOL_SIZE)]
        self.pool_full = False
        self.mem_offsets = {}

        self.memory_file_name = "memory_file.txt"
        file_path = path.join(folder, self.memory_file_name)
        if not path.isfile(file_path):
            self.mem_file = open(file_path, "w+b")
        else:
            self.mem_file = open(file_path, "r+b")

        # When num_open_memory > BUFFERPOOL_SIZE then begin evicting
        self.num_open_page = 0

    def serveNextInQueue(self):
        # Get the next request from the queue first element should be the function to call
        for queue in self.queues_in:
            if queue.empty(): continue
            next_request = queue.get()
            if next_request[0] == BufferpoolCalls.READ_TAIL_PAGE:
                self.readTailPage(queue, next_request[1:])
            elif next_request[0] == BufferpoolCalls.READ_BASE_PAGE:
                self.readBasePage(queue, next_request[1:])
            elif next_request[0] == BufferpoolCalls.WRITE_TAIL_PAGE:
                self.writeTailPage(queue, next_request[1:])
            elif next_request[0] == BufferpoolCalls.WRITE_BASE_PAGE:
                self.writeBasePage(queue, next_request[1:])
            elif next_request[0] == BufferpoolCalls.WRITE_NEW_TAIL_PAGE:
                self.writeNewTailPage(queue, next_request[1:])
            elif next_request[0] == BufferpoolCalls.WRITE_NEW_BASE_PAGE:
                self.writeNewBasePage(queue, next_request[1:])
            elif next_request[0] == BufferpoolCalls.CLOSE_PAGE:
                self.closePage(queue, next_request[1:])

    def readTailPage(self, param):
        pass

    def readBasePage(self, param):
        pass

    def writeTailPage(self, param):
        pass

    def writeBasePage(self, param):
        pass

    def writeNewTailPage(self, param):
        page = BasePage()

        self.mem_file.seek(0, 2)
        mem_loc = self.mem_file.tell()
        # Set the offset to be the end of the file
        self.mem_offsets[param[0]] = mem_loc

        open_page_rep: PageRep = self.getOpenPool()

        open_page_rep.set_page(page)
        open_page_rep.place_write()

        queue.put(page)

    # Param[0] = pid
    def writeNewBasePage(self,queue: Queue, param):
        page = BasePage()

        self.mem_file.seek(0, 2)
        mem_loc = self.mem_file.tell()
        # Set the offset to be the end of the file
        self.mem_offsets[param[0]] = mem_loc

        open_page_rep: PageRep = self.getOpenPool()

        open_page_rep.set_page(page)
        open_page_rep.place_write()

        queue.put(page)

    def getOpenPool(self):
        if self.pool_full:
            pass
            # return = self.vacate()
        else:
            open = None
            for page_rep in self.pool:
                if page_rep.empty:
                    open = page_rep
            if open is None:
                self.pool_full = True
                # return self.vacate()

    def closePage(self, param):
        pass


class PageRep:
    def __init__(self):
        self.empty = True
        self.read_pins = 0
        self.write_pin = 0
        self.page = None

    def set_page(self, page):
        self.page = page

    def get_page(self):
        return self.page

    def place_read(self):
        self.read_pins += 1

    def place_write(self):
        self.write_pin += 1

    def remove_read(self):
        if self.read_pins == 0:
            raise ValueError('Number of pins cannot be less than 0')
        self.read_pins -= 1

    def remove_write(self):
        if self.write_pin == 0:
            raise ValueError('Number of pins cannot be less than 0')
        self.write_pin -= 1
