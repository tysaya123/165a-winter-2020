from multiprocessing import Queue

from os import path

from config import BufferPoolCalls, BUFFERPOOL_SIZE, PAGE_SIZE
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
        for queue in self.queues:
            try:
                next_request = queue.get()
            except:
                continue
            # TODO: Optimize by which happens most freq
            if next_request[0] == BufferPoolCalls.READ_TAIL_PAGE:
                self.readTailPage(queue, next_request[1:])
            elif next_request[0] == BufferPoolCalls.READ_BASE_PAGE:
                self.readBasePage(queue, next_request[1:])
            elif next_request[0] == BufferPoolCalls.WRITE_TAIL_PAGE:
                self.writeTailPage(queue, next_request[1:])
            elif next_request[0] == BufferPoolCalls.WRITE_BASE_PAGE:
                self.writeBasePage(queue, next_request[1:])
            elif next_request[0] == BufferPoolCalls.WRITE_NEW_TAIL_PAGE:
                self.writeNewTailPage(queue, next_request[1:])
            elif next_request[0] == BufferPoolCalls.WRITE_NEW_BASE_PAGE:
                self.writeNewBasePage(queue, next_request[1:])
            elif next_request[0] == BufferPoolCalls.CLOSE_PAGE:
                self.closePage(next_request[1:])

    def readTailPage(self, param):
        pass

    def readBasePage(self, param):
        pass

    # Param[0] = pid, param[1] = num_cols
    def writeTailPage(self, queue, param):
        # Will place the page in the queue if it exists(or will place False if its being used) else will return false
        if self.returnPageInPool(param): return
        # If the page was not already in the bufferpool find a space for it
        page_data = self.getPageData(param)
        page = TailPage(param[1])
        page.unpack(page_data)

        # Find a open space for the page and set its data
        open_page_rep: PageRep = self.getOpenPool()
        open_page_rep.set_page(page)
        open_page_rep.set_rid(param[0])
        open_page_rep.place_write()

        queue.put(page)

    # Param[0] = pid
    def writeBasePage(self, queue: Queue, param):
        # Will place the page in the queue if it exists(or will place False if its being used) else will return false
        if self.returnPageInPool(param): return
        # If the page was not already in the bufferpool find a space for it

        page_data = self.getPageData(param)
        page = BasePage()
        page.unpack(page_data)

        # Find a open space for the page and set its data
        open_page_rep: PageRep = self.getOpenPool()
        open_page_rep.set_page(page)
        open_page_rep.set_rid(param[0])
        open_page_rep.place_write()

        queue.put(page)

    def getPageData(self, param):
        page_offset = self.mem_offsets[param[0]]
        self.mem_file.seek(page_offset)
        page_data = self.mem_file.read(PAGE_SIZE)
        return page_data

    def returnPageInPool(self, param):
        page_rep = self.getRidInPool(param[0])
        if page_rep is not None:
            # If someone else is already reading then return false otherwise return the page
            if page_rep.get_read() > 0 or page_rep.get_write() > 0:
                queue.put(False)
            queue.put(page_rep.get_page())
            return True
        return False

    # Param[0] = pid, param[1] = num_cols
    def writeNewTailPage(self, queue: Queue, param):
        page = TailPage(param[1])

        self.writeNewPage(page, param, queue)

    # Param[0] = pid
    def writeNewBasePage(self, queue: Queue, param):
        page = BasePage()

        self.writeNewPage(page, param, queue)

    def writeNewPage(self, page, param, queue):
        self.mem_file.seek(0, 2)
        mem_loc = self.mem_file.tell()
        # Write to save space for the page
        self.mem_file.write(page.pack())
        # Set the offset to be the end of the file
        self.mem_offsets[param[0]] = mem_loc
        open_page_rep: PageRep = self.getOpenPool()
        open_page_rep.set_page(page)
        open_page_rep.set_rid(param[0])
        open_page_rep.place_write()
        queue.put(page)

    # Will find a open location in the bufferpool and will vacate if necessary
    def getOpenPool(self):
        if self.pool_full:
            pass
            # return = self.vacate()
        else:
            open = None
            for page_rep in self.pool:
                if page_rep.page is None:
                    return page_rep
            if open is None:
                self.pool_full = True
                # return self.vacate()

    # Will remove a write pin if it exists and a read pin if there is no write pin
    # Param[0] = pid
    def closePage(self, param):
        page_rep = self.getRidInPool(param[0])
        if page_rep is None:
            raise KeyError("Could not find Page in Pool with Rid")
        if page_rep.get_write() == 1:
            page_rep.remove_write()
            if page_rep.get_read() > 0:
                raise ValueError("Had read pins as write pin was being removed")
        else:
            page_rep.remove_read()
            if page_rep.get_read() < 0:
                raise ValueError("Cannot have less than 0 read pins")

    # Will get a page_rep from the bufferpool with the Rid and will return none if it is not in the bufferpool
    def getRidInPool(self, rid):
        for page_rep in self.pool:
            if page_rep.get_rid() == rid:
                return page_rep
        return None


class PageRep:
    def __init__(self):
        self.read_pins = 0
        self.write_pin = 0
        self.page = None
        self.rid = None

    def set_page(self, page):
        self.page = page

    def set_rid(self, rid):
        self.rid = rid

    def get_rid(self):
        return self.rid

    def get_page(self):
        return self.page

    def get_write(self):
        return self.write_pin

    def get_read(self):
        return self.read_pins

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
