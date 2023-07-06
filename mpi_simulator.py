import typing
import time
import random
from multiprocessing import Process, Queue
import csv


number_of_processes_to_simulate = 4

MPI_ANY_SOURCE = -1

class FindMin:
    def __init__(self, nums):
        self.nums = nums
        self.type = 'min_task'
    def execute(self):
        min_number = None
        for n in self.nums:
            if min_number is None or n < min_number:
                min_number = n
        print(f"The smallest number in {self.nums} is {min_number}")
        return min_number

class FindMax:
    def __init__(self, nums):
        self.nums = nums
        self.type = 'max_task'

    def execute(self):
        max_number = None
        for n in self.nums:
            if max_number is None or n > max_number:
                max_number = n
        print(f"The largest number in {self.nums} is {max_number}")
        return max_number

class FindMed:
    def __init__(self, nums):
        self.nums = nums
        self.type = 'med_task'

    def execute(self):
        test_list = self.nums
        test_list.sort()
        mid = len(test_list) // 2
        res = (test_list[mid] + test_list[~mid]) / 2
        print(f"The median of {self.nums} is {res}")
        return res

class FindMean:
    def __init__(self, nums):
        self.nums = nums
        self.type = 'mean_task'

    def execute(self):
        sum = 0
        num_len = len(self.nums)
        for n in self.nums:
            sum += n
        mean = sum/num_len
        print(f"The mean of {self.nums} is {mean}")
        return mean

def mpi_application(
        rank:int,
        size:int,
        send_f:typing.Callable[[typing.Any,int],None],
        recv_f:typing.Callable[[int], typing.Any]
    ):
    # NOTE for the assignment you can not specify the reception of a message
    # from a single source, you only need to receive from any source using:

    # data = recv_f(MPI_ANY_SOURCE)


    # NOTE to send a message/data from a process to process with rank 2, you
    # use:

    # send_f(data,2)


    # NOTE ensure the coordinator sends a message to inform each process to
    # end and the coordinator should end as well.  If the application
    # does not end, you likely messed this up.

    # TODO implement your MPI application logic here using the parameters above
    # instead of mpi4py

    if rank == 0:
        # TODO implement coordinator logic
        randomlist = []
        for _ in range(0,10):
            n = random.randint(1,30)
            randomlist.append(n)
        print(randomlist)
        tasks = [FindMin,FindMax,FindMed,FindMean]
        task_1 = random.choice(tasks)
        task_2 = random.choice(tasks)
        task_3 = random.choice(tasks)
        send_f(task_1(randomlist),dest=1)
        send_f(task_2(randomlist), dest=2)
        send_f(task_3(randomlist), dest=3)


        
        for _ in range(1, size):
            print(recv_f(MPI_ANY_SOURCE))
    else:
        # TODO implement worker logic
        task = recv_f(MPI_ANY_SOURCE)
        task_name = task.type
        print(f"{task_name} received by {rank}")
        val = task.execute()
        row = [task_name,val]
        with open('values.csv','a') as fd:
            writer = csv.writer(fd)
            writer.writerow(row)
        send_f(f"Reply: I, process of rank {rank}, received {task_name}", dest=0)


###############################################################################
# This is the simulator code, do not adjust

def _run_app(process_rank, size, app_f, send_queues):
    send_f = _generate_send_f(process_rank, send_queues)
    recv_f = _generate_recv_f(process_rank, send_queues)
    
    app_f(process_rank, size, send_f, recv_f)

def _generate_recv_f(process_rank, send_queues):

    def recv_f(from_source:int):
        while send_queues[process_rank].empty():
            time.sleep(1)
        return send_queues[process_rank].get()[1]
    return recv_f


def _generate_send_f(process_rank, send_queues):

    def send_F(data, dest):
        send_queues[dest].put((process_rank,data))
    return send_F


def _simulate_mpi(n:int, app_f):
    
    send_queues = {}

    for process_rank in range(n):
        send_queues[process_rank] = Queue()
    
    ps = []
    for process_rank in range(n):
        
        p = Process(
            target=_run_app,
            args=(
                process_rank,
                n,
                app_f,
                send_queues
            )
        )
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
###############################################################################


if __name__ == "__main__":
    _simulate_mpi(number_of_processes_to_simulate, mpi_application)
