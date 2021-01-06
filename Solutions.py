import Queue
import PS
import importlib


importlib.reload(Queue)

class Solutions:

    def __init__(self, solution_name, avg_task_size, DTmax):

        if solution_name == "QUEUE":
            self.engine = Queue.Queue(avg_task_size, DTmax)
        elif solution_name == "PS":
            self.engine = PS.PS(avg_task_size, DTmax)
        else:
            raise Exception("Nieznane sposób rozwiązywania problemu")

    def process(self, task):
        self.engine.process(task)

    def finish(self):
        self.engine.finish()