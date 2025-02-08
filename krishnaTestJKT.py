
import time
import threading
import heapq
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

class Task:
    def __init__(self, task_id, processing_time, dependencies, priority):
        self.task_id = task_id
        self.processing_time = processing_time
        self.dependencies = set(dependencies)
        self.priority = priority 
    def __lt__(self, other):
        return self.priority < other.priority  

class TaskScheduler:
    def __init__(self, max_concurrent):
        self.max_concurrent = max_concurrent
        self.task_map = {}  
        self.dependency_graph = defaultdict(set) 
        self.ready_queue = [] 
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.completed_tasks = set()
        self.task_counter = 0 
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent)
    def add_tasks(self, tasks):
        with self.lock:
            for task_data in tasks:
                task = Task(**task_data)
                self.task_map[task.task_id] = task
                if not task.dependencies:
                    heapq.heappush(self.ready_queue, task) 
                for dep in task.dependencies:
                    self.dependency_graph[dep].add(task.task_id)
    def mark_task_complete(self, task_id):
        with self.lock:
            self.completed_tasks.add(task_id)
            self.task_counter += 1
            print(f"Tasks Completed: {self.task_counter}")
            for dependent_task_id in self.dependency_graph[task_id]:
                task = self.task_map[dependent_task_id]
                task.dependencies.remove(task_id)
                if not task.dependencies:
                    heapq.heappush(self.ready_queue, task)
            self.condition.notify_all()  
    def run_task(self, task):
        print(f"Task {task.task_id} started. (Priority: {task.priority})")
        time.sleep(task.processing_time)  
        print(f"Task {task.task_id} completed.")
        self.mark_task_complete(task.task_id)
    def run(self):
        while True:
            with self.lock:
                if len(self.completed_tasks) == len(self.task_map):
                    break  
                while len(self.ready_queue) > 0 and self.executor._threads:
                    task = heapq.heappop(self.ready_queue)
                    self.executor.submit(self.run_task, task)
                self.condition.wait()
        self.executor.shutdown(wait=True) 
tasks = [

    {"task_id": "task1", "processing_time": 2, "dependencies": [], "priority": 1},

    {"task_id": "task2", "processing_time": 1, "dependencies": ["task1"], "priority": 2},

    {"task_id": "task3", "processing_time": 3, "dependencies": ["task1"], "priority": 1},

    {"task_id": "task4", "processing_time": 1, "dependencies": ["task2", "task3"], "priority": 3},

    {"task_id": "task5", "processing_time": 2, "dependencies": ["task4"], "priority": 2},

    {"task_id": "task6", "processing_time": 2, "dependencies": ["task5"], "priority": 1},

    {"task_id": "task7", "processing_time": 1, "dependencies": ["task5"], "priority": 3},

    {"task_id": "task8", "processing_time": 2, "dependencies": ["task5"], "priority": 2},

]

scheduler = TaskScheduler(max_concurrent=2)
scheduler.add_tasks(tasks)
scheduler.run()


