import time
import random
import os
from multiprocessing import Process, Queue, Value, Lock, Condition
from typing import List

class Barrier:
    def __init__(self, num_nodes: int):
        self.num_nodes = num_nodes
        self.count = Value('i', 0)
        self.lock = Lock()
        self.condition = Condition(self.lock)
        self.recovery_times = []  # List to store recovery times of all nodes

    def wait(self):
        with self.lock:
            self.count.value += 1
            if self.count.value == self.num_nodes:
                self.count.value = 0
                self.condition.notify_all()
                # All nodes have finished recovery, print recovery durations
                self.print_recovery_duration()
            else:
                self.condition.wait()

    def set_recovery_time(self, recovery_time: float):
        with self.lock:
            self.recovery_times.append(recovery_time)

    def print_recovery_duration(self):
        if self.recovery_times:
            total_duration = sum(self.recovery_times)
            avg_duration = total_duration / len(self.recovery_times)
            print(f"Total recovery duration: {total_duration:.2f} seconds")
            print(f"Average recovery duration: {avg_duration:.2f} seconds")
        else:
            print("No recovery times recorded.")

class Node:
    def __init__(self, id, to_parent: Queue, num_nodes: int, failure_signal: List[Queue], barrier: Barrier) -> None:
        self.id = id
        self.neighbors = []
        self.to_parent = to_parent
        self.proc = None
        self.last_checkpoint_time = time.time()
        self.last_handled_message = None
        self.log_file = f"node_{self.id}_log.txt"  # Log file to follow process and its events
        self.state_file = f"node_{self.id}_state.txt"   # State file to follow process and its current state
        self.failed = False
        self.time_vector = [0] * num_nodes
        self.fail_vector = [0] * num_nodes
        self.num_nodes = num_nodes
        self.event_log = []  # In-memory log of events since last checkpoint
        self.failure_signal = failure_signal  # Queues to broadcast failure signal
        self.save_interval = 10  # State save interval (in seconds)
        self.last_save_time = time.time()
        self.recovery_mode = False  # Flag to control logging during recovery
        self.barrier = barrier

        # Timing attributes
        self.failure_start_time = None
        self.recovery_end_time = None

    def connect(self, other):  # nodes connect to each other
        to_other = Queue()
        from_other = Queue()
        self.add_neighbor(other.id, to_other, from_other)
        other.add_neighbor(self.id, from_other, to_other)

    def add_neighbor(self, neighbor_id, to_other: Queue, from_other: Queue):  # nodes register their connections
        self.neighbors.append((neighbor_id, to_other, from_other))

    def send(self, neighbor_id, message):  # sending a message
        if self.failed:
            return  # If node failed, it cannot send messages
        for neighbor in self.neighbors:
            if neighbor[0] == neighbor_id:
                self.time_vector[self.id] += 1
                message['time_vector'] = self.time_vector[:]  # Include time vector in the message
                message['fail_vector'] = self.fail_vector[:]  # Include fail vector in the message
                self.log_event('send', neighbor_id, message)  # Log the send event
                neighbor[1].put((self.id, message))
                self.last_handled_message = message
                break

    def receive(self):  # receiving a message
        if self.failed:
            return  # If node failed, it cannot receive messages

        for neighbor in self.neighbors:
            if not neighbor[2].empty():
                received_data = neighbor[2].get()
                if isinstance(received_data, tuple) and len(received_data) == 2:
                    sender_id, message = received_data
                    self.time_vector[self.id] += 1
                    self.time_vector = [max(self.time_vector[i], message['time_vector'][i]) for i in range(self.num_nodes)]
                    self.fail_vector = [max(self.fail_vector[i], message['fail_vector'][i]) for i in range(self.num_nodes)]
                    self.log_event('receive', sender_id, message)  # Log the receive event
                    self.last_handled_message = message
                elif isinstance(received_data, str) and received_data == "rollback":
                    if not self.recovery_mode:  # Only process rollback if not in recovery mode
                        self.enter_recovery_mode()

    def enter_recovery_mode(self):
        """Node enters recovery mode, performing rollback and re-executing events."""
        if self.recovery_mode:
            return  # Do nothing if already in recovery mode
        
        self.recovery_mode = True  # Enter recovery mode
        self.rollback()
        self.reexecute_events()
        self.recovery_mode = False  # Exit recovery mode
        
        # Update recovery end time
        self.recovery_end_time = time.time()
        
        # Record recovery duration in the barrier
        if self.failure_start_time:
            recovery_duration = self.recovery_end_time - self.failure_start_time
            self.barrier.set_recovery_time(recovery_duration)
        
        # Signal the barrier that this node has completed recovery
        self.barrier.wait()

    def log_event(self, event_type, neighbor_id, message):
        if self.recovery_mode:
            return  # Don't log events during recovery mode
        timestamp = time.time()
        event_entry = (event_type, neighbor_id, message, timestamp)
        self.event_log.append(event_entry)
        with open(self.log_file, 'a') as f:
            f.write(str(event_entry) + '\n')  # Persist the event log

    def save_state(self):  # save the state of the node
        with open(self.state_file, 'w') as f:
            f.write(str(self.time_vector) + '\n')
            f.write(str(self.fail_vector) + '\n')
            f.write(str(self.last_checkpoint_time) + '\n')
            f.write(str(self.event_log) + '\n')  # Save event log
        self.event_log.clear()  # Clear in-memory event log after saving
        with open(self.log_file, 'w') as f:
            f.write('')  # Clear the persistent log file after saving

    def rollback(self):  # rollback to the last checkpoint
        if not os.path.exists(self.state_file):
            return
        with open(self.state_file, 'r') as f:
            lines = f.readlines()
            self.time_vector = eval(lines[0])
            self.fail_vector = eval(lines[1])
            self.last_checkpoint_time = eval(lines[2])
            self.event_log = eval(lines[3])  # Load event log since last checkpoint

    def reexecute_events(self):
        """Re-execute all send/receive events from the in-memory event log."""
        for event_entry in self.event_log:
            event_type, neighbor_id, message, timestamp = event_entry
            if timestamp > self.last_checkpoint_time:  # Only re-execute events after the last checkpoint
                if event_type == 'send':
                    self.send(neighbor_id, message)
                elif event_type == 'receive':
                    self.receive()  # Re-execute receive

    def simulate_failure(self):
        """Simulates a failure in the node and triggers rollback for all nodes."""
        if self.recovery_mode:
            return  # Prevent failure during recovery
        self.failed = True
        self.fail_vector[self.id] += 1
        #self.save_state()
        
        # Update failure start time
        self.failure_start_time = time.time()
        
        # Broadcast failure signal to all connected neighbors
        for neighbor in self.neighbors:
            _, to_neighbor, _ = neighbor
            to_neighbor.put("rollback")

    def recover(self):
        """Simulates recovery from failure."""
        self.failed = False
        self.enter_recovery_mode()

    def run(self):  # main process run function
        while True:
            # Check for failure signal from other nodes more frequently
            for signal_queue in self.failure_signal:
                while not signal_queue.empty():
                    signal = signal_queue.get()
                    if signal == 'rollback':
                        if not self.recovery_mode:  # Only process rollback if not in recovery mode
                            self.enter_recovery_mode()

            # Simulate random failure
            if not self.failed and random.random() < 0.01:  # 1% chance of failure each iteration
                self.simulate_failure()
                time.sleep(1)  # Simulate downtime during failure
                self.recover()

            # Process any incoming messages
            self.receive()

            # Simulate sending messages with a random number to connected neighbors
            if not self.failed and random.random() < 0.2: # 20% chance of sending message
                for neighbor in self.neighbors:
                    random_number = random.randint(1, 100)  # Generate a random number between 1 and 100
                    self.send(neighbor[0], {'data': f"Random number: {random_number}"})
        
            # Periodically save the state
            current_time = time.time()
            if current_time - self.last_save_time >= self.save_interval:
                self.last_save_time = current_time
                self.save_state()

            time.sleep(0.4)  # Reduced sleep time to check the queue more frequently


def print_recovery_duration(nodes: List[Node]):
    """Print the duration between failure and recovery for all nodes."""
    recovery_times = []
    for node in nodes:
        if node.failure_start_time and node.recovery_end_time:
            duration = node.recovery_end_time - node.failure_start_time
            recovery_times.append(duration)
    
    if recovery_times:
        total_duration = sum(recovery_times)
        avg_duration = total_duration / len(recovery_times)
        print(f"Total recovery duration: {total_duration:.2f} seconds")
        print(f"Average recovery duration: {avg_duration:.2f} seconds")

if __name__ == "__main__":
    num_nodes = 6
    failure_signal = [Queue() for _ in range(num_nodes)]  # Create a queue for each node
    barrier = Barrier(num_nodes)  # Create the barrier for synchronisation
    nodes = [Node(i, None, num_nodes, failure_signal, barrier) for i in range(num_nodes)]

    # Connect each node to every other node
    for i in range(num_nodes):
        for j in range(num_nodes):
            if i != j:  # Ensure a node does not connect to itself
                nodes[i].connect(nodes[j])

    processes = []
    for node in nodes:
        p = Process(target=node.run)
        p.start()
        node.proc = p
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
