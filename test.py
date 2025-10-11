import threading
import time

counter = 0
lost_updates = 0
successful_updates = 0
lock_for_tracking = threading.Lock()  # Lock for our tracking variables

def increment_unsafe():
    global counter, lost_updates, successful_updates
    for _ in range(1000):
        # Read
        old_value = counter
        
        # Simulate some processing
        time.sleep(0.00001)
        
        # Check if another thread changed it while we were processing
        # (We need to check BEFORE we write)
        with lock_for_tracking:
            if counter != old_value:
                lost_updates += 1  # Someone else changed it!
            else:
                successful_updates += 1  # We're the first to update
        
        # Write (still unsafe for counter itself)
        counter = old_value + 1

threads = [threading.Thread(target=increment_unsafe) for _ in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print(f"Counter: {counter}")
print(f"Expected: {10 * 1000}")
print(f"Successful updates: {successful_updates}")
print(f"Lost updates: {lost_updates}")
print(f"Total operations: {successful_updates + lost_updates}")