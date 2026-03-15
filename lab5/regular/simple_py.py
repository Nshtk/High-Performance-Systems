import time
import secrets

def current_milli_time():
    return round(time.time() * 1000)

def average_time(func, repeats=5):
    total = 0
    for _ in range(repeats):
        total += func()
    return total / repeats

"""### Добавление в конец списка"""

def test_append_end(n=10000):
    def run():
        a = []
        start = current_milli_time()
        for _ in range(n):
            a.append(secrets.randbelow(99) + 1)
        end = current_milli_time()
        return end - start
    avg = average_time(run)
    print(f"Добавление в конец списка для {n} элементов: {avg:.2f} мс")

"""Сложность: O(1)

### Добавление в начало списка
"""

def test_insert_start(n=10000):
    def run():
        a = []
        start = current_milli_time()
        for _ in range(n):
            a.insert(0, secrets.randbelow(99) + 1)
        end = current_milli_time()
        return end - start
    avg = average_time(run)
    print(f"Добавление в начало списка для {n} элементов: {avg:.2f} мс")

"""Сложность: O(n)

### Пузырьковая сортировка
"""

def bubble_sort(arr):
    n = len(arr)
    for i in range(n):
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]

def test_bubble_sort(n=1000):
    def run():
        arr = [(secrets.randbelow(1000) + 1) for _ in range(n)]
        start = current_milli_time()
        bubble_sort(arr)
        end = current_milli_time()
        return end - start
    avg = average_time(run, repeats=3)
    print(f"Bubble Sort для {n} элементов: {avg:.2f} мс")

"""Сложность: O(n²)

### Встроенная сортировка
"""

def test_builtin_sort(n=100000):
    def run():
        arr = [(secrets.randbelow(1000000) + 1) for _ in range(n)]
        start = current_milli_time()
        arr.sort()
        end = current_milli_time()
        return end - start
    avg = average_time(run)
    print(f"Встроенная sort() для {n} элементов: {avg:.2f} мс")

"""Сложность: O(n*log n)

### Тестирование
"""

for size in [1000, 10000, 100000, 1000000]:
    test_append_end(size)
    test_insert_start(size)

for size in [1000, 5000, 10000]:
    test_bubble_sort(size)

for size in [1000, 10000, 100000, 1000000]:
    test_builtin_sort(size)