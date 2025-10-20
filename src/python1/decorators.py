from datetime import datetime

def decorator(func):
    def wrapper(*args, **kwargs):
        print("Start wywołania funkcji "+datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        func(*args, **kwargs)
        print("Koniec wywołania funkcji " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    return wrapper
