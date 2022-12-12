import time

timer_on = False

class MyTimer():

    def __init__(self, caption):
        self.start = time.time()
        self.caption = caption

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if timer_on:
            msg = self.caption + ': ' + str(round((time.time() - self.start)*1000)) + ' ms'
            print(msg)