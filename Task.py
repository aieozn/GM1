from utils import max_delay
from fractions import Fraction

class Task:
    
    def __init__(self, begin: int):
        self.begin = Fraction(begin)

    def __init__(self, begin: int, duration: int):
        self.begin = Fraction(begin)
        self.set_duration(duration)
        
    def __str__(self):
        return "<" \
            "BEGIN: " + str(self.begin) + ", " \
            "DURATION: " + str(self.duration) + ", " \
            "F_DEAD: " + str(self.fdl) + ", " \
            "S_DEAD:" + str(self.sdl) + ", " \
            "TIME_LEFT:" + str(self.time_left) + " >"

    def made(self):
        return self.duration - self.time_left

    def set_duration(self, duration):
        self.duration = Fraction(duration)
        self.time_left = Fraction(duration)

        self.duration =  Fraction(duration)
        self.time_left = Fraction(duration)
        self.max_delay = Fraction(max_delay(self))
        self.sdl = self.begin + self.max_delay
        self.fdl = self.begin + self.duration + max_delay(self) 
        self.processing_finished = None

    def set_max_delay(self, md):
        self.max_delay = Fraction(md)
        self.sdl = self.begin + self.max_delay
        self.fdl = self.begin + self.duration + max_delay(self) 