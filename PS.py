import logging
import random
import importlib

import numpy as np

from math import ceil
from fractions import Fraction

class PS:

    def __init__(self, avg_task_size, DT_max):
        logging.info("Utworzenie kolejki")
        self.time = Fraction(0, 1)
        self.previous_time = Fraction(0, 1)
        self.avg_task_size = avg_task_size
        self.PQPS = []
        self.PQ = []

        self.new_task_event = False # Informuje, że pojawiło się nowe zadnie - można losować dostęp do ostatniego miejsca kolejki BQPS

        self.PQPS_active = True

        self.next_event = None

    def finish(self):
        while self.next_event:
            self.time = self.next_event
            sleep_duration = self.time - self.previous_time
            self.update_queues(sleep_duration)
            self.previous_time = self.time
        
        logging.info("Zakończenie wszystkich zadań")

    def process(self, task):

        if task.begin < self.time:
            raise Exception("Opóźnione zadanie")

        logging.info(" " + str(self.time) + ":\tNew task arived: " + str(task))
        i = 0

        while self.next_event and self.next_event <= task.begin:
            self.time = self.next_event
            sleep_duration = self.time - self.previous_time
            self.update_queues(sleep_duration)
            self.previous_time = self.time


        self.new_task_event = True
        self.time = Fraction(task.begin, 1)

        sleep_duration = self.time - self.previous_time

        logging.info(" " + str(self.time) + ":\tRozpoczynanie przetwarzania nowego zadania: " + str(task))


        logging.info(" " + str(self.time) + ':\tDodanie zadania do kolejki PQ: ' + str(task))
        self.PQ.append(task)

        self.update_queues(sleep_duration)
        self.previous_time = self.time

    def update_queues(self, period: Fraction):
        # Proces musi zostać uruchomiony po każdej zmianie w systemie!!
        # POMNIEJSZENIE CZASÓW ZADAŃ, PRZENOSZENIE ZADAŃ POMIĘDZY KOLEJKAMI I USUNIĘCIE ZAKOŃCZONYCH ZADAŃ Z KOLEJEK
        # Zwraca czas zakończenia kolejnego zadania lub null, jeżeli zadania nie są przetwarzane



        self.PQPS_active = True

        for task in self.PQPS:
            task.time_left -= period /  len(self.PQPS)

        # Usunięcie zadań zakończonych
        finished = [t for t in self.PQPS if t.time_left <= 0]
        for f in finished:
            f.processing_finished = self.time
            logging.info(" " + str(self.time) + ':\tZakończenie zadania z kolejki PQ: ' + str(f))

        self.PQPS = [t for t in self.PQPS if t.time_left > 0]

        self.PQPS.extend(self.PQ)
        self.PQ = []

        if len(self.PQPS) > 0:
            min_break = self.PQPS[0].time_left * len(self.PQPS)

            for t in self.PQPS:
                if (t.time_left * len(self.PQPS)) < min_break:
                    min_break = t.time_left * len(self.PQPS)

            logging.info(" " + str(self.time) + ':\tNajbliższe wydarzenie w kolejce PQPS : ' + str(self.time + min_break))
            self.next_event = self.time + min_break
        else:

            logging.info(" " + str(self.time) + ':\tOczekiwanie na nadchodzące zadania')
            self.next_event = None