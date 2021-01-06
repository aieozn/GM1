import logging
import random
import importlib

import numpy as np

from math import ceil
from fractions import Fraction

class Queue:

    def __init__(self, avg_task_size, DT_max):
        logging.info("Utworzenie kolejki")

        self.BQ = []
        self.time = Fraction(0, 1)
        self.previous_time = Fraction(0, 1)
        self.avg_task_size = avg_task_size

        self.BQPS = []
        self.PQPS = []

        self.BQPS_size = int(DT_max / avg_task_size) + 1
        self.BQPS_chance = (DT_max % avg_task_size) / (avg_task_size * 1.0)
        
        if self.BQPS_size == 1:
            logging.error('BQPS_size ma bardzo niską wartość (1)!')

        self.new_task_event = False # Informuje, że pojawiło się nowe zadnie - można losować dostęp do ostatniego miejsca kolejki BQPS

        self.BQPS_active = False
        self.PQPS_active = False

        self.next_event = None

        logging.info(" " + str(self.time) + ":\tBQPS size: " + str(self.BQPS_size))

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

        while self.next_event and self.next_event <= task.begin:
            self.time = self.next_event
            sleep_duration = self.time - self.previous_time
            self.update_queues(sleep_duration)
            self.previous_time = self.time


        self.new_task_event = True
        self.time = Fraction(task.begin, 1)

        sleep_duration = self.time - self.previous_time

        logging.info(" " + str(self.time) + ":\tRozpoczynanie przetwarzania nowego zadania: " + str(task))

        if self.time >= task.sdl:
            logging.info(" " + str(self.time) + ':\tDodanie zadania do kolejki PQ: ' + str(task))
            self.PQPS.append(task)
        else:
            logging.info(" " + str(self.time) + ':\tDodanie zadania do kolejki BQ: ' + str(task))
            self.BQ.append(task)

        self.update_queues(sleep_duration)
        self.previous_time = self.time

    def update_queues(self, period: Fraction):
        # Proces musi zostać uruchomiony po każdej zmianie w systemie!!
        # POMNIEJSZENIE CZASÓW ZADAŃ, PRZENOSZENIE ZADAŃ POMIĘDZY KOLEJKAMI I USUNIĘCIE ZAKOŃCZONYCH ZADAŃ Z KOLEJEK
        # Zwraca czas zakończenia kolejnego zadania lub null, jeżeli zadania nie są przetwarzane

        # PQPS
        if len(self.PQPS) > 0 and len(self.BQPS) == 0:

            self.PQPS_active = True
            self.BQPS_active = False

            for task in self.PQPS:
                task.time_left -= period /  len(self.PQPS)

            # Usunięcie zadań zakończonych
            finished = [t for t in self.PQPS if t.time_left <= 0]
            for f in finished:
                f.processing_finished = self.time
                logging.info(" " + str(self.time) + ':\tZakończenie zadania z kolejki PQ: ' + str(f))

            self.PQPS = [t for t in self.PQPS if t.time_left > 0]

        # BQPS
        elif len(self.BQPS) > 0:

            self.BQPS_active = True

            if self.PQPS_active:
                self.PQPS_active = False

            # Pomniejszenie czasów
            for task in self.BQPS:
                task.time_left -= period / len(self.BQPS)
                logging.debug("Pomniejszenie czasu o " + str(period / len(self.BQPS)) + " dla zadania " + str(task))

            # Usunięcie zadań zakończonych
            finished = [t for t in self.BQPS if t.time_left <= 0]
            for f in finished:
                f.processing_finished = self.time
                logging.info(" " + str(self.time) + ':\tZakończenie zadania z kolejki BQ: ' + str(f))

            self.BQPS = [t for t in self.BQPS if t.time_left > 0]

            # Przeniesienie zadań opóźnionych
            moved = [t for t in self.BQPS if (t.sdl + (t.duration - t.time_left)) <= self.time]
            for m in moved:
                logging.info(" " + str(self.time) + ':\tPrzeniesienie zadania z kolejki BQPS do PQ: ' + str(m))

            self.PQPS.extend(moved)
            self.BQPS = [t for t in self.BQPS if t not in moved]
        else:
            self.PQPS_active = False
            self.BQPS_active = False

        # Przenoszenie z BQ FCFS do BQPS
        if len(self.BQPS) >= self.BQPS_size:
            pass
        elif self.BQPS_size > 1 and len(self.BQ) > 0:
            self.BQPS_active = True
            # Miejsca do przydziału (+ jedno miejsce z ograniczonym prawdopodobieństwem)
            avalible_slots = self.BQPS_size - len(self.BQPS) - 1
            logging.debug(" " + str(self.time) + ':\tDostępne wolne sloty: ' + str(avalible_slots))

            for _ in range(avalible_slots):
                if len(self.BQ) == 0:
                    break

                first_fifo_task = self.BQ[0]
                logging.info(" " + str(self.time) + ':\tPrzeniesienie zadania z kolejki BQ do BQPS: ' + str(first_fifo_task))
                self.BQ.pop(0)
                self.BQPS.append(first_fifo_task)

            if self.new_task_event and (random.random() <= self.BQPS_chance) and len(self.BQ) > 0:
                first_fifo_task = self.BQ[0]
                logging.info(" " + str(self.time) + ':\tPrzeniesienie zadania z kolejki BQ do BQPS: ' + str(first_fifo_task))
                self.new_task_event = False
                self.BQ.pop(0)
                self.BQPS.append(first_fifo_task)

        elif len(self.BQ) > 0:
            self.BQPS_active = True
            first_fifo_task = self.BQ[0]
            self.BQ.pop(0)
            self.BQPS.append(first_fifo_task)


        # Przeniesienie zadań opóźnionych z BQ
        moved = [t for t in self.BQ if t.sdl <= self.time]
        for m in moved:
            logging.info(" " + str(self.time) + ':\tPrzeniesienie zadania z kolejki BQ do PQ: ' + str(m))

        self.PQPS.extend(moved)
        self.BQ = [t for t in self.BQ if t not in moved]

        

        # Zwrócenie czasu najbliższego eventu
        if self.BQPS_active and len(self.BQPS) > 0:
            min_break = self.BQPS[0].time_left * len(self.BQPS)

            for t in self.BQPS:
                if (t.time_left * len(self.BQPS)) < min_break:
                    min_break = t.time_left * len(self.BQPS)
                
                # Czas przeniesienia do kolejki BQPS
                if len(self.BQPS) > 1:
                    transfer_time = (t.sdl + t.made() - self.time) / (Fraction(1) - (Fraction(1) / Fraction(len(self.BQPS))))
                    
                    if transfer_time < min_break:
                        min_break = transfer_time
            
            if len(self.BQ) > 0:
                # Najcześniejsze zadanie przenoszone z BQ to pq
                min_break_bq_to_pq = min([x.sdl for x in self.BQ])

                if min_break_bq_to_pq < min_break:
                    min_break = min_break_bq_to_pq

            logging.info(" " + str(self.time) + ':\tNajbliższe wydarzenie w kolejce BQPS : ' + str(self.time + min_break))
            self.next_event = self.time + min_break

        elif len(self.PQPS) > 0:
            min_break = self.PQPS[0].time_left * len(self.PQPS)

            for t in self.PQPS:
                if (t.time_left * len(self.PQPS)) < min_break:
                    min_break = t.time_left * len(self.PQPS)

            if len(self.BQ) > 0:
                # Najcześniejsze zadanie przenoszone z BQ to pq
                min_break_bq_to_pq = min([x.sdl for x in self.BQ])

                if min_break_bq_to_pq < min_break:
                    min_break = min_break_bq_to_pq

            logging.info(" " + str(self.time) + ':\tNajbliższe wydarzenie w kolejce PQPS : ' + str(self.time + min_break))
            self.next_event = self.time + min_break
        else:

            logging.info(" " + str(self.time) + ':\tOczekiwanie na nadchodzące zadania')
            self.next_event = None
