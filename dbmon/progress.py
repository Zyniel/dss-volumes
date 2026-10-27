"""Progress traces in the job log: one line per major block, with the time of day.

    12:04:31  DSS extraction: 120 projects, 4 at the same time, metrics ALL
    12:04:33  project 1/120 SALES: OK, 11 datasets, 1 folders, 5 links in 2.1 s

Lines are printed at once (flush), so the job log shows them while the recipe runs. Several
threads can print at the same time: a lock keeps each line whole.
"""
import datetime
import threading

PRINT_LOCK = threading.Lock()


def log(message):
    """Print one progress line, starting with the time of day."""
    now = datetime.datetime.now().strftime("%H:%M:%S")
    with PRINT_LOCK:
        print("%s  %s" % (now, message), flush=True)


class Counter(object):
    """Counts the items done out of a total, from several threads: 1/120, 2/120..."""

    def __init__(self, total):
        self.total = total
        self.done = 0
        self.lock = threading.Lock()

    def next(self):
        """Count one more item done. Returns the text "done/total"."""
        with self.lock:
            self.done = self.done + 1
            return "%d/%d" % (self.done, self.total)
