#!/usr/bin/python

from vimap_fold import VimapFold
import re

WORD_RE = re.compile(r"[\w']+")


class VimapFoldWordcount(VimapFold):
    """Count the number of words in the input files"""

    def map(self, infile):
        result = 0
        with open(infile, 'rb') as f:
            for line in f:
                result += len(WORD_RE.findall(line))
        return result

    @property
    def initial_value(self):
        return 0

    def fold(self, accum, value):
        return accum + value


if __name__ == "__main__":
    vimap_fold = VimapFoldWordcount()
    vimap_fold.run()
