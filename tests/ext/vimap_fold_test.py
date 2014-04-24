import re
from contextlib import nested

import mock
import testify as T

from vimap.ext.vimap_fold import VimapFold


WORD_RE = re.compile(r"[\w']+")


class VimapFoldWordcount(VimapFold):
    """Count the number of words in the input files"""

    def map(self, infile):
        result = 0
        with open(infile, 'r') as f:
            for line in f:
                result += len(WORD_RE.findall(line))
        return result

    @property
    def initial_value(self):
        return 0

    def fold(self, accum, value):
        return accum + value


class MockOptions(object):
    datadir = 'tests/ext/test_data/'
    dataglob = '*.txt'
    num_workers = 4
    outfile = False
    progress = False


class WordCountTest(T.TestCase):
    def test_word_count(self):
        word_counter = VimapFoldWordcount()
        with nested(
                mock.patch.object(word_counter.VIMAP_RUNNER,
                                  'parse_options', MockOptions),
                mock.patch.object(word_counter.VIMAP_RUNNER,
                                  'output_result')) as (_, output_mock):
            word_counter.run()
            ((value,), _), = output_mock.call_args_list
            T.assert_equal(value, 19)
