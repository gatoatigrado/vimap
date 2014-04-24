from StringIO import StringIO
from contextlib import nested

import mock
import testify as T

from vimap.testing import VimapFoldWordcount


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
                mock.patch('sys.stdout', new_callable=StringIO)) as (_, out):
            word_counter.run()
            T.assert_equal(int(out.getvalue()), 12)
