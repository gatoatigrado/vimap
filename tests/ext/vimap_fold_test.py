import testify as T
from contextlib import nested
from StringIO import StringIO
import mock

from vimap.ext.vimap_fold_wordcount import VimapFoldWordcount


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
                mock.patch.object(word_counter, 'parse_options', MockOptions),
                mock.patch('sys.stdout', new_callable=StringIO)) as (_, out):
            word_counter.run()
            T.assert_equal(int(out.getvalue()), 12)
