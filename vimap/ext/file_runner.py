import datetime
import glob
import optparse
import pickle
import pprint
import sys

import vimap.pool


class FileRunner(object):
    def __init__(self, worker_fcn):
        self.worker_fcn = worker_fcn
        self.progress = False
        self.options = self.parse_options()

    def parse_options(self):
        parser = optparse.OptionParser()

        parser.add_option('-i', '--data-directory', dest='datadir',
                          help='Directory containing files to read')
        parser.add_option('-g', '--data-glob', dest='dataglob',
                          help='Glob pattern to apply to the data directory')
        parser.add_option('-o', '--output', dest='outfile',
                          help='File to write output to as pickled data',
                          metavar="FILE")
        parser.add_option('-n', '--num-workers', dest='num_workers',
                          help='Number of workers to spin up',
                          default=4, type=int)
        parser.add_option('-p', '--progress', dest='progress',
                          action="store_true", help='Show progress on files')

        options, args = parser.parse_args()

        if not(options.datadir and options.dataglob):
            print "ERROR: You must supply a data directory and a data glob\n"
            parser.print_help()
            sys.exit(1)

        return options

    def run_function_over_input(self):
        start = datetime.datetime.now()

        self.progress = self.options.progress

        files = glob.glob(self.options.datadir + self.options.dataglob)
        pool = vimap.pool.fork_identical(
            self.worker_fcn,
            num_workers=self.options.num_workers,
        )

        for input, output in pool.imap(files).zip_in_out():
            if self.progress:
                print "Processing", input

            yield input, output

        end = datetime.datetime.now()

        if self.progress:
            print "\n Started at: ", start.isoformat()
            print "Finished at: ", end.isoformat()
            print "Processed {0} files in {1} seconds".format(
                len(files), (end - start).seconds)

    def output_result(self, result):
        if self.options.outfile:
            with open(self.options.outfile, 'w') as outfile:
                pickle.dump(result, outfile)
        else:
            pprint.pprint(result)
