import datetime
import glob
import optparse
import pickle
import pprint
import sys

import vimap
import vimap.pool
import vimap.worker_process


class VimapFold(object):
    """Helper class to do the common task of map + fold on files

    Typically one defines map, intial_value, and fold, which will apply
    map to each of your input files in parallel and then fold together
    all the results.
    """
    progress = False

    @vimap.worker_process.instancemethod_worker
    def read_file_process_data(self, files):
        for f in files:
            if self.progress:
                print "Processing", f
            yield self.map(f)

    def map(self, infile):
        """Perform an operation on the provided infile.

        NOTE: This function executes in parallel

        :param infile: The name of the file to open and process.
        :returns: The result of the computation, can be any type as long as
                  your fold function knows what to do.
        """
        return {}

    @property
    def initial_value(self):
        """The initial value of the computation

        NOTE: This function is called exactly once to provide an initial
              value to fold onto.

        :returns: The value to start folding with
        """
        return None

    def fold(self, global_result, map_result):
        """Fold a newly acquired result into the global result

        NOTE: This function executes in serial on the results from map
              calls

        :param global_result: The previous value of computation
        :param map_result: The result of a single self.map call, you should
                             fold this into global_result

        :returns The result of folding the map_result into the global_result
        """
        return global_result

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

    def run(self):
        start = datetime.datetime.now()

        options = self.parse_options()
        self.progress = options.progress

        files = glob.glob(options.datadir + options.dataglob)
        pool = vimap.pool.fork_identical(
            self.read_file_process_data,
            num_workers=options.num_workers,
        )

        final_data = self.initial_value
        for input, output in pool.imap(files).zip_in_out():
            final_data = self.fold(final_data, output)

        end = datetime.datetime.now()

        if self.progress:
            print ""
            print " Started at: ", start.isoformat()
            print "Finished at: ", end.isoformat()
            print "Processed %d files in %d seconds" % (len(files), (end-start).seconds)

        if options.outfile:
            with open(options.outfile, 'w') as outfile:
                pickle.dump(final_data, outfile)
        else:
            pprint.pprint(final_data)
