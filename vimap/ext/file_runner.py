# -*- coding: utf-8 -*-
"""
An extenstion to vimap's functionality that makes it easy to run an arbitrary
function in parallel that expects file names as input and then outputs a
result to either a file or stdout. Most of this is just argparse manipulation.
"""
import datetime
import glob
import json
import pickle
import pprint

import argparse

import vimap.pool


class FileRunner(object):
    """Class to run a function over files in parallel

    :param worker_fcn: The function that should be executed in parallel. This
                       function will be given file *names*, so you have to
                       open, decode, and close that file in your function
    :param progress: A boolean that indicates if the runner should print
                     progress information to the screen
    :param options: The result of parsing all options, just the options part
                    of argparse.ArgumentParser.parse_args()
    """
    def __init__(self, worker_fcn):
        self.worker_fcn = worker_fcn
        self.progress = False
        self.options = self.parse_options()

    def parse_options(self):
        parser = argparse.ArgumentParser()

        parser.add_argument(dest='datadir', nargs=1, type=str,
                            help='Directory containing files to read')
        parser.add_argument('-g', '--data-glob', dest='dataglob', default='*',
                            help='Glob pattern to apply to the data directory')
        parser.add_argument('-o', '--output-file', dest='outfile',
                            help='File to write output to.',
                            type=argparse.FileType('w'),
                            metavar="FILE")
        parser.add_argument('-j', '--json', dest='output_format',
                            const=json, action='store_const',
                            help='Format output as json')
        parser.add_argument('-k', '--pickle', dest='output_format',
                            const=pickle, action='store_const',
                            help='Format output as pickled data')
        parser.add_argument('-n', '--num-workers', dest='num_workers',
                            help='Number of workers to spin up',
                            default=4, type=int)
        parser.add_argument('-p', '--progress', dest='progress',
                            action="store_true", help='Show progress on files')

        options = parser.parse_args()

        if not options.datadir or len(options.datadir) > 1:
            parser.print_help()
            parser.error("You must supply a single data directory")
        options.datadir = options.datadir[0]

        return options

    def run_function_over_input(self):
        """Wrapper for fork_identical and zip_in_out()

        Takes care of the standard "fork a bunch of workers to execute a
        function and then feed them all the file names"

        yields: input, output tuples from the zip_in_out() call. You should
                use this function as follows:

                for input, output in runner.run_function_over_input():
                    # do something with input/output
        """
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
        """Displays the result of compution either to a file or to stdout"""
        serializer = self.options.output_format

        if self.options.outfile:
            serializer = serializer or pickle
            serializer.dump(result, self.options.outfile)
        else:
            if serializer:
                print serializer.dumps(result)
            else:
                pprint.pprint(result)
