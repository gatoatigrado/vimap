# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty

import vimap
import vimap.worker_process
from file_runner import FileRunner


class VimapFold(object):
    """Helper class to do the common task of map + fold on files

    Typically one defines map, intial_value, and fold, which will apply
    map to each piece of your input in parallel and then fold together
    all the results.
    """
    __metaclass__ = ABCMeta

    VIMAP_RUNNER = FileRunner

    @vimap.worker_process.instancemethod_worker
    def read_input_process_data(self, worker_input):
        for inp in worker_input:
            yield self.map(inp)

    @abstractmethod
    def map(self, inp):
        """Perform an operation on the provided input.

        NOTE: This function executes in parallel

        :param inp: The input to do something with
        :returns: The result of the computation, can be any type as long as
                  your fold function knows what to do.
        """
        return {}

    @abstractproperty
    def initial_value(self):
        """The initial value of the computation

        NOTE: This function is called exactly once to provide an initial
              value to fold onto.

        :returns: The value to start folding with
        """
        return None

    @abstractmethod
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

    def run(self):
        runner = self.VIMAP_RUNNER(self.read_input_process_data)

        final_data = self.initial_value
        for input, output in runner.run_function_over_input():
            final_data = self.fold(final_data, output)

        runner.output_result(final_data)
