# -*- coding: utf-8 -*-
"""
A chunked version of WorkerRoutine. This worker routine supposes that you've
written worker functions which are essentially,

    f : X -> Y

(i.e. yielding one f(x) for every x), but want to feed it as input arrays X^n.
Given one input,

    [x_1, ..., x_n]

taken from the input queue, this routine will put

    [f(x_1), ..., f(x_n)]

on the output queue.
"""
from __future__ import absolute_import
from __future__ import print_function

import vimap.real_worker_routine


class ChunkedWorkerRoutine(vimap.real_worker_routine.WorkerRoutine):
    def __init__(self, *args, **kwargs):
        super(ChunkedWorkerRoutine, self).__init__(*args, **kwargs)
        # Buffer of outputs processed, before the last. When the last is
        # processed, we'll output this buffer and clear it.
        self.output_array_buffer = None
        self.input_array_length = None

    def worker_input_generator(self):
        for array in super(ChunkedWorkerRoutine, self).worker_input_generator():
            assert not self.output_array_buffer, "Internal error (output wasn't written)"
            self.output_array_buffer = []
            self.input_array_length = len(array)
            for i, element in enumerate(array):
                assert len(self.output_array_buffer) == i, (
                    "Didn't write output for element {0}".format(i-1))
                yield element

    def handle_output(self, output):
        self.output_array_buffer.append(output)
        if len(self.output_array_buffer) == self.input_array_length:
            super(ChunkedWorkerRoutine, self).handle_output(self.output_array_buffer)
            self.output_array_buffer = None
