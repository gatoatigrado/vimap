'''
Provides methods for tests.
'''
import multiprocessing

get_func = lambda x: lambda y: x + y
unpickleable = (get_func(3), 3)
