import vimap.api.v1 as vimap

@vimap.fcn_worker
def double_it(x):
    return x * 2

inputs = (1, 2, 3, 4)
with vimap.fork(double_it()) as pool:
    outputs = tuple(pool.imap(inputs))

print outputs  # returns doubled numbers in arbitrary order, like (6, 8, 2, 4)
