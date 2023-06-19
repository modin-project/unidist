import unidist
import numpy as np

unidist.init()


@unidist.remote
def f(arr):
    print(f"{len(arr)}: {arr.sum()}")


refs = []
data = np.array(range(100**3))
for i in range(120):
    data_ref = unidist.put(data)
    refs.append(f.remote(data_ref))

unidist.wait(refs)
