import numpy as np


def convert_bo3_p_to_bo5(p3):
    p1 = np.roots([-2, 3, 0, -1*p3])[1]
    p5 = (p1**3)*(4 - 3*p1 + (6*(1-p1)*(1-p1)))
    return p5
