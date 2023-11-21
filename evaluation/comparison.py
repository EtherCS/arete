import math
import scipy . special
import numpy
from fractions import Fraction
import argparse

# Compute probability of having too many corrupted parties in committee with
# n = total population
# s = number of corruptions in total population
# m = committee size
# f = corruption ration in a committee
# h = minimum number of honest parties required in committee
# pMax = maximal value for which pFail returns correct value . Output is 1 if pFail > pMax .
def pFail(n, s, m, h, pMax):
    p = 0
    denom = scipy.special.comb(n, m, exact = True) # compute n choose m as exact integer
    for i in range(m - h + 1, m + 1):
        p += Fraction(scipy.special.comb(int(s), int(i), exact = True) * scipy.special.comb(int(n - s), int(m - i), exact = True), denom)
        if p > pMax:
            return 1
    return p

# Find minimum committee size with corruption ration f such that pFail <= 2^{ -k}
def minCSize(n, s, f, k):
    pMax = Fraction(1, 2 ** k)
    for m in range(1, n + 1):
        h = math.ceil((1 - f) * m) # we want at least h honest parties to not violate corruption threshold
        if pFail(n, s, m, h, pMax) <= pMax :
            return m

# return the probability of compromising liveness with a given shard size
def pFailWithShardSize(n, s, m, h):
    p = 0
    denom = scipy.special.comb(n, m, exact = True) # compute n choose m as exact integer
    for i in range(m - h + 1, m + 1):
        p += Fraction(scipy.special.comb(int(s), int(i), exact = True) * scipy.special.comb(int(n - s), int(m - i), exact = True), denom)
    return p

def main(n, k, s_t):
    value_width = 15
    # corruption threshold percentages we are interested in
    crps = range(99 , 32 , -1)
    print(f"Total nodes: {n:<{value_width}} Security parameter: {k:<{value_width}} Total Byzantine ratio: {s_t:<{value_width}}")
    
    print("{:<{width}}".format("protocol", width=value_width) + "{:<{width}}".format("s", width=value_width) + "{:<{width}}".format("fS", width=value_width) + "{:<{width}}".format("fL", width=value_width) + "{:<{width}}".format("m", width=value_width) + "{:<{width}}".format("liveness_ratio", width=value_width))

    print("{:<{width}}".format("OmniLedger", width=value_width) + "{:<{width}}".format("25%", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format(minCSize(n, n*Fraction(25, 100), Fraction(1, 3), k), width=value_width) + "{:<{width}}".format("1-2^("+str(k)+")", width=value_width))
    
    print("{:<{width}}".format("RapidChain", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format("49%", width=value_width) + "{:<{width}}".format("49%", width=value_width) + "{:<{width}}".format(minCSize(n, n*Fraction(1, 3), Fraction(49, 100), k), width=value_width) + "{:<{width}}".format("1-2^("+str(k)+")", width=value_width))
    
    print("{:<{width}}".format("AHL", width=value_width) + "{:<{width}}".format("30%", width=value_width) + "{:<{width}}".format("49%", width=value_width) + "{:<{width}}".format("49%", width=value_width) + "{:<{width}}".format(minCSize(n, n*Fraction(3, 10), Fraction(49, 100), k), width=value_width) + "{:<{width}}".format("1-2^("+str(k)+")", width=value_width))
    
    print("{:<{width}}".format("Pyramid", width=value_width) + "{:<{width}}".format("12.5%", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format(minCSize(n, n*Fraction(1, 8), Fraction(1, 3), k), width=value_width) + "{:<{width}}".format("1-2^("+str(k)+")", width=value_width))
    
    print("{:<{width}}".format("RIVET", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format("49%", width=value_width) + "{:<{width}}".format("49%", width=value_width) + "{:<{width}}".format(minCSize(n, n*Fraction(1, 3), Fraction(49, 100), k), width=value_width) + "{:<{width}}".format("1-2^("+str(k)+")", width=value_width))
    
    m_min=minCSize(n, n*Fraction(1, 3), Fraction(66, 100), k)
    print("{:<{width}}".format("CoChain", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format("66%", width=value_width) + "{:<{width}}".format("66%", width=value_width) + "{:<{width}}".format(m_min, width=value_width) + "{:<{width}}".format(float(1-pFailWithShardSize(n, n*Fraction(33, 100), m_min, int(m_min*Fraction(100-33, 100)))), width=value_width))

    # consensus group
    print(" ")
    s_n = s_t
    print("{:<{width}}".format("Beacon/Order", width=value_width) + "{:<{width}}".format(str(s_n)+"%", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format("33.3%", width=value_width) + "{:<{width}}".format(minCSize(n, n*Fraction(s_n, 100), Fraction(1, 3), k), width=value_width) + "{:<{width}}".format("1-2^("+str(k)+")", width=value_width))
    
    # GEARBOX
    crps = range(0 , 34 , 1)
    print(" ")
    s_n = s_t
    for crp in crps:
        f_l = crp 
        f_s = 100-2*f_l-1
        m_min = minCSize(n, n*Fraction(s_n, 100), Fraction(f_s, 100), k)
        
        print("{:<{width}}".format("GEARBOX", width=value_width) + "{:<{width}}".format(str(s_n)+"%", width=value_width) + "{:<{width}}".format(str(f_s)+"%", width=value_width) + "{:<{width}}".format(str(f_l)+"%", width=value_width) + "{:<{width}}".format(m_min, width=value_width) + "{:<{width}}".format(float(1-pFailWithShardSize(n, n*Fraction(s_n, 100), m_min, int(m_min*Fraction(100-f_l, 100)))), width=value_width))
    print(" ")

    # ARETE
    crps = range(99 , 49 , -1)
    print(" ")
    s_n = s_t
    for crp in crps:
        f_s = crp 
        f_l = 100-f_s-1
        m_min = minCSize(n, n*Fraction(s_n, 100), Fraction(f_s, 100), k)
        
        print("{:<{width}}".format("ARETE", width=value_width) + "{:<{width}}".format(str(s_n)+"%", width=value_width) + "{:<{width}}".format(str(f_s)+"%", width=value_width) + "{:<{width}}".format(str(f_l)+"%", width=value_width) + "{:<{width}}".format(m_min, width=value_width) + "{:<{width}}".format(float(1-pFailWithShardSize(n, n*Fraction(s_n, 100), m_min, int(m_min*Fraction(100-f_l, 100)))), width=value_width))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, help="Total number of nodes")
    parser.add_argument("--l", type=int, help="Security parameter")
    parser.add_argument("--s", type=int, help="The total byzantine ratio (0, 100)")
    
    args = parser.parse_args()
    
    # Compute values for n total parties and k bit security
    main(args.n, args.l, args.s)
    
    