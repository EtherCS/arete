import math
import scipy . special
import numpy
from fractions import Fraction

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

# Compute values for 10000 total parties , 30% corruption , and 60 bit security
n = 10000
s = 3000
k = 60

# corruption threshold percentages we are interested in
crps = range(99 , 32 , -1)
print("Total nodes:", n, "\t", "Security parameter:", k)
print("protocol\t s\t fS\t fL\t m")
# for crp in crps:
#     f = Fraction(crp, 100) # convert percentage to fraction
#     print (float(f) , "\t", minCSize(n, s, f, k) , "\t")
print("OmniLedger\t 25%\t 33.3%\t 33.3%\t", minCSize(n, n*Fraction(25, 100), Fraction(1, 3), k))
print("RapidChain\t 33.3%\t 49%\t 49%\t", minCSize(n, n*Fraction(1, 3), Fraction(49, 100), k))
print("AHL\t\t 30%\t 49%\t 49%\t", minCSize(n, n*Fraction(3, 10), Fraction(49, 100), k))
print("Pyramid\t\t 12.5%\t 33.3%\t 33.3%\t", minCSize(n, n*Fraction(1, 8), Fraction(1, 3), k))
print("RIVET\t\t 33.3%\t 49%\t 49%\t", minCSize(n, n*Fraction(1, 3), Fraction(49, 100), k))
print("GEARBOX\t\t 30%\t 39%\t 30%\t", minCSize(n, n*Fraction(3, 10), Fraction(39, 100), k))
print("GEARBOX\t\t 25%\t 49%\t 25%\t", minCSize(n, n*Fraction(25, 100), Fraction(49, 100), k))
print("ARETE\t\t 33.3%\t 66.6%\t 33.3%\t", minCSize(n, n*Fraction(1, 3), Fraction(2, 3), k))
print("ARETE\t\t 25%\t 74%\t 25%\t", minCSize(n, n*Fraction(25, 100), Fraction(74, 100), k))
# print("ARETE\t\t 30%\t 59%\t 40%\t", minCSize(n, n*Fraction(3, 10), Fraction(40, 100), k))