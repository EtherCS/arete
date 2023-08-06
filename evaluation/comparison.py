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

# return the probability of compromising liveness with a given shard size
def pFailWithShardSize(n, s, m, h):
    p = 0
    denom = scipy.special.comb(n, m, exact = True) # compute n choose m as exact integer
    for i in range(m - h + 1, m + 1):
        p += Fraction(scipy.special.comb(int(s), int(i), exact = True) * scipy.special.comb(int(n - s), int(m - i), exact = True), denom)
    return p

# Compute values for 10000 total parties , 30% corruption , and 60 bit security
n = 10000
s = 250
k = 60

# corruption threshold percentages we are interested in
crps = range(99 , 32 , -1)
print("Total nodes:", n, " ", "Security parameter:", k)
# print("protocol\t s\t fS\t fL\t m\t")
print("protocol\t s\t fS\t fL\t m\t liveness_ratio")

print("OmniLedger\t 25%\t 33.3%\t 33.3%\t", minCSize(n, n*Fraction(25, 100), Fraction(1, 3), k), "\t", "1-2^("+str(k)+")")
print("RapidChain\t 33.3%\t 49%\t 49%\t", minCSize(n, n*Fraction(1, 3), Fraction(49, 100), k), "\t", "1-2^("+str(k)+")")
print("AHL\t\t 30%\t 49%\t 49%\t", minCSize(n, n*Fraction(3, 10), Fraction(49, 100), k), "\t", "1-2^("+str(k)+")")
print("Pyramid\t\t 12.5%\t 33.3%\t 33.3%\t", minCSize(n, n*Fraction(1, 8), Fraction(1, 3), k), "\t", "1-2^("+str(k)+")")
print("RIVET\t\t 33.3%\t 49%\t 49%\t", minCSize(n, n*Fraction(1, 3), Fraction(49, 100), k), "\t", "1-2^("+str(k)+")")

# GEARBOX
crps = range(0 , 34 , 1)
print(" ")
s_n = 25    # consider s=25%
for crp in crps:
    f_l = crp 
    f_s = 100-2*f_l-1
    m_min = minCSize(n, n*Fraction(s_n, 100), Fraction(f_s, 100), k)
    print("GEARBOX\t\t", str(s_n) + "%\t", str(f_s) + "%\t", str(f_l) + "%\t", m_min, "\t", float(1-pFailWithShardSize(n, n*Fraction(s_n, 100), m_min, int(m_min*Fraction(100-f_l, 100)))))

print(" ")
s_n = 30    # consider s=30%
for crp in crps:
    f_l = crp 
    f_s = 100-2*f_l-1
    m_min = minCSize(n, n*Fraction(s_n, 100), Fraction(f_s, 100), k)
    print("GEARBOX\t\t", str(s_n) + "%\t", str(f_s) + "%\t", str(f_l) + "%\t", m_min, "\t", float(1-pFailWithShardSize(n, n*Fraction(s_n, 100), m_min, int(m_min*Fraction(100-f_l, 100)))))

# ARETE
crps = range(99 , 49 , -1)
print(" ")
s_n = 25    # consider s=25%
for crp in crps:
    f_s = crp 
    f_l = 100-f_s-1
    m_min = minCSize(n, n*Fraction(s_n, 100), Fraction(f_s, 100), k)
    print("ARETE\t\t", str(s_n) + "%\t", str(f_s) + "%\t", str(f_l) + "%\t", m_min, "\t", float(1-pFailWithShardSize(n, n*Fraction(s_n, 100), m_min, int(m_min*Fraction(100-f_l, 100)))))

print(" ")
s_n = 30    # consider s=30%
for crp in crps:
    f_s = crp 
    f_l = 100-f_s-1
    m_min = minCSize(n, n*Fraction(s_n, 100), Fraction(f_s, 100), k)
    print("ARETE\t\t", str(s_n) + "%\t", str(f_s) + "%\t", str(f_l) + "%\t", m_min, "\t", float(1-pFailWithShardSize(n, n*Fraction(s_n, 100), m_min, int(m_min*Fraction(100-f_l, 100)))))