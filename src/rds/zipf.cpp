#include <stdio.h>
#include <vector>
#include <unordered_map>
#include <math.h>

#include "stdafx.h"

#include <stdint.h>

#include <algorithm>



#include "prng.h"
#include "zipf.h"



ZipfianGenerator::ZipfianGenerator(int nItems, double order, unsigned long seed) :
  _curr_number(0.0), _count(1), _order(order),
  _discretized_cdf(nItems, 0.0), 
  prng(seed)
{}

// Formula for calculating Zipfian CDF from Wikipedia
void ZipfianGenerator::Init(){
  if (_order == 0.0){
    // Shape is 0.0, using uniform distribution
    return;
  }
  // using zipfian distribution
  for (size_t i = 0; i < _discretized_cdf.size(); ++i){
    _discretized_cdf[i] = getNextGeneralizedHarmonicNumber();
  }
  for (size_t i = 0; i < _discretized_cdf.size(); ++i){
    _discretized_cdf[i] /= _discretized_cdf[_discretized_cdf.size() - 1];
  }
}

// Perform binary search to find the highest index less than a random (0.0, 1.0).
// This effective "inverts" the CDF, giving us the target distribution.
int ZipfianGenerator::GetIndex(){
  double p = (double)prng.next() / UINT64_MAX;
  auto pos = std::lower_bound(_discretized_cdf.cbegin(), _discretized_cdf.cend(), p);
  return static_cast<int>(std::distance(_discretized_cdf.cbegin(), pos));
}

void ZipfianGenerator::print(){
  for (auto i = _discretized_cdf.cbegin(); i != _discretized_cdf.cend(); ++i){
    printf("%lld: %f\n", (long long) std::distance(_discretized_cdf.cbegin(), i), *i);
  }
}

#if 0
int do_zipfian_test(){
  ZipfianGenerator zg(200000, 1);
  zg.Init();
  std::unordered_map<int,int> histogram;
  {
    for (auto i = 0; i < 1000000; ++i){
      int j = zg.GetIndex();
      if (histogram.find(j) != histogram.end()){
        histogram[j]++;
      } else {
        histogram[j] = 1;
      }
    }
  }
  for (auto i = 0; i < 100; ++i){
    printf("%d: %f\n", i, histogram[i]/double(1000000));
  }
  return 0;
}

int main(){
  do_zipfian_test();
  return 0;
}
#endif
