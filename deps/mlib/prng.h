// prng.h
// Pseudo-random number generators
//
// (c) Copyright 2014 Microsoft Corporation
// Written by Marcos K. Aguilera


#ifndef _PRNG_H
#define _PRNG_H

#include <time.h>

class SimplePrng { // Simple generator used to seed the better generator
private:
  unsigned long n;
public:
  SimplePrng(){ n = (unsigned long) time(0); }
  void SetSeed(long s){ n=s; }
  SimplePrng(int seed){ n = seed; }
  unsigned long next(void){
    n = n * 1103515245 + 12345;
    return (unsigned int)(n/65536) % 32768;
  }
};

class Prng {
private:
  SimplePrng seeder;
  unsigned __int64 Y[55];
  int j,k;
public:
  Prng() : seeder() { init(); }
  Prng(unsigned long seed) : seeder(seed){ init(seed); }
  void init(unsigned long seed=0){ // uses simpler generator to produce a seed
    if (seed!=0) seeder.SetSeed(seed);
    else seeder.SetSeed((unsigned long) time(0));
    unsigned __int64 v;
    for (int i=0; i < 55; ++i){
      // generate 64-bit random number of 7-bit seeder
      v = 0;
      for (int h=0; h < 10; ++h) v = (v << 7) | seeder.next();
      Y[i] = v;
    }
    j=23;
    k=54;
  }
  unsigned __int64 next(void){
    unsigned __int64 retval;
    Y[k] = Y[k] + Y[j];
    retval = Y[k];
    --j;
    if (j<0) j=54;
    --k;
    if (k<0) k=54;
    return retval;
  }
};

#endif
