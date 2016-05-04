// prng.h
// Pseudo-random number generators
//
// (c) Copyright 2014 Microsoft Corporation
// Written by Marcos K. Aguilera

#ifndef _PRNG_H
#define _PRNG_H

void prng_new();
void prng_delete();
unsigned __int64 prng_next();

#endif
