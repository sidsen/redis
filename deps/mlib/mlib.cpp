// mlib.cpp
//
// (c) Copyright 2014 Microsoft Corporation
// Written by Marcos K. Aguilera


#include "stdafx.h"
#include "tmalloc.h"

//#define TEST1 // test code for thread-local malloc

#ifdef TEST1
int _tmain(int argc, _TCHAR* argv[])
{
  _tmreportprocessor(0);
  void *buf = malloc(10);
  free(buf);
  getchar();
}
#endif

