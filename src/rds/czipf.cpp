#include <assert.h>
#include "zipf.h"

extern "C" {
#include "czipf.h"
}

void *zipfInit(int nItems, double order, unsigned long seed){
  ZipfianGenerator *zg;
  zg = new ZipfianGenerator(nItems, order, seed);
  zg->Init();
  return (void*) zg;
}

int zipfGetIndex(void *zg_void){
  ZipfianGenerator *zg = (ZipfianGenerator*) zg_void;
  return zg->GetIndex();
}



