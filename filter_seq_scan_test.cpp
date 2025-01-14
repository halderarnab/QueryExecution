//===----------------------------------------------------------------------===//
//
//                         Rutgers CS539 - Database System
//                         ***DO NO SHARE PUBLICLY***
//
// Identification:   test/filter_seq_scan_test.cpp
//
// Copyright (c) 2022, Rutgers University
//
//===----------------------------------------------------------------------===/

#include <iostream>
#include <string>
#include <vector>

#include "filter_seq_scan_executor.h"

using std::cout;
using std::endl;
using std::vector;

int main() {
  Table t1;
  t1.insert(-1, 0, "0");
  t1.insert(-2, 0, "0");

  for (int i = 0; i < 10; i++) {
    t1.insert(i, i, std::to_string(i));
  }

  FilterPredicate predicate(4, PredicateType::EQUAL);

  FilterSeqScanExecutor executor(&t1, &predicate);

  executor.Init();
  Tuple tuple;
  while (executor.Next(&tuple)) {
    cout << "Successfully get tuple! id: " << tuple.id
         << " val1: " << tuple.val1 << " val2: " << tuple.val2 << endl;
  }
 
  return 0;
}
