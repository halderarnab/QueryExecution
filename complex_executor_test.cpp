//===----------------------------------------------------------------------===//
//
//                         Rutgers CS539 - Database System
//                         ***DO NO SHARE PUBLICLY***
//
// Identification:   test/complex_executor_test.cpp
//
// Copyright (c) 2022, Rutgers University
//
//===----------------------------------------------------------------------===/

#include <iostream>
#include <string>
#include <vector>

#include "filter_seq_scan_executor.h"
#include "hash_join_executor.h"
#include "nested_loop_join_executor.h"
#include "seq_scan_executor.h"

using std::cout;
using std::endl;
using std::vector;

int main() {

	Table t1;

	for (int i = 5; i < 25; i++) {

		t1.insert(i, i, std::to_string(i));

	}

	Table t2;

	for (int i = 0; i < 15; i++) {

		t2.insert(i, i, std::to_string(i));

	}

	Table t3;

	for (int i = 4; i < 10; i++) {

		for (int j = 0; j < 2; j++) {

			t3.insert(i * 2 + j, i, std::to_string(i));

		}

	}

	Table t4;

	for (int i = 0; i < 2; i++) {

		t4.insert(i, 10, std::to_string(9));

	}

	for (int i = 2; i < 5; i++) {

		t4.insert(i, 11, std::to_string(3));

	}

	FilterPredicate pred1(15, PredicateType::LESS);

	FilterPredicate pred2(3, PredicateType::GREATER);

	FilterSeqScanExecutor scan1(&t1, &pred1);

	FilterSeqScanExecutor scan2(&t2, &pred2);

	SeqScanExecutor scan3(&t3);

	SeqScanExecutor scan4(&t4);

	SimpleHashFunction hash_fn1("val1");

	SimpleHashFunction hash_fn2("val2");

	HashJoinExecutor hash1(&scan1, &scan2, &hash_fn1);

	HashJoinExecutor hash2(&scan3, &scan4, &hash_fn2);

	NestedLoopJoinExecutor executor(&hash1, &hash2, "val1");

	executor.Init();

	Tuple tuple;

	while (executor.Next(&tuple)) {

		cout << "Successfully get tuple! id: " << tuple.id

			<< " val1: " << tuple.val1 << " val2: " << tuple.val2 << endl;

	}
	
	return 0;

}