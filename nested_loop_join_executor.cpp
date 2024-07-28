#include "nested_loop_join_executor.h"

NestedLoopJoinExecutor::NestedLoopJoinExecutor(
	AbstractExecutor *left_child_executor,
	AbstractExecutor *right_child_executor, const std::string join_key)
	: left_(left_child_executor),
	right_(right_child_executor),
	join_key_(join_key) {};

void NestedLoopJoinExecutor::Init() { 
	Tuple t1;
	Tuple t2;
	if (!is_init) {
		is_init = true;
		left_->Init();
		right_->Init();		
		while (left_->Next(&t1)) {
			while (right_->Next(&t2)) {
				if (join_key_ == "id") {
					if (t1.id == t2.id) {
						store.push_back(t1);
					}
				}
				else if (join_key_ == "val1") {
					if (t1.val1 == t2.val1) {
						store.push_back(t1);
					}
				}
				else if (join_key_ == "val2") {
					if (t1.val2 == t2.val2) {
						store.push_back(t1);
					}
				}
			}
			right_->Init();
		}

	}
	iter_ = store.begin();	
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple) { 
	while (iter_ != store.end()) {
		const Tuple &curr_tuple = *iter_;
		*tuple = Tuple(curr_tuple);
		++iter_;
		return true;
	}
	return false;
}
