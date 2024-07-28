#include "hash_join_executor.h"

HashJoinExecutor::HashJoinExecutor(AbstractExecutor *left_child_executor,
	AbstractExecutor *right_child_executor,
	SimpleHashFunction *hash_fn)
	: left_(left_child_executor),
	right_(right_child_executor),
	hash_fn_(hash_fn) {}

void HashJoinExecutor::Init() {
	Tuple t;
	left_->Init();
	right_->Init();	
	ht = SimpleHashJoinHashTable();
	while (left_->Next(&t)) {
		ht.Insert(hash_fn_->GetHash(t), t);
	}
};

bool HashJoinExecutor::Next(Tuple *tuple) { 
	Tuple t1;
	while (right_->Next(&t1)) {
		std::vector<Tuple> res;
		ht.GetValue(hash_fn_->GetHash(t1), &res);
		for (Tuple t : res) {
			store.push_back(t);
		}
	}

	if (is_empty(&store)) {
		*tuple = Tuple(store[0]);
		store.erase(store.begin());
		return true;
	}
	return false; 
}

bool HashJoinExecutor::is_empty(std::vector<Tuple> *tuple) {
	if (tuple->empty())
		return false;
	return true;
}
