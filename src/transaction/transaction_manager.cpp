#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

TransactionManager *global_transaction_manager = nullptr;

TransactionManager::TransactionManager(AttachedDatabase &db) : db(db) {
    global_transaction_manager = this; // Set the global transaction manager to this instance
}

TransactionManager::~TransactionManager() {
}

} // namespace duckdb
