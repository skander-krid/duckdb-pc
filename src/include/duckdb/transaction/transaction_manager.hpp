//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/atomic.hpp"
#include <iostream>
#include <vector>
#include <unordered_map>

namespace duckdb {

class AttachedDatabase;
class ClientContext;
class Catalog;
struct ClientLockWrapper;
class DatabaseInstance;
class Transaction;

class Bitmap {
    std::vector<uint64_t> data;

public:
    Bitmap() = default;

    // Set bit at given index, auto-growing if needed
    void set(size_t index) {
        size_t required_size = (index / 64) + 1;
        if (data.size() < required_size) {
            data.resize(required_size, 0);
        }
        data[index / 64] |= (uint64_t(1) << (index % 64));
    }

    // Check if bit is set
    bool get(size_t index) const {
        size_t block = index / 64;
        if (block >= data.size()) {
            return false;
        }
        return (data[block] >> (index % 64)) & 1;
    }

    // Combine this bitmap with another via bitwise AND
    void intersect(const Bitmap& other) {
        size_t min_size = std::min(data.size(), other.data.size());
        for (size_t i = 0; i < min_size; ++i) {
            data[i] &= other.data[i];
        }
        // Zero out excess bits if this bitmap is larger
        for (size_t i = min_size; i < data.size(); ++i) {
            data[i] = 0;
        }
    }

	void add(const Bitmap &other) {
        size_t max_size = std::max(data.size(), other.data.size());
		size_t min_size = std::min(data.size(), other.data.size());
        for (size_t i = 0; i < min_size; ++i) {
            data[i] |= other.data[i];
        }
		if (data.size() < max_size) {
			data.resize(max_size, 0);
			for (size_t i = min_size; i < data.size(); ++i) {
				data[i] |= other.data[i];
			}
		}
    }

    // Print bitmap (for debugging, prints in reverse for clarity)
    void print(size_t max_bits = 0) const {
        size_t total_bits = max_bits ? max_bits : data.size() * 64;
        for (size_t i = 0; i < total_bits; ++i) {
            std::cout << get(i);
        }
        std::cout << std::endl;
    }
};

class PredicateCache {
public:
	PredicateCache() = default;

	void Add(const std::string &table_name, const std::string &filter_fingerprint, const unsigned long offset, const Bitmap &bitmap) {
		predicateCacheMutex.lock();
		internalCache[table_name][filter_fingerprint][offset] = bitmap;
	}

	// Get a bitmap from the cache by key
	const Bitmap* Get(const std::string &table_name, const std::string &filter_fingerprint, const unsigned long offset) const {
		// 1) Locate the table bucket
		auto tblIt = internalCache.find(table_name);
		if (tblIt == internalCache.end()) {
			return nullptr;
		}

		// 2) Locate the fingerprint bucket within that table
		auto fpIt = tblIt->second.find(filter_fingerprint);
		if (fpIt == tblIt->second.end()) {
			return nullptr;
		}

		// 3) Locate the bitmap at the requested offset
		auto offIt = fpIt->second.find(offset);
		if (offIt == fpIt->second.end()) {
			return nullptr;
		}

		return &offIt->second;   // Success
	}

private:
	using TableName = std::string;
	using FilterFingerprint = std::string;
	using Offset = unsigned long;
	std::unordered_map<TableName, std::unordered_map<FilterFingerprint, unordered_map<Offset, Bitmap>>> internalCache;
	std::mutex predicateCacheMutex;
};

//! The Transaction Manager is responsible for creating and managing
//! transactions
class TransactionManager {
public:
	explicit TransactionManager(AttachedDatabase &db);
	virtual ~TransactionManager();

	//! Start a new transaction
	virtual Transaction &StartTransaction(ClientContext &context) = 0;
	//! Commit the given transaction. Returns a non-empty error message on failure.
	virtual ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) = 0;
	//! Rollback the given transaction
	virtual void RollbackTransaction(Transaction &transaction) = 0;

	virtual void Checkpoint(ClientContext &context, bool force = false) = 0;

	static TransactionManager &Get(AttachedDatabase &db);

	virtual bool IsDuckTransactionManager() {
		return false;
	}

	AttachedDatabase &GetDB() {
		return db;
	}

	PredicateCache predicateCache;

protected:
	//! The attached database
	AttachedDatabase &db;
};

} // namespace duckdb
