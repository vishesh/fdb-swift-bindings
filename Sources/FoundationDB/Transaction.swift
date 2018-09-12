/*
 * Transaction.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import NIO

/**
This protocol describes a transaction that is occurring on a database.
*/
public protocol Transaction {
	/**
	This method reads a value from the data store.
	
	This will automatically add a read conflict for the key, so that if it
	has changed since the start of this transaction this transaction will
	not be accepted.
	
	If this is a snapshot read, this will not add a read conflict.
	
	- parameter key:		The key that we are reading.
	- parameter snapshot:	Whether we should do a snapshot read.
	- returns:				The value that we are reading, if any exists.
	*/
	func read(_ key: DatabaseValue, snapshot: Bool) -> EventLoopFuture<DatabaseValue?>
	
	/**
	This method finds a key using a key selector.
	
	- parameter selector:		The selector telling us where to find the
	key.
	- parameter snapshot:		Whether we should perform a snapshot read
	when finding the key.
	- returns:					The first key matching this selector.
	*/
	func findKey(selector: KeySelector, snapshot: Bool) -> EventLoopFuture<DatabaseValue?>
	
	/**
	This method reads a range of values for a range of keys matching two
	key selectors.
	
	The keys included in the result range will be from the first key
	matching the start key selector to the first key matching the end key
	selector. The start key will be included in the results, but the end key
	will not.
	
	The results will be ordered in lexographic order by their keys.
	
	This will automatically add a read conflict for the range, so that if
	any key has changed in this range since the start of this transaction
	this transaction will not be accepted.
	
	If this is a snapshot read, this will not add a read conflict.
	
	- parameter start:		The selector for the beginning of the range.
	- parameter end:		The selector for the end of the range.
	- parameter limit:		The maximum number of values to return.
	- parameter mode:		A mode specifying how we should chunk the return
	values from each iteration of the read.
	- parameter snapshot:	Whether we should treat this as a snapshot read.
	- parameter reverse:	Whether we should reverse the order of the rows.
	- returns:				A list of tuples with the keys and their
	corresponding values.
	*/
	func readSelectors(from start: KeySelector, to end: KeySelector, limit: Int?, mode: StreamingMode, snapshot: Bool, reverse: Bool) ->	EventLoopFuture<ResultSet>
	
	/**
	This method stores a value for a key.
	
	- parameter key:		The key that we are storing the value under.
	- parameter value:		The value that we are storing.
	*/
	func store(key: DatabaseValue, value: DatabaseValue)
	
	/**
	This method clears a value for a key.
	
	- parameter key:		The key that we are clearing.
	*/
	func clear(key: DatabaseValue)
	
	/**
	This method clears a range of keys.
	
	- parameter range:		The range of keys to clear.
	*/
	func clear(range: Range<DatabaseValue>)
	
	/**
	This method adds a range of keys that we want to reserve for reading.
	
	If the transaction is committed and the database has any changes to keys
	in this range, the commit will fail.
	
	- parameter range:		The range of keys to add the conflict on.
	*/
	func addReadConflict(on range: Range<DatabaseValue>)
	
	/**
	This method adds a range of keys that we want to reserve for writing.
	
	If the system commits this transaction, and another transaction has a
	read conflict on one of these keys, that second transaction will then
	fail to commit.
	
	- parameter range:		The range of keys to add the conflict on.
	*/
	func addWriteConflict(on range: Range<DatabaseValue>)
	
	/**
	This method gets the version of the database that this transaction is
	reading from.
	*/
	func getReadVersion() -> EventLoopFuture<Int64>
	
	/**
	This method gets the version of the database that this transaction
	should read from.
	
	- parameter version:		The new version.
	*/
	func setReadVersion(_ version: Int64)
	
	/**
	This method gets the version of the database that this transaction
	committed its changes at.
	
	If the transaction has not committed, this will return -1.
	*/
	func getCommittedVersion() -> EventLoopFuture<Int64>
	
	/**
	This method attempts to retry a transaction after an error.
	
	If the error is retryable, this will reset the transaction and fire the
	returned future when the transaction is ready to use again. If the error
	is not retryable, the returned future will rethrow the error.
	
	- parameter error:	The error that the system encountered.
	- returns:			A future indicating when the transaction is
						ready again.
	*/
	func attemptRetry(error: Error) -> EventLoopFuture<Void>
	
	/**
	This method resets the transaction to its initial state.
	*/
	func reset()
	
	/**
	This method cancels the transaction, preventing it from being committed
	and freeing up some associated resources.
	*/
	func cancel()
	
	/**
	This method performs an atomic operation against a key and value.
	
	- parameter operation:		The operation to perform.
	- parameter key:			The key to read for the operation.
	- parameter value:			The new value to provide to the operation.
	*/
	func performAtomicOperation(operation: MutationType, key: DatabaseValue, value: DatabaseValue)
	
	/**
	This method gets a version stamp, which is a key segment containing the
	committed version of the transaction.
	
	This can be called before the transaction is committed, and it will only
	return a value once the transaction is committed.
	*/
	func getVersionStamp() -> EventLoopFuture<DatabaseValue>
	
	/**
	This method sets an option on the transaction.
	
	Some options require values to be set on them, and some do not. The
	options that do not require a value have the semantics of setting a
	flag to true.
	
	See TransactionOption for more details on what options require a value.
	
	- parameter option:		The option to set.
	- parameter value:		The value to set for the option.
	*/
	func setOption(_ option: TransactionOption, value: DatabaseValue?)
}

extension Transaction {
	/**
	This method reads a value from the database.
	
	- parameter key:	The key to read.
	- returns:			The value for that key.
	*/
	public func read(_ key: DatabaseValue) -> EventLoopFuture<DatabaseValue?> {
		return read(key, snapshot: false)
	}
	
	/**
	This method reads a range of values for a range of keys matching two
	key selectors.
	
	The keys included in the result range will be from the first key
	matching the start key selector to the first key matching the end key
	selector. The start key will be included in the results, but the end key
	will not.
	
	The results will be ordered in lexographic order by their keys.
	
	This will automatically add a read conflict for the range, so that if
	any key has changed in this range since the start of this transaction
	this transaction will not be accepted.
	
	- parameter start:		The selector for the beginning of the range.
	- parameter end:		The selector for the end of the range.
	- parameter limit:		The maximum number of values to return.
	- parameter mode:		A mode specifying how we should chunk the return
							values from each iteration of the read.
	- parameter snapshot:	Whether we should treat this as a snapshot read.
	- parameter reverse:	Whether we should reverse the order of the rows.
	- returns:				A list of tuples with the keys and their
							corresponding values.
	*/
	public func read(from start: KeySelector, to end: KeySelector, limit: Int? = nil, mode: StreamingMode = .iterator, snapshot: Bool = false, reverse: Bool = false) -> EventLoopFuture<ResultSet> {
		return self.readSelectors(from: start, to: end, limit: limit, mode: mode, snapshot: snapshot, reverse: reverse)
	}
	
	/**
	This method reads a range of values for a range of keys.
	
	The results will be ordered in lexographic order by their keys.
	
	This will automatically add a read conflict for the range, so that if
	any key has changed in this range since the start of this transaction
	this transaction will not be accepted.
	
	- parameter range:	The range of keys to read.
	- returns:			A list of tuples with the keys and their
						corresponding values.
	*/
	public func read(range: Range<DatabaseValue>) -> EventLoopFuture<ResultSet> {
		return self.readSelectors(from: KeySelector(greaterThan: range.lowerBound, orEqual: true), to: KeySelector(greaterThan: range.upperBound, orEqual: true), limit: nil, mode: .iterator, snapshot: false, reverse: false)
	}
	
	/**
	This method adds a read conflict on a single key.
	
	If this key has been changed since the transaction started, the
	transaction will be rejected at commit time.
	
	- parameter key:		The key to add a conflict to.
	*/
	public func addReadConflict(key: DatabaseValue) {
		var end = key
		end.increment()
		self.addReadConflict(on: key ..< end)
	}
	
	/**
	This method adds a write conflict on a single key.
	
	If another outstanding transaction has read the key, that transaction
	will be rejected at commit time.
	
	- parameter key:		The key to add a conflict to.
	*/
	public func addWriteConflict(key: DatabaseValue) {
		var end = key
		end.increment()
		self.addWriteConflict(on: key ..< end)
	}
	
	/**
	This method sets an option on the transaction to true.
	
	- parameter option:		The option to set.
	*/
	public func setOption(_ option: TransactionOption) {
		self.setOption(option, value: nil)
	}
}
