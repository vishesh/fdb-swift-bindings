/*
 * Transaction+Tuple.swift
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

import Foundation
import NIO

extension Transaction {
	/**
	This method reads a key as a tuple.
	
	- parameter tuple:    The key to read.
	- returns:        The value for that key.
	*/
	public func read(_ key: Tuple, snapshot: Bool = false) -> EventLoopFuture<Tuple?> {
		return self.read(key.databaseValue as DatabaseValue, snapshot: snapshot).map { $0.map { Tuple(databaseValue: $0) } }
	}
	
	/**
	This method stores a value as a tuple.
	
	- parameter key:		The key to store under.
	- parameter value:		The value to store.
	*/
	public func store(key: Tuple, value: Tuple) {
		self.store(key: key.databaseValue, value: value.databaseValue)
	}
	
	/**
	This method clears a key, specified as a tuple.
	*/
	public func clear(key: Tuple) {
		self.clear(key: key.databaseValue)
	}
	
	/**
	This method converts a closed range of tuples to an open range.
	
	- parameter range:		The closed range.
	- returns:				The open range.
	*/
	private func openRangeEnd(_ range: ClosedRange<Tuple>) -> Range<Tuple> {
		return range.lowerBound ..< range.upperBound.appendingNullByte()
	}
	
	/**
	This method reads a range of values for a range of keys.
	
	The results will be ordered in lexographic order by their keys.
	
	This will automatically add a read conflict for the range, so that if
	any key has changed in this range since the start of this transaction
	this transaction will not be accepted.
	
	- parameter range:    The range of keys to read.
	- returns:        A list of tuples with the keys and their
	corresponding values.
	*/
	public func read(range: Range<Tuple>) ->  EventLoopFuture<TupleResultSet> {
		return read(range: range.lowerBound.databaseValue ..< range.upperBound.databaseValue).map { TupleResultSet($0) }
	}
	
	/**
	This method reads a range of values for a range of keys.
	
	The results will be ordered in lexographic order by their keys.
	
	This will automatically add a read conflict for the range, so that if
	any key has changed in this range since the start of this transaction
	this transaction will not be accepted.
	
	- parameter range:    The range of keys to read.
	- returns:        A list of tuples with the keys and their
	corresponding values.
	*/
	public func read(range: ClosedRange<Tuple>) -> EventLoopFuture<TupleResultSet> {
		return read(range: openRangeEnd(range))
	}
	
	/**
	This method clears a range of keys.
	
	- parameter range:		The keys to clear.
	*/
	public func clear(range: Range<Tuple>) {
		clear(range: range.lowerBound.databaseValue ..< range.upperBound.databaseValue)
	}
	
	/**
	This method clears a range of keys.
	
	- parameter range:		The range of keys to clear.
	*/
	public func clear(range: ClosedRange<Tuple>) {
		clear(range: openRangeEnd(range))
	}
	
	/**
	This method adds a range of keys that we want to reserve for reading.
	
	If the transaction is committed and the database has any changes to keys
	in this range, the commit will fail.
	
	- parameter range:		The range of keys to add the conflict on.
	*/
	public func addReadConflict(on range: Range<Tuple>) {
		addReadConflict(on: DatabaseValue(range.lowerBound.data) ..< DatabaseValue(range.upperBound.data))
	}
	
	/**
	This method adds a range of keys that we want to reserve for reading.
	
	If the transaction is committed and the database has any changes to keys
	in this range, the commit will fail.
	
	- parameter range:		The range of keys to add the conflict on.
	*/
	public func addReadConflict(on range: ClosedRange<Tuple>) {
		addReadConflict(on: openRangeEnd(range))
	}
}
