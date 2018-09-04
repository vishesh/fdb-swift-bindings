/*
 * TupleResultSet.swift
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

/**
	This type describes the results of a reading a range of keys from the
	database.
	*/
public struct TupleResultSet: Equatable {
	/** The keys and values that we read. */
	public let rows: [(key: Tuple, value: Tuple)]
	
	public init(_ resultSet: ResultSet) {
		self.rows = resultSet.rows.map { (key: Tuple(databaseValue: $0.0), value: Tuple(databaseValue: $0.1)) }
	}
	
	public init(rows: [(key: Tuple, value: Tuple)]) {
		self.rows = rows
	}
	
	/**
		This type provides errors that can be thrown when reading fields from a
		tuple in a result set.
		
		These wrap around the errors in Tuple.ParsingError to replace
		the index with the database key, which is more useful when reading
		fields from a result set.
		*/
	public enum ParsingError : Error {
		/** We tried to read a field beyond the end of the tuple. */
		case missingField(key: Tuple)
	
		/**
			We tried to read a field of a different type than the one
			actually stored.
			*/
		case incorrectTypeCode(key: Tuple, desired: Set<UInt8>, actual: UInt8)
	}
	
	/**
		This method reads a key from the result set.
		
		This will automatically convert it into the requested return type. If
		it cannot be converted into the requested return type, it will rethrow
		the resulting error.
		
		If the value is missing, this will throw
		`Tuple.ParsingError.MissingKey`.
		
		- parameter key:		The key to fetch.
		- returns:				The converted value.
		- throws:				Tuple.ParsingError.
		*/
	public func read<ReturnType: TupleConvertible>(_ key: Tuple) throws -> ReturnType {
		let value = try self.read(key) as Tuple
		do {
			return try value.read(at: 0) as ReturnType
		}
		catch TupleDecodingError.missingField {
			throw ParsingError.missingField(key: key)
		}
		catch let TupleDecodingError.incorrectTypeCode(_, desired, actual) {
			throw ParsingError.incorrectTypeCode(key: key, desired: desired, actual: actual)
		}
		catch {
			throw error
		}
	}
	
	/**
		This method reads a key from the result set.
		
		If the value is missing, this will throw
		`Tuple.ParsingError.MissingKey`.
		
		- parameter key:		The key to fetch.
		- returns:				The converted value.
		- throws:				Tuple.ParsingError.
		*/
	public func read(_ key: Tuple) throws -> Tuple {
		if let value = self.read(key, range: rows.startIndex..<rows.endIndex) {
			return value
		}
		else {
			throw TupleResultSet.ParsingError.missingField(key: key)
		}
	}
	
	
	private func read(_ key: Tuple, range: Range<Int>) -> Tuple? {
		let middleIndex = range.lowerBound + (range.upperBound - range.lowerBound) / 2
		guard range.contains(middleIndex) else {
			return nil
		}
		let middleKey = rows[middleIndex].key
		if middleKey == key {
			return rows[middleIndex].value
		}
		else if middleIndex == range.lowerBound {
			return nil
		}
		else if middleKey < key {
			return read(key, range: middleIndex ..< range.upperBound)
		}
		else {
			return read(key, range: range.lowerBound ..< middleIndex)
		}
	}
	
	/**
		This method reads a key from the result set.
		
		This will automatically convert it into the requested return type. If
		it cannot be converted into the requested return type, it will rethrow
		the resulting error.
		
		If the value is missing, this will return nil.
		
		- parameter key:		The key to fetch.
		- returns:				The converted value.
		- throws:				Tuple.ParsingError.
		*/
	public func read<ReturnType: TupleConvertible>(_ key: Tuple) throws -> ReturnType? {
		do {
			return try self.read(key) as ReturnType
		}
		catch ParsingError.missingField {
			return nil
		}
		catch let e {
			throw e
		}
	}
}

/**
	This method determines if two result sets have the same results.

	- parameter lhs:		The first result set.
	- parameter rhs:		The second result set.
	*/
public func ==(lhs: TupleResultSet, rhs: TupleResultSet) -> Bool {
	if lhs.rows.count != rhs.rows.count { return false }
	for index in 0..<lhs.rows.count {
		if lhs.rows[index].key != rhs.rows[index].key { return false }
		if lhs.rows[index].value != rhs.rows[index].value { return false }
	}
	return true
}
