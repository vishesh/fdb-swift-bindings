/*
 * ResultSet.swift
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
public struct ResultSet: Equatable {
	/** The keys and values that we read. */
	public let rows: [(key: DatabaseValue, value: DatabaseValue)]
}

/**
	This method determines if two result sets have the same results.

	- parameter lhs:		The first result set.
	- parameter rhs:		The second result set.
	*/
public func ==(lhs: ResultSet, rhs: ResultSet) -> Bool {
	if lhs.rows.count != rhs.rows.count { return false }
	for index in 0..<lhs.rows.count {
		if lhs.rows[index].key != rhs.rows[index].key { return false }
		if lhs.rows[index].value != rhs.rows[index].value { return false }
	}
	return true
}
