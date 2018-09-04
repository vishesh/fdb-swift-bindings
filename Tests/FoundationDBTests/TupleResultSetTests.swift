/*
 * TupleResultSetTests.swift
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

import XCTest
@testable import FoundationDB

class TupleResultSetTests: XCTestCase {
	static var allTests: [(String, (TupleResultSetTests) -> () throws -> Void)] {
		return [
			("testInitializeSetsResults", testInitializeSetsResults),
			("testTupleResultSetsWithSameDataAreEqual", testTupleResultSetsWithSameDataAreEqual),
			("testTupleResultSetsWithDifferentKeysAreUnequal", testTupleResultSetsWithDifferentKeysAreUnequal),
			("testTupleResultSetsWithDifferentValuesAreUnequal", testTupleResultSetsWithDifferentValuesAreUnequal),
			("testTupleResultSetsWithDifferentCountsAreUnequal", testTupleResultSetsWithDifferentCountsAreUnequal),
			("testReadWithValidValueReturnsValue", testReadWithValidValueReturnsValue),
			("testReadWithMissingFieldThrowsMissingFieldError", testReadWithMissingFieldThrowsMissingFieldError),
			("testReadWithInvalidDataRethrowsError", testReadWithInvalidDataRethrowsError),
			("testReadWithOptionalResultWithValidValueReturnsValue", testReadWithOptionalResultWithValidValueReturnsValue),
			("testReadWithOptionalResultWithMissingFieldReturnsNil", testReadWithOptionalResultWithMissingFieldReturnsNil),
			("testReadWithOptionalResultWithInvalidDataRethrowsError", testReadWithOptionalResultWithInvalidDataRethrowsError),
		]
	}
	
	func testInitializeSetsResults() {
		let set = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		XCTAssertEqual(set.rows.count, 2)
		if set.rows.count < 2 { return }
		XCTAssertEqual(set.rows[0].key, Tuple("Key1"))
		XCTAssertEqual(set.rows[0].value, Tuple("Value1"))
		XCTAssertEqual(set.rows[1].key, Tuple("Key2"))
		XCTAssertEqual(set.rows[1].value, Tuple("Value2"))
	}
	
	func testTupleResultSetsWithSameDataAreEqual() {
		let set1 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		let set2 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		XCTAssertEqual(set1, set2)
	}
	
	func testTupleResultSetsWithDifferentKeysAreUnequal() {
		let set1 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		let set2 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key3"), value: Tuple("Value2"))
			])
		XCTAssertNotEqual(set1, set2)
	}
	
	func testTupleResultSetsWithDifferentValuesAreUnequal() {
		let set1 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		let set2 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value3"))
			])
		XCTAssertNotEqual(set1, set2)
	}
	
	func testTupleResultSetsWithDifferentCountsAreUnequal() {
		let set1 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		let set2 = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2")),
			(key: Tuple("Key3"), value: Tuple("Value3"))
			])
		XCTAssertNotEqual(set1, set2)
	}
	
	func testReadWithValidValueReturnsValue() throws {
		let set = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		let value = try set.read(Tuple("Key1")) as String
		XCTAssertEqual(value, "Value1")
	}
	
	func testReadWithMissingFieldThrowsMissingFieldError() throws {
		do {
			let set = TupleResultSet(rows: [
				(key: Tuple("Key1"), value: Tuple("Value1")),
				(key: Tuple("Key2"), value: Tuple("Value2"))
				])
			_ = try set.read(Tuple("Key3")) as String
			XCTFail()
		}
		catch let TupleResultSet.ParsingError.missingField(key) {
			XCTAssertEqual(key, Tuple("Key3"))
		}
		catch let e {
			throw e
		}
	}
	
	func testReadWithInvalidDataRethrowsError() throws {
		do {
			let set = TupleResultSet(rows: [
				(key: Tuple("Key1"), value: Tuple("Value1")),
				(key: Tuple("Key2"), value: Tuple("Value2"))
				])
			_ = try set.read(Tuple("Key1")) as Int
			XCTFail()
		}
		catch let TupleResultSet.ParsingError.incorrectTypeCode(key, _ , actual) {
			XCTAssertEqual(key, Tuple("Key1"))
			XCTAssertEqual(actual, 0x02)
		}
		catch let e {
			throw e
		}
	}
	
	func testReadWithOptionalResultWithValidValueReturnsValue() throws {
		let set = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		let value = try set.read(Tuple("Key1")) as String?
		XCTAssertEqual(value, "Value1")
	}
	
	func testReadWithOptionalResultWithMissingFieldReturnsNil() throws {
		let set = TupleResultSet(rows: [
			(key: Tuple("Key1"), value: Tuple("Value1")),
			(key: Tuple("Key2"), value: Tuple("Value2"))
			])
		let value = try set.read(Tuple("Key3")) as String?
		XCTAssertNil(value)
	}
	
	func testReadWithOptionalResultWithInvalidDataRethrowsError() throws {
		do {
			let set = TupleResultSet(rows: [
				(key: Tuple("Key1"), value: Tuple("Value1")),
				(key: Tuple("Key2"), value: Tuple("Value2"))
				])
			_ = try set.read(Tuple("Key1")) as Int?
			XCTFail()
		}
		catch let TupleResultSet.ParsingError.incorrectTypeCode(key, _ , actual) {
			XCTAssertEqual(key, Tuple("Key1"))
			XCTAssertEqual(actual, 0x02)
		}
		catch let e {
			throw e
		}
		
	}
}
