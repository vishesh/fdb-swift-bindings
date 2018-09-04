/*
 * ResultSetTests.swift
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

class ResultSetTests: XCTestCase {
	static var allTests: [(String, (ResultSetTests) -> () throws -> Void)] {
		return [
			("testInitializeSetsResults", testInitializeSetsResults),
			("testResultSetsWithSameDataAreEqual", testResultSetsWithSameDataAreEqual),
			("testResultSetsWithDifferentKeysAreUnequal", testResultSetsWithDifferentKeysAreUnequal),
			("testResultSetsWithDifferentValuesAreUnequal", testResultSetsWithDifferentValuesAreUnequal),
			("testResultSetsWithDifferentCountsAreUnequal", testResultSetsWithDifferentCountsAreUnequal),
		]
	}
	
	func testInitializeSetsResults() {
		let set = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value2")
			])
		XCTAssertEqual(set.rows.count, 2)
		if set.rows.count < 2 { return }
		XCTAssertEqual(set.rows[0].key, "Key1")
		XCTAssertEqual(set.rows[0].value, "Value1")
		XCTAssertEqual(set.rows[1].key, "Key2")
		XCTAssertEqual(set.rows[1].value, "Value2")
	}
	
	func testResultSetsWithSameDataAreEqual() {
		let set1 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value2")
			])
		let set2 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value2")
			])
		XCTAssertEqual(set1, set2)
	}
	
	func testResultSetsWithDifferentKeysAreUnequal() {
		let set1 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value2")
			])
		let set2 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key3", value: "Value2")
			])
		XCTAssertNotEqual(set1, set2)
	}
	
	func testResultSetsWithDifferentValuesAreUnequal() {
		let set1 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value2")
			])
		let set2 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value3")
			])
		XCTAssertNotEqual(set1, set2)
	}
	
	func testResultSetsWithDifferentCountsAreUnequal() {
		let set1 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value2")
			])
		let set2 = ResultSet(rows: [
			(key: "Key1", value: "Value1"),
			(key: "Key2", value: "Value2"),
			(key: "Key3", value: "Value3")
			])
		XCTAssertNotEqual(set1, set2)
	}
}
