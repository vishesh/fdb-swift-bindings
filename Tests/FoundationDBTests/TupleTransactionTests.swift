/*
 * TupleTransactionTests.swift
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
import NIO

class TupleTransactionTests: XCTestCase {
	let eventLoop = EmbeddedEventLoop()
	var connection: InMemoryDatabaseConnection!
	var transaction: InMemoryTransaction!
	
	static var allTests: [(String, (TupleTransactionTests) -> () throws -> Void)] {
		return [
			("testReadGetsValueFromDatabase",testReadGetsValueFromDatabase),
			("testReadRangeGetsValuesFromDatabase",testReadRangeGetsValuesFromDatabase),
			("testReadClosedRangeGetsValuesFromDatabase",testReadClosedRangeGetsValuesFromDatabase),
			("testStoreSetsValueInDatabase",testStoreSetsValueInDatabase),
			("testClearClearsValueInDatabase",testClearClearsValueInDatabase),
			("testClearRangeClearsValueInDatabase",testClearRangeClearsValueInDatabase),
			("testClearClosedRangeClearsValueInDatabase",testClearClosedRangeClearsValueInDatabase),
			("testAddReadConflictOnRangeAddsConflict",testAddReadConflictOnRangeAddsConflict),
			("testAddReadConflictOnClosedRangeAddsConflict",testAddReadConflictOnClosedRangeAddsConflict),
		]
	}
	
	override func setUp() {
		super.setUp()
		connection = InMemoryDatabaseConnection(eventLoop: eventLoop)
		transaction = InMemoryTransaction(version: 5, database: connection)
		connection[Tuple("Test", "Key", 1).databaseValue] = Tuple("Test", "Value", 1).databaseValue
		connection[Tuple("Test", "Key", 2).databaseValue] = Tuple("Test", "Value", 2).databaseValue
		connection[Tuple("Test", "Key", 3).databaseValue] = Tuple("Test", "Value", 3).databaseValue
		connection[Tuple("Test", "Key", 4).databaseValue] = Tuple("Test", "Value", 4).databaseValue
	}
	
	func testReadGetsValueFromDatabase() throws {
		self.runLoop(eventLoop) {
			self.transaction.read(Tuple("Test", "Key", 1)).map { value in
				XCTAssertEqual(value, Tuple("Test", "Value", 1))
				}.catch(self)
		}
	}
	
	func testReadRangeGetsValuesFromDatabase() throws {
		self.runLoop(eventLoop) {
			self.transaction.read(range: Tuple("Test", "Key", 2) ..< Tuple("Test", "Key", 4)).map {
				let values = $0.rows
				XCTAssertEqual(values.count, 2)
				if values.count < 2 { return }
				XCTAssertEqual(values[0].key, Tuple("Test", "Key", 2))
				XCTAssertEqual(values[0].value, Tuple("Test", "Value", 2))
				XCTAssertEqual(values[1].key, Tuple("Test", "Key", 3))
				XCTAssertEqual(values[1].value, Tuple("Test", "Value", 3))
				}.catch(self)
		}
	}
	
	func testReadClosedRangeGetsValuesFromDatabase() throws {
		self.runLoop(eventLoop) {
			self.transaction.read(range: Tuple("Test", "Key", 2) ... Tuple("Test", "Key", 4)).map {
				let values = $0.rows
				XCTAssertEqual(values.count, 3)
				if values.count < 3 { return }
				XCTAssertEqual(values[0].key, Tuple("Test", "Key", 2))
				XCTAssertEqual(values[0].value, Tuple("Test", "Value", 2))
				XCTAssertEqual(values[1].key, Tuple("Test", "Key", 3))
				XCTAssertEqual(values[1].value, Tuple("Test", "Value", 3))
				XCTAssertEqual(values[2].key, Tuple("Test", "Key", 4))
				XCTAssertEqual(values[2].value, Tuple("Test", "Value", 4))
				}.catch(self)
		}
	}
	
	func testStoreSetsValueInDatabase() throws {
		self.runLoop(eventLoop) {
			self.transaction.store(key: Tuple("Test", "Key", 5), value: Tuple("Test", "Value", 5))
			self.connection.commit(transaction: self.transaction)
				.map {
					XCTAssertEqual(self.connection[Tuple("Test", "Key", 5).databaseValue], Tuple("Test", "Value", 5).databaseValue)
				}.catch(self)
		}
	}
	
	func testClearClearsValueInDatabase() throws {
		self.runLoop(eventLoop) {
			self.transaction.clear(key: Tuple("Test", "Key", 2))
			self.connection.commit(transaction: self.transaction)
				.map { _ -> Void in
					XCTAssertNotNil(self.connection[Tuple("Test", "Key", 1).databaseValue])
					XCTAssertNil(self.connection[Tuple("Test", "Key", 2).databaseValue])
					XCTAssertNotNil(self.connection[Tuple("Test", "Key", 3).databaseValue])
				}.catch(self)
		}
	}
	
	func testClearRangeClearsValueInDatabase() throws {
		self.runLoop(eventLoop) {
			self.transaction.clear(range: Tuple("Test", "Key", 1) ..< Tuple("Test", "Key", 3))
			self.connection.commit(transaction: self.transaction).map { _ -> Void in
				XCTAssertNil(self.connection[Tuple("Test", "Key", 1).databaseValue])
				XCTAssertNil(self.connection[Tuple("Test", "Key", 2).databaseValue])
				XCTAssertNotNil(self.connection[Tuple("Test", "Key", 3).databaseValue])
				XCTAssertNotNil(self.connection[Tuple("Test", "Key", 4).databaseValue])
				}.catch(self)
		}
	}
	
	func testClearClosedRangeClearsValueInDatabase() throws {
		self.runLoop(eventLoop) {
			self.transaction.clear(range: Tuple("Test", "Key", 1) ... Tuple("Test", "Key", 3))
			self.connection.commit(transaction: self.transaction).map { _ in
				XCTAssertNil(self.connection[Tuple("Test", "Key", 1).databaseValue])
				XCTAssertNil(self.connection[Tuple("Test", "Key", 2).databaseValue])
				XCTAssertNil(self.connection[Tuple("Test", "Key", 3).databaseValue])
				XCTAssertNotNil(self.connection[Tuple("Test", "Key", 4).databaseValue])
				}.catch(self)
		}
	}
	
	func testAddReadConflictOnRangeAddsConflict() throws {
		transaction.addReadConflict(on: Tuple("Test", "Key", 1) ..< Tuple("Test", "Key", 3))
		XCTAssertEqual(transaction.readConflicts.count, 1)
		if transaction.readConflicts.count < 1 { return }
		XCTAssertEqual(transaction.readConflicts[0].lowerBound, Tuple("Test", "Key", 1).databaseValue)
		XCTAssertEqual(transaction.readConflicts[0].upperBound, Tuple("Test", "Key", 3).databaseValue)
	}
	
	func testAddReadConflictOnClosedRangeAddsConflict() throws {
		transaction.addReadConflict(on: Tuple("Test", "Key", 1) ... Tuple("Test", "Key", 3))
		XCTAssertEqual(transaction.readConflicts.count, 1)
		if transaction.readConflicts.count < 1 { return }
		XCTAssertEqual(transaction.readConflicts[0].lowerBound, Tuple("Test", "Key", 1).databaseValue)
		XCTAssertEqual(transaction.readConflicts[0].upperBound, Tuple("Test", "Key", 3).appendingNullByte().databaseValue)
	}
}
