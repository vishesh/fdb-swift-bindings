/*
 * CommandTests.swift
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

@testable import FoundationDBBindingTest
import FoundationDB
import XCTest
import Foundation

class CommandTests: XCTestCase {
	static var allTests: [(String, (CommandTests) -> () throws -> Void)] {
		return [
			("testInitializationWithPushCommandExtractsArguments", testInitializationWithPushCommandExtractsArguments),
			("testInitalizationWithPushCommandWithNoArgumentsHasEmptyArgument", testInitalizationWithPushCommandWithNoArgumentsHasEmptyArgument),
			("testInitializationWithPopCommandExtractsNoArguments", testInitializationWithPopCommandExtractsNoArguments),
			("testInitializationWithGetSetsDirectAndSnapshotFlags", testInitializationWithGetSetsDirectAndSnapshotFlags),
			("testInitializationWithGetDatabaseSetsDirectAndSnapshotFlags", testInitializationWithGetDatabaseSetsDirectAndSnapshotFlags),
			("testInitializationWithGetSnapshotSetsDirectAndSnapshotFlags", testInitializationWithGetSnapshotSetsDirectAndSnapshotFlags),
		]
	}
	
	override func setUp() {
	}
	
	func testInitializationWithPushCommandExtractsArguments() throws {
		let command = Command(data: Tuple("PUSH", "Test Data"))
		
		XCTAssertEqual(command?.operation, Command.Operation.push)
		XCTAssertEqual(command?.argument as? String, "Test Data")
	}
	
	func testInitalizationWithPushCommandWithNoArgumentsHasEmptyArgument() {
		let command = Command(data: Tuple("PUSH"))
		XCTAssertEqual(command?.operation, Command.Operation.push)
		XCTAssertEqual(command?.argument as? Int, 0)
	}
	
	func testInitializationWithPopCommandExtractsNoArguments() {
		let command = Command(data: Tuple("POP"))
		XCTAssertEqual(command?.operation, Command.Operation.pop)
		XCTAssertNil(command?.argument)
	}
	
	func testInitializationWithGetSetsDirectAndSnapshotFlags() {
		guard let command = Command(data: Tuple("GET")) else {
			return XCTFail()
		}
		XCTAssertEqual(command.operation, Command.Operation.get)
		XCTAssertFalse(command.direct)
		XCTAssertFalse(command.snapshot)
	}
	
	func testInitializationWithGetDatabaseSetsDirectAndSnapshotFlags() {
		guard let command = Command(data: Tuple("GET_DATABASE")) else {
			return XCTFail()
		}
		XCTAssertEqual(command.operation, Command.Operation.get)
		XCTAssertTrue(command.direct)
		XCTAssertFalse(command.snapshot)
	}
	
	func testInitializationWithGetSnapshotSetsDirectAndSnapshotFlags() {
		guard let command = Command(data: Tuple("GET_SNAPSHOT")) else {
			return XCTFail()
		}
		XCTAssertEqual(command.operation, Command.Operation.get)
		XCTAssertFalse(command.direct)
		XCTAssertTrue(command.snapshot)
	}
}
