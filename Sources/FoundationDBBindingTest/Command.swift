/*
 * Command.swift
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
import FoundationDB

/**
	This type represents a command that can be given to a stack machine.
	*/
struct Command: Equatable {
	/**
		The operations that the stack machine can execute.
	
		The string values for this enum are the string representations of
		the operation in the database tuples.
		*/
	enum Operation: String, Equatable {
		case push = "PUSH"
		case dup = "DUP"
		case empty = "EMPTY_STACK"
		case swap = "SWAP"
		case pop = "POP"
		case sub = "SUB"
		case concat = "CONCAT"
		case log = "LOG_STACK"
		case newTransaction = "NEW_TRANSACTION"
		case useTransaction = "USE_TRANSACTION"
		case onError = "ON_ERROR"
		case get = "GET"
		case getKey = "GET_KEY"
		case getRange = "GET_RANGE"
		case getRangeStartingWith = "GET_RANGE_STARTS_WITH"
		case getRangeSelector = "GET_RANGE_SELECTOR"
		case getReadVersion = "GET_READ_VERSION"
		case getVersionStamp = "GET_VERSIONSTAMP"
		case set = "SET"
		case setReadVersion = "SET_READ_VERSION"
		case clear = "CLEAR"
		case clearRange = "CLEAR_RANGE"
		case clearRangeStartingWith = "CLEAR_RANGE_STARTS_WITH"
		case atomicOperation = "ATOMIC_OP"
		case addReadConflictOnRange = "READ_CONFLICT_RANGE"
		case addWriteConflictOnRange = "WRITE_CONFLICT_RANGE"
		case addReadConflictOnKey = "READ_CONFLICT_KEY"
		case addWriteConflictOnKey = "WRITE_CONFLICT_KEY"
		case disableWriteConflict = "DISABLE_WRITE_CONFLICT"
		case commit = "COMMIT"
		case reset = "RESET"
		case cancel = "CANCEL"
		case getCommittedVersion = "GET_COMMITTED_VERSION"
		case waitFuture = "WAIT_FUTURE"
		case tuplePack = "TUPLE_PACK"
		case tupleUnpack = "TUPLE_UNPACK"
		case tupleRange = "TUPLE_RANGE"
		case tupleSort = "TUPLE_SORT"
		case encodeFloat = "ENCODE_FLOAT"
		case encodeDouble = "ENCODE_DOUBLE"
		case decodeFloat = "DECODE_FLOAT"
		case decodeDouble = "DECODE_DOUBLE"
		case startThread = "START_THREAD"
		case waitEmpty = "WAIT_EMPTY"
		case unitTests = "UNIT_TESTS"
	}
	
	/** The operation this command executes. */
	let operation: Operation
	
	/** The arguments that were passed with the command. */
	let argument: Any?
	
	let direct: Bool
	let snapshot: Bool

	init?(operation: Operation, argument: Any? = nil, direct: Bool = false, snapshot: Bool = false) {
		self.operation = operation
		self.argument = argument
		self.direct = direct
		self.snapshot = snapshot
	}

	/**
		This initializer creates a command from a row in the database.

		The first entry in the tuple must be the command name, and the
		remaining entries in the tuple are the arguments for the command.
	
		If the command name is invalid, or the command does not have the
		required arguments, this will return nil.
	
		- parameter data:	The value from the database.
		*/
	init?(data: Tuple) {
		do {
			var operationName: String = try data.read(at: 0)
			if operationName.hasSuffix("_SNAPSHOT") {
				self.direct = false
				self.snapshot = true
				operationName.removeLast("_SNAPSHOT".count)
			}
			else if operationName.hasSuffix("DATABASE") {
				self.direct = true
				self.snapshot = false
				operationName.removeLast("_DATABASE".count)
			}
			else {
				self.direct = false
				self.snapshot = false
			}
			guard let operation = Operation(rawValue: operationName) else {
				print("Invalid command sent to stack machine")
				print("Command: \(operationName)")
				print("Error: Command name does not match any command")
				return nil
			}
			let argument: Any?
			if operation == .push {
				do {
					argument = try data.readDynamically(at: 1)
				}
				catch {
					print("No argument for push command")
					argument = 0
				}
			}
			else {
				argument = nil
			}
			
			self.operation = operation
			self.argument = argument
		}
		catch {
			print("Invalid command sent to stack machine")
			print("Command: \(data)")
			print("Error: \(error)")
			return nil
		}
	}
}

/**
	This method determines if two commands are equal.

	- parameter lhs:	The first command.
	- parameter rhs:	The second command.
	*/
func ==(lhs: Command, rhs: Command) -> Bool {
	return lhs.operation == rhs.operation
}

extension Tuple {
	func readDynamically(at index: Int) throws -> Any {
		switch(self.type(at: index)) {
		case .some(.string): return try self.read(at: index) as String
		case .some(.byteArray): return try self.read(at: index) as Data
		case .some(.integer):	return try self.read(at: index) as Int
		case .some(.falseValue), .some(.trueValue): return try self.read(at: index) as Bool
		case .some(.float): return try self.read(at: index) as Float
		case .some(.double): return try self.read(at: index) as Double
		case .some(.uuid): return try self.read(at: index) as UUID
		case .some(.tuple): return try self.read(at: index) as Tuple
		case .some(.null), .some(.rangeEnd), .none: throw TupleDecodingError.missingField(index: index)
		}
	}
}
