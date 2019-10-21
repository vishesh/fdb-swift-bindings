/*
 * Tuple.swift
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
	This type describes a tuple of values that are part of a key or a value in
	the database.
	*/
public struct Tuple: Equatable, Hashable, Comparable {
	/** The raw data that is stored in the database. */
	internal private(set) var data: Data
	
	/** The offsets in the data where each field in the tuple starts. */
	fileprivate var offsets: [Int]
	
	/** The indices that can be used to read entries from the tuple. */
	public typealias Index = Array<Int>.Index
	
	/**
	The types of entries that can be stored in the tuple.
	*/
	public enum EntryType: UInt8, Equatable {
		case null = 0x00
		case byteArray = 0x01
		case string = 0x02
		case tuple = 0x05
		case integer = 0x0C
		case float = 0x20
		case double = 0x21
		case falseValue = 0x26
		case trueValue = 0x27
		case uuid = 0x30
		case rangeEnd = 0xFF
		
		fileprivate init(headerCode: UInt8) {
			if headerCode >= EntryType.integer.rawValue && headerCode <= EntryType.integer.rawValue + 0x10 {
				self = .integer
			}
			else if let type = EntryType(rawValue: headerCode) {
				self = type
			}
			else {
				fatalError("Undefined Tuple type \(headerCode)")
			}
		}
	}
	
	/**
		This initializer creates an empty tuple.
		*/
	public init() {
		self.data = Data()
		self.offsets = []
	}
	
	/**
		This initializer creates a tuple from raw data from the database.
		
		This is only intended to be used internally when deserializing data.
		*/
	internal init(rawData: Data) {
		let (offsets, _) = Tuple.readOffsets(from: rawData, at: rawData.startIndex, nested: false)
		self.init(data: rawData, offsets: offsets)
	}
	
	fileprivate init(data: Data, offsets: [Int]) {
		self.data = data
		self.offsets = offsets
	}
	
	/**
		This method adds a value to the tuple.
	
		- parameter string:		The string to add.
		*/
	public mutating func append<ValueType: TupleConvertible> (_ value: ValueType) {
		self.offsets.append(self.data.count)
		ValueType.FoundationDBTupleAdapter.write(value: value, into: &self.data)
	}
	
	/**
		This method adds the entries from another tuple to this tuple.
	
		- parameter string:		The string to add.
		*/
	public mutating func append(contentsOf tuple: Tuple) {
		self.offsets.append(contentsOf: tuple.offsets)
		self.data.append(contentsOf: tuple.data)
	}
	
	/**
		This method adds a null byte to the tuple.
		*/
	public mutating func appendNullByte() {
		self.offsets.append(self.data.count)
		self.data.append(EntryType.null.rawValue)
	}
	
	/**
		This method gets the tuple with this tuple's data, but with a 0xFF byte
		on the end.
		
		This can be useful in range queries. If you want a range to include all
		valid tuples that start with this tuple's data, you can have the start
		of the range be this tuple and the end of the range be the copy with the
		range byte added.
		*/
	public mutating func appendRangeEndByte() {
		self.offsets.append(self.data.count)
		self.data.append(0xFF)
	}
	
	/**
		This method gets the tuple that comes immediately after this one,
		lexographically.
		*/
	public func appendingNullByte() -> Tuple {
		var result = self
		result.appendNullByte()
		return result
	}
	
	/**
		The number of entries in the tuple.
		*/
	public var count: Int {
		return self.offsets.count
	}
	
	/**
		This method gets the type of a field in this tuple.
		
		If the index is outside the bounds of the tuple, this will return nil.
		
		- parameter index:		The field we want to read.
		- returns:				The type of the field.
		*/
	public func type(at index: Index) -> EntryType? {
		if index >= offsets.startIndex && index < offsets.endIndex {
			let byte = self.data[Int(offsets[index])]
			return EntryType(headerCode: byte)
		}
		else {
			return nil
		}
	}
	
	/**
		This method reads a value from the tuple.
	
		If the index is outside our bounds, this will throw a `ParsingError`.
		If the entry at that index is of a different type, this will throw a
		`ParsingError`.
	
		- parameter index:		The index of the entry we want to read.
		- returns:				The value at that entry.
		- throws:				A `ParsingError` explaining why we can't read
								this entry.
		*/
	public func read<ValueType: TupleConvertible>(at index: Int) throws -> ValueType {
		let allowedCodes = ValueType.FoundationDBTupleAdapter.typeCodes
		let typeCode: UInt8
		let offset: Int
		if index >= offsets.startIndex && index < offsets.endIndex {
			offset = Int(offsets[index])
			typeCode = self.data[offset]
		}
		else {
			throw TupleDecodingError.missingField(index: index)
		}
		if !allowedCodes.contains(typeCode) {
			throw TupleDecodingError.incorrectTypeCode(index: index, desired: allowedCodes, actual: typeCode)
		}
		return try ValueType.FoundationDBTupleAdapter.read(from: self.data, at: offset)
	}
	
	/**
		This method reads a range of values as a sub-tuple.
	
		If the range is outside our bounds, this will throw a `ParsingError`.
		If the entry at that index is of a different type, this will throw a
		`ParsingError`.
	
		- parameter range:		The range of values we want to read.
		- returns:				The values at that entry.
		- throws:				A `ParsingError` explaining why we can't read
								these entries.
		*/
	public func read(range: CountableRange<Int>) throws -> Tuple {
		if range.lowerBound < 0 {
			throw TupleDecodingError.missingField(index: range.lowerBound)
		}
		if range.upperBound > self.offsets.count {
			throw TupleDecodingError.missingField(index: range.upperBound)
		}
		if range.upperBound == self.offsets.count {
			return Tuple(rawData: self.data[self.offsets[range.lowerBound] ..< self.data.count])
		}
		else {
			return Tuple(rawData: self.data[self.offsets[range.lowerBound] ..< self.offsets[range.upperBound]])
		}
	}
	
	/**
		This method gets a range containing all tuples that have this tuple as
		a prefix.
		
		If this tuple contains the entries "test" and "key", this will include
		the tuple ("test", "key"), and ("test", "key", "foo"), but not
		("test", "keys").
		
		The upper bound of this range will be a special tuple that should not
		be used for anything other than as the upper bound of a range.
		*/
	public var childRange: Range<Tuple> {
		var start = self
		start.offsets.append(start.data.count)
		start.data.append(0x00)
		var end = self
		end.offsets.append(end.data.count)
		end.data.append(0xFF)
		return start ..< end
	}
	
	/**
		This method determines if this tuple has another as a prefix.
		
		This is true whenever the raw data for this tuple begins with the same
		bytes as the raw data for the other tuple.
		
		- parameter prefix:		The tuple we are checking as a possible prefix.
		- returns:				Whether this tuple has the other tuple as its
								prefix.
		*/
	public func hasPrefix(_ prefix: Tuple) -> Bool {
        if prefix.data.count > self.data.count { return false }
        for index in 0..<prefix.data.count {
            if data[data.index(data.startIndex, offsetBy: index)] != prefix.data[prefix.data.index(prefix.data.startIndex, offsetBy: index)] { return false }
        }
        return true
	}
	
	/**
		This method gets the hash code for this tuple.
		*/
	public var hashValue: Int {
		return data.hashValue
	}
	
	/**
		This method increments the last entry in the tuple.
		
		For an integer entry, this will perform a simple integer increment. If
		the number exceeds the maximum for the number of bytes we used to store
		the original number, this will overflow and reset it to zero.
		
		For a string or data entry, this will increment the byte values for the
		contents of the entry.
		
		For all other entries, this will do nothing.
		*/
	public mutating func incrementLastEntry() {
		guard self.data.count > 0 else { return }
		let incrementRange: CountableRange<Int>
		guard let type = self.type(at: self.count - 1) else { return }
		switch(type) {
		case .integer:
			incrementRange = self.offsets[self.count - 1] + 1 ..< self.data.endIndex
		case .string, .byteArray:
			incrementRange = self.offsets[self.count - 1] + 1 ..< self.data.endIndex - 1
		case .null, .rangeEnd, .trueValue, .falseValue, .uuid, .float, .double, .tuple:
			return
		}
		data.withUnsafeMutableBytes {
			(bytes: UnsafeMutablePointer<UInt8>) in
			for index in incrementRange.reversed() {
				let pointer = bytes.advanced(by: index)
				if pointer.pointee == 255 {
					pointer.pointee = 0
				}
				else {
					pointer.pointee += 1
					break
				}
			}
		}
	}
	
	fileprivate static func readOffsets(from data: Data, at start: Data.Index, nested: Bool) -> ([Int], Int) {
		var offsets: [Int] = []
		var currentType = EntryType.null
		var entryBytesRemaining: UInt8 = 0
		var indexOfByte = start
		while indexOfByte < data.endIndex {
			let byte = data[indexOfByte]
			if currentType == .null && entryBytesRemaining == 0 {
				currentType = EntryType(headerCode: byte)
				if currentType == .null && nested {
					if indexOfByte + 1 >= data.count || data[indexOfByte + 1] != 0xFF {
						return (offsets, indexOfByte)
					}
				}
				offsets.append(indexOfByte)
				if currentType == .integer {
					entryBytesRemaining = UInt8(abs(Int(byte) - 20))
					if entryBytesRemaining == 0 {
						currentType = .null
					}
				}
				else if currentType == .uuid {
					entryBytesRemaining = 16
				}
				else if currentType == .float {
					entryBytesRemaining = 4
				}
				else if currentType == .double {
					entryBytesRemaining = 8
				}
				else if currentType == .trueValue || currentType == .falseValue {
					currentType = .null
				}
				else if currentType == .null {
					entryBytesRemaining = nested ? 1 : 0
				}
				else if currentType == .tuple {
					let (_, endByte) = Tuple.readOffsets(from: data, at: indexOfByte + 1, nested: true)
					indexOfByte = endByte
					currentType = .null
				}
			}
			else {
				switch(currentType) {
				case .rangeEnd, .trueValue, .falseValue, .tuple:
					currentType = .null
				case .string, .byteArray:
					if byte == 0x00 {
						if indexOfByte >= data.endIndex - 2 || data[indexOfByte + 1] != 0xFF {
							currentType = .null
						}
					}
				case .integer, .uuid, .double, .float, .null:
					if entryBytesRemaining > 0 {
						entryBytesRemaining -= 1
					}
					if entryBytesRemaining == 0 {
						currentType = .null
					}
				}
			}
			indexOfByte += 1
		}
		return (offsets, data.count)
	}
}

extension Tuple: CustomStringConvertible {
	/**
	This method gets a human-readable description of the tuple's contents.
	*/
	public var description: String {
		var result = "("
		for index in 0..<count {
			let entry: String?
			guard let type = self.type(at: index) else { continue }
			switch(type) {
			case .null: entry = "\\x00"
			case .rangeEnd: entry = "\\xFF"
			case .byteArray: entry = (try? self.read(at: index) as Data)?.description
			case .string: entry = try? self.read(at: index) as String
			case .float: entry = try? (self.read(at: index) as Float).description
			case .double: entry = try? (self.read(at: index) as Double).description
			case .trueValue: entry = "true"
			case .falseValue: entry = "false"
			case .uuid: entry = "Unimplemented"
			case .tuple: entry = try? (self.read(at: index) as Tuple).description
			case .integer:
				let length = Int(self.data[self.offsets[index]]) - 20
				if length < 0 { entry = (try? self.read(at: index) as Int)?.description }
				else {	entry = (try? self.read(at: index) as UInt64)?.description}
			}
			if index > 0 { result.append(", ") }
			if let _entry = entry { result.append(_entry) }
		}
		result.append(")")
		return result
	}
}

/**
	This method determines if two tuples are equal.

	- parameter lhs:		The first tuple.
	- parameter rhs:		The second tuple.
	*/
public func ==(lhs: Tuple, rhs: Tuple) -> Bool {
	return lhs.data == rhs.data
}

/**
	This method gets an ordering for two tuples.

	The tuples will be compared based on the bytes in their raw data.

	- parameter lhs:		The first tuple in the comparison.
	- parameter rhs:		The second tuple in the comparison.
	- returns:				The comparison result
	*/
public func <(lhs: Tuple, rhs: Tuple) -> Bool {
	return lhs.data.lexicographicallyPrecedes(rhs.data)
}

extension Tuple: DatabaseValueConvertible {
	public init(databaseValue: DatabaseValue) {
		self.init(rawData: databaseValue.data)
	}
	public var databaseValue: DatabaseValue {
		return DatabaseValue(data)
	}
}

extension Tuple: TupleConvertible {
	public final class FoundationDBTupleAdapter: TupleAdapter {
		public static let typeCodes = Set<UInt8>([0x05])
		public static func read(from buffer: Data, at offset: Int) -> Tuple {
			var (offsets, endByte) = Tuple.readOffsets(from: buffer, at: offset+1, nested: true)
			var nestedData = Data(buffer[offset+1..<endByte])
			var bytesRemoved = 0
			for indexOfEntry in (0..<offsets.count) {
				let entryOffset = offsets[indexOfEntry] - bytesRemoved - offset - 1
				if nestedData[entryOffset] == 0x00 {
					bytesRemoved += 1
					nestedData.remove(at: entryOffset + 1)
				}
				offsets[indexOfEntry] = entryOffset
			}
			return Tuple(data: nestedData, offsets: offsets)
		}
		
		public static func write(value: Tuple, into buffer: inout Data) {
			buffer.append(0x05)
			for index in 0..<value.count {
				let offset = value.offsets[index]
				let nextOffset = (index == value.count - 1 ? value.data.count : value.offsets[index + 1])
				buffer.append(value.data[offset ..< nextOffset])
				if value.data[offset] == 0x00 {
					buffer.append(0xFF)
				}
			}
			buffer.append(0x00)
		}
	}
}
