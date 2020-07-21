/*
 * TupleConvertible.swift
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
	This type describes a value that can be directly encoded as a Tuple.
	*/
public protocol TupleConvertible {
	/** The adapter that describdes the tuple encoding. */
	associatedtype FoundationDBTupleAdapter: TupleAdapter where FoundationDBTupleAdapter.ValueType == Self
}

/**
	This type describes an adapter that can encode and decode a value as a
	FoundationDB Tuple.
	*/
public protocol TupleAdapter {
	/** The type of value that this adapter encodes. */
	associatedtype ValueType
	
	/** The tuple type codes that this adapter can read. */
	static var typeCodes: Set<UInt8> { get }
	
	/**
		This method writes a value into a buffer.

		- parameter value:	The value to write.
		- parameter buffer:	The buffer to write into.
		*/
	static func write(value: ValueType, into buffer: inout Data)
	
	/**
		This method reads a value from a buffer.
	
		The implementation can assume that the type code is valid for this type,
		and that the offset is less than the length of the buffer.

		- parameter buffer:	The buffer to read from.
		- parameter offset:	The offset in the buffer to read from. This will
							be the position of the entry's type code.
		- returns:			The parsed value.
		- throws:			If the data cannot be parsed, this should throw an
							exception.
		*/
	static func read(from buffer: Data, at offset: Int) throws -> ValueType
}

extension TupleAdapter where ValueType: Sequence {
	/**
		This method writes a sequence of bytes into a buffer.

		Any zero bytes in the sequence will be replaced with a zero byte
		followed by 0xFF.

		An additional zero byte will be written at the end of the sequence.
		*/
	public static func write<T: Sequence>(bytes: T, into buffer: inout Data) where T.Iterator.Element == UInt8 {
		for byte in bytes {
			buffer.append(byte)
			if byte == 0 {
				buffer.append(0xFF)
			}
		}
		buffer.append(0x00)
	}
	
	/**
		This method reads a sequence of bytes from a buffer.
	
		Any ocurrence of 0x00 followed by 0xFF will be replaced with 0x00. When
		this encounters a byte of 0x00 that is not followed by 0xFF, it will
		stop reading and return the array.

		- parameter buffer:		The buffer we are reading from.
		- parameter offset:		The position to start reading from. This will
								be the index of the first byte in the sequence.
		- returns:				The decoded bytes.
		*/
	public static func readBytes(from buffer: Data, offset start: Data.Index) -> [UInt8] {
		var bytes = [UInt8]()
		var lastWasNull = false
		for indexOfByte in start ..< buffer.endIndex {
			let value = buffer[indexOfByte]
			if value == 0 && (indexOfByte >= buffer.endIndex - 2 || buffer[indexOfByte + 1] != 0xFF) {
				break
			}
			else if value == 0xFF && lastWasNull {
				lastWasNull = false
				continue
			}
			lastWasNull = value == 0
			bytes.append(value)
		}
		return bytes
	}
}

extension TupleAdapter where ValueType: FixedWidthInteger {
	/**
		The type codes that can represent fixed width integers.
		*/
	public static func integerTypeCodes() -> Set<UInt8> {
		return Set(0x0C ... 0x1C)
	}
	
	public static func write(value: ValueType, into buffer: inout Data) {
		var int = value
		if int < 0 {
			int -= 1
		}

		let maxShift = ValueType.bitWidth - 8
		let byteCount = ValueType.bitWidth / 8
		let bytes = (0..<byteCount).map { (byteIndex: Int) -> UInt8 in
			let byte = (int >> (maxShift - byteIndex * 8)) & 0xFF
			return UInt8(byte)
		}
		let blankByte: UInt8 = (int < 0 ? 0xFF : 0x00)
		let sign = (int < 0 ? -1 : 1)
		let firstRealByte = bytes.firstIndex { $0 != blankByte } ?? bytes.endIndex
		buffer.append(UInt8(20 + sign * (bytes.count - firstRealByte)))
		#if os(OSX)
		buffer.append(contentsOf: bytes[firstRealByte..<bytes.count])
		#else
		buffer.append(Data(bytes: bytes[firstRealByte..<bytes.count]))
		#endif
	}
	
	public static func read(from buffer: Data, at offset: Int) throws -> ValueType {
		var length = Int(buffer[offset]) - 20
		if length == 0 { return 0 }
		
		var value: ValueType = 0
		if length < 0 {
			if ValueType.min == 0 {
				throw TupleDecodingError.negativeValueForUnsignedType
			}
			for _ in 0 ..< (8 + length) {
				value = value << 8 | 0xFF
			}
			length = -1 * length
		}
		if length * 8 > ValueType.bitWidth {
			throw TupleDecodingError.integerOverflow
		}
		for index in (offset + 1) ..< (offset + length + 1) {
			let byte = index < buffer.count ? ValueType(buffer[index]) : 0
			value = value << 8 | byte
		}
		if value < 0 {
			value += 1
		}
		return value
	}
}

extension Int: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = Int
		public static let typeCodes = integerTypeCodes()
	}
}

extension UInt64: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = UInt64
		public static let typeCodes = integerTypeCodes()
	}
}

extension UInt32: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = UInt32
		public static let typeCodes = integerTypeCodes()
	}
}

extension UInt16: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = UInt16
		public static let typeCodes = integerTypeCodes()
	}
}
extension UInt8: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = UInt8
		public static let typeCodes = integerTypeCodes()
	}
}

extension Int64: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = Int64
		public static let typeCodes = integerTypeCodes()
	}
}

extension Int32: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = Int32
		public static let typeCodes = integerTypeCodes()
	}
}

extension Int16: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = Int16
		public static let typeCodes = integerTypeCodes()
	}
}

extension Int8: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = Int8
		public static let typeCodes = integerTypeCodes()
	}
}

extension TupleAdapter where ValueType: BinaryFloatingPoint {
	public static func write<IntegerType: FixedWidthInteger>(int: IntegerType, into buffer: inout Data) {
		let maxShift = IntegerType.bitWidth - 8
		let byteCount = IntegerType.bitWidth / 8
		let sign = int >> maxShift & 0x80
		var bytes = (0..<byteCount).map { (byteIndex: Int) -> UInt8 in
			let byte = (int >> (maxShift - byteIndex * 8)) & 0xFF
			return UInt8(byte)
		}
		if sign > 0 {
			bytes = bytes.map { ~$0 }
		}
		else {
			bytes[0] = bytes[0] ^ 0x80
		}
		buffer.append(contentsOf: bytes)
	}
	
	public static func readBitPattern(from buffer: Data, at offset: Int) throws -> UInt64 {
		var bitPattern: UInt64 = 0
		let byteCount = (1 + ValueType.exponentBitCount + ValueType.significandBitCount) / 8
		var _positive: Bool? = nil
		for byteIndex in offset + 1 ..< offset + 1 + byteCount {
			let byte = buffer[byteIndex]
			let positive = _positive ?? (byte & 0x80 > 0)
			
			if _positive == nil {
				_positive = positive
				bitPattern = UInt64(positive ? byte ^ 0x80 : ~byte)
			}
			else {
				bitPattern = (bitPattern << 8) | UInt64(positive ? byte : ~byte)
			}
		}
		
		return bitPattern
	}
}

extension Float32: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = Float32
		public static let typeCodes = Set([Tuple.EntryType.float.rawValue])
		
		public static func write(value: ValueType, into buffer: inout Data) {
			buffer.append(0x20)
			self.write(int: value.bitPattern, into: &buffer)
		}
		
		public static func read(from buffer: Data, at offset: Int) throws -> ValueType {
			return try ValueType(bitPattern: UInt32(readBitPattern(from: buffer, at: offset)))
		}
	}
}

extension Float64: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public typealias ValueType = Float64
		public static let typeCodes = Set([Tuple.EntryType.double.rawValue])
		
		public static func write(value: ValueType, into buffer: inout Data) {
			buffer.append(0x21)
			self.write(int: value.bitPattern, into: &buffer)
		}
		
		public static func read(from buffer: Data, at offset: Int) throws -> ValueType {
			return try ValueType(bitPattern: readBitPattern(from: buffer, at: offset))
		}
	}
}
extension Data: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public static let typeCodes = Set([Tuple.EntryType.byteArray.rawValue])
		public static func write(value: Data, into buffer: inout Data) {
			buffer.append(Tuple.EntryType.byteArray.rawValue)
			self.write(bytes: value, into: &buffer)
		}
		
		public static func read(from buffer: Data, at offset: Int) -> Data {
			return Data(readBytes(from: buffer, offset: offset + 1))
		}
	}
}

extension String: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public static let typeCodes = Set([Tuple.EntryType.string.rawValue])
		public static func write(value: String, into buffer: inout Data) {
			buffer.append(Tuple.EntryType.string.rawValue)
			self.write(bytes: value.utf8, into: &buffer)
		}
		
		public static func read(from buffer: Data, at offset: Int) throws -> String {
			guard let string = String(bytes: readBytes(from: buffer, offset: offset + 1), encoding: .utf8) else {
				throw TupleDecodingError.invalidString
			}
			return string
		}
	}
}

extension Bool: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public static let typeCodes = Set([Tuple.EntryType.falseValue.rawValue, Tuple.EntryType.trueValue.rawValue])
		public static func write(value: Bool, into buffer: inout Data) {
			buffer.append(value ? Tuple.EntryType.trueValue.rawValue : Tuple.EntryType.falseValue.rawValue)
		}
		
		public static func read(from buffer: Data, at offset: Int) -> Bool {
			return buffer[offset] == Tuple.EntryType.trueValue.rawValue
		}
	}
}

extension UUID: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public static let typeCodes = Set([Tuple.EntryType.uuid.rawValue])
		
		public static func write(value: UUID, into buffer: inout Data) {
			buffer.append(Tuple.EntryType.uuid.rawValue)
			buffer.append(value.uuid.0)
			buffer.append(value.uuid.1)
			buffer.append(value.uuid.2)
			buffer.append(value.uuid.3)
			buffer.append(value.uuid.4)
			buffer.append(value.uuid.5)
			buffer.append(value.uuid.6)
			buffer.append(value.uuid.7)
			buffer.append(value.uuid.8)
			buffer.append(value.uuid.9)
			buffer.append(value.uuid.10)
			buffer.append(value.uuid.11)
			buffer.append(value.uuid.12)
			buffer.append(value.uuid.13)
			buffer.append(value.uuid.14)
			buffer.append(value.uuid.15)
		}
		
		public static func read(from buffer: Data, at offset: Int) throws -> UUID {
			if buffer.count < offset + 17 {
				throw TupleDecodingError.missingUUIDData
			}
			
			return UUID(uuid: (
				buffer[offset+1],
				buffer[offset+2],
				buffer[offset+3],
				buffer[offset+4],
				buffer[offset+5],
				buffer[offset+6],
				buffer[offset+7],
				buffer[offset+8],
				buffer[offset+9],
				buffer[offset+10],
				buffer[offset+11],
				buffer[offset+12],
				buffer[offset+13],
				buffer[offset+14],
				buffer[offset+15],
				buffer[offset+16]
			))
		}
	}
}

extension NSNull: TupleConvertible {
	public struct FoundationDBTupleAdapter: TupleAdapter {
		public static let typeCodes = Set([Tuple.EntryType.null.rawValue])
		public static func read(from buffer: Data, at offset: Int) -> NSNull {
			return NSNull()
		}
		public static func write(value: NSNull, into buffer: inout Data) {
			buffer.append(Tuple.EntryType.null.rawValue)
		}
	}
}

public enum TupleDecodingError: Error {
	/** We tried to read a field beyond the end of the tuple. */
	case missingField(index: Tuple.Index)
	
	/**
		We tried to read a field of a different type than the one
		actually stored.
		*/
	case incorrectTypeCode(index: Tuple.Index, desired: Set<UInt8>, actual: UInt8)
	
	/**
		We tried to read a negative integer into an unsigned type.
		*/
	case negativeValueForUnsignedType
	
	/**
		We tried to read an integer value that was too large for the destination
		type.
		*/
	case integerOverflow
	
	/**
		We read a value that would overflow the bounds of an integer type.
		*/
	
	/**
		We tried to read a string that was not a valid UTF-8 sequence.
		*/
	case invalidString
	
	/**
		We tried to read a UUID that did not have the full UUID data.
		*/
	case missingUUIDData
}
