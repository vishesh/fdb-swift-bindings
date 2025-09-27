/*
 * Tuple.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2025 Apple Inc. and the FoundationDB project authors
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

public enum TupleError: Error, Sendable {
    case invalidTupleElement
    case invalidEncoding
    case invalidDecoding(String)
    case unsupportedType
}

public enum TupleTypeCode: UInt8, CaseIterable {
    case null = 0x00
    case bytes = 0x01
    case string = 0x02
    case nested = 0x05
    case intZero = 0x14
    case positiveIntEnd = 0x1D
    case negativeIntStart = 0x1B
    case float = 0x20
    case double = 0x21
    case boolFalse = 0x26
    case boolTrue = 0x27
    case uuid = 0x30
    case versionstamp = 0x33
}

public protocol TupleElement: Sendable {
    func encodeTuple() -> Fdb.Bytes
    static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> Self
}

public struct Tuple: Sendable {
    private let elements: [any TupleElement]

    public init(_ elements: any TupleElement...) {
        self.elements = elements
    }

    public init(_ elements: [any TupleElement]) {
        self.elements = elements
    }

    public subscript(index: Int) -> (any TupleElement)? {
        guard index >= 0 && index < elements.count else { return nil }
        return elements[index]
    }

    public var count: Int {
        return elements.count
    }

    public func encode() -> Fdb.Bytes {
        var result = Fdb.Bytes()
        for element in elements {
            result.append(contentsOf: element.encodeTuple())
        }
        return result 
    }

    public static func decode(from bytes: Fdb.Bytes) throws -> [any TupleElement] {
        var elements: [any TupleElement] = []
        var offset = 0

        while offset < bytes.count {
            let typeCode = bytes[offset]
            offset += 1

            switch typeCode {
            case TupleTypeCode.null.rawValue:
                elements.append(TupleNil())
            case TupleTypeCode.bytes.rawValue:
                let element = try Fdb.Bytes.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.string.rawValue:
                let element = try String.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.boolFalse.rawValue, TupleTypeCode.boolTrue.rawValue:
                let element = try Bool.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.float.rawValue:
                let element = try Float.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.double.rawValue:
                let element = try Double.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.uuid.rawValue:
                let element = try UUID.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            default:
                throw TupleError.invalidDecoding("Unknown type code: \(typeCode)")
            }
        }

        return elements
    }
}

public struct TupleNil: TupleElement {
    public func encodeTuple() -> Fdb.Bytes {
        return [TupleTypeCode.null.rawValue]
    }

    public static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> TupleNil {
        return TupleNil()
    }
}

extension String: TupleElement {
    public func encodeTuple() -> Fdb.Bytes {
        var encoded = [TupleTypeCode.string.rawValue]
        let utf8Bytes = Array(self.utf8)

        for byte in utf8Bytes {
            if byte == 0x00 {
                encoded.append(contentsOf: [0x00, 0xFF])
            } else {
                encoded.append(byte)
            }
        }
        encoded.append(0x00)
        return encoded
    }

    public static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> String {
        var decoded = Fdb.Bytes()

        while offset < bytes.count {
            let byte = bytes[offset]
            offset += 1

            if byte == 0x00 {
                if offset < bytes.count && bytes[offset] == 0xFF {
                    offset += 1
                    decoded.append(0x00)
                } else {
                    break
                }
            } else {
                decoded.append(byte)
            }
        }

        return String(bytes: decoded, encoding: .utf8)!
    }
}

extension Fdb.Bytes: TupleElement {
    public func encodeTuple() -> Fdb.Bytes {
        var encoded = [TupleTypeCode.bytes.rawValue]
        for byte in self {
            if byte == 0x00 {
                encoded.append(contentsOf: [0x00, 0xFF])
            } else {
                encoded.append(byte)
            }
        }
        encoded.append(0x00)
        return encoded
    }

    public static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> Fdb.Bytes {
        var decoded = Fdb.Bytes()

        while offset < bytes.count {
            let byte = bytes[offset]
            offset += 1

            if byte == 0x00 {
                if offset < bytes.count && bytes[offset] == 0xFF {
                    offset += 1
                    decoded.append(0x00)
                } else {
                    break
                }
            } else {
                decoded.append(byte)
            }
        }

        return decoded
    }
}

extension Bool: TupleElement {
    public func encodeTuple() -> Fdb.Bytes {
        return self ? [TupleTypeCode.boolTrue.rawValue] : [TupleTypeCode.boolFalse.rawValue]
    }

    public static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> Bool {
        guard offset > 0 else { throw TupleError.invalidDecoding("Bool decoding requires type code") }
        let typeCode = bytes[offset - 1]

        switch typeCode {
        case TupleTypeCode.boolTrue.rawValue:
            return true
        case TupleTypeCode.boolFalse.rawValue:
            return false
        default:
            throw TupleError.invalidDecoding("Invalid bool type code: \(typeCode)")
        }
    }
}

extension Float: TupleElement {
    public func encodeTuple() -> Fdb.Bytes {
        var encoded = [TupleTypeCode.float.rawValue]
        let bitPattern = self.bitPattern
        let bytes = withUnsafeBytes(of: bitPattern.bigEndian) { Array($0) }
        encoded.append(contentsOf: bytes)
        return encoded
    }

    public static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> Float {
        guard offset + 4 <= bytes.count else {
            throw TupleError.invalidDecoding("Not enough bytes for Float")
        }

        let floatBytes = Array(bytes[offset..<offset + 4])
        offset += 4

        let bigEndianValue = floatBytes.withUnsafeBytes { bytes in
            bytes.load(as: UInt32.self)
        }
        let bitPattern = UInt32(bigEndian: bigEndianValue)
        return Float(bitPattern: bitPattern)
    }
}

extension Double: TupleElement {
    public func encodeTuple() -> Fdb.Bytes {
        var encoded = [TupleTypeCode.double.rawValue]
        let bitPattern = self.bitPattern
        let bytes = withUnsafeBytes(of: bitPattern.bigEndian) { Array($0) }
        encoded.append(contentsOf: bytes)
        return encoded
    }

    public static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> Double {
        guard offset + 8 <= bytes.count else {
            throw TupleError.invalidDecoding("Not enough bytes for Double")
        }

        let doubleBytes = Array(bytes[offset..<offset + 8])
        offset += 8

        let bigEndianValue = doubleBytes.withUnsafeBytes { bytes in
            bytes.load(as: UInt64.self)
        }
        let bitPattern = UInt64(bigEndian: bigEndianValue)
        return Double(bitPattern: bitPattern)
    }
}

extension UUID: TupleElement {
    public func encodeTuple() -> Fdb.Bytes {
        var encoded = [TupleTypeCode.uuid.rawValue]
        let (u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11, u12, u13, u14, u15, u16) = self.uuid
        encoded.append(contentsOf: [u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11, u12, u13, u14, u15, u16])
        return encoded
    }

    public static func decodeTuple(from bytes: Fdb.Bytes, at offset: inout Int) throws -> UUID {
        guard offset + 16 <= bytes.count else {
            throw TupleError.invalidDecoding("Not enough bytes for UUID")
        }

        let uuidBytes = Array(bytes[offset..<offset + 16])
        offset += 16

        let uuidTuple = (uuidBytes[0], uuidBytes[1], uuidBytes[2], uuidBytes[3],
                        uuidBytes[4], uuidBytes[5], uuidBytes[6], uuidBytes[7],
                        uuidBytes[8], uuidBytes[9], uuidBytes[10], uuidBytes[11],
                        uuidBytes[12], uuidBytes[13], uuidBytes[14], uuidBytes[15])

        return UUID(uuid: uuidTuple)
    }
}
