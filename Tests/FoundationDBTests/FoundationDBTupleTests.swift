/*
 * FoundationDBTupleTests.swift
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

import Testing
import Foundation

@testable import FoundationDB

@Test("TupleNil encoding and decoding")
func testTupleNil() throws {
    let tupleNil = TupleNil()
    let encoded = tupleNil.encodeTuple()
    #expect(encoded == [TupleTypeCode.null.rawValue], "TupleNil should encode to null type code")

    var offset = 1
    let decoded = try TupleNil.decodeTuple(from: encoded, at: &offset)
    #expect(decoded is TupleNil, "Should decode back to TupleNil")
    #expect(offset == 1, "Offset should not advance for TupleNil")
}

@Test("TupleString encoding and decoding")
func testTupleString() throws {
    let testString = "Hello, World!"
    let encoded = testString.encodeTuple()

    #expect(encoded.first == TupleTypeCode.string.rawValue, "TupleString should start with string type code")
    #expect(encoded.last == 0x00, "TupleString should end with null terminator")

    var offset = 1
    let decoded = try String.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testString, "Should decode back to original string")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleString with null bytes")
func testTupleStringWithNulls() throws {
    let testString = "Hello\u{0}World"
    let encoded = testString.encodeTuple()

    #expect(encoded.contains(0x00), "Encoded string should contain null bytes")
    #expect(encoded.contains(0xFF), "Null bytes should be escaped with 0xFF")

    var offset = 1
    let decoded = try String.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testString, "Should decode back to original string with nulls")
}

@Test("TupleBool encoding and decoding")
func testTupleBool() throws {
    let testTrue = true
    let testFalse = false

    let encodedTrue = testTrue.encodeTuple()
    let encodedFalse = testFalse.encodeTuple()

    #expect(encodedTrue == [TupleTypeCode.boolTrue.rawValue], "true should encode to boolTrue type code")
    #expect(encodedFalse == [TupleTypeCode.boolFalse.rawValue], "false should encode to boolFalse type code")

    var offsetTrue = 1
    var offsetFalse = 1

    let decodedTrue = try Bool.decodeTuple(from: encodedTrue, at: &offsetTrue)
    let decodedFalse = try Bool.decodeTuple(from: encodedFalse, at: &offsetFalse)

    #expect(decodedTrue == true, "Should decode back to true")
    #expect(decodedFalse == false, "Should decode back to false")
}

@Test("TupleFloat encoding and decoding")
func testTupleFloat() throws {
    let testFloat: Float = 3.14159
    let encoded = testFloat.encodeTuple()

    #expect(encoded.first == TupleTypeCode.float.rawValue, "Float should start with float type code")
    #expect(encoded.count == 5, "Float should be 5 bytes (1 type code + 4 data)")

    var offset = 1
    let decoded = try Float.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testFloat, "Should decode back to original float")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleDouble encoding and decoding")
func testTupleDouble() throws {
    let testDouble: Double = 3.141592653589793
    let encoded = testDouble.encodeTuple()

    #expect(encoded.first == TupleTypeCode.double.rawValue, "Double should start with double type code")
    #expect(encoded.count == 9, "Double should be 9 bytes (1 type code + 8 data)")

    var offset = 1
    let decoded = try Double.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testDouble, "Should decode back to original double")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleUUID encoding and decoding")
func testTupleUUID() throws {
    let testUUID = UUID()
    let encoded = testUUID.encodeTuple()

    #expect(encoded.first == TupleTypeCode.uuid.rawValue, "UUID should start with uuid type code")
    #expect(encoded.count == 17, "UUID should be 17 bytes (1 type code + 16 data)")

    var offset = 1
    let decoded = try UUID.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testUUID, "Should decode back to original UUID")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}
