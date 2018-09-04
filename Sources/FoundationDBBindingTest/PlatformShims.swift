/*
 * PlatformShims.swift
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

#if os(OSX)
#else
	extension Data {
		mutating func append(_ byte: UInt8) {
			self.append(Data(bytes: [byte]))
		}
	}
#endif

#if os(OSX)
	internal func startStackMachineInThread(prefix: Data) {
		let prefixPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: prefix.count + 1)
		prefixPointer.pointee = UInt8(prefix.count)
		prefix.copyBytes(to: prefixPointer.advanced(by: 1), count: prefix.count)
		var thread: pthread_t? = nil
		pthread_create(&thread, nil, startStackMachine, prefixPointer)
	}
	internal func startStackMachine(prefixPointer: UnsafeMutableRawPointer) -> UnsafeMutableRawPointer? {
		let prefixBytes = prefixPointer.assumingMemoryBound(to: UInt8.self)
		let length = prefixBytes.pointee
		let data = Data(bytes: UnsafeRawPointer(prefixPointer.advanced(by: 1)), count: Int(length))
		free(prefixPointer)
		
		guard let prefix = String(data: data, encoding: .utf8) else {
			return nil
		}
		let machine = StackMachine(connection: StackMachine.connection!, commandPrefix: prefix)
		machine.run()
		return nil;
}
#else
	
	internal func startStackMachineInThread(prefix: Data) {
		let prefixPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: prefix.count + 1)
		prefixPointer.pointee = UInt8(prefix.count)
		prefix.copyBytes(to: prefixPointer.advanced(by: 1), count: prefix.count)
		var thread = pthread_t()
		pthread_create(&thread, nil, startStackMachine, prefixPointer)
	}
	internal func startStackMachine(prefixPointer: UnsafeMutableRawPointer?) -> UnsafeMutableRawPointer? {
		guard let prefixPointer = prefixPointer else { return nil }
		let prefixBytes = prefixPointer.assumingMemoryBound(to: UInt8.self)
		let length = prefixBytes.pointee
		let data = Data(bytes: UnsafeRawPointer(prefixPointer.advanced(by: 1)), count: Int(length))
		free(prefixPointer)
		
		guard let prefix = String(data: data, encoding: .utf8) else {
			return nil
		}
		let machine = StackMachine(connection: StackMachine.connection!, commandPrefix: prefix)
		machine.run()
		return nil;
	}
#endif
