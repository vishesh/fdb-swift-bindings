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
import CFoundationDB

/**
	This enum provides a namespace for functions that hack around inconsistent
	behavior between platforms.
	*/
enum PlatformShims {
	internal static var environment: [String:String] {
		#if os(OSX)
			return ProcessInfo().environment
		#else
			return [:]
		#endif
	}
}


#if os(OSX)
#else
	extension Data {
		mutating func append(_ byte: UInt8) {
			append(Data(bytes: [byte]))
		}
		
		mutating func reserveCapacity(_ capacity: Int) {
			
		}
		
		var hashValue: Int {
			return NSData(data: self).hashValue;
		}
	}
	
	extension OperationQueue {
		func addOperation(_ block: @escaping () -> Void) {
			addOperation(BlockOperation(block: block))
		}
	}
	
	extension Date {
		func addingTimeInterval(_ interval: TimeInterval) -> Date {
			return Date(timeInterval: interval, since: self)
		}
	}
#endif

#if os(OSX)
	internal func fdb_run_network_in_thread() {
		var thread: pthread_t? = nil
		pthread_create(&thread, nil, fdb_run_network_wrapper, nil)
	}
	internal func fdb_run_network_wrapper(_: UnsafeMutableRawPointer) -> UnsafeMutableRawPointer? {
		fdb_run_network()
		return nil
	}
#else
	internal func fdb_run_network_in_thread() {
		var thread: pthread_t = pthread_t()
		pthread_create(&thread, nil, fdb_run_network_wrapper, nil)
	}
	internal func fdb_run_network_wrapper(_: UnsafeMutableRawPointer?) -> UnsafeMutableRawPointer? {
		fdb_run_network()
		return nil
	}
#endif
