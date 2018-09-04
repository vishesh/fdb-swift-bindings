/*
 * KeySelector.swift
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
	This type provides a selector for specifying keys in a range read.

	Key ranges are specified relative to an anchor key, and allow finding the
	first key greater than or less than that key. They also allow specifying
	offsets to say that we should skip a certain number of keys forward or
	backward before finding the real key for the range.
	*/
public struct KeySelector {
	/** The value that the selector is anchored around. */
	let anchor: DatabaseValue
	
	/** The offset from the first matching value. */
	let offset: Int32
	
	/** Whether we should also match on the anchor value. */
	let orEqual: Int32
	
	/**
		This initializer creates a key selector from the constituent parts, as
		understood by the FoundationDB C API.

		- parameter anchor:		The reference point for the selector.
		- parameter orEqual:	Whether we should allow the selector to match
								the anchor. For positive offsets, 1 means false
								and 0 means true. For zero and negative offsets,
								1 means true and 0 means false.
		- parameter offset:		The number of steps we should skip forward or
								backward from the first matching key to find the
								returned key. This also encodes the direction
								of the comparison. A positive offset means a
								greater than comparison, skipping forward by
								`offset - 1`. A zero or negative offset means a
								less than comparison, skipping backward by
								`-1 * offset`.
		*/
	public init(anchor: DatabaseValue, orEqual: Int, offset: Int) {
		self.anchor = anchor
		self.orEqual = Int32(orEqual)
		self.offset = Int32(offset)
	}
	
	/**
		This initializer creates a selector for finding keys greater than or
		equal to a given key.

		- parameter value:		The anchor key.
		- parameter orEqual:	Whether we should include the anchor key.
		- parameter offset:		The number of keys that we should skip forward.
		*/
	public init(greaterThan value: DatabaseValue, orEqual: Bool = false, offset: Int = 0) {
		self.anchor = value
		self.offset = 1 + Int32(offset)
		self.orEqual = orEqual ? 0 : 1
	}
	
	
	/**
		This initializer creates a selector for finding keys less than or
		equal to a given key.
		
		- parameter value:		The anchor key.
		- parameter orEqual:	Whether we should include the anchor key.
		- parameter offset:		The number of keys that we should skip backward.
		*/
	public init(lessThan value: DatabaseValue, orEqual: Bool = false, offset: Int = 0) {
		self.anchor = value
		self.offset = Int32(-1 * offset)
		self.orEqual = orEqual ? 1 : 0
	}
}
