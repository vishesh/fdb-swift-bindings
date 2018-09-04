/*
 * TupleConstructors.swift
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

extension Tuple {
	/**
		This constructor creates a Tuple from a list of entries.
		*/
	public init<A: TupleConvertible> (_ a: A) {
		self.init()
		self.append(a)
	}
	
	/**
		This constructor creates a Tuple from a list of entries.
		*/
	public init<A: TupleConvertible, B: TupleConvertible> (_ a: A, _ b: B) {
		self.init()
		self.append(a)
		self.append(b)
	}
	
	/**
		This constructor creates a Tuple from a list of entries.
		*/
	public init<A: TupleConvertible, B: TupleConvertible, C: TupleConvertible> (_ a: A, _ b: B, _ c: C) {
		self.init()
		self.append(a)
		self.append(b)
		self.append(c)
	}
	
	/**
	This constructor creates a Tuple from a list of entries.
	*/
	public init<A: TupleConvertible, B: TupleConvertible, C: TupleConvertible, D: TupleConvertible> (_ a: A, _ b: B, _ c: C, _ d: D) {
		self.init()
		self.append(a)
		self.append(b)
		self.append(c)
		self.append(d)
	}
	
	/**
	This constructor creates a Tuple from a list of entries.
	*/
	public init<A: TupleConvertible, B: TupleConvertible, C: TupleConvertible, D: TupleConvertible, E: TupleConvertible> (_ a: A, _ b: B, _ c: C, _ d: D, _ e: E) {
		self.init()
		self.append(a)
		self.append(b)
		self.append(c)
		self.append(d)
		self.append(e)
	}
	
	/**
	This constructor creates a Tuple from a list of entries.
	*/
	public init<A: TupleConvertible, B: TupleConvertible, C: TupleConvertible, D: TupleConvertible, E: TupleConvertible, F: TupleConvertible> (_ a: A, _ b: B, _ c: C, _ d: D, _ e: E, _ f: F) {
		self.init()
		self.append(a)
		self.append(b)
		self.append(c)
		self.append(d)
		self.append(e)
		self.append(f)
	}
}
