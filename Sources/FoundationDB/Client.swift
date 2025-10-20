/*
 * Client.swift
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
import CFoundationDB

/// The main client interface for FoundationDB operations.
///
/// `FDBClient` provides the primary entry point for connecting to and interacting
/// with a FoundationDB cluster. It handles network initialization, database connections,
/// and global configuration settings.
///
/// ## Usage Example
/// ```swift
/// // Initialize the client
/// try await FDBClient.initialize()
///
/// // Open a database connection
/// let database = try FDBClient.openDatabase()
/// ```
// TODO: Remove hard-coded error codes.
public final class FDBClient: Sendable {
    /// FoundationDB API version constants.
    public static let defaultApiVersion: Int = 710

    /// Initializes the FoundationDB client with the specified API version.
    ///
    /// This method must be called before performing any other FoundationDB operations.
    /// It sets up the network layer and starts the network thread.
    ///
    /// - Parameter version: The FoundationDB API version to use. Defaults to the current version.
    /// - Throws: `FDBError` if initialization fails.
    public static func initialize(version: Int = FDBClient.defaultApiVersion) async throws {
        try FDBNetwork.shared.initialize(version: version)
    }

    /// Returns true if FDB network is initialized.
    public static var isInitialized: Bool { FDBNetwork.shared.isInitialized }

    /// Opens a connection to a FoundationDB database.
    ///
    /// Creates and returns a database handle that can be used to create transactions
    /// and perform database operations.
    ///
    /// - Parameter clusterFilePath: Optional path to the cluster file. If nil, uses the default cluster file.
    /// - Returns: An `FDBDatabase` instance for performing database operations.
    /// - Throws: `FDBError` if the database connection cannot be established.
    public static func openDatabase(clusterFilePath: String? = nil) throws -> FDBDatabase {
        var database: OpaquePointer?
        let error = fdb_create_database(clusterFilePath, &database)
        if error != 0 {
            throw FDBError(code: error)
        }

        guard let db = database else {
            throw FDBError(.clientError)
        }

        return FDBDatabase(database: db)
    }

    /// Sets a network option with an optional byte array value.
    ///
    /// - Parameters:
    ///   - option: The network option to set.
    ///   - value: Optional byte array value for the option.
    /// - Throws: `FDBError` if the option cannot be set.
    public static func setNetworkOption(_ option: FDB.NetworkOption, value: [UInt8]? = nil) throws {
        try FDBNetwork.shared.setNetworkOption(option, value: value)
    }

    /// Sets a network option with a string value.
    ///
    /// - Parameters:
    ///   - option: The network option to set.
    ///   - value: String value for the option.
    /// - Throws: `FDBError` if the option cannot be set.
    public static func setNetworkOption(_ option: FDB.NetworkOption, value: String) throws {
        try FDBNetwork.shared.setNetworkOption(option, value: value)
    }

    /// Sets a network option with an integer value.
    ///
    /// - Parameters:
    ///   - option: The network option to set.
    ///   - value: Integer value for the option.
    /// - Throws: `FDBError` if the option cannot be set.
    public static func setNetworkOption(_ option: FDB.NetworkOption, value: Int) throws {
        try FDBNetwork.shared.setNetworkOption(option, value: value)
    }

    // MARK: - Convenience methods for common network options

    /// Enables tracing and sets the trace directory.
    ///
    /// - Parameter directory: The directory where trace files will be written.
    /// - Throws: `FDBError` if tracing cannot be enabled.
    public static func enableTrace(directory: String) throws {
        try setNetworkOption(.traceEnable, value: directory)
    }

    /// Sets the maximum size of trace files before they are rolled over.
    ///
    /// - Parameter sizeInBytes: The maximum size in bytes for trace files.
    /// - Throws: `FDBError` if the trace roll size cannot be set.
    public static func setTraceRollSize(_ sizeInBytes: Int) throws {
        try setNetworkOption(.traceRollSize, value: sizeInBytes)
    }

    /// Sets the trace log group identifier.
    ///
    /// - Parameter logGroup: The log group identifier for trace files.
    /// - Throws: `FDBError` if the trace log group cannot be set.
    public static func setTraceLogGroup(_ logGroup: String) throws {
        try setNetworkOption(.traceLogGroup, value: logGroup)
    }

    /// Sets the format for trace output.
    ///
    /// - Parameter format: The trace format specification.
    /// - Throws: `FDBError` if the trace format cannot be set.
    public static func setTraceFormat(_ format: String) throws {
        try setNetworkOption(.traceFormat, value: format)
    }

    /// Sets a FoundationDB configuration knob.
    ///
    /// Knobs are internal configuration parameters that can be used to tune
    /// FoundationDB behavior.
    ///
    /// - Parameter knobSetting: The knob setting in "name=value" format.
    /// - Throws: `FDBError` if the knob cannot be set.
    public static func setKnob(_ knobSetting: String) throws {
        try setNetworkOption(.knob, value: knobSetting)
    }

    /// Sets the path to the TLS certificate file.
    ///
    /// - Parameter path: The file path to the TLS certificate.
    /// - Throws: `FDBError` if the TLS certificate path cannot be set.
    public static func setTLSCertPath(_ path: String) throws {
        try setNetworkOption(.tlsCertPath, value: path)
    }

    /// Sets the path to the TLS private key file.
    ///
    /// - Parameter path: The file path to the TLS private key.
    /// - Throws: `FDBError` if the TLS key path cannot be set.
    public static func setTLSKeyPath(_ path: String) throws {
        try setNetworkOption(.tlsKeyPath, value: path)
    }

    /// Sets the temporary directory for client operations.
    ///
    /// - Parameter path: The directory path for temporary files.
    /// - Throws: `FDBError` if the temporary directory cannot be set.
    public static func setClientTempDirectory(_ path: String) throws {
        try setNetworkOption(.clientTmpDir, value: path)
    }

    /// Disables client statistics logging.
    ///
    /// - Throws: `FDBError` if client statistics logging cannot be disabled.
    public static func disableClientStatisticsLogging() throws {
        try setNetworkOption(.disableClientStatisticsLogging, value: nil as [UInt8]?)
    }
}
