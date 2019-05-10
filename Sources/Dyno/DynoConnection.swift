//
//  DynoConnection.swift
//  Dyno
//
//  Created by strictlyswift on 3-May-19.
//

import Foundation
import PythonKit

/// Abstract connection to a DynamoDB instance
public protocol DynoConnection {
    /// Is this a valid connection - ie can we use it to actually get data from DynamoDB ?
    func isValid() -> Bool
    
    /// What was the last error on the connection, if any
    func lastError() -> DynoError?
    
    /// Tries to reconnect to a remote server. Note that this may make invalid connections
    /// valid -- or vice versa (eg if a connection was dropped)
    ///
    /// - Returns: True if reconnect was successful.
    mutating func tryReconnect() -> Bool
    
    /// Returns an item from a DynamoDB instance, given a key
    ///
    /// - Parameters:
    ///   - from: Named entity (eg table) to retrieve key from
    ///   - key: Key field, and value of key field, to search for and return.
    /// - Returns: Object representing a dictionary of values, or .failure if the value could not be retrieved.
    func getItem(from: String, key:[String: PythonObject]) -> DynoResult<PythonObject>
    
    /// Scans a table in a DynamoDB instance. You can pass in parameters in the arguments to add filters.
    ///
    /// - Parameters:
    ///   - table: Table name
    ///   - args: Arguments - see documentation
    /// - Returns: .success with the PythonObject dictionary result, or .failure if the result could not be obtained (or the connection is invalid)
    func scan(table: String, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject>
    
    
    /// Sets an item into a table
    ///
    /// - Parameters:
    ///   - table: The table to write to
    ///   - args: The item to write
    /// - Returns: .success if value was written properly, or .failure otherwise.
    func setItem(into table: String, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject>
}

/// Options for the Dyno connection
public struct DynoOptions {
    let addVersioning : Bool
    let timeout : Int
    let pageSize : Int?
    let log : Bool
    let dummyUrl : Bool
    /// Options for the Dyno connection
    ///
    /// - Parameters:
    ///   - addVersioning: **Currently unsupported** Defaults to False
    ///   - timeout: Number of seconds before the connection forcibly times out. Defaults to 5. **Note** you should set this to <30 otherwise Boto3 will time out instead.
    ///   - pageSize: Use this to limit the size of pages (ie number of rows) returned by large queries (eg scan). Dyno will automatically concatenate pages together into a full result set, so this is probably not much use right now. Defaults to `nil`, which means the Boto3 default of 1MB pages.
    ///   - log: Set to **true** to log onto standard output.
    public init(addVersioning : Bool = false,
                timeout: Int = 5,
                pageSize: Int? = nil,
                log: Bool = false,
                dummyUrl : Bool = false) {
        self.addVersioning = addVersioning
        self.timeout = timeout
        self.pageSize = pageSize
        self.log = log
        self.dummyUrl = dummyUrl
    }
}
