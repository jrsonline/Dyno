//
//  DynoConnection.swift
//  Dyno
//
//  Created by strictlyswift on 3-May-19.
//

import Foundation
import PythonKit

/// A type which can be stored in Dyno
public indirect enum DynoObject : Hashable {
    case bool(Bool)
    case string(String)
    case number(Double)
    case numberSet([Double])
    case stringSet([String])
    case binary(Data, String.Encoding)
    case binarySet([Data], String.Encoding)
    case null
    case list([DynoObject])
    case map(Dictionary<String,DynoObject>)
}

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
    func getItem(from table: String, key:(String, DynoObject)) -> DynoResult<Dictionary<String, DynoObject>>
    
    /// Scans a table in a DynamoDB instance. You can pass in parameters in the arguments to add filters.
    ///
    /// - Parameters:
    ///   - table: Table name
    ///   - args: Arguments - see documentation
    /// - Returns: .success with the PythonObject dictionary result, or .failure if the result could not be obtained (or the connection is invalid)
    func scan(table: String, filter: DynoFilter?, scanLimit: Int) -> DynoResult<[Dictionary<String, DynoObject>]>

    /// Sets an item into a table
    ///
    /// - Parameters:
    ///   - table: The table to write to
    ///   - args: The item to write
    /// - Returns: .success if value was written properly, or .failure otherwise.
    func setItem(into table: String, withItemInfo args: Dictionary<String,DynoObject>) -> DynoResult<Dictionary<String,DynoObject>>
}
