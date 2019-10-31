//
//  ActionSetItem.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import PythonKit
import RxSwift
import Combine
import PythonCodable

/// Represents a "Set item with key" action
struct DynoSetItem<T> : DynoBotoAction {
    let table: String
    let value: T
    let writing: (T) -> DynoResult<Dictionary<String,DynoObject>>
    let options: DynoOptions
    var logName : String { get { return "\(T.self) Setter"} }

    func perform(connection: DynoConnection) -> DynoResult<T> {
        guard case let .success(dict) = writing(value)  else { return .failure(DynoError("Could not build object to write"))}
        if options.log { NSLog("DynoSetItem [\(logName)] Putting item \(value) in table \(table)") }
        
        return connection.setItem(into:self.table, withItemInfo: dict).map { _ in value }
    }
}


extension Dyno {
    
    /// Creates or updates an item.  The item data is assumed to include the key field (eg "id"), so DynamoDB
    /// knows which item to create/update.
    ///
    /// - Parameters:
    ///   - table: Table to update in
    ///   - value: Object to write
    ///   - writing: Function to convert an object into a String:PythonObject representation to store.  *Note* There is an Encodable overload of this function which doesn't need the `building` parameter.
    /// - Returns: an observable sequence,  `[.loadInProgress, .fullSuccess(T)]` with the object
    /// originally passed in if the save was successful, otherwise a `DynoActivity.failure(DynoError)`
    /// describing the error.
    public func setItem<T>(inTable table: String, value: T, writing:@escaping (T) -> DynoResult<Dictionary<String,DynoObject>>) ->  DynoPublisher<T> {
        return self.perform(action: DynoSetItem(table: table,
                                                value: value,
                                                writing: writing,
                                                options: self.options))
    }
    
    /// Creates or updates an item.  The item data is assumed to include the key field (eg "id"), so DynamoDB
    /// knows which item to create/update.
    ///
    /// - Parameters:
    ///   - table: Table to update in
    ///   - value: Object to write
    /// - Returns: an observable sequence,  `[.loadInProgress, .fullSuccess(T)]` with the object
    /// originally passed in if the save was successful, otherwise a `DynoActivity.failure(DynoError)`
    /// describing the error.
    public func setItem<T : Encodable>(inTable table: String, value: T) ->  DynoPublisher<T> {
        return self.perform(action: DynoSetItem(table: table,
                                                value: value,
                                                writing: PythonEncoder.toWriter,
                                                options: self.options))
    }
}
