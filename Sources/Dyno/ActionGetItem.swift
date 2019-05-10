//
//  ActionGetItem.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import PythonKit
import RxSwift

/// Represents a "Get item with given key" action
struct DynoGetItem<T> : DynoAction {
    let table: String
    let keyField: String
    let keyValue: PythonObject
    let building: (Dictionary<String,PythonObject>) -> DynoResult<T>
    let options: DynoOptions
    var logName : String { get { return "\(T.self) Getter"} }
    
    func perform(connection: DynoConnection) -> DynoResult<T> {
        if options.log { NSLog("DynoGetItem [\(logName)] Attempting retrieval of item with \(keyField)=\(keyValue) in table \(table)") }
        
        let result = connection.getItem(from: self.table, key:[keyField:keyValue])
            
        
        return result.flatMap { lookup in
            
            if lookup.get("Item") == Python.None {
                return .failure(DynoError("Object \(keyField) = \(keyValue) not found"))
            }
            
            let item : Dictionary<String, PythonObject>? = Dictionary(lookup["Item"])
            if let item = item {
                return building(item)
            } else {
                return .failure(DynoError("Object  \(keyField) = \(keyValue) not found"))
            }
        }
    }
}


extension Dyno {
    
    /**
     getItem returns the item with a given (single) keyfield, eg a UUID.
     - Parameters:
     - fromTable: The name of the table to query
     - keyField: The key field to query, eg "id"
     - value: the value of that keyfield as a string, eg "abcd123"
     - building: a function that creates the return object from a dictionary of values returned. *Note* There is a Decodable overload of this function which doesn't need the `building` parameter.
     
     - Returns:
     - an observable sequence,  `[.loadInProgress, .fullSuccess(T)]` with the object built by the `building` function if the retrieval was successful,
     otherwise a `DynoActivity.failure(DynoError)` describing the error.
     
     - See Also: [More Info](http://github.com/blah)
     */
    public func getItem<T>(fromTable table: String, keyField: String = "id", value: String, building:@escaping (Dictionary<String,PythonObject>) -> DynoResult<T>) -> Observable<DynoActivity<T>> {
        
        return self.perform(action: DynoGetItem(table: table,
                                                keyField: keyField,
                                                keyValue: PythonObject(value),
                                                building: building,
                                                options: self.options))
    }
    
    /**
     getItem returns the item with a given (single) keyfield, eg a UUID.
     - Parameters:
     - fromTable: The name of the table to query
     - keyField: The key field to query, eg "id"
     - value: the value of that keyfield as a string, eg "abcd123"
     
     - Returns:
     - an observable sequence,  `[.loadInProgress, .fullSuccess(T)]` with the object built by the `building` function if the retrieval was successful,
     otherwise a `DynoActivity.failure(DynoError)` describing the error.
     
     - See Also: [More Info](http://github.com/blah)
     */
    public func getItem<T>(fromTable table: String, keyField: String = "id", value: String, ofType: T.Type) -> Observable<DynoActivity<T>>
    where T : Decodable {
        return self.perform(action: DynoGetItem(table: table,
                                                keyField: keyField,
                                                keyValue: PythonObject(value),
                                                building: Dyno.convertDecodableToBuilder(type: ofType),
                                                options: self.options))
    }
}
