//
//  DynoScanAll.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation


struct DynoScanAll<T> : DynoBotoAction {
 
    let table: String
    let building: (Dictionary<String,DynoObject>) -> DynoResult<T>
    let options: DynoOptions
    let filter: DynoFilter?
    var logName : String { get { return "\(table) Scan"} }
     
    // deal with paging... not with longer timeouts...
    func perform(connection: DynoConnection) -> DynoResult<[T]> {
        let result = connection.scan(table: self.table,
                                     filter: self.filter,
                                     scanLimit: options.pageSize ?? 100)
        
        return result.flatMap { rs in
            rs.reduce(DynoResult<[T]>.success(Array<T>())) { resultSoFar, protoObject in
                switch (resultSoFar, building(protoObject)) {
                case (.success(let ss), .success(let s)): return .success(ss + [s])
                default: return .failure(DynoError("Failed to produce scan result"))
                }
            }
        }
    }
}

extension Dyno {
    
    /// Returns values in the table, filtered according to the `filter`.
    /// DynamoDB may page large results; currently Dyno hides this from you
    /// so you will always get all the results returned (unless this takes too long and causes a timeout, in
    /// which case you won't get anything.)
    ///
    /// - Important: You should almost certainly filter results but please note AWS will charge you for reading the whole table each time, as the filter is applied **after** the scan is complete.  (Your transfer costs will be lower though)
    /// You should therefore avoid using scan on large tables.
    ///
    /// Note that `scan` currently does **not** support Consistent Reads, Secondary Indexes, or Parallel Scans.
    ///
    /// See pricing information [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html).
    /// - Parameters:
    ///   - table: Table values to return
    ///   - filter: Filter to apply, or leave out to return all rows, though see pricing considerations above
    ///   - building: a function that creates the return object from a dictionary of values returned. *Note* There is a Decodable overload of this function which doesn't need the `building` parameter.
    /// - Returns: Observable list of results as per the description
    public func scan<T>(inTable table: String, filter: DynoFilter? = nil, building:@escaping (Dictionary<String,DynoObject>) -> DynoResult<T>) -> DynoPublisher<[T]> {
        return self.perform(action: DynoScanAll(table: table,
                                                building: building,
                                                options: self.options,
                                                filter: filter))
    }
    
    /// Returns values in the table, filtered according to the `filter`.
    /// DynamoDB may page large results; currently Dyno hides this from you
    /// so you will always get all the results returned (unless this takes too long and causes a timeout, in
    /// which case you won't get anything.)
    ///
    /// - Important: You should almost certainly filter results but please note AWS will charge you for reading the whole table each time, as the filter is applied **after** the scan is complete.  (Your transfer costs will be lower though)
    /// You should therefore avoid using scan on large tables.
    ///
    /// Note that `scan` currently does **not** support Consistent Reads, Secondary Indexes, or Parallel Scans.
    ///
    /// See pricing information [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html).
    /// - Parameters:
    ///   - table: Table values to return
    ///   - filter: Filter to apply, or leave out to return all rows, though see pricing considerations above
    /// - Returns: Observable list of results as per the description
    public func scan<T>(inTable table: String, filter: DynoFilter? = nil, ofType: T.Type) -> DynoPublisher<[T]>
    where T : Decodable {
        return self.perform(action: DynoScanAll(table: table,
                                                building: PythonDecoder.toBuilder(type: ofType),
                                                options: self.options,
                                                filter: filter))
    }
}
