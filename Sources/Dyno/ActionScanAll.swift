//
//  DynoActions.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import PythonKit
import RxSwift


/// Represents a "Scan All Values in Table" (then filter) action
struct DynoScanAll<T> : DynoAction {
    
    let table: String
    let building: (Dictionary<String,PythonObject>) -> DynoResult<T>
    let options: DynoOptions
    let filter: DynoFilter?
    var logName : String { get { return "\(table) Scan"} }


    // deal with paging... not with longer timeouts...
    func perform(connection: DynoConnection) -> DynoResult<[T]> {
        var more = true
        var result : DynoResult<[T]> = DynoResult.success([])
        
        if options.log { NSLog("DynoScanAll [\(logName)] Filter: \(filter?.description ?? "None") \(options.pageSize.map{x in ", page size \(x)"} ?? "")") }
        
        let scanLimit = options.pageSize ?? 100
        var scanParameters : KeyValuePairs<String,PythonConvertible> = ["Limit":scanLimit]
        
        // Add filter if necessary (KeyValuePairs isn't mutable!)
        let boto3Filter  = filter.map { f in f.asFilter() }
        if let boto3Filter = boto3Filter {
            scanParameters = ["Limit":scanLimit,"FilterExpression":boto3Filter]
        }
        
        while case .success(_) = result, more {
            // get a page
            let page = connection.scan(table: self.table, withArgs: scanParameters)
            let tempResult = page.flatMap { (scan: PythonObject) -> DynoResult<[T]> in
                if scan.get("Items") == Python.None { return .failure(DynoError("Could not read any items from \(table)"))}
                let items = scan["Items"]
                
                // Do we have more items to get?
                if scan.get("LastEvaluatedKey") == Python.None {
                    more = false
                } else {
                    scanParameters = ["ExclusiveStartKey":scan["LastEvaluatedKey"], "Limit":scanLimit]
                    if let boto3Filter = boto3Filter {
                        scanParameters = ["ExclusiveStartKey":scan["LastEvaluatedKey"], "Limit":scanLimit,"FilterExpression":boto3Filter]
                    }
                    more = true
                }
                
                // convert page into an array
                return items.flatMap { (item:PythonObject) -> DynoResult<T>  in
                    if let dictItem : Dictionary<String, PythonObject> = Dictionary(item) {
                        let item = building(dictItem)
                        if options.log {
                            switch item {
                                case .failure(let f): NSLog("DynoScanAll [\(logName)] failed to build object from \(dictItem), error was \(String(describing:f))")
                                case .success(_): break // NSLog("DynoScanAll [\(logName)] Built \(s)")
                            }
                        }
                        return item
                    } else {
                        return .failure(DynoError("Could not get item details for item \(item)"))
                    }
                }
            }
            
            // Append the pages
            result = result.flatMap { r in
                switch tempResult {
                case .success(let s): return .success(r + s)
                case .failure(let f): return .failure(f)
                }
            }
        }
        return result
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
    public func scan<T>(inTable table: String, filter: DynoFilter? = nil, building:@escaping (Dictionary<String,PythonObject>) -> DynoResult<T>) -> Observable<DynoActivity<[T]>> {
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
    public func scan<T>(inTable table: String, filter: DynoFilter? = nil, ofType: T.Type) -> Observable<DynoActivity<[T]>>
    where T : Decodable {
        return self.perform(action: DynoScanAll(table: table,
                                                building: Dyno.convertDecodableToBuilder(type: ofType),
                                                options: self.options,
                                                filter: filter))
    }
}

