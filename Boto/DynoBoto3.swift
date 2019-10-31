//
//  DynoBoto3.swift
//  Dyno
//
//  Created by strictlyswift on 7-Apr-19.
//

import Foundation
import PythonKit
import Combine

/// Global connection to boto3 Python library  (https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
let BOTO3 = Python.import("boto3")

extension DynoObject {
    func toPythonObject() -> PythonObject {
        switch self {
        case .bool(let b):
            return PythonObject(b)
        case .string(let s):
            return PythonObject(s)
        case .number(let n):
            return PythonObject(n)
        case .numberSet(let ns):
            return PythonObject(ns)
        case .stringSet(let ss):
            return PythonObject(ss)
        case .binary(let blob, let encoding):
            return PythonObject(String(data: blob, encoding: encoding))
        case .binarySet(let blobs, let encoding):
            return PythonObject(blobs.map {String(data: $0, encoding: encoding) })
        case .null:  // don't quite understand this one
            return Python.None
        case .list(let dynos):
            return PythonObject(dynos.map {$0.toPythonObject()})
        case .map(let dynoMap):
            return PythonObject(dynoMap.mapValues {$0.toPythonObject()})
        @unknown default:
            fatalError("Unknown type of DynoObject")
        }
    }
    
    static func fromPythonObject(po: PythonObject) -> DynoObject? {
        switch Python.builtins.type(po) {
        case "int": return Double(po).map { DynoObject.number($0) }
        case "bool": return Bool(po).map { DynoObject.bool($0) }
        case "string": return String(po).map { DynoObject.string($0) }
        case "str": return String(po).map { DynoObject.string($0) }
        case "list":
            guard Python.builtins.len(po) > 0 else { return DynoObject.stringSet([]) } // empty list assumed to be String !?
            switch Python.builtins.type(po[0]) {
                case "int": return DynoObject.numberSet( po.compactMap { Double($0) } )
                case "str": return DynoObject.stringSet( po.compactMap { String($0) } )
                default: return DynoObject.list( po.compactMap { fromPythonObject(po: $0) })
            }
        case "NoneType": return .null
        case "dict":
            let types = Python.builtins.set( po.keys().map { Python.builtins.type($0) } )
            guard types.count == 1 && Python.builtins.type(po[0]) == "str" else { return nil } // can't understand non-string keys
            return DynoObject.map(
                Dictionary(uniqueKeysWithValues: zip(
                    po.keys().map { String($0) ?? ""},
                    po.values().map {fromPythonObject(po: $0) ?? .null }
                ))
            )
        default: return nil
        }
    }
}

/// Represents a Boto3-mediated connection to the AWS DynamoDB database
public class DynoBoto3 : DynoConnection {


    var remoteConnection : DynoResult<PythonObject>
    let resource: String
    let accessKeyId: String?
    let secretAccessKey: String?
    let region: String?
    let options : DynoOptions
    
    public init( resource: String = "dynamodb",
                 accessKeyId: String? = nil,
                 secretAccessKey: String? = nil,
                 region: String? = nil,
                 _ options : DynoOptions) {
        
        self.resource = resource
        self.accessKeyId = accessKeyId
        self.secretAccessKey = secretAccessKey
        self.region = region
        self.options = options


        self.remoteConnection = DynoBoto3.getRawConnection(resource: resource, accessKeyId: accessKeyId, secretAccessKey: secretAccessKey, region: region, options: options)
    }

    public func isValid() -> Bool {
        switch remoteConnection {
            case let .success(conn): return DynoBoto3.confirmUsableConnection(conn)
            default: return false
        }
    }
    
    public func lastError() -> DynoError? {
        switch remoteConnection {
            case .success(_): return nil
            case let .failure(f): return f
        }
    }
    
    public func tryReconnect() -> Bool {
        self.remoteConnection = DynoBoto3.getRawConnection(resource: self.resource, accessKeyId: self.accessKeyId, secretAccessKey: self.secretAccessKey, region: self.region, options: self.options)
        
        return self.isValid()
    }

    internal static func getRawConnection( resource: String = "dynamodb", accessKeyId: String? = nil, secretAccessKey: String? = nil, region: String? = nil, options: DynoOptions) -> DynoResult<PythonObject> {
        do {
            let endpoint_url = options.dummyUrl ? "https://dynamodb.dummy.com/" : Python.None
            
            return try .success( BOTO3.resource.throwingCall(withKeywordArguments:
                ["":resource,
                 "aws_access_key_id":accessKeyId ?? Python.None,
                 "aws_secret_access_key":secretAccessKey ?? Python.None,
                 "region_name":region ?? Python.None,
                 "endpoint_url":endpoint_url
                ]))
        } catch {
            return .failure(DynoError("Connection error: \(error)"))
        }
    }
    
    
    
    /// Calls a dynamoDB function on remote db (via boto3) and ensures a) it doesn't throw an exception ;
    /// b) it returns a valid HTTP code
    ///
    /// - Parameters:
    ///   - callable: method to call
    ///   - args: named arguments to call on the method
    /// - Returns: .success<PythonObject> as the value returned from the call; or .failure
//    internal func boto3Call(_ callable:PythonObject, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject> {
    internal func boto3Call(_ callable:PythonObject, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject> {        do {
            // Important for next line to be a throwingCall if we have no actual connection, else we will
            // get a fatal error 30 seconds later when boto3 decides to terminate!
            let response = try callable.throwingCall(withKeywordArguments: args )
            if  response.get("ResponseMetadata") == Python.None ||
                response["ResponseMetadata"]["HTTPStatusCode"] < 200 ||
                response["ResponseMetadata"]["HTTPStatusCode"] > 299 {
                return .failure(DynoError("Invalid HTTP response"))
            }
            
            return .success(response)
            
        } catch {
            return .failure(DynoError("Boto3 error :\(error)"))
        }
    }
    
    
    
    /// Do we have a usable connection - checks if we have Table object
    internal static func confirmUsableConnection(_ conn: PythonObject) -> Bool {
        // a valid connection allows us to look at a Table
        return conn.checking.Table != nil
    }
    
    
    public func getItem(from table: String, key:(String, DynoObject)) -> DynoResult<Dictionary<String, DynoObject>>  {
        let (keyField, keyValue) = key
        let pythonKeyValue = keyValue.toPythonObject()
        let pythonResult : DynoResult<Dictionary<String, PythonObject>> = self
            .remoteConnection
            .flatMap { rc in
                self.boto3Call( rc.Table(table).get_item, withArgs: ["Key":[keyField: pythonKeyValue ]])
            }
            .flatMap { lookup in
                if lookup.get("Item") == Python.None {
                    return .failure(DynoError("Object \(keyField) = \(keyValue) not found"))
                }
                
                let item : Dictionary<String, PythonObject>? = Dictionary(lookup["Item"])
                if let item = item {
                    return .success(item)
                } else {
                    return .failure(DynoError("Object  \(keyField) = \(keyValue) not found"))
                }
            }
        return pythonResult.map { dict in
            dict.compactMapValues { DynoObject.fromPythonObject(po: $0) }
        }
    }
    
    
    public func scan(table: String, filter: DynoFilter? = nil, scanLimit: Int = 100) -> DynoResult<[Dictionary<String, DynoObject>]> {
        var more = true
        var result : DynoResult<[Dictionary<String, PythonObject>]> = DynoResult.success([])
        
        var scanParameters : KeyValuePairs<String,PythonConvertible> = ["Limit":scanLimit]
        
        // Add filter if necessary (KeyValuePairs isn't mutable!)
        let boto3Filter  = filter.map { f in f.asFilter() }
        if let boto3Filter = boto3Filter {
            scanParameters = ["Limit":scanLimit,"FilterExpression":boto3Filter]
        }
        
        while case .success(_) = result, more {
            // get a page
            let page = self.remoteConnection.flatMap { rc in
                self.boto3Call(
                    rc.Table(table).scan,
                    withArgs: scanParameters
                )
            }
            
            let tempResult = page.flatMap { (scan: PythonObject) -> DynoResult<[Dictionary<String, PythonObject>]> in
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
                return items.flatMap { (item:PythonObject) -> DynoResult<Dictionary<String, PythonObject>> in
                    if let dictItem : Dictionary<String, PythonObject> = Dictionary(item) {
//                        let item = building(dictItem)
//                        if options.log {
//                            switch item {
//                            case .failure(let f): NSLog("DynoScanAll [\(logName)] failed to build object from \(dictItem), error was \(String(describing:f))")
//                            case .success(_): break // NSLog("DynoScanAll [\(logName)] Built \(s)")
//                            }
//                        }
                        return .success(dictItem)
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
        let dynoResult = result.map { d in
            d.map { p in p.compactMapValues { DynoObject.fromPythonObject(po: $0)} }
        }
        return dynoResult
    }

    public func setItem(into table: String, withItemInfo args: Dictionary<String,DynoObject>) -> DynoResult<Dictionary<String, DynoObject>> {
        return self.remoteConnection.flatMap { rc in
            self.boto3Call(
                rc.Table(table).put_item,
                withArgs: ["Item": args.mapValues { $0.toPythonObject() } ]
            )
        }.mapNilAsError(
            { _ in args },
            error: DynoError("Cannot translate python object to Dyno object")
        )
    }
}
