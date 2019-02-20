//
//  🦕.swift
//  Dyno
//
//  Created by RedPanda on 16-Feb-19.
//

import Foundation
import Dispatch
import RxSwift
import PythonKit

/// Global connection to boto3 Python library  (https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
let BOTO3 : PythonObject = Python.import("boto3")
let BOTO3_CONDITIONS : PythonObject  = Python.import("boto3.dynamodb.conditions")

/// Dyno 🦕 represents an AWS DynamoDB database, and provides a number of 'safe' functions for handling data interactions in a Reactive manner.
struct Dyno {
    let dynoQueue : DispatchQueue // queue enforces serial access to boto3 resources
    let connection : Result<PythonObject, DynoError>
    var timeout : Int = 5
    
    internal static func getRawConnection( resource: String = "dynamodb", accessKeyId: String? = nil, secretAccessKey: String? = nil, region: String? = nil) throws -> Result<PythonObject,DynoError> {
        do {
            if let accessKeyId = accessKeyId, let secretAccessKey = secretAccessKey, let region = region {

                return try .success(BOTO3.Session.throwingCall(withKeywordArguments: ["aws_access_key_id": accessKeyId, "aws_secret_access_key": secretAccessKey, "region_name": region]))
            } else {
                return try .success(BOTO3.resource.throwingCall(withArguments:[resource]))
            }
        } catch {
            throw DynoError("Connection error: \(error.localizedDescription)")
        }
    }
    
    /// Creates a connection to a dynamodb. If the accessKeyId/secretAccessKey/region is not passed in, it uses the information in ~/.aws/credentials and ~/.aws/config.
    /// (You must pass all of them, or none of them).
    /// Note that if the connection can't be created, the Dyno object is created but isValid returns false.
    init( resource: String = "dynamodb", accessKeyId: String? = nil, secretAccessKey: String? = nil, region: String? = nil) {
        dynoQueue = DispatchQueue(label: "DynoQueue", qos: .default, attributes: [], autoreleaseFrequency: .inherit, target: nil)
        connection = Dyno.syncMethod { (returning:(Result<PythonObject, DynoError>) -> Void) in
            do {
                let result = try Dyno.getRawConnection(resource: resource, accessKeyId: accessKeyId, secretAccessKey: secretAccessKey, region: region)
                
                let conn: Result<PythonObject, DynoError>
                
                switch result {
                    case .failure(_): conn = result
                    case .success(let s):
                        if Dyno.checkConnection(s).isNil() { conn = .failure(DynoError("Invalid Connection")) }
                        else { conn = .success(s) }
                }
                returning ( conn )
            } catch {
                returning(.failure(DynoError("Connection error: \(error.localizedDescription)")))
            }
        }
    }
    
    /// Returns true if the connection is valid (eg, not timed out, and actually points to a real database)
    /// . To determine any error, use connectionError.
    func isValidConnection() -> Bool {
        switch connection {
            case .failure(_): return false
            case .success(let s): return Dyno.checkConnection(s).isNonNil()
        }
    }
    
    /// Returns the most recent connection error, if one exists.
    func connectionError() -> DynoError? {
        switch connection {
            case .failure(let f): return f
            case .success(_): return nil
        }
    }
    
    /// Do we have a valid connection - checks if we have Table object
    internal static func checkConnection(_ conn: PythonObject) -> PythonObject? {
        // a valid connection allows us to look at a Table
        if conn.checking.Table != nil { return conn }
        return nil
    }
    
    internal static func syncMethod<T>(timeout: Double = 5, asyncOperation: @escaping ((Result<T, DynoError>) -> Void) -> Void) -> Result<T, DynoError> {
        let semaphore = DispatchSemaphore(value: 0)
        let queue = DispatchQueue.global()
        
        var response: Result<T, DynoError> = .failure( DynoError("Timeout") )
        queue.async {
            asyncOperation { (r:Result<T, DynoError>) -> Void in
                response = r
                semaphore.signal()
            }
        }
        _ = semaphore.wait(timeout: .now() + timeout)
        return response
    }

}


extension Dyno {
    
    /// getItem returns the item with a given UUID
    func getItem_result<T>(inTable table: String, keyField: String = "id", key: String, building:@escaping (Dictionary<String,PythonObject>) -> Result<T,DynoError>) -> Result<T,DynoError> {
        return connection.flatMap { conn in
            let result = Dyno.syncMethod { (returning:(Result<T,DynoError>) -> Void) in
                returning( self.getItemImpl(conn: conn, inTable: table, keyField: keyField, key: key, building: building) )
            }
            
            return result 
        }
    }
    
    /**
     getItem returns the item with a given (single) keyfield, eg a UUID.
     - Parameters:
        - fromTable: The name of the table to query
        - keyField: The key field to query, eg "id"
        - value: the value of that keyfield as a string, eg "abcd123"
        - building: a function that creates the return object from a dictionary of values returned
     
     - Returns:
        - an observable sequence, returning _DynoActivity.fullSuccess(T)_ with the object built by the `building` function if the retrieval was successful,
          otherwise a _DynoActivity.failure(DynoError)_ describing the error.
     
     - See Also: [More Info](http://github.com/blah)
     */
    func getItem<T>(fromTable table: String, keyField: String = "id", value: String, building:@escaping (Dictionary<String,PythonObject>) -> Result<T,DynoError>) -> Observable<DynoActivity<T>> {
        
        return Observable<DynoActivity<T>>.create { observer in
            DispatchQueue.global().async {
                switch self.connection {
                case .success(let connection):
                    observer.onNext( DynoActivity<T>.loadInProgress )
                    
                    // Run next part async on semaphore... need the WorkItem to cancel
                    let semaphore = DispatchSemaphore(value: 0)
                    var cancelFlag = false
                    let workItem = DispatchWorkItem {
                        switch self.getItemImpl(conn: connection, inTable: table, keyField: keyField, key:value, building: building) {
                        case .success(let v):
                            observer.onNext(DynoActivity.fullSuccess(v))
                            observer.onCompleted()
                            semaphore.signal()
                            
                        case .failure(let f):
                            if !cancelFlag {  observer.onNext( DynoActivity<T>.failure(f) ) }
                            semaphore.signal()
                        }
                    }
                    
                    let stopTime = DispatchTime.now() + Double(self.timeout)
                    self.dynoQueue.async(execute: workItem)  // run on 'dynoQueue' to force serial access to shared Boto3 resources
                    if semaphore.wait(timeout: stopTime) == .timedOut {
                        observer.onNext(DynoActivity.failure(DynoError("Timeout")))
                        cancelFlag = true
                        workItem.cancel()
                    }
                    
                case .failure(let fail):
                    observer.onNext( DynoActivity<T>.failure(fail) )
                }
                
            }
            return Disposables.create()
        }
    }
    
    private func getItemImpl<T>( conn: PythonObject, inTable table: String, keyField: String = "id", key: String, building:@escaping (Dictionary<String,PythonObject>) -> Result<T,DynoError>) -> Result<T, DynoError> {
        
        let table = conn.Table(table)
            
        do {
            // Important for next line to be a throwingCall if we have no actual connection, else we will
            // get a fatal error 30 seconds later when boto3 decides to terminate!
            let lookup = try table.get_item.throwingCall(withKeywordArguments:["Key":[keyField: key ]])
            
            if lookup.get("Item") == Python.None {
                return .failure(DynoError("Object \(keyField) = \(key) not found"))
            }
            
            let item : Dictionary<String, PythonObject>? = Dictionary(lookup["Item"])
            if let item = item {
                return building(item)
            } else {
                return .failure(DynoError("Object  \(keyField) = \(key) not found"))
            }
            
        } catch {
            return .failure(DynoError("Boto3 error :\(error.localizedDescription)"))
        }
    }
    
}


extension Dyno {
    static func getStr(_ dict: Dictionary<String,PythonObject>, _ key: String) -> Result<String, DynoError> {
        guard let v = dict[key].flatMap (String.init)
            else { return .failure(DynoError("Can't read key \(key)")) }
        return .success(v)
    }
}
