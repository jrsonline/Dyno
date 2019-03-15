//
//  🦕.swift
//  Dyno
//
//  Created by strictlyswift on 16-Feb-19.
//

import Foundation
import Dispatch
import RxSwift
import PythonKit

public typealias DynoResult<T> = Result<T,DynoError>
typealias DynoConnection = PythonObject

/// Global connection to boto3 Python library  (https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
let BOTO3 : PythonObject = Python.import("boto3")
let BOTO3_CONDITIONS  = Python.import("boto3.dynamodb.conditions")

/// Dyno 🦕 represents an AWS DynamoDB database, and provides a number of 'safe' functions for handling data interactions in a Reactive manner.
public struct Dyno {
    let dynoQueue : DispatchQueue // queue enforces serial access to boto3 resources
    let connection : DynoResult<DynoConnection>
    let options : DynoOptions
    
    let dynoSemaphore = DispatchSemaphore(value: 0)


    /// Creates a connection to a dynamodb. If the accessKeyId/secretAccessKey/region is not passed in, it uses the information in ~/.aws/credentials and ~/.aws/config.
    /// (You must pass all of them, or none of them).
    /// The connection is _not_ accessed immediately, but only lazily when it is used to access data. `isValid`
    /// actually does a table connection to force evaluation.
    public init( resource: String = "dynamodb",
                 accessKeyId: String? = nil,
                 secretAccessKey: String? = nil,
                 region: String? = nil,
                 _ options : DynoOptions = DynoOptions() ) {
        self.dynoQueue = DispatchQueue(label: "DynoQueue", qos: .default, attributes: [], autoreleaseFrequency: .inherit, target: nil)
        self.options = options
        
        connection = Dyno.getRawConnection(resource: resource, accessKeyId: accessKeyId, secretAccessKey: secretAccessKey, region: region)
    }
    
    /// Returns true if the connection is valid (eg, not timed out, and actually points to a real database).
    /// To determine any error, use connectionError. Forces a table connection to confirm validity.
    public func isValidConnection() -> Bool {
        switch connection {
            case .failure(_): return false
            case .success(let s): return Dyno.checkConnection(s).isNonNil()
        }
    }
    
    /// Returns the most recent connection error, if one exists.
    public func connectionError() -> DynoError? {
        switch connection {
            case .failure(let f): return f
            case .success(_): return nil
        }
    }
    
    /// Do we have a valid connection - checks if we have Table object
    internal static func checkConnection(_ conn: PythonObject) -> DynoConnection? {
        // a valid connection allows us to look at a Table
        if conn.checking.Table != nil { return conn }
        return nil
    }
    
    
    internal static func getRawConnection( resource: String = "dynamodb", accessKeyId: String? = nil, secretAccessKey: String? = nil, region: String? = nil) -> DynoResult<PythonObject> {
        do {
            if let accessKeyId = accessKeyId, let secretAccessKey = secretAccessKey, let region = region {
                
                return try .success(BOTO3.Session.throwingCall(withKeywordArguments: ["aws_access_key_id": accessKeyId, "aws_secret_access_key": secretAccessKey, "region_name": region]))
            } else {
                return try .success(BOTO3.resource.throwingCall(withArguments:[resource]))
            }
        } catch {
            return .failure(DynoError("Connection error: \(error.localizedDescription)"))
        }
    }
    
    /// Calls a dynamoDB function on remote db (via boto3) and ensures a) it doesn't throw an exception ;
    /// b) it returns a valid HTTP code
    ///
    /// - Parameters:
    ///   - callable: method to call
    ///   - args: named arguments to call on the method
    /// - Returns: .success<PythonObject> as the value returned from the call; or .failure
    internal static func boto3Call(_ callable:PythonObject, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject> {
        do {
            // Important for next line to be a throwingCall if we have no actual connection, else we will
            // get a fatal error 30 seconds later when boto3 decides to terminate!
            let response = try callable.throwingCall(withKeywordArguments: args)
            if  response.get("ResponseMetadata") == Python.None ||
                response["ResponseMetadata"]["HTTPStatusCode"] < 200 ||
                response["ResponseMetadata"]["HTTPStatusCode"] > 299 {
                return .failure(DynoError("Invalid HTTP response"))
            }
            
            return .success(response)
            
        } catch {
            return .failure(DynoError("Boto3 error :\(error.localizedDescription)"))
        }
    }
 
    /// Performs an action on the DynamoDB database. Tries hard to not get 'stuck' if DynamoDB doesn't
    /// respond in time; and allows for simultaneous calls to the same connection, which seems to be an
    /// issue otherwise.  Note that we re-try the Dyno connection each time (no connection caching).
    ///
    /// The observable sequence returned will always start with `.activityInProgress`.
    /// If there is an error (eg Timeout), the next entry will be `DynoActivity.failure(DynoError)`
    /// Otherwise, you will currently always get `DynoActivity.fullSuccess(T)`
    ///
    /// - Parameter action: Action to perform
    /// - Returns: Observable sequence constructed as above.
    internal func perform<T,D>(action: D) -> Observable<DynoActivity<T>> where D : DynoAction, D.T == T {
        return Observable<DynoActivity<T>>.create { observer in
            DispatchQueue.global().async {
                switch self.connection {  
                case .success(let connection):
                    observer.onNext( DynoActivity<T>.activityInProgress )
                    
                    // Run next part async on semaphore... need the WorkItem to cancel
                    let semaphore = DispatchSemaphore(value: 0)
                    var cancelFlag = false
                    let workItem = DispatchWorkItem {
                        switch action.perform(connection: connection) {
                        case .success(let v):
                            if !cancelFlag {
                                observer.onNext(DynoActivity.fullSuccess(v))
                                observer.onCompleted()
                            }
                            semaphore.signal()
                            
                        case .failure(let f):
                            if !cancelFlag {  observer.onNext( DynoActivity<T>.failure(f) ) }
                            semaphore.signal()
                        }
                    }
                    
                    let stopTime = DispatchTime.now() + Double(self.options.timeout)
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
        }.log(action.logName, do: self.options.log)
    }
}

