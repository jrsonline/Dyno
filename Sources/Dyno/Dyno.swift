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

/// Dyno 🦕 represents an AWS DynamoDB database, and provides a number of 'safe' functions for handling data interactions in a Reactive manner.
public struct Dyno {
    let dynoQueue : DispatchQueue // queue enforces serial access to boto3 resources
    let connection : DynoConnection
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
        
        self.connection = DynoBoto3(resource: resource, accessKeyId: accessKeyId, secretAccessKey: secretAccessKey, region: region, options)
    }
    
    public init( connection: DynoConnection,
                 _ options : DynoOptions = DynoOptions() ) {
        self.dynoQueue = DispatchQueue(label: "DynoQueue", qos: .default, attributes: [], autoreleaseFrequency: .inherit, target: nil)
        self.options = options
        
        self.connection = connection
    }
    
    
    /// Returns true if the connection is valid (eg, not timed out, and actually points to a real database).
    /// To determine any error, use connectionError. Forces a table connection to confirm validity.
    public func isValidConnection() -> Bool {
        return self.connection.isValid()
    }
    
    /// Returns the most recent connection error, if one exists.
    public func connectionError() -> DynoError? {
        return self.connection.lastError()
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
                switch self.connection.isValid() {
                case true:
                    observer.onNext( DynoActivity<T>.activityInProgress )
                    
                    // Run next part async on semaphore... need the WorkItem to cancel
                    let semaphore = DispatchSemaphore(value: 0)
                    var cancelFlag = false
                    let workItem = DispatchWorkItem {
                        switch action.perform(connection: self.connection) {
                        case .success(let v):
                            if !cancelFlag {
                                observer.onNext(DynoActivity.fullSuccess(v))
                                observer.onCompleted()
                            }
                            semaphore.signal()
                            
                        case .failure(let f):
                            if !cancelFlag {
                                observer.onNext( DynoActivity.failure(f) )
                                observer.onCompleted()
                            }
                            semaphore.signal()
                        }
                    }
                    
                    let stopTime = DispatchTime.now() + Double(self.options.timeout)
                    self.dynoQueue.async(execute: workItem)  // run on 'dynoQueue' to force serial access to shared resources
                    if semaphore.wait(timeout: stopTime) == .timedOut {
                        observer.onNext(DynoActivity.failure(DynoError("Timeout")))
                        observer.onCompleted()
                        cancelFlag = true
                        workItem.cancel()
                    }
                    
                case false:
                    observer.onNext( DynoActivity<T>.failure(self.connectionError()!) )
                    observer.onCompleted()
                }
                
            }
            return Disposables.create()
        }.log(action.logName, do: self.options.log)
    }

    /// Helper function to convert a Decodable into a 'builder'
    ///
    internal static func convertDecodableToBuilder<T>(type: T.Type) -> ([String: PythonObject]) -> DynoResult<T>
    where T : Decodable {
        return { dict in
            do {
                return .success(try PythonDecoder().decode(T.self, from: dict.pythonObject))
            }
            catch {
                return .failure(DynoError(error))
            }
        }
    }
    
    /// Helper function to convert an Encodable into a 'writer'
    ///
    internal static func convertEncodableToWriter<T : Encodable>(obj: T) -> DynoResult<Dictionary<String,PythonObject>> {
        do {
            if let dict = Dictionary<String,PythonObject>(try PythonEncoder().encode(obj)) {
                return DynoResult.success( dict )
            } else {
                return DynoResult.failure(DynoError("Could not encode \(obj) into Dictionary"))
            }
        }
        catch {
            return .failure(DynoError(error))
        }
    }

}
