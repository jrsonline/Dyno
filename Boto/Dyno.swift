//
//  🦕.swift
//  Dyno
//
//  Created by strictlyswift on 16-Feb-19.
//

import Foundation
import Dispatch
import RxSwift
import Combine
import PythonKit
import PythonCodable
import StrictlySwiftLib

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
    
    internal func carryOutActivity<T,D>(action: D, completion:  @escaping (DynoResult<T>) -> Void)
        where D : DynoBotoAction, D.T == T {
        switch self.connection.isValid() {
        case true:
            // Run next part async on semaphore... need the WorkItem to cancel
            let semaphore = DispatchSemaphore(value: 0)
            var cancelFlag = false
            let workItem = DispatchWorkItem {
                switch action.perform(connection: self.connection) {
                case .success(let v):
                    if !cancelFlag {
                        completion(.success(v))
                    }
                    semaphore.signal()
                    
                case .failure(let f):
                    if !cancelFlag {
                        completion(.failure(f) )
                    }
                    semaphore.signal()
                }
            }
            
            let stopTime = DispatchTime.now() + Double(self.options.timeout)
            self.dynoQueue.async(execute: workItem)  // run on 'dynoQueue' to force serial access to shared resources
            if semaphore.wait(timeout: stopTime) == .timedOut {
                completion(.failure(DynoError("Timeout")))
                cancelFlag = true
                workItem.cancel()
            }
            
        case false:
            completion( .failure(self.connectionError()!) )
        }
        
    }

    /// Performs an action on the DynamoDB database. Tries hard to not get 'stuck' if DynamoDB doesn't
    /// respond in time; and allows for simultaneous calls to the same connection, which seems to be an
    /// issue otherwise.  Note that we re-try the Dyno connection each time (no connection caching).
    ///
    /// If there is an error (eg Timeout), the next entry will be `DynoActivity.failure(DynoError)`
    /// Otherwise, you will currently always get `DynoActivity.fullSuccess(T)`
    ///
    /// - Parameter action: Action to perform
    /// - Returns: Observable sequence constructed as above.
    internal func perform<T,D>(action: D) -> DynoPublisher<T>
        where D : DynoBotoAction, D.T == T {
            Combine.Future<T,DynoError> { promise in
                        self.carryOutActivity(action: action) { result in
                            promise(result)
                        }
            }
            .print(action.logName)
            .eraseToAnyPublisher()
            
            // Removed the 'activityInProgress'..  don't think we need this?
            // though note I am not wrapping the above in DispatchQueue.global().async {
            // do I need to..?
    }

    internal func performOld<T,D>(action: D) -> Observable<DynoActivity<T>> where D : DynoBotoAction, D.T == T {
        return Observable.create { observer in
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
        }//.log(action.logName, do: self.options.log)
    }
    
}
