//
//  RxLogger.swift
//  Dyno
//
//  Created by Unicorn on 2/17/19.
//

import Foundation
import RxSwift
import RxCocoa

public extension ObservableType {
    /**
     Logs a stream
     */
    func log(_ stream: String) -> Observable<E> {
        return  self.asObservable()
            .do(onNext: {elt in NSLog("Observables [\(stream)] - Next: \(elt)")},
                onCompleted: {NSLog("Observables [\(stream)] - Completed.") },
                onSubscribe: {NSLog("Observables [\(stream)] - Subscribed") },
                onSubscribed: {NSLog("Observables [\(stream)] - Unsubscribed") },
                onDispose: {NSLog("Observables [\(stream)] - Disposed.") })
    }
}

public extension SharedSequence {
    /**
     Logs a stream
     - parameter stream: Name of stream to show in logger
     */
    func log(_ stream: String) -> SharedSequence<SharingStrategy, E> {
        return self.do(onNext: {elt in NSLog("Drivers [\(stream)] - Next: \(elt)")},
                       onCompleted: {NSLog("Drivers [\(stream)] - Completed.") },
                       onSubscribe: {NSLog("Drivers [\(stream)] - Subscribed") },
                       onSubscribed: {NSLog("Drivers [\(stream)] - Unsubscribed") },
                       onDispose: {NSLog("Drivers [\(stream)] - Disposed.") })
    }
}
