//
//  RxLogger.swift
//  Dyno
//
//  Created by strictlyswift on 2/17/19.
//

import Foundation
import RxSwift
//import RxCocoa

public extension ObservableType {
    /**
     Logs a stream
     */
    func log(_ stream: String, do: Bool = true) -> Observable<E> {
        if !`do` {
            return self.asObservable()
        } else {
            return  self.asObservable()
                .do(onNext: {elt in NSLog("Observable [\(stream)] - Next: \(elt)")},
                    onCompleted: {NSLog("Observable [\(stream)] - Completed.") },
                    onSubscribed: {NSLog("Observable [\(stream)] - Subscribed") },
                    onDispose: {NSLog("Observable [\(stream)] - Disposed.") })
        }
    }
}

//public extension SharedSequence {
//    /**
//     Logs a stream
//     - parameter stream: Name of stream to show in logger
//     */
//    func log(_ stream: String) -> SharedSequence<SharingStrategy, E> {
//        return self.do(onNext: {elt in NSLog("Driver [\(stream)] - Next: \(elt)")},
//                       onCompleted: {NSLog("Driver [\(stream)] - Completed.") },
//                       onSubscribed: {NSLog("Driver [\(stream)] - Subscribed") },
//                       onDispose: {NSLog("Driver [\(stream)] - Disposed.") })
//    }
//}
