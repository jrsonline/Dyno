//
//  DynoStructs.swift
//  Dyno
//
//  Created by strictlyswift on 19-Feb-19.
//

import Foundation

/// Represents the various stages of the data activity lifecycle.
public enum DynoActivity<T> {
    case activityInProgress
    case partialSuccess(T)
    case fullSuccess(T)
    case failure(DynoError)
    
    /// Transforms the content of a DynoActivity
    public func map<S>(_ transform:(T) -> S) -> DynoActivity<S> {
        switch self {
            case .activityInProgress : return DynoActivity<S>.activityInProgress
            case .partialSuccess(let t): return DynoActivity<S>.partialSuccess( transform(t))
            case .fullSuccess(let t): return DynoActivity<S>.fullSuccess( transform(t))
            case .failure(let e): return DynoActivity<S>.failure(e)
        }
    }
}



/// Represents a Dyno-specific error.
public struct DynoError : Error {
    let reason: String
    public init(_ reason: String) { self.reason = reason }
    public init(_ wrapping: Error) { self.reason = wrapping.localizedDescription }
}

/// Options for the Dyno connection
public struct DynoOptions {
    let addVersioning : Bool
    let timeout : Int
    let pageSize : Int?
    let log : Bool
    /// Options for the Dyno connection
    ///
    /// - Parameters:
    ///   - addVersioning: **Currently unsupported** Defaults to False
    ///   - timeout: Number of seconds before the connection forcibly times out. Defaults to 5. **Note** you should set this to <30 otherwise Boto3 will time out instead.
    ///   - pageSize: Use this to limit the size of pages (ie number of rows) returned by large queries (eg scan). Dyno will automatically concatenate pages together into a full result set, so this is probably not much use right now. Defaults to `nil`, which means the Boto3 default of 1MB pages.
    ///   - log: Set to **true** to log onto standard output.
    public init(addVersioning : Bool = false, timeout: Int = 5, pageSize: Int? = nil, log: Bool = false) {
        self.addVersioning = addVersioning
        self.timeout = timeout
        self.pageSize = pageSize
        self.log = log
    }
}
