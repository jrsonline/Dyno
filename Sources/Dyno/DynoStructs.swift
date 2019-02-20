//
//  DynoStructs.swift
//  Dyno
//
//  Created by RedPanda on 19-Feb-19.
//

import Foundation

/// Represents the various stages of the data activity lifecycle.
enum DynoActivity<T> {
    case loadInProgress
    case storeInProgress
    case partialSuccess(T)
    case fullSuccess(T)
    case failure(DynoError)
}

/// Represents a Dyno-specific error.
struct DynoError : Error {
    let reason: String
    init(_ reason: String) { self.reason = reason }
    init(_ wrapping: Error) { self.reason = wrapping.localizedDescription }
}
