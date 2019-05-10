//
//  DynoActivity.swift
//  Dyno
//
//  Created by strictlyswift on 19-Feb-19.
//

import Foundation
import PythonKit

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
    
    public func isActivityInProgress() -> Bool {
        if case .activityInProgress = self { return true }
        return false
    }
    
    public func isPartialSuccess() -> T? {
        if case let .partialSuccess(v) = self { return v }
        return nil
    }
    
    public func isFullSuccess() -> T? {
        if case let .fullSuccess(v) = self { return v }
        return nil
    }
    
    public func isFailure() -> DynoError? {
        if case let .failure(e) = self { return e }
        return nil
    }
}

