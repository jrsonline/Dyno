//
//  DynoStructs.swift
//  Dyno
//
//  Created by strictlyswift on 19-Feb-19.
//

import Foundation
import PythonKit


/// Represents a Dyno-specific error.
public struct DynoError : Error {
    public let reason: String
    public let wrappedError: Error?
    
    var localizedDescription: String { get { return reason }}
    
    public init(_ reason: String) {
        self.reason = reason
        self.wrappedError = nil
    }
    public init(_ wrapping: Error) {
        self.reason = wrapping.localizedDescription
        self.wrappedError = wrapping
    }
}

public protocol DynoStorage {
    associatedtype T
    var value: T { get }
}

extension PythonObject : DynoStorage {
    public var value: PythonObject { get { return self }}
}
