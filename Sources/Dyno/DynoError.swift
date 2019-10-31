//
//  File.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation

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
