//
//  PythonHelper.swift
//  Dyno
//
//  Created by RedPanda on 19-Feb-19.
//

import Foundation
import PythonKit

extension PythonObject {
    /// throwingCall allows a Python call which might fail to be called, throwing an error if appropriate
    func throwingCall(
        withArguments args: [PythonConvertible] = []
        ) throws -> PythonObject {
        return try throwing.dynamicallyCall(withArguments: args)
    }
    
    /// throwingCall allows a Python call which might fail to be called, throwing an error if appropriate
    func throwingCall(
        withKeywordArguments args:
        KeyValuePairs<String, PythonConvertible> = [:]
        ) throws -> PythonObject {
        return try throwing.dynamicallyCall(withKeywordArguments: args)
    }
}
