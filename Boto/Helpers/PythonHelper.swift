//
//  PythonHelper.swift
//  Dyno
//
//  Created by strictlyswift on 19-Feb-19.
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

    /// "Early escaping" flatMap which applies the transform as long as it is successful
    /// but if not, it early exits with the first .failure result.
    ///
    ///  If we have `[T]` and a mapping `(T) -> Result<S,Error>`
    ///  this will return `.success([S])` with all elements mapped, or `.failure(Error)`
    ///  returning the first failure result found.
    ///
    /// - Parameter transform: mapping to a Result type
    /// - Returns: .success([S]) if all map successfully, else first .failure(Failure)
    func flatMap<S,Failure>(_ transform: (Element) -> Result<S,Failure>) -> Result<[S],Failure>  {
        guard self.count > 0 else { return Result<[S],Failure>.success([]) }
        
        var resultArray: [S] = []
        for x in self {
            switch transform(x) {
            case .success(let s): resultArray += [s]
            case .failure(let f): return Result<[S],Failure>.failure(f)
            }
        }
        return .success(resultArray)
    }
}
