//
//  DynoHelpers.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import PythonKit
import PythonCodable

/*
extension Dyno {
    public static func getStr(_ dict: Dictionary<String,PythonObject>, _ key: String) -> DynoResult<String> {
        guard let v = dict[key].flatMap (String.init)
            else { return .failure(DynoError("Can't read key \(key)")) }
        return .success(v)
    }
}
*/

extension Result {
    public func mapNilAsError<NewSuccess>(_ f:(Success) -> NewSuccess?, error: Failure) -> Result<NewSuccess, Failure> {
        switch self {
        case .success(let s):
            switch f(s) {
            case .some(let v): return .success(v)
            case .none: return .failure(error)
            }
        case .failure(let f): return .failure(f)
        }
    }
}
