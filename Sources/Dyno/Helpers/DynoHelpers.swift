//
//  DynoHelpers.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import PythonKit

extension Dyno {
    public static func getStr(_ dict: Dictionary<String,PythonObject>, _ key: String) -> DynoResult<String> {
        guard let v = dict[key].flatMap (String.init)
            else { return .failure(DynoError("Can't read key \(key)")) }
        return .success(v)
    }
}
