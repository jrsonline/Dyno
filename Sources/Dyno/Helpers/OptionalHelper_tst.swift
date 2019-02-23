//
//  OptionalHelper.swift
//  Dyno
//
//  Created by RedPanda on 19-Feb-19.
//

import Foundation

extension Optional {

    /// Convert an optional value to a Result, where nil is converted to the `withNil` value ; and non-nil is wrapped as **success**
    ///
    /// - Parameter withNil: "Failure" to return if the optional is nil.
    /// - Returns: Optional value wrapped as above.
    func toResult<Failure>(withNil: Failure) -> Result<Wrapped,Failure> {
        if let value = self {
            return .success(value)
        } else {
            return .failure(withNil)
        }
    }
    
    /// True if this optional value is "nil"
    func isNonNil() -> Bool {
        if case .none = self { return false }
        return true
    }
    
    /// True iff the optional value is NOT nil
    func isNil() -> Bool {
        return !isNonNil()
    }
}
