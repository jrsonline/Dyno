//
//  DynoResult.swift
//  
//
//  Created by RedPanda on 31-Oct-19.
//

import Foundation

/// Represents a result of a DynamoDb operation, including the capacity used by the operation.
///
/// - Note: Always returns an array, even if a single value is returned by the action (so the array would contain just a single value)
public struct DynoResult<T>  {
    let result: [T]
    let consumedCapacity: DynoConsumedCapacity
}

extension Array {
    /// Aggregates a list of DynoResults to a single result. Probably more useful for testing.
    func aggregated<T>() -> DynoResult<T> where Element == DynoResult<T> {
        return DynoResult<T>( result: Array<T>(self.map {$0.result}.joined()), consumedCapacity: self.map {$0.consumedCapacity}.reduce(DynoConsumedCapacity()) { $0 + $1 })
    }
}
