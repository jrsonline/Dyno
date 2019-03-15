
//
//  DynoAction.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation

/// DynoAction performs actions on the DynamoDB database
internal protocol DynoAction {
    associatedtype T
    var options: DynoOptions { get }
    func perform(connection: DynoConnection) -> DynoResult<T>
    var logName: String { get }
}

