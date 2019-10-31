
//
//  DynoBotoAction.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import PythonKit

/// DynoAction performs actions on the DynamoDB database
internal protocol DynoBotoAction {
    associatedtype T
    var options: DynoOptions { get }
    func perform(connection: DynoConnection) -> DynoResult<T>
    var logName: String { get }
}
