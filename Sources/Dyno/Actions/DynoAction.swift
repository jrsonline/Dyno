//
//  File.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation
import Combine

internal protocol DynoAction : AWSAction {
}

extension DynoAction {
    func service() -> AWSService {
        return .dynamodb
    }
}
