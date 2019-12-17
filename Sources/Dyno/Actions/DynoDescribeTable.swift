//
//  DynoDescribeTable.swift
//  
//
//  Created by RedPanda on 7-Dec-19.
//

import Foundation


import Foundation
import Combine

public extension Dyno {
    /// Describes a DynamoDB table
    func describeTable(name: String ) -> AnyPublisher<DynoTableDescription, Error> {
        
        return DynoDescribeTable( name: name,
                                  options: self.options)
            .sendRequest(forConnection: self.connection)
            .eraseToAnyPublisher()
    }
}

public struct DynoDescribeTable : DynoAction {
    let name: String
    let options: DynoOptions
    
    func actionName() -> String {
        "DynamoDB_20120810.DescribeTable"
    }
    
    func body() -> String {
        let descRequest = DynoDescribeTableRequest(TableName: self.name)
        
        return  (try? String(data: JSONEncoder().encode(descRequest), encoding: .utf8)) ?? ""
    }
    
    func sendRequest(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoTableDescription, Error> {
        return self.decodeResult(connection: conn, from: DynoDescribeTableResponse.self)
            .map { $0.Table }
        .eraseToAnyPublisher()
    }
}

internal struct DynoDescribeTableRequest : Encodable {
    let TableName: String
}

internal struct DynoDescribeTableResponse : Decodable {
    let Table : DynoTableDescription
}
