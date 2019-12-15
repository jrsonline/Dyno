//
//  DynoGet.swift
//  
//
//  Created by RedPanda on 4-Dec-19.
//

import Foundation

import Foundation
import Combine

public extension Dyno {
    /// Reads an object with the given key/value pair from  the given table. The object must be codable.
    ///
    /// This only allows for retrieval of a single item. Use scan with an "in" filter to retrieve multiple.
    /// - Parameter table: The DynamoDb table to write to.
    func get<T : Codable>(table: String,
                          keyField: String,
                          keyValue: DynoConvertibleValue,
                          consistentRead: Bool = true,
                          projection: [DynoItemPath]? = nil,
                          type: T.Type) -> AnyPublisher<DynoResult<T?>, Error> {
        
        return DynoGet( table: table,
                        keyField: keyField,
                        keyValue: keyValue,
                        options: self.options,
                        consistentRead: consistentRead,
                        projection: projection)
            .sendRequest(forConnection: self.connection)
            .eraseToAnyPublisher()
    }
}

public struct DynoGet : DynoAction {
    let table: String
    let keyField: String
    let keyValue: DynoConvertibleValue
    let options: DynoOptions
    let consistentRead: Bool
    let projection: [DynoItemPath]?
    
    init(
        table: String,
        keyField: String,
        keyValue: DynoConvertibleValue,
        options: DynoOptions,
        consistentRead: Bool,
        projection: [DynoItemPath]?
    ) {
        self.table = table
        self.keyField = keyField
        self.keyValue = keyValue
        self.options = options
        self.consistentRead = consistentRead
        self.projection = projection
    }
    
    func actionName() -> String {
        "DynamoDB_20120810.GetItem"
    }
    
    func body() -> String {
        let key = [self.keyField : self.keyValue.toDynoAttributeValue() ]
        let (projectionAttributeNames, _) = self.encodeProjectionExpression(from: 0, projection: self.projection)

        let getRequest = DynoGetRequest(ConsistentRead: self.consistentRead,
                                        ExpressionAttributeNames: projectionAttributeNames,
                                        Key: key,
                                        ProjectionExpression: (projectionAttributeNames?.keys).map (Array.init),
                                        ReturnedConsumedCapacity: .INDEXES,
                                        TableName: self.table
                                        )
        
        return  (try? String(data: JSONEncoder().encode(getRequest), encoding: .utf8)) ?? ""
    }
    
    
    func sendRequest<T : Decodable>(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoResult<T?>, Error> {
        return decodeResultAndConstructItem(connection: conn,
                                            from: DynoGetResponse.self,
                                            to: T.self,
                                            attributes: \DynoGetResponse.Item,
                                            consumed: \DynoGetResponse.ConsumedCapacity)
    }
    
}

internal struct DynoGetRequest : Encodable {
    let ConsistentRead: Bool
    let ExpressionAttributeNames: [String:String]?
    let Key: [String:DynoAttributeValue]
    let ProjectionExpression: [String]?
    let ReturnedConsumedCapacity: DynoConsumedCapacityDetailLevel
    let TableName: String
    
    enum CodingKeys : CodingKey {
        case ConsistentRead
        case ExpressionAttributeNames
        case Key
        case ProjectionExpression
        case ReturnedConsumedCapacity
        case TableName
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        if (ExpressionAttributeNames?.count ?? 0) > 0 {
            try container.encode(ExpressionAttributeNames, forKey: .ExpressionAttributeNames)
        }
        
        try container.encode(ProjectionExpression?.joined(separator: ", "), forKey: .ProjectionExpression)
        try container.encode(ConsistentRead, forKey: .ConsistentRead)
        try container.encode(ReturnedConsumedCapacity, forKey: .ReturnedConsumedCapacity)
        try container.encode(Key, forKey: .Key)
        try container.encode(TableName, forKey: .TableName)
    }
}

internal struct DynoGetResponse : Decodable {
    let Item : [String:DynoAttributeValue]?
    let ConsumedCapacity: DynoConsumedCapacity
    
    enum CodingKeys: String, CodingKey {
        case Item
        case ConsumedCapacity
    }
    
    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        
        if values.contains(.ConsumedCapacity) {
            self.ConsumedCapacity = try values.decode(DynoConsumedCapacity.self, forKey: .ConsumedCapacity)
        } else {
            self.ConsumedCapacity = DynoConsumedCapacity()
        }
        
        self.Item = try values.decode([String:DynoAttributeValue]?.self, forKey: .Item)
    }
}


