//
//  File.swift
//  
//
//  Created by RedPanda on 30-Nov-19.
//

import Foundation
import Combine

public extension Dyno {
    /// Writes a new object to the given table. The object must be encodable.
    /// - Parameter table: The DynamoDb table to write to.
    /// - Parameter item: Item to write. Must be Codable
    /// - Parameter customEncoder: If present, a way to convert the item into a `[String:DynoAttributeValue]` map
    /// - Parameter condition: This is checked before the write; if false, the value is not written
    /// - Parameter returnOriginal: `true` to return the prior value
    ///
    /// By default an empty array is returned; if you pass `true` to `returnOriginal` the original value is returned as the first
    /// (and only) value in the returned array.
    ///
    /// To prevent overwriting an existing value, pass `.attributeNotExists("id")` (where `id` is the name of the key field).
    func put<T : Codable>(table: String,
                          item: T,
                          customEncoder: ((T) -> [String:DynoAttributeValue])? = nil,
                          condition: DynoCondition? = nil,
                          returnOriginal: Bool = false) -> AnyPublisher<DynoResult<T?>, Error> {
        
        return DynoPut( table: table,
                        item: item,
                        customEncoder: customEncoder,
                        options: self.options,
                        condition: condition,
                        returnOriginal: returnOriginal)
            .sendRequest(forConnection: self.connection)
            .eraseToAnyPublisher()
    }
}

public struct DynoPut<T:Encodable> : DynoAction {
    let table: String
    let item: T
    let customEncoder: ((T) -> [String:DynoAttributeValue])?
    let options: DynoOptions
    let condition: DynoCondition?
    let returnOriginal: Bool
    
    
    init(
        table: String,
        item: T,
        customEncoder: ((T) -> [String:DynoAttributeValue])? = nil,
        options: DynoOptions = DynoOptions(),
        condition: DynoCondition? = nil,
        returnOriginal: Bool = false
    ) {
        self.table = table
        self.item = item
        self.customEncoder = customEncoder
        self.options = options
        self.condition = condition
        self.returnOriginal = returnOriginal
    }
    
    func actionName() -> String {
        "DynamoDB_20120810.PutItem"
    }
    
    func body() -> String {
        let condition = self.condition?.toPayload(from: 0)
        let encodedItem = encodeItem(self.item, using: customEncoder)
        
        let putRequest = DynoPutRequest(ConditionExpression: condition?.toDynoFilterExpression(),
                                        ExpressionAttributeNames: condition?.toDynoExpressionAttributeNames(),
                                        ExpressionAttributeValues: condition?.toDynoExpressionAttributeValues(),
                                        Item: encodedItem,
                                        ReturnedConsumedCapacity: .INDEXES,
                                        ReturnItemCollectionMetrics: "NONE",
                                        ReturnValues: self.returnOriginal ? "ALL_OLD" : "NONE",
                                        TableName: self.table
                                        )
        
        return  (try? String(data: JSONEncoder().encode(putRequest), encoding: .utf8)) ?? ""
    }
    
    func encodeItem(_ item: T, using builder: ((T) -> [String:DynoAttributeValue])? ) -> [String:DynoAttributeValue] {
        if let builder = builder {
            return builder(item)
        } else {
            return DynoAttributeValue.fromTypedObject(item)
        }
    }
    
    func sendRequest<T : Decodable>(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoResult<T?>, Error> {
        return decodeResultAndConstructItem(connection: conn,
                                            from: DynoPutResponse.self,
                                            to: T.self,
                                            attributes: \DynoPutResponse.Attributes,
                                            consumed: \DynoPutResponse.ConsumedCapacity)
//        return decodeResult(connection: conn, from: DynoPutResponse.self)
//            .tryMap { response in
//                let item : [T] = try self.constructItem(attributes: response.Attributes)
//                return DynoResult<T>(result: item, consumedCapacity: response.ConsumedCapacity)
//        }
//        .eraseToAnyPublisher()
    }
}

internal struct DynoPutRequest : Encodable {
    let ConditionExpression: String?
    let ExpressionAttributeNames: [String:String]?
    let ExpressionAttributeValues: [String:DynoAttributeValue]?
    let Item: [String:DynoAttributeValue]
    let ReturnedConsumedCapacity: DynoConsumedCapacityDetailLevel
    let ReturnItemCollectionMetrics: String
    let ReturnValues: String
    let TableName: String
    
    enum CodingKeys : CodingKey {
        case ConditionExpression
        case ExpressionAttributeNames
        case ExpressionAttributeValues
        case Item
        case ReturnedConsumedCapacity
        case ReturnItemCollectionMetrics
        case ReturnValues
        case TableName
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        if let ConditionExpression = ConditionExpression {
            try container.encode(ConditionExpression, forKey: .ConditionExpression)
        }
        
        if (ExpressionAttributeNames?.count ?? 0) > 0 {
            try container.encode(ExpressionAttributeNames, forKey: .ExpressionAttributeNames)
        }
        
        if (ExpressionAttributeValues?.count ?? 0) > 0 {
            try container.encode(ExpressionAttributeValues, forKey: .ExpressionAttributeValues)
        }
        
        try container.encode(Item, forKey: .Item)
        try container.encode(ReturnedConsumedCapacity, forKey: .ReturnedConsumedCapacity)
        try container.encode(ReturnItemCollectionMetrics, forKey: .ReturnItemCollectionMetrics)
        try container.encode(ReturnValues, forKey: .ReturnValues)
        try container.encode(TableName, forKey: .TableName)
    }
}

internal struct DynoPutResponse : Decodable {
    let Attributes : [String:DynoAttributeValue]?
    let ConsumedCapacity: DynoConsumedCapacity
    
    enum CodingKeys: String, CodingKey {
        case Attributes
        case ConsumedCapacity
    }
    
    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        
        if values.contains(.ConsumedCapacity) {
            self.ConsumedCapacity = try values.decode(DynoConsumedCapacity.self, forKey: .ConsumedCapacity)
        } else {
            self.ConsumedCapacity = DynoConsumedCapacity()
        }
        
        if values.contains(.Attributes) {
            self.Attributes = try values.decode([String:DynoAttributeValue].self, forKey: .Attributes)
        } else {
            self.Attributes = nil
        }
    }
}


