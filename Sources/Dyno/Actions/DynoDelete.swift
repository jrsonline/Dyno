//
//  DynoDelete.swift
//  
//
//  Created by RedPanda on 5-Dec-19.
//

import Foundation
import Combine

public extension Dyno {
    /// Deletes the item with a given key from a DynamoDB table.
    func delete(table: String,
                keyField: String,
                keyValue: DynoConvertibleValue,
                condition: DynoCondition? = nil,
                returnOriginal: Bool = false) -> AnyPublisher<DynoResult<Void>, Error> {
        
        return DynoDelete<Void>( table: table,
                           keyField: keyField,
                           keyValue: keyValue,
                           options: self.options,
                           condition: condition,
                           returnOriginal: returnOriginal)
            .sendRequest(forConnection: self.connection)
            .eraseToAnyPublisher()
    }
}

public struct DynoDelete<T>: DynoAction {
    let table: String
    let options: DynoOptions
    let keyField: String
    let keyValue: DynoConvertibleValue
    let condition: DynoCondition?
    let returnOriginal: Bool
    
    init(
        table: String,
        keyField: String,
        keyValue: DynoConvertibleValue,
        options: DynoOptions,
        condition: DynoCondition?,
        returnOriginal: Bool) {
        self.table = table
        self.keyField = keyField
        self.keyValue = keyValue
        self.options = options
        self.condition = condition
        self.returnOriginal = returnOriginal
    }
    
    func actionName() -> String {
        "DynamoDB_20120810.DeleteItem"
    }
    
    
    func body() -> String {
        let condition = self.condition?.toPayload(from: 0)
        let key = [self.keyField : self.keyValue.toDynoAttributeValue() ]

        let deleteRequest = DynoDeleteRequest(ConditionExpression: condition?.toDynoFilterExpression(),
                                              ExpressionAttributeNames: condition?.toDynoExpressionAttributeNames(),
                                              ExpressionAttributeValues: condition?.toDynoExpressionAttributeValues(),
                                              Key: key,
                                              ReturnedConsumedCapacity: .INDEXES,
                                              ReturnItemCollectionMetrics: "NONE",
                                              ReturnValues: self.returnOriginal ? "ALL_OLD" : "NONE",
                                              TableName: self.table
        )
        
        return  (try? String(data: JSONEncoder().encode(deleteRequest), encoding: .utf8)) ?? ""
    }
    
    func sendRequest(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoResult<Void>, Error>  {
        return decodeResult(connection: conn, from: DynoDeleteResponse.self)
            .tryMap { response in
                return DynoResult<Void>(result: (), consumedCapacity: response.ConsumedCapacity)
        }
        .eraseToAnyPublisher()
    }
}

extension DynoDelete where T:Decodable {
    func sendRequest(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoResult<T?>, Error>  {
        return decodeResultAndConstructItem(connection: conn,
                                            from: DynoDeleteResponse.self,
                                            to: T.self,
                                            attributes: \DynoDeleteResponse.Attributes,
                                            consumed: \DynoDeleteResponse.ConsumedCapacity)
    }
}


internal struct DynoDeleteRequest : Encodable {
    let ConditionExpression: String?
    let ExpressionAttributeNames: [String:String]?
    let ExpressionAttributeValues: [String:DynoAttributeValue]?
    let Key: [String:DynoAttributeValue]
    let ReturnedConsumedCapacity: DynoConsumedCapacityDetailLevel
    let ReturnItemCollectionMetrics: String
    let ReturnValues: String
    let TableName: String
    
    enum CodingKeys : CodingKey {
        case ConditionExpression
        case ExpressionAttributeNames
        case ExpressionAttributeValues
        case Key
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
        
        try container.encode(Key, forKey: .Key)
        try container.encode(ReturnedConsumedCapacity, forKey: .ReturnedConsumedCapacity)
        try container.encode(ReturnItemCollectionMetrics, forKey: .ReturnItemCollectionMetrics)
        try container.encode(ReturnValues, forKey: .ReturnValues)
        try container.encode(TableName, forKey: .TableName)
    }
}


internal struct DynoDeleteResponse : Decodable {
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
