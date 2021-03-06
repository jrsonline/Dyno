//
//  DynoActions.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import Combine


public extension Dyno {
    /// Scans the given table, returning a single resulting array (even if the original table is very large).
    /// - Parameter table: The DynamoDb table to read.
    /// - Parameter filter: If present, filters the data. Note the filter happens _after_ the data is read, so will incur read costs.
    /// - Parameter consistentRead: See the AWS description of 'scan'. Defaults to True.
    /// - Parameter projection: A list of fields to return. If not present, all fields are returned.
    /// - Parameter indexName: The index to use. If not present, the default index is used.
    /// - Parameter sortedBy: A sorting function. May be nil in which case order is not guaranteed and may not be 'database table order'. Note that the sort is performed by Dyno, not on the database.
    /// - Parameter type: Required. The type to return. Must be Decodable.
    func scan<T : Decodable>(table: String,
                             filter: DynoCondition? = nil,
                             consistentRead: Bool = true,
                             projection: [DynoItemPath]? = nil,
                             indexName: String? = nil,
                             sortedBy: ((T,T) -> Bool)? = nil,
                             type:T.Type) -> AnyPublisher<DynoResult<[T]>, Error> {
        
        return DynoScan(table: table,
                            options: self.options,
                            filter: filter,
                            lastEvaluatedKey: nil,
                            consistentRead: consistentRead,
                            indexName: indexName,
                            projectionExpression: projection)
        .sendRequest(forConnection: self.connection, type:type)
        .collect()
        .map { $0.aggregateAndSort(by: sortedBy) }
        .eraseToAnyPublisher()

    }
    
    /// Scans the given table, returning a single resulting array of DynamoDb type descriptor-coded objects, as per here: [https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html#Programming.LowLevelAPI.DataTypeDescriptors].
    /// - Parameter table: The DynamoDb table to read.
    /// - Parameter filter: If present, filters the data. Note the filter happens _after_ the data is read, so will incur read costs.
    /// - Parameter consistentRead: See the AWS description of 'scan'. Defaults to True.
    /// - Parameter projection: A list of fields to return. If not present, all fields are returned.
    /// - Parameter indexName: The index to use. If not present, the default index is used.
    func scanToTypeDescriptors(table: String,
                               filter: DynoCondition? = nil,
                               consistentRead: Bool = true,
                               projection: [DynoItemPath]? = nil,
                               indexName: String? = nil) -> AnyPublisher<DynoResult<[[String : DynoAttributeValue]]>, Error>{
        
        return DynoScan(table: table,
                        options: self.options,
                        filter: filter,
                        lastEvaluatedKey: nil,
                        consistentRead: consistentRead,
                        indexName: indexName,
                        projectionExpression: projection)
        .sendRequestForTypeDescriptors(forConnection: self.connection)
        .collect()
        .map { $0.aggregated() }
        .eraseToAnyPublisher()
    }
}


/// Represents a "Scan All Values in Table" (then filter) action
public struct DynoScan : DynoAction {
    
    let table: String
    let options: DynoOptions
    let filter: DynoCondition?
    let lastEvaluatedKey: [String:DynoAttributeValue]?
    let consistentRead: Bool
    let indexName: String?
    let projectionExpression: [DynoItemPath]?
    var logName : String { get { return "\(table) Scan"} }
    
    init( table: String,
          options: DynoOptions = DynoOptions(),
          filter: DynoCondition? = nil,
          lastEvaluatedKey: [String:DynoAttributeValue]? = nil,
          consistentRead: Bool = true,
          indexName: String? = nil,
          projectionExpression: [DynoItemPath]? = nil
          ) {
        self.table = table
        self.options = options
        self.filter = filter
        self.lastEvaluatedKey = lastEvaluatedKey
        self.consistentRead = consistentRead
        self.indexName = indexName
        self.projectionExpression = projectionExpression
    }
    
    func actionName() -> String {
        "DynamoDB_20120810.Scan"
    }
    
    func body() -> String {
        let (projectionAttributeNames, pxMaxId) = self.encodeProjectionExpression(from: 0, projection: self.projectionExpression)
        let filter = self.filter?.toPayload(from:pxMaxId)

        let attributeNames = (filter?.toDynoExpressionAttributeNames()).combine(b: projectionAttributeNames, f: {$0}, g:{ $0.append($1) })
        
        let scanRequest = DynoScanRequest(FilterExpression: filter?.toDynoFilterExpression(),
                                          ExpressionAttributeNames: attributeNames,
                                          ExpressionAttributeValues: filter?.toDynoExpressionAttributeValues(),
                                          Limit: self.options.pageSize ?? 100,
                                          TableName: self.table,
                                          ExclusiveStartKey: lastEvaluatedKey,
                                          ConsistentRead: self.consistentRead,
                                          IndexName: self.indexName,
                                          ProjectionExpression: (projectionAttributeNames?.keys).map (Array.init),
                                          ReturnedConsumedCapacity: .INDEXES)
        return (try? String(data: JSONEncoder().encode(scanRequest), encoding: .utf8)) ?? ""
    }
    
    // sends the request, then maps the retrieved items back to the requested type
    public func sendRequest<T>(forConnection conn: DynoHttpConnection, type: T.Type) -> AnyPublisher<DynoResult<[T]>, Error> where T:Decodable {
        return do_sendRequest(forConnection: conn)
            .tryMap { response in
                let items = response.Items
                var output = Array<T>()
                for i in items where i.count > 0  {
                    if let item = (try self.constructItem(attributes: i) as T?) {
                        output += [item]
                    }
                }
                return DynoResult<[T]>(result: output, consumedCapacity: response.ConsumedCapacity)
            }.eraseToAnyPublisher()
    }
    
    // sends the request, and returns the AWS-formatted JSON as the result
    public func sendRequestForTypeDescriptors(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoResult<[[String : DynoAttributeValue]]>, Error> {
        return do_sendRequest(forConnection: conn)
            .tryMap { response in
                let items = response.Items
                return DynoResult<[[String : DynoAttributeValue]]>(result: items, consumedCapacity: response.ConsumedCapacity)
            }.eraseToAnyPublisher()
    }
    
    // if the number of items requested is smaller than the total, we need to repeatedly request
    private func do_sendRequest(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoScanResponse, Error> {
        let awsHttpRequest = conn.request(for: self)
        
        let v = awsHttpRequest
            .flatMap { (data:Data) -> AnyPublisher<DynoScanResponse, Error> in
                guard let returned : DynoScanResponse = try? JSONDecoder().decode(DynoScanResponse.self, from: data) else {
                    return Fail(error: DynoError("Could not decode response \(data.debugDescription) as DynoScanResponse"))
                        .eraseToAnyPublisher()
                }
                
                let publishSubsequent : AnyPublisher<DynoScanResponse, Error>
                if let lastEvaluatedKey = returned.LastEvaluatedKey {
                    let scanNext = DynoScan(table: self.table,
                                            options: self.options,
                                            filter: self.filter,
                                            lastEvaluatedKey: lastEvaluatedKey)
                    publishSubsequent = scanNext.do_sendRequest(forConnection: conn)
                } else {
                    publishSubsequent = Empty<DynoScanResponse, Error>().eraseToAnyPublisher()
                }
                
                return Result<DynoScanResponse,Error>.Publisher(.success(returned))
                    .append(publishSubsequent)
                    .eraseToAnyPublisher()

        }.eraseToAnyPublisher()
        
        return v

    }

}


internal struct DynoScanRequest : Encodable {
    let FilterExpression: String?
    let ExpressionAttributeNames: [String:String]?
    let ExpressionAttributeValues: [String:DynoAttributeValue]?
    let Limit: Int?
    let TableName: String
    let ExclusiveStartKey: [String:DynoAttributeValue]?
    let ConsistentRead: Bool
    let IndexName: String?
    let ProjectionExpression: [String]?
    let ReturnedConsumedCapacity: DynoConsumedCapacityDetailLevel
//    let Segment: Int   // TODO: Not currently supported
//    let Select: DynoSelectAttributesOption?   // Not supported (legacy I think - only count used really and you can get that elsewhere)
//    let TotalSegments: Int  // TODO: Not currently supported
    
    enum CodingKeys : CodingKey {
        case FilterExpression
        case ExpressionAttributeNames
        case ExpressionAttributeValues
        case Limit
        case TableName
        case ExclusiveStartKey
        case ConsistentRead
        case IndexName
        case ProjectionExpression
        case ReturnConsumedCapacity
    }
        
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(FilterExpression, forKey: .FilterExpression)
        if (ExpressionAttributeNames?.count ?? 0) > 0 {
            try container.encode(ExpressionAttributeNames, forKey: .ExpressionAttributeNames)
        }
        if (ExpressionAttributeValues?.count ?? 0) > 0 {
            try container.encode(ExpressionAttributeValues, forKey: .ExpressionAttributeValues)
        }
        try container.encode(Limit, forKey: .Limit)
        try container.encode(TableName, forKey: .TableName)
        try container.encode(ExclusiveStartKey, forKey: .ExclusiveStartKey)
        try container.encode(ConsistentRead, forKey: .ConsistentRead)
        try container.encode(IndexName, forKey: .IndexName)
        try container.encode(ProjectionExpression?.joined(separator: ", "), forKey: .ProjectionExpression)
        try container.encode(ReturnedConsumedCapacity, forKey: .ReturnConsumedCapacity)
    }
}


internal struct DynoScanResponse : Decodable {
    let ConsumedCapacity: DynoConsumedCapacity
    let Count: Int
    let Items: [[String:DynoAttributeValue]]
    let LastEvaluatedKey: [String:DynoAttributeValue]?
    let ScannedCount: Int?
    
    enum CodingKeys: String, CodingKey {
        case ConsumedCapacity
        case Count
        case Items
        case LastEvaluatedKey
        case ScannedCount
    }
    
    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.ConsumedCapacity = try values.decode(DynoConsumedCapacity.self, forKey: .ConsumedCapacity)
        self.Count = try values.decode(Int.self, forKey: .Count)
        self.Items = try values.decode([[String:DynoAttributeValue]].self, forKey: .Items)

        if values.contains(.LastEvaluatedKey) {
            self.LastEvaluatedKey = try? values.decode([String:DynoAttributeValue].self, forKey: .LastEvaluatedKey)
        } else {
            self.LastEvaluatedKey = nil
        }
        
        if values.contains(.ScannedCount) {
            self.ScannedCount = try? values.decode(Int.self, forKey: .ScannedCount)
        } else {
            self.ScannedCount = nil
        }
    }
}
