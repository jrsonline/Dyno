//
//  DynoActions.swift
//  Dyno
//
//  Created by strictlyswift on 8-Mar-19.
//

import Foundation
import Combine


extension Dyno {
    func scan<T : Decodable>(table: String,
                             filter: DynoScanFilter? = nil,
                             type:T.Type) -> AnyPublisher<[T], Error> {
        
        let scan = DynoScan(table: table,
                            options: self.options,
                            filter: filter,
                            lastEvaluatedKey: nil)
        
        return scan.sendRequest(forConnection: self.connection, type:type)
    }
}

/// Represents a "Scan All Values in Table" (then filter) action
struct DynoScan : DynoAction {
    
    let table: String
    let options: DynoOptions
    let filter: DynoScanFilter?
    let lastEvaluatedKey: [String:DynoAttributeValue]?
    var logName : String { get { return "\(table) Scan"} }
    
    init( table: String,
          options: DynoOptions = DynoOptions(),
          filter: DynoScanFilter? = nil,
          lastEvaluatedKey: [String:DynoAttributeValue]? = nil) {
        self.table = table
        self.options = options
        self.filter = filter
        self.lastEvaluatedKey = lastEvaluatedKey
    }
    
    func actionName() -> String {
        "DynamoDB_20120810.Scan"
    }
    
    func body() -> String {
        let filter = self.filter?.toPayload()
        let scanRequest = DynoScanRequest(FilterExpression: filter?.toDynoFilterExpression(),
                                          ExpressionAttributeNames: filter?.toDynoExpressionAttributeNames(),
                                          ExpressionAttributeValues: filter?.toDynoExpressionAttributeValues(),
                                          Limit: self.options.pageSize ?? 100,
                                          TableName: self.table,
                                          ExclusiveStartKey: lastEvaluatedKey)
        let output = (try? String(data: JSONEncoder().encode(scanRequest), encoding: .utf8)) ?? ""
        
        if options.log {
            NSLog("Scan request:")
            NSLog(output)
        }
         
        return output
    }
    
    // sends the request, then maps the retrieved items back to the requested type
    func sendRequest<T>(forConnection conn: DynoHttpConnection, type: T.Type) -> AnyPublisher<[T], Error> where T:Decodable {
        return do_sendRequest(forConnection: conn)
            .map { response in
                let items = response.Items
                var output = Array<T>()
                for i in items where i.count > 0  {
                    let json = DynoAttributeValue.constructJson(i)
                    if let constructedItem = try? JSONDecoder().decode(type, from: json) {
                        output.append(constructedItem)
                    }
                }
                return output
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
 
    func httpMethod() -> AWSHTTPVerb {
        .POST
    }
    
    func headers() -> [String : String] {
        [:]
    }
    
    func servicePath() -> String {
        "/"
    }
    
    func queryParameters() -> [String : String] {
        [:]
    }
    
}


internal struct DynoScanRequest : Codable {
    let FilterExpression: String?
    let ExpressionAttributeNames: [String:String]?
    let ExpressionAttributeValues: [String:DynoAttributeValue]?
    let Limit: Int?
    let TableName: String
    let ExclusiveStartKey: [String:DynoAttributeValue]?
    
    enum CodingKeys : CodingKey {
        case FilterExpression
        case ExpressionAttributeNames
        case ExpressionAttributeValues
        case Limit
        case TableName
        case ExclusiveStartKey
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
    }
}


internal struct DynoScanResponse : Decodable {
    let Count: Int
    let Items: [[String:DynoAttributeValue]]
    let LastEvaluatedKey: [String:DynoAttributeValue]?
    let ScannedCount: Int?
    
    enum CodingKeys: String, CodingKey {
        case Count
        case Items
        case LastEvaluatedKey
        case ScannedCount
    }
    
    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
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
