//
//  DynoCreateTable.swift
//  
//
//  Created by RedPanda on 6-Dec-19.
//

import Foundation
import Combine

public extension Dyno {
    /// Creates a DynamoDB table.
    ///
    /// Note that DynamoDB may take some time to create the table. You can check if the table
    /// is created by checking the `TableState` attribute of the returned table description structure - wait for `.active`.
    /// You can also use `DynoDescribeTable` to poll for the table state.
    ///
    /// Alternatively, use the `createTableAndWait` function to poll for the table to be actually created.
    /// - Parameters:
    ///   - name: Table to create
    ///   - partitionKeyField: (name,type) pair, indicating the field which will hold the primary key of the table and the type of the key.
    ///   - sortKeyField: (name,type) pair for the secondary key. Refer to DynamoDB documentation
    ///   - billingMode: how AWS will bill for access to this table.
    ///   - tags: dictionary of key/value strings used to tag the table.
    func createTable(name: String,
                     partitionKeyField: (name:String,type:DynoAttributeDefinition),
                     sortKeyField: (name:String,type:DynoAttributeDefinition)? = nil,
                     billingMode: DynoBillingMode = .provisioned(throughput: 5),
                     tags: [String:String] = [:] ) -> AnyPublisher<DynoTableDescription, Error> {
        
        return DynoCreateTable( name: name,
                                partitionKeyField: partitionKeyField,
                                options: self.options,
                                sortKeyField: sortKeyField,
                                billingMode: billingMode,
                                tags: tags)
            .sendRequest(forConnection: self.connection)
            .eraseToAnyPublisher()
    }
    
    /// Creates a DynamoDB table and repeatedly checks until it's actually created (returning True).
    /// The publisher will not return anything until the table is created, unless an error
    /// occurs. You probably want to limit the time taken in the check.
    /// - Parameters:
    ///   - name: Table to create
    ///   - partitionKeyField: (name,type) pair, indicating the field which will hold the primary key of the table and the type of the key.
    ///   - sortKeyField: (name,type) pair for the secondary key. Refer to DynamoDB documentation
    ///   - billingMode: how AWS will bill for access to this table.
    ///   - tags: dictionary of key/value strings used to tag the table.
    ///   - pollInterval: How frequently to check for the creation - defaults to 1 second
    @available(OSX 15.0, *)
    func createTableWaitActive(name: String,
                     partitionKeyField: (name:String,type:DynoAttributeDefinition),
                     sortKeyField: (name:String,type:DynoAttributeDefinition)? = nil,
                     billingMode: DynoBillingMode = .provisioned(throughput: 5),
                     tags: [String:String] = [:] ,
                     pollInterval: Double = 1) -> AnyPublisher<Bool, Error> {
        
        let timer = Timer.publish(every: pollInterval, on: .main, in: .common)
            .autoconnect()
            .mapError { _ in DynoError("") }  // we need to promote Timer to a DynoError from Never, as describeTable can fail
            .flatMap { _ in self.describeTable(name: name)  }
            .map { $0.TableStatus == .active }
        

        return DynoCreateTable( name: name,
                                partitionKeyField: partitionKeyField,
                                options: self.options,
                                sortKeyField: sortKeyField,
                                billingMode: billingMode,
                                tags: tags)
            .sendRequest(forConnection: self.connection)
            .mapError { err in DynoError(err) }
            .map { $0.TableStatus == .active }
            .append(timer)
            .first(where: {$0 == true})
            .eraseToAnyPublisher()
    }
}

public enum DynoAttributeDefinition : String {
    case string = "S"
    case number = "N"
    case binary = "B"
}

public enum DynoBillingMode {
    case provisioned(throughput: Int)
    case payPerRequest
    
    
    public func billingMode() -> String {
        switch self {
        case .provisioned(throughput: _):
            return "PROVISIONED"
        case .payPerRequest:
            return "PAY_PER_REQUEST"
        }
    }
    
    public func provisionedThroughput() -> [String:Int] {
        switch self {
        case .provisioned(throughput: let p):
            return ["ReadCapacityUnits":p,
                    "WriteCapacityUnits":p]

        case .payPerRequest:
            return [:]
        }
    }
}

public struct DynoCreateTable : DynoAction {
    let name: String
    let partitionKeyField: (name:String,type:DynoAttributeDefinition)
    let options: DynoOptions
    let sortKeyField: (name:String,type:DynoAttributeDefinition)?
    let billingMode: DynoBillingMode
    let tags: [String:String]
    
    func actionName() -> String {
        "DynamoDB_20120810.CreateTable"
    }
    
    func body() -> String {
        let keySchema = [(partitionKeyField.name, "HASH")] + (sortKeyField != nil ? [(sortKeyField!.name, "RANGE")] : [])
        let attributeDefinitions = [partitionKeyField.name:partitionKeyField.type].append( sortKeyField != nil ? [sortKeyField!.name: sortKeyField!.type] : Dictionary<String,DynoAttributeDefinition>() )
        
        let getRequest = DynoCreateTableRequest(TableName: self.name,
                                                AttributeDefinitions: attributeDefinitions,
                                                KeySchema: keySchema,
                                                BillingMode: self.billingMode,
                                                Tags: self.tags)
         
         return  (try? String(data: JSONEncoder().encode(getRequest), encoding: .utf8)) ?? ""
     }
    
    func sendRequest(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoTableDescription, Error> {
        return self.decodeResult(connection: conn, from: DynoCreateTableResponse.self)
            .map {  $0.TableDescription }
        .eraseToAnyPublisher()
    }
}

internal struct DynoCreateTableRequest : Encodable {
    let TableName: String
    let AttributeDefinitions: [String:DynoAttributeDefinition]
    let KeySchema: [(String,String)]
    let BillingMode: DynoBillingMode
    let Tags: [String:String]
    
    enum CodingKeys : CodingKey {
        case TableName
        case AttributeDefinitions
        case KeySchema
        case BillingMode
        case ProvisionedThroughput
        case Tags
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(TableName, forKey: .TableName)
        try container.encode(AttributeDefinitions.map { ["AttributeName":$0.0, "AttributeType":$0.1.rawValue] }, forKey: .AttributeDefinitions)
        try container.encode(KeySchema.map { ["AttributeName":$0.0, "KeyType":$0.1]}, forKey: .KeySchema)
        try container.encode(BillingMode.billingMode(), forKey: .BillingMode)
        
        if BillingMode.provisionedThroughput().count > 0 {
            try container.encode(BillingMode.provisionedThroughput(), forKey: .ProvisionedThroughput)
        }
        
        try container.encode(Tags.map { [ "Key":$0.0, "Value":$0.1 ] }, forKey:.Tags)
    }
}


internal struct DynoCreateTableResponse : Decodable {
    let TableDescription : DynoTableDescription
}
