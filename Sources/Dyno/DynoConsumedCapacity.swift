//
//  DynoConsumedCapacity.swift
//  
//
//  Created by RedPanda on 31-Oct-19.
//

import Foundation

enum DynoConsumedCapacityDetailLevel : String, Encodable {
    case INDEXES
    case TOTAL
    case NONE
}

public struct DynoCapacityDetail {
    let CapacityUnits: Double?
    let ReadCapacityUnits: Double?
    let WriteCapacityUnits: Double?
    
    enum CodingKeys: String, CodingKey {
        case CapacityUnits
        case ReadCapacityUnits
        case WriteCapacityUnits
    }
}

func + (a: Double?, b: Double?) -> Double? {
    return (a ?? 0.0) + (b ?? 0.0)
}

extension DynoCapacityDetail : Decodable {
    init() {
        self.CapacityUnits = nil
        self.ReadCapacityUnits = nil
        self.WriteCapacityUnits = nil
    }
    
    static func +(a: DynoCapacityDetail, b: DynoCapacityDetail) -> DynoCapacityDetail {
        return DynoCapacityDetail(CapacityUnits: a.CapacityUnits + b.CapacityUnits,
                                  ReadCapacityUnits: a.ReadCapacityUnits + b.ReadCapacityUnits,
                                  WriteCapacityUnits: a.WriteCapacityUnits + b.WriteCapacityUnits)
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        
        if values.contains(.CapacityUnits) {
            self.CapacityUnits = try? values.decode(Double.self, forKey: .CapacityUnits)
        } else {
            self.CapacityUnits = nil
        }

        if values.contains(.ReadCapacityUnits) {
            self.ReadCapacityUnits = try? values.decode(Double.self, forKey: .ReadCapacityUnits)
        } else {
            self.ReadCapacityUnits = nil
        }
        
        if values.contains(.WriteCapacityUnits) {
            self.WriteCapacityUnits = try? values.decode(Double.self, forKey: .WriteCapacityUnits)
        } else {
            self.WriteCapacityUnits = nil
        }
    }
}

public struct DynoConsumedCapacity {
    let TotalConsumedCapacity: DynoCapacityDetail
    let TableConsumedCapacity: [String:DynoCapacityDetail]
    let GlobalSecondaryIndexes: [String:DynoCapacityDetail]
    let LocalSecondaryIndexes: [String:DynoCapacityDetail]
    
    enum CodingKeys : String, CodingKey {
        case CapacityUnits
        case GlobalSecondaryIndexes
        case LocalSecondaryIndexes
        case ReadCapacityUnits
        case Table
        case TableName
        case WriteCapacityUnits
    }
}

extension DynoConsumedCapacity : Decodable {
    init() {
        self.TotalConsumedCapacity = DynoCapacityDetail()
        self.TableConsumedCapacity = [:]
        self.GlobalSecondaryIndexes = [:]
        self.LocalSecondaryIndexes = [:]
    }
    
    public init(from decoder: Decoder) throws {
        var totalCapacityUnits : Double? = nil
        var totalReadCapacityUnits : Double? = nil
        var totalWriteCapacityUnits : Double? = nil
        var tableName: String? = nil
        var tableConsumedCapacity: DynoCapacityDetail?
        
        let values = try decoder.container(keyedBy: CodingKeys.self)
        
        if values.contains(.TableName) {
            tableName = try? values.decode(String.self, forKey: .TableName)
        }
        
        if values.contains(.CapacityUnits) {
            totalCapacityUnits = (try? values.decode(Double.self, forKey: .CapacityUnits)) ?? 0.0
        }
        if values.contains(.ReadCapacityUnits) {
            totalReadCapacityUnits = (try? values.decode(Double.self, forKey: .ReadCapacityUnits)) ?? 0.0
        }
        if values.contains(.WriteCapacityUnits) {
            totalWriteCapacityUnits = (try? values.decode(Double.self, forKey: .WriteCapacityUnits)) ?? 0.0
        }
        
        if values.contains(.Table) {
            tableConsumedCapacity = try? values.decode(DynoCapacityDetail.self, forKey: .Table)
        }
        
        if values.contains(.GlobalSecondaryIndexes), let globalSecondaryIndexes = try? values.decode([String:DynoCapacityDetail].self, forKey: .GlobalSecondaryIndexes) {
            self.GlobalSecondaryIndexes = globalSecondaryIndexes
        } else {
            self.GlobalSecondaryIndexes = [:]
        }
        
        if values.contains(.LocalSecondaryIndexes), let localSecondaryIndexes = try? values.decode([String:DynoCapacityDetail].self, forKey: .LocalSecondaryIndexes) {
            self.LocalSecondaryIndexes = localSecondaryIndexes
        } else {
            self.LocalSecondaryIndexes = [:]
        }
        
        self.TotalConsumedCapacity = DynoCapacityDetail(CapacityUnits: totalCapacityUnits, ReadCapacityUnits: totalReadCapacityUnits, WriteCapacityUnits: totalWriteCapacityUnits)
        
        if let tableName = tableName, let tableConsumedCapacity = tableConsumedCapacity {
            self.TableConsumedCapacity = [tableName : tableConsumedCapacity]
        } else {
            self.TableConsumedCapacity = [:]
        }
    }
    
    static func + (a: DynoConsumedCapacity, b: DynoConsumedCapacity) -> DynoConsumedCapacity {
        return DynoConsumedCapacity(TotalConsumedCapacity: a.TotalConsumedCapacity + b.TotalConsumedCapacity,
                                    TableConsumedCapacity: a.TableConsumedCapacity.append(b.TableConsumedCapacity, uniquingKeysWith: { $0 + $1 }),
                                    GlobalSecondaryIndexes: a.GlobalSecondaryIndexes.append(b.GlobalSecondaryIndexes, uniquingKeysWith: { $0 + $1 }),
                                    LocalSecondaryIndexes: a.LocalSecondaryIndexes.append(b.LocalSecondaryIndexes, uniquingKeysWith: { $0 + $1 }))
    }
}
