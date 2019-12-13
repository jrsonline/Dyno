//
//  File.swift
//  
//
//  Created by RedPanda on 7-Dec-19.
//

import Foundation
import StrictlySwiftLib

public enum DynoTableStatus : String, Codable, Equatable {
    case creating = "CREATING"
    case active = "ACTIVE"
    case deleting = "DELETING"
}

public struct DynoTableDescription : Decodable {
    let CreationDateTime: Date?
    let GlobalTableVersion: String?
    let ItemCount: Int
    let TableArn: String
    let TableId: String?
    let TableSizeBytes: Int
    let TableStatus: DynoTableStatus

    enum CodingKeys: String, CodingKey {
        case CreationDateTime
        case GlobalTableVersion
        case ItemCount
        case TableArn
        case TableId
        case TableSizeBytes
        case TableStatus
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        
        self.ItemCount = try values.decode(Int.self, forKey: .ItemCount)
        self.TableArn = try values.decode(String.self, forKey: .TableArn)
        self.TableId = try values.mapIfContains(.TableId) { try values.decode(String.self, forKey: $0) }
        self.TableSizeBytes = try values.decode(Int.self, forKey: .TableSizeBytes)
        self.TableStatus = try values.decode(DynoTableStatus.self, forKey: .TableStatus)

        self.CreationDateTime = try values.mapIfContains(.CreationDateTime) { Date(timeIntervalSince1970: try values.decode(Double.self, forKey: $0)) }
        
        self.GlobalTableVersion = try values.mapIfContains(.GlobalTableVersion) { try values.decode(String.self, forKey: $0) }


    }
}
