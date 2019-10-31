//
//  DynoAttributeValue.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation

/// Represents a value coded to a DynamoDb type descriptor as per here: [https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html#Programming.LowLevelAPI.DataTypeDescriptors].
/// For example, `50` would be represented as `{"N":50}`.
public enum DynoAttributeValue : Codable, Equatable {
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        
        if values.contains(.S), let value = try? values.decode(String.self, forKey: .S) {
            self = .S(value)
            return
        }
        
        if values.contains(.BOOL) , let value = try? values.decode(Bool.self, forKey: .BOOL) {
            self = .BOOL(value)
            return
        }
        
        if values.contains(.NULL) {
            self = .NULL(true)
            return
        }
        
        if values.contains(.SS), let value = try? values.decode([String].self, forKey: .SS) {
            self = .SS(value)
            return
        }
        
        if values.contains(.N), let value = (try? values.decode(String.self, forKey: .N)) {
            self = .N(value)
            return
        }
        
        if values.contains(.NS), let value = (try? values.decode([String].self, forKey: .NS)) {
            self = .NS(value)
            return
        }
        
        if values.contains(.L), let value = try? values.decode([DynoAttributeValue].self, forKey: .L) {
            self = .L(value)
            return
        }
        
        if values.contains(.B), let value = try? Data(base64Encoded: values.decode(String.self, forKey: .B)) {
            self = .B(value)
            return
        }
        
        if values.contains(.BS), let value = try? (values.decode([String].self, forKey: .L).compactMap { str in Data(base64Encoded: str) }) {
            self = .BS(value)
            return
        }
        
        if values.contains(.M), let value = try? values.decode([String:DynoAttributeValue].self, forKey: .M) {
            self = .M(value)
            return
        }
        
        throw DynoError("Could not decode values '\(values)'")
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
            case let .N(number): try container.encode(number, forKey: .N)
            case let .B(value): try container.encode(value, forKey: .B)
            case let .BOOL(value): try container.encode(value, forKey: .BOOL)
            case let .BS(value): try container.encode(value, forKey: .BS)
            case let .M(value): try container.encode(value, forKey: .M)
            case let .S(value): try container.encode(value, forKey: .S)
            case let .NS(value): try container.encode(value, forKey: .NS)
            case let .NULL(value): try container.encode(value, forKey: .NULL)
            case let .SS(value): try container.encode(value, forKey: .SS)
            case let .L(value): try container.encode(value, forKey: .L)
        }
    }
    
    enum CodingKeys: String, CodingKey {
         case N
         case S
         case L
         case BOOL
         case BS
         case M
         case NS
         case SS
         case B
         case NULL
     }
    
    case B(Data)
    case BOOL(Bool)
    case BS([Data])
    case M([String:DynoAttributeValue])
    case S(String)
    case N(String)
    case NS([String])
    case NULL(Bool)
    case SS([String])
    case L([DynoAttributeValue])
    
    func jsonRepresentation(withNumbersQuoted: Bool = false) -> String {
        switch self {
        case .S(let string):return "\"\(string)\""
        case .N(let num):
            if withNumbersQuoted {
                return "'\(num)'"
            } else {
                return "\(num)"
            }
        case .BOOL(let bool): return "\(bool)"
        case .L(let list): return "[\(list.map {$0.jsonRepresentation()}.joined(separator: ","))]"
        case .B(let blob): return "\"\(blob.base64EncodedString())\""
        case .BS(let blobs): return "[\(blobs.map {"\"\($0.base64EncodedString())\""}.joined(separator: ","))]"
        case .NS(let nums): return "[\(nums.map {"\($0)"}.joined(separator: ","))]"
        case .SS(let strings): return "[\(strings.map {"\"\($0)\""}.joined(separator: ","))]"
        case .NULL(_): return "\(true)"
        case .M(let map): return String(data:DynoAttributeValue.constructJson(map), encoding: .utf8) ?? ""
        }
    }
    
    static func constructJsonString(_ map:[String:DynoAttributeValue], withNumbersQuoted: Bool = false) -> String {
        var jsonEntries = Array<String>()
        for entry in map.toSortedArray() {
            jsonEntries.append( "\"\(entry.0)\":\(entry.1.jsonRepresentation(withNumbersQuoted: withNumbersQuoted))" )
        }
        return "{\(jsonEntries.joined(separator:","))}"
    }
    
    static func constructJson(_ map:[String:DynoAttributeValue], withNumbersQuoted: Bool = false) -> Data {
        return DynoAttributeValue.constructJsonString(map, withNumbersQuoted: withNumbersQuoted).data(using: .utf8) ?? Data()

    }

}