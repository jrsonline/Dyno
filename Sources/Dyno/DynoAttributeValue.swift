//
//  DynoAttributeValue.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation

/// Represents a value coded to a DynamoDb type descriptor as per here: [https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html#Programming.LowLevelAPI.DataTypeDescriptors].
/// Note that numbers are represented via strings For example, `50` would be represented as `{"N":50}`.
/// There is also no date representation.
public enum DynoAttributeValue : Codable, Equatable, Hashable {
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

    static func fromTypedObject<T>(_ item:T, depth: Int = 0) -> [String:DynoAttributeValue] {
        guard depth < 10 else { fatalError("Trying to get into a deep hierarchy (>10 levels) when encoding \(item) to [String:DynoAttributeValue]. Possible infinite recursion? Provide custom Dyno encoding instead.") }
        let mirror = Mirror(reflecting: item)
        var output = Dictionary<String,DynoAttributeValue>()
        for m in mirror.children {
            if let label = m.label {
                output[label] = Self.fromTypedValue(m.value, depth: depth+1)
            }
        }
        
        return output
    }
    
    static func fromTypedValue(_ value: Any, depth: Int) -> DynoAttributeValue {
        guard depth < 10 else { fatalError("Trying to get into a deep hierarchy (>10 levels) when encoding \(value) to DynoAttributeValue. Possible infinite recursion? Provide custom Dyno encoding instead.") }
        
        switch value {
        case let b as Bool: return .BOOL(b)
        case let d as Data: return .B(d)
        case let d as Date: return .S(d.isoFormat())
        case let s as String: return .S(s)
        case let i as Int: return .N("\(i)")
        case let f as Float: return .N("\(f)")
        case let d as Double: return .N("\(d)")
        case let u as UInt: return .N("\(u)")

        case let array as Array<Any>:
            // can't switch on collection type :-(
            if array is Array<Data>  { return .BS(array as! Array<Data>) }
            else if array is Array<Double>  { return .NS((array as! Array<Double>).map {"\($0)"}) }
            else if array is Array<Int>  { return .NS((array as! Array<Int>).map {"\($0)"}) }
            else if array is Array<Float>  { return .NS((array as! Array<Float>).map {"\($0)"}) }
            else if array is Array<UInt>  { return .NS((array as! Array<UInt>).map {"\($0)"}) }
 //           else if array is Array<String>  { return .SS(array as! Array<String>)}     // ?? why not
            else {
                return .L( array.map { Self.fromTypedValue($0, depth: depth+1)  } )
            }
        case let dict as Dictionary<String,Any>: return .M( dict.mapValues { Self.fromTypedValue($0, depth: depth+1) })
            
        default:
            let o = value as Optional<Any>
            switch o {
            case let .some(v): return .M(Self.fromTypedObject(v, depth: depth+1))
            case .none: return .NULL(true)
            }
        }
    }
    
}
