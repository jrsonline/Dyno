//
//  DynoFilter.swift
//  Dyno
//
//  Created by strictlyswift on 3/2/19.
//

import Foundation
import PythonKit

let BOTO3_CONDITIONS  = Python.import("boto3.dynamodb.conditions")
let BOTO3_KEY = BOTO3_CONDITIONS.Key
let BOTO3_ATTR = BOTO3_CONDITIONS.Attr


public protocol DynoPath {
    var attr: String { get }
    var boto3func :  PythonObject { get }
}

/// Represents a expression referring to a key in a DynamoDb table
public struct DynoPathKey : DynoPath, ExpressibleByStringLiteral, CustomStringConvertible {
    public typealias StringLiteralType = String
    
    public let attr: String
    public let boto3func: PythonObject = BOTO3_KEY
    public init(stringLiteral key:String) { self.attr = key }
    
    public var description : String { get { return "Key(\(attr))" }}
}

/// Represents a expression referring to non-key attribute in a DynamoDb table
public struct DynoPathNonKey : DynoPath, ExpressibleByStringLiteral, CustomStringConvertible {
    public typealias StringLiteralType = String
    
    public let attr: String
    public let boto3func: PythonObject = BOTO3_ATTR
    public init(stringLiteral attr:String) { self.attr = attr }
    
    public var description : String { get { return "Non-key(\(attr))" }}

    
    /// Currently not implemented properly
    private func size() -> DynoPathNonKey {
        return DynoPathNonKey(stringLiteral: "size(\(attr))")
    }
}

public enum DynoComparator : String {
    case lt = "<"
    case le = "<="
    case gt = ">"
    case ge = ">="
    case eq = "="
    
}
/// Describes a scan filter. Note that DynamoDb distinguishes between 'key' attributes in a table, and
/// non-key. The key is set up as part of the table definition. Not all DynoFilter capabilities are
/// available on key attributes; you may incur a performance cost for using the non-key ones.
///
/// - compare: Compare a Key or non-key attribute against a value (always given as a string)
/// - between: Does this attribute exist between from: and to:  (always given as strings)
/// - in: Is this attribute in this list of values?
/// - beginsWith: Does this attribute start with this string?
/// - and: AND filters together
/// - or: OR filters together
/// - not: invert a filter
/// - attributeExists: (_non-key only_) Does this attribute exist?
/// - attributeNotExists: (_non-key only_) Does this attribute NOT exist?
/// - attributeType: (_non-key only_) Does this attribute match the given type?
/// - contains: (_non-key only_) Does this attribute contain the given string
/// - size: (_non-key only_) **Currently unsupported** . Returns the size of the attibute. **Important** this must then be used with another filter operator
indirect public enum DynoFilter : CustomStringConvertible {
    case compare(DynoPath, DynoComparator, PythonConvertible)
    case between(DynoPath, from: PythonConvertible, to: PythonConvertible)
    case `in`(DynoPathNonKey, [PythonObject])
    case and(DynoFilter, DynoFilter)
    case or(DynoFilter, DynoFilter)
    case not(DynoFilter)
    case attributeExists(DynoPathNonKey)
    case attributeNotExists(DynoPathNonKey)
    case attributeType(DynoPathNonKey,String)
    case beginsWith(DynoPath, String)
    case contains(DynoPathNonKey, String)
//    case size(DynoPathNonKey)   Currently unsupported
    
    public var description : String { get {
        switch self {
        case let .compare(left, compare, right): return "\(left) \(compare.rawValue) \(right)"
        case let .between(p, from:from, to:to): return "\(p) BETWEEN \(from) AND \(to)"
        case let .in(p, ps): return "\(p) IN (\(ps.map { "\($0)" }.joined(separator: ", ")))"
            
        case let .and(left, right): return "(\(left.description)) AND (\(right.description))"
        case let .or(left, right): return "(\(left.description)) OR (\(right.description))"
        case let .not(p): return "NOT (\(p.description))"
        case let .beginsWith(path, prefix):return "\(path) BEGINS_WITH \(prefix)"
        case let .contains(path, str):return "\(path) CONTAINS \(str)"
        case let .attributeExists(path):return "attribute_exists(\(path))"
        case let .attributeNotExists(path):return "attribute_not_exists(\(path))"
        case let .attributeType(path,type):return "attribute_type(\(path),\(type))"
  //      case let .size(path):return "size(\(path))"
        }
        }}
    
    public func asFilter() -> PythonObject {
        let keyNonKeyChooser : (DynoPath) -> PythonObject = { path in
            return path.boto3func(path.attr)
        }
        
        switch self {
        case let .compare(path, .lt, value): return keyNonKeyChooser(path).lt(value)
        case let .compare(path, .le, value): return keyNonKeyChooser(path).le(value)
        case let .compare(path, .gt, value): return keyNonKeyChooser(path).gt(value)
        case let .compare(path, .ge, value): return keyNonKeyChooser(path).ge(value)
        case let .compare(path, .eq, value): return keyNonKeyChooser(path).eq(value)
        case let .between(path, from: from, to: to): return keyNonKeyChooser(path).between(from,to)
        case let .beginsWith(path, prefix):return keyNonKeyChooser(path).begins_with(prefix)
            
        // Logical operators
        case let .and(f1, f2): return f1.asFilter().__and__(f2.asFilter())
        case let .or(f1, f2): return f1.asFilter().__or__(f2.asFilter())
        case let .not(f1): return f1.asFilter().__invert__()
            
        // These are for attributes only
        case let .in(attr, ls): return attr.boto3func(attr.attr).is_in(ls)
        case let .attributeExists(attr): return attr.boto3func(attr.attr).exists()
        case let .attributeNotExists(attr): return attr.boto3func(attr.attr).not_exists()
        case let .attributeType(attr,type): return attr.boto3func(attr.attr).attribute_type(type)
        case let .contains(attr, str): return attr.boto3func(attr.attr)[dynamicMember:"contains"].dynamicallyCall(withArguments: [str])  // convoluted as 'contains' is a static member on the type also
    //    case let .size(attr): return boto3NonKey(attr.attr).size()
        }
    }
}
