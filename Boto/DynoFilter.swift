//
//  DynoFilter.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation



indirect public enum DynoFilter : CustomStringConvertible {
    case compare(DynoPath, DynoComparator, PythonConvertible)
    case between(sizeOf:DynoPath, from: PythonConvertible, to: PythonConvertible)
    case `in`(DynoPathNonKey, [PythonObject])
    case and(DynoFilter, DynoFilter)
    case or(DynoFilter, DynoFilter)
    case not(DynoFilter)
    case attributeExists(DynoPathNonKey)
    case attributeNotExists(DynoPathNonKey)
    case attributeType(DynoPathNonKey,String)
    case beginsWith(DynoPath, String)
    case contains(DynoPathNonKey, String)
    
    public var description : String { get {
        switch self {
        case let .compare(left, compare, right): return "\(left) \(compare.rawValue) \(right)"
        case let .between(sizeOf: p, from:from, to:to): return "\(p) BETWEEN \(from) AND \(to)"
        case let .in(p, ps): return "\(p) IN (\(ps.map { "\($0)" }.joined(separator: ", ")))"
            
        case let .and(left, right): return "(\(left.description)) AND (\(right.description))"
        case let .or(left, right): return "(\(left.description)) OR (\(right.description))"
        case let .not(p): return "NOT (\(p.description))"
        case let .beginsWith(path, prefix):return "\(path) BEGINS_WITH \(prefix)"
        case let .contains(path, str):return "\(path) CONTAINS \(str)"
        case let .attributeExists(path):return "attribute_exists(\(path))"
        case let .attributeNotExists(path):return "attribute_not_exists(\(path))"
        case let .attributeType(path,type):return "attribute_type(\(path),\(type))"
        }
        }}
    
    public func asFilterExpression() -> String {
        return self.description
    }
    
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
