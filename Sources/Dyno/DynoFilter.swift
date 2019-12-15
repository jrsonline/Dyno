//
//  DynoFilter.swift
//  Dyno
//
//  Created by strictlyswift on 3/2/19.
//

import Foundation
import StrictlySwiftLib

public enum DynoComparator : String {
    case lt = "<"
    case le = "<="
    case gt = ">"
    case ge = ">="
    case eq = "="
    case ne = "<>"
}
public typealias DynoItemPath = String

/// Describes a scan filter. *DynoItemPath* is the name (string) of an attribute, including period separators for sub-queries if necessary, eg `"age"` or `"Dinosaur.size"`
///
/// - compare: Compare an attribute against a value
/// - compareSize: Compare the size of an attribute against a value. Strings are sized with their length; lists with their count.
/// - betweenValue: Is the value of this attribute  between from: and to:
/// - betweenSize: Is the size (as per `compareSize`) of this attribute  between from: and to:
/// - in: Is this attribute in this list of values?
/// - beginsWith: Does this attribute start with this string?
/// - and: AND filters together
/// - or: OR filters together
/// - not: invert a filter
/// - attributeExists: Does this attribute exist?
/// - attributeNotExists: Does this attribute NOT exist?
/// - attributeType: Does this attribute match the given type? The type is as per the AWS low-level type descriptors [https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html#Programming.LowLevelAPI.DataTypeDescriptors]
/// - contains: Does this attribute contain the given string?
indirect public enum DynoCondition: CustomStringConvertible {
    case compare(DynoItemPath, DynoComparator, DynoConvertibleValue)
    case compareSize(DynoItemPath, DynoComparator, DynoConvertibleValue)
    case betweenValue(of:DynoItemPath, from: DynoConvertibleValue, to: DynoConvertibleValue)
    case betweenSize(of:DynoItemPath, from: DynoConvertibleValue, to: DynoConvertibleValue)
    case `in`(DynoItemPath, [DynoConvertibleValue])
    case and(DynoCondition, DynoCondition)
    case or(DynoCondition, DynoCondition)
    case not(DynoCondition)
    case attributeExists(DynoItemPath)
    case attributeNotExists(DynoItemPath)
    case attributeType(DynoItemPath,String)
    case beginsWith(DynoItemPath, String)
    case contains(DynoItemPath, String)
    
    
    public var description : String { get {
        switch self {
        case let .compare(left, compare, right): return "\(left) \(compare.rawValue) \(right.toDynoAttributeValue())"
        case let .compareSize(left, compare, right): return "size(\(left)) \(compare.rawValue) \(right.toDynoAttributeValue())"
        case let .betweenValue(of:p, from:from, to:to): return "\(p) BETWEEN \(from.toDynoAttributeValue()) AND \(to.toDynoAttributeValue())"
        case let .betweenSize(of:p, from:from, to:to): return "size(\(p)) BETWEEN \(from.toDynoAttributeValue()) AND \(to.toDynoAttributeValue())"
        case let .in(p, ps): return "\"\(p)\" IN (\(ps.map { "\($0.toDynoAttributeValue())" }.joined(separator: ", ")))"
            
        case let .and(left, right): return "(\(left.description)) AND (\(right.description))"
        case let .or(left, right): return "(\(left.description)) OR (\(right.description))"
        case let .not(p): return "NOT (\(p.description))"
        case let .beginsWith(path, prefix):return "\(path) BEGINS_WITH \"\(prefix)\""
        case let .contains(path, str):return "\"\(path)\" CONTAINS \"\(str)\""
        case let .attributeExists(path):return "attribute_exists(\"\(path)\")"
        case let .attributeNotExists(path):return "attribute_not_exists(\"\(path)\")"
        case let .attributeType(path,type):return "attribute_type(\"\(path)\",\"\(type)\")"
        }
        }
    }
    
    internal func toPayload(from: Int = 0) -> DynoScanFilterPayload {
        return DynoScanFilterEAV.createExpressionAttributeAliases(filterExpression: self, from: from).0
    }
    
    public func asFilterExpression() -> String {
        return self.description
    }
}

internal typealias DynoExpressionAttributeName = String
internal typealias DynoExpressionAttributeValue = String

internal struct DynoScanFilterPayload {
    let filterEAV : DynoScanFilterEAV
    let nameMap : [DynoExpressionAttributeName:String]
    let valueMap : [DynoExpressionAttributeValue:DynoConvertibleValue]
    
    func toDynoFilterExpression() -> String {
        return filterEAV.toFilterDescription()
    }
    
    func toDynoExpressionAttributeNames() -> [String:String] {
        return nameMap
    }
    
    func toDynoExpressionAttributeValues() -> [String:DynoAttributeValue] {
        let valuesMap = self.valueMap.mapValues {$0.toDynoAttributeValue() }
                
        return valuesMap
    }
}

/// "Shadow" of DynoScanFilter, but 'aliasing' all of the identifiers (paths or values) into the AWS typed identifiers
/// like `{"N":"50"}` to represent the number 50.
///
/// `createExpressionAttributeAliases()` converts between *DynoScanFilter* and *DynoScanFilterEAV*.
indirect internal enum DynoScanFilterEAV {
    case compare(DynoExpressionAttributeName, DynoComparator, DynoExpressionAttributeValue)
    case compareSize(DynoExpressionAttributeName, DynoComparator, DynoExpressionAttributeValue)
    case between(DynoExpressionAttributeName, from: DynoExpressionAttributeValue, to: DynoExpressionAttributeValue)
    case betweenSize(DynoExpressionAttributeName, from: DynoExpressionAttributeValue, to: DynoExpressionAttributeValue)
    case `in`(DynoExpressionAttributeName, [DynoExpressionAttributeValue])
    case and(DynoScanFilterEAV, DynoScanFilterEAV)
    case or(DynoScanFilterEAV, DynoScanFilterEAV)
    case not(DynoScanFilterEAV)
    case attributeExists(DynoExpressionAttributeName)
    case attributeNotExists(DynoExpressionAttributeName)
    case attributeType(DynoExpressionAttributeName,DynoExpressionAttributeValue)
    case beginsWith(DynoExpressionAttributeName, DynoExpressionAttributeValue)
    case contains(DynoExpressionAttributeName, DynoExpressionAttributeValue)
    
    internal func toFilterDescription() -> String {
        switch self {
        case let .compare(left, compare, right): return "\(left) \(compare.rawValue) \(right)"
        case let .compareSize(left, compare, right): return "size(\(left)) \(compare.rawValue) \(right)"
        case let .between(p, from:from, to:to): return "\(p) BETWEEN \(from) AND \(to)"
        case let .betweenSize(p, from:from, to:to): return "size(\(p)) BETWEEN \(from) AND \(to)"
        case let .in(p, ps): return "\(p) IN (\(ps.joined(separator: ",")))"
            
        case let .and(left, right): return "(\(left.toFilterDescription()) AND \(right.toFilterDescription()))"
        case let .or(left, right): return "(\(left.toFilterDescription()) OR \(right.toFilterDescription()))"
        case let .not(p): return "NOT \(p.toFilterDescription())"
        case let .beginsWith(path, prefix):return "begins_with(\(path),\(prefix))"
        case let .contains(path, str):return "contains(\(path),\(str))"
        case let .attributeExists(path):return "attribute_exists(\(path))"
        case let .attributeNotExists(path):return "attribute_not_exists(\(path))"
        case let .attributeType(path,type):return "attribute_type(\(path),\(type))"
        }
    }
    
    static func createExpressionAttributeAliases(filterExpression: DynoCondition, from: Int = 0)
        -> (DynoScanFilterPayload, Int) {
        switch filterExpression {
            
        case .compare(let left, let compare, let right):
            let newName = "#n\(from)"
            let newValueId = ":v\(from)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.compare(newName, compare, newValueId),
                    nameMap: [newName:left],
                    valueMap: [newValueId:right]),
                    from+1)
            
        case .compareSize(let left, let compare, let right):
            let newName = "#n\(from)"
            let newValueId = ":v\(from)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.compareSize(newName, compare, newValueId),
                    nameMap: [newName:left],
                    valueMap: [newValueId:right]),
                    from+1)
            
        case .betweenValue(let name, let fromV, let toV):
            let newName = "#n\(from)"
            let newFrom = ":v\(from)"
            let newTo = ":v\(from+1)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.between(newName, from: newFrom, to: newTo),
                    nameMap: [newName:name],
                    valueMap: [newFrom:fromV, newTo:toV]),
                    from+2)
            
        case .betweenSize(let name, let fromV, let toV):
            let newName = "#n\(from)"
            let newFrom = ":v\(from)"
            let newTo = ":v\(from+1)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.betweenSize(newName, from: newFrom, to: newTo),
                    nameMap: [newName:name],
                    valueMap: [newFrom:fromV, newTo:toV]),
                    from+2)
            
        case .in(let name, let collection):
            let newName = "#n\(from)"
            var id = from
            let newIds = collection.map { (value:DynoConvertibleValue) -> (String,[String:DynoConvertibleValue]) in
                let newValueId = ":v\(id)"
                id += 1
                return (newValueId, [newValueId:value])
            }
            let newCollection = newIds.map {$0.0}
            let newValueMap = newIds.reduce(Dictionary<String,DynoConvertibleValue>()) { r,k in
                r.append(k.1)
            }
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.in(newName, newCollection),
                    nameMap: [newName:name],
                    valueMap: newValueMap),
                    id+1)
            
        case .and(let left, let right):
            let (leftEAV, firstFrom) = Self.createExpressionAttributeAliases(filterExpression: left, from: from)
            let (rightEAV, secondFrom) = Self.createExpressionAttributeAliases(filterExpression: right, from:firstFrom)
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.and(leftEAV.filterEAV, rightEAV.filterEAV),
                    nameMap: (leftEAV.nameMap).append(rightEAV.nameMap),
                    valueMap: (leftEAV.valueMap).append(rightEAV.valueMap)),
                    secondFrom)
            
        case .or(let left, let right):
            let (leftEAV, firstFrom) = Self.createExpressionAttributeAliases(filterExpression: left, from: from)
            let (rightEAV, secondFrom) = Self.createExpressionAttributeAliases(filterExpression: right, from:firstFrom)
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.or(leftEAV.filterEAV, rightEAV.filterEAV),
                    nameMap: (leftEAV.nameMap).append(rightEAV.nameMap),
                    valueMap: (leftEAV.valueMap).append(rightEAV.valueMap) ),
                    secondFrom)
            
        case .not(let filter):
            let (filterEAV, moreFrom) = Self.createExpressionAttributeAliases(filterExpression: filter, from:from)
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.not(filterEAV.filterEAV),
                    nameMap: filterEAV.nameMap,
                    valueMap: filterEAV.valueMap),
                    moreFrom)
            
        case .attributeExists(let attr):
            let newName = "#n\(from)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.attributeExists(newName),
                    nameMap: [newName:attr],
                    valueMap: [:]),
                    from+1)
            
        case .attributeNotExists(let attr):
            let newName = "#n\(from)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.attributeNotExists(newName),
                    nameMap: [newName:attr],
                    valueMap: [:]),
                    from+1)
            
        case .attributeType(let attr, let type):
            let newName = "#n\(from)"
            let newType = ":v\(from)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.attributeType(newName, newType),
                    nameMap: [newName:attr],
                    valueMap: [newType:type]),
                    from+1)
            
        case .beginsWith(let attr, let str):
            let newName = "#n\(from)"
            let newStr = ":v\(from)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.beginsWith(newName, newStr),
                    nameMap: [newName:attr],
                    valueMap: [newStr:str]),
                    from+1)
            
        case .contains(let attr, let str):
            let newName = "#n\(from)"
            let newStr = ":v\(from)"
            return (DynoScanFilterPayload( filterEAV: DynoScanFilterEAV.contains(newName, newStr),
                    nameMap: [newName:attr],
                    valueMap: [newStr:str]),
                    from+1)
        }
    }
}
