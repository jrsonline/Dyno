//
//  DynoConvertibleValue.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation


/// A value which can be represented in DynamoDb. Includes String, Int, Double, Array, Boolean, Data, Dictionary.
public protocol DynoConvertibleValue {
    func toDynoAttributeValue() -> DynoAttributeValue
}

extension String: DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.S(self)
    }
}

extension Int: DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.N("\(self)")
    }
}

extension Double: DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.N("\(self)")
    }
}

extension Float: DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.N("\(self)")
    }
}

extension UInt: DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.N("\(self)")
    }
}

extension Bool: DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.BOOL(self)
    }
}

extension Data: DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.B(self)
    }
}

extension Array where Element : StringProtocol {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.SS(self.map { "\($0)"})
    }
}

extension Array where Element : Numeric {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.NS(self.map {"\($0)" })
    }
}

extension Array where Element == Data {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.BS(self)
    }
}

extension Array where Element : DynoConvertibleValue {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.L(self.map {$0.toDynoAttributeValue() })
    }
}

extension Dictionary where Key : StringProtocol, Value : DynoConvertibleValue  {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.M(Dictionary<String,DynoAttributeValue>(uniqueKeysWithValues: self.map {("\($0.0)",$0.1.toDynoAttributeValue()) }))
    }
}
