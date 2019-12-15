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

public protocol DynoConvertibleElement {
    static func toDynoAttributeValue(array: [Self]) -> DynoAttributeValue
}

extension Array : DynoConvertibleValue  where Element : DynoConvertibleElement {
    public func toDynoAttributeValue() -> DynoAttributeValue  {
        return Element.toDynoAttributeValue(array:self) // DynoAttributeValue.SS(self.map { "\($0)"})
    }
}

extension String : DynoConvertibleElement {
    public static func toDynoAttributeValue(array:[String]) -> DynoAttributeValue  {
        return DynoAttributeValue.SS(array.map { "\($0)"})
    }
}

extension Int : DynoConvertibleElement {
    public static func toDynoAttributeValue(array:[Int]) -> DynoAttributeValue {
        return DynoAttributeValue.NS(array.map {"\($0)" })
    }
}

extension Float : DynoConvertibleElement {
    public static func toDynoAttributeValue(array:[Float]) -> DynoAttributeValue {
        return DynoAttributeValue.NS(array.map {"\($0)" })
    }
}

extension UInt : DynoConvertibleElement {
    public static func toDynoAttributeValue(array:[UInt]) -> DynoAttributeValue {
        return DynoAttributeValue.NS(array.map {"\($0)" })
    }
}

extension Double : DynoConvertibleElement {
    public static func toDynoAttributeValue(array:[Double]) -> DynoAttributeValue {
        return DynoAttributeValue.NS(array.map {"\($0)" })
    }
}


extension Data : DynoConvertibleElement {
    public static func toDynoAttributeValue(array:[Data]) -> DynoAttributeValue {
        return DynoAttributeValue.BS(array)
    }
}


extension Dictionary : DynoConvertibleValue where Key : StringProtocol, Value : DynoConvertibleValue  {
    public func toDynoAttributeValue() -> DynoAttributeValue {
        return DynoAttributeValue.M(Dictionary<String,DynoAttributeValue>(uniqueKeysWithValues: self.map {("\($0.0)",$0.1.toDynoAttributeValue()) }))
    }
}
