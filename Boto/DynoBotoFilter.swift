//
//  DynoBotoFilter.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
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
