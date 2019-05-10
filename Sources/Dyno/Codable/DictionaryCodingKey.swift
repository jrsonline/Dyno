//
//  DictionaryCodingKey.swift
//  Dyno
//
//  Created by simplyswift on 26-Mar-19.
//


//===----------------------------------------------------------------------===//
//
// This source file is largely a copy of code from Swift.org open source project's
// files JSONEncoder.swift and Codeable.swift.
//
// I've adapted these, after https://elegantchaos.com, to encode Python objects, specifically
// for storage on DynamoDB.
//
// The original code is copyright (c) 2014 - 2017 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
// Modifications and additional code here is copyright (c) 2019 strictlyswift, and
// is licensed under the same terms.
//
//===----------------------------------------------------------------------===//

import Foundation
import PythonKit


internal struct DictionaryCodingKey : CodingKey {
    public var stringValue: String
    public var intValue: Int?
    
    public init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }
    
    public init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }
    
    public init(stringValue: String, intValue: Int?) {
        self.stringValue = stringValue
        self.intValue = intValue
    }
    
    internal init(index: Int) {
        self.stringValue = "Index \(index)"
        self.intValue = index
    }
    
    internal static let `super` = DictionaryCodingKey(stringValue: "super")!
}

