//
//  PythonObjectEncoder.swift
//  Dyno
//
//  Created by strictlyswift on 26-Mar-19.
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


//===----------------------------------------------------------------------===//
// Dictionary Encoder
//===----------------------------------------------------------------------===//

/// `DictionaryEncoder` facilitates the encoding of `Encodable` values into Dictionary.
open class PythonEncoder {
    // MARK: Options
    
    /// The strategy to use for encoding `Date` values.
    public enum DateEncodingStrategy {
        /// Defer to `Date` for choosing an encoding. This is the default strategy.
        case deferredToDate
        
        /// Encode the `Date` as a UNIX timestamp (as a Dictionary number).
        case secondsSince1970
        
        /// Encode the `Date` as UNIX millisecond timestamp (as a Dictionary number).
        case millisecondsSince1970
        
        /// Encode the `Date` as an ISO-8601-formatted string (in RFC 3339 format).
        case iso8601
        
        /// Encode the `Date` as a string formatted by the given formatter.
        case formatted(DateFormatter)
        
        /// Encode the `Date` as a custom value encoded by the given closure.
        ///
        /// If the closure fails to encode a value into the given encoder, the encoder will encode an empty automatic container in its place.
        case custom((Date, Encoder) throws -> Void)
    }
    
    /// The strategy to use for encoding `Data` values.
    public enum DataEncodingStrategy {
        /// Defer to `Data` for choosing an encoding.
        case deferredToData
        
        /// Encoded the `Data` as a Base64-encoded string. This is the default strategy.
        case base64
        
        /// Encode the `Data` as a custom value encoded by the given closure.
        ///
        /// If the closure fails to encode a value into the given encoder, the encoder will encode an empty automatic container in its place.
        case custom((Data, Encoder) throws -> Void)
    }
    
    /// The strategy to use for non-Dictionary-conforming floating-point values (IEEE 754 infinity and NaN).
    public enum NonConformingFloatEncodingStrategy {
        /// Throw upon encountering non-conforming values. This is the default strategy.
        case `throw`
        
        /// Encode the values using the given representation strings.
        case convertToString(positiveInfinity: String, negativeInfinity: String, nan: String)
    }
    
    /// The strategy to use for automatically changing the value of keys before encoding.
    public enum KeyEncodingStrategy {
        /// Use the keys specified by each type. This is the default strategy.
        case useDefaultKeys
        
        /// Convert from "camelCaseKeys" to "snake_case_keys" before writing a key to Dictionary payload.
        ///
        /// Capital characters are determined by testing membership in `CharacterSet.uppercaseLetters` and `CharacterSet.lowercaseLetters` (Unicode General Categories Lu and Lt).
        /// The conversion to lower case uses `Locale.system`, also known as the ICU "root" locale. This means the result is consistent regardless of the current user's locale and language preferences.
        ///
        /// Converting from camel case to snake case:
        /// 1. Splits words at the boundary of lower-case to upper-case
        /// 2. Inserts `_` between words
        /// 3. Lowercases the entire string
        /// 4. Preserves starting and ending `_`.
        ///
        /// For example, `oneTwoThree` becomes `one_two_three`. `_oneTwoThree_` becomes `_one_two_three_`.
        ///
        /// - Note: Using a key encoding strategy has a nominal performance cost, as each string key has to be converted.
        case convertToSnakeCase
        
        /// Provide a custom conversion to the key in the encoded Dictionary from the keys specified by the encoded types.
        /// The full path to the current encoding position is provided for context (in case you need to locate this key within the payload). The returned key is used in place of the last component in the coding path before encoding.
        /// If the result of the conversion is a duplicate key, then only one value will be present in the result.
        case custom((_ codingPath: [CodingKey]) -> CodingKey)
        
        internal static func _convertToSnakeCase(_ stringKey: String) -> String {
            guard stringKey.count > 0 else { return stringKey }
            
            var words : [Range<String.Index>] = []
            // The general idea of this algorithm is to split words on transition from lower to upper case, then on transition of >1 upper case characters to lowercase
            //
            // myProperty -> my_property
            // myURLProperty -> my_url_property
            //
            // We assume, per Swift naming conventions, that the first character of the key is lowercase.
            var wordStart = stringKey.startIndex
            var searchRange = stringKey.index(after: wordStart)..<stringKey.endIndex
            
            // Find next uppercase character
            while let upperCaseRange = stringKey.rangeOfCharacter(from: CharacterSet.uppercaseLetters, options: [], range: searchRange) {
                let untilUpperCase = wordStart..<upperCaseRange.lowerBound
                words.append(untilUpperCase)
                
                // Find next lowercase character
                searchRange = upperCaseRange.lowerBound..<searchRange.upperBound
                guard let lowerCaseRange = stringKey.rangeOfCharacter(from: CharacterSet.lowercaseLetters, options: [], range: searchRange) else {
                    // There are no more lower case letters. Just end here.
                    wordStart = searchRange.lowerBound
                    break
                }
                
                // Is the next lowercase letter more than 1 after the uppercase? If so, we encountered a group of uppercase letters that we should treat as its own word
                let nextCharacterAfterCapital = stringKey.index(after: upperCaseRange.lowerBound)
                if lowerCaseRange.lowerBound == nextCharacterAfterCapital {
                    // The next character after capital is a lower case character and therefore not a word boundary.
                    // Continue searching for the next upper case for the boundary.
                    wordStart = upperCaseRange.lowerBound
                } else {
                    // There was a range of >1 capital letters. Turn those into a word, stopping at the capital before the lower case character.
                    let beforeLowerIndex = stringKey.index(before: lowerCaseRange.lowerBound)
                    words.append(upperCaseRange.lowerBound..<beforeLowerIndex)
                    
                    // Next word starts at the capital before the lowercase we just found
                    wordStart = beforeLowerIndex
                }
                searchRange = lowerCaseRange.upperBound..<searchRange.upperBound
            }
            words.append(wordStart..<searchRange.upperBound)
            let result = words.map({ (range) in
                return stringKey[range].lowercased()
            }).joined(separator: "_")
            return result
        }
    }
    
    /// The strategy to use in encoding dates. Defaults to `.deferredToDate`.
    open var dateEncodingStrategy: DateEncodingStrategy = .iso8601
    
    /// The strategy to use in encoding binary data. Defaults to `.base64`.
    open var dataEncodingStrategy: DataEncodingStrategy = .base64
    
    /// The strategy to use in encoding non-conforming numbers. Defaults to `.throw`.
    open var nonConformingFloatEncodingStrategy: NonConformingFloatEncodingStrategy = .throw
    
    /// The strategy to use for encoding keys. Defaults to `.useDefaultKeys`.
    open var keyEncodingStrategy: KeyEncodingStrategy = .useDefaultKeys
    
    /// Contextual user-provided information for use during encoding.
    open var userInfo: [CodingUserInfoKey : Any] = [:]
    
    /// Options set on the top-level encoder to pass down the encoding hierarchy.
    fileprivate struct _Options {
        let dateEncodingStrategy: DateEncodingStrategy
        let dataEncodingStrategy: DataEncodingStrategy
        let nonConformingFloatEncodingStrategy: NonConformingFloatEncodingStrategy
        let keyEncodingStrategy: KeyEncodingStrategy
        let userInfo: [CodingUserInfoKey : Any]
    }
    
    /// The options set on the top-level encoder.
    fileprivate var options: _Options {
        return _Options(dateEncodingStrategy: dateEncodingStrategy,
                        dataEncodingStrategy: dataEncodingStrategy,
                        nonConformingFloatEncodingStrategy: nonConformingFloatEncodingStrategy,
                        keyEncodingStrategy: keyEncodingStrategy,
                        userInfo: userInfo)
    }
    
    // MARK: - Constructing a Python Encoder
    /// Initializes `self` with default strategies.
    public init() {}
    
    // MARK: - Encoding Values
    /// Encodes the given top-level value and returns its Dictionary representation.
    ///
    /// - parameter value: The value to encode.
    /// - returns: A new `Data` value containing the encoded Dictionary data.
    /// - throws: `EncodingError.invalidValue` if a non-conforming floating-point value is encountered during encoding, and the encoding strategy is `.throw`.
    /// - throws: An error if any value throws an error during encoding.
    open func encode<T : Encodable>(_ value: T) throws -> PythonObject {
        let encoder = _PythonEncoder(options: self.options)
        
        guard let topLevel = try encoder.box_(value) else {
            throw EncodingError.invalidValue(value, EncodingError.Context(codingPath: [], debugDescription: "Top-level \(T.self) did not encode any values."))
        }

        return topLevel
    }
    
}

// MARK: - _PythonEncoder
fileprivate class _PythonEncoder : Encoder {
    // MARK: Properties
    /// The encoder's storage.
    fileprivate var storage: _PythonEncodingStorage
    
    /// Options set on the top-level encoder.
    fileprivate let options: PythonEncoder._Options
    
    /// The path to the current point in encoding.
    public var codingPath: [CodingKey]
    
    /// Contextual user-provided information for use during encoding.
    public var userInfo: [CodingUserInfoKey : Any] {
        return self.options.userInfo
    }
    
    // MARK: - Initialization
    /// Initializes `self` with the given top-level encoder options.
    fileprivate init(options: PythonEncoder._Options, codingPath: [CodingKey] = []) {
        self.options = options
        self.storage = _PythonEncodingStorage()
        self.codingPath = codingPath
    }
    
    /// Returns whether a new element can be encoded at this coding path.
    ///
    /// `true` if an element has not yet been encoded at this coding path; `false` otherwise.
    fileprivate var canEncodeNewValue: Bool {
        // Every time a new value gets encoded, the key it's encoded for is pushed onto the coding path (even if it's a nil key from an unkeyed container).
        // At the same time, every time a container is requested, a new value gets pushed onto the storage stack.
        // If there are more values on the storage stack than on the coding path, it means the value is requesting more than one container, which violates the precondition.
        //
        // This means that anytime something that can request a new container goes onto the stack, we MUST push a key onto the coding path.
        // Things which will not request containers do not need to have the coding path extended for them (but it doesn't matter if it is, because they will not reach here).
        return (self.storage.count ) == self.codingPath.count
    }
    
    // MARK: - Encoder Methods
    public func container<Key : CodingKey>(keyedBy: Key.Type) -> KeyedEncodingContainer<Key> {
        
        // If an existing keyed container was already requested, return that one.
        var topContainer: _PythonDict
        if self.canEncodeNewValue {
            // We haven't yet pushed a container at this level; do so here.
            topContainer = self.storage.pushKeyedContainer()
        } else {
            guard let containerType = self.storage.containers.last,
                  let container = containerType.asPythonDict() else {
                preconditionFailure("Attempt to push new keyed encoding container when already previously encoded at this path.")
            }
            
            topContainer = container
        }
        
        let container = _PythonDictionaryEncodingContainer<Key>(referencing: self, codingPath: self.codingPath, wrapping: topContainer)
        return KeyedEncodingContainer(container)
    }
    
    public func unkeyedContainer() -> UnkeyedEncodingContainer {
        // If an existing unkeyed container was already requested, return that one.
        
        let topContainer: _PythonList
        if self.canEncodeNewValue {
            // We haven't yet pushed a container at this level; do so here.
            topContainer = self.storage.pushUnkeyedContainer()
        } else {
            guard let containerType = self.storage.containers.last,
                  let container = containerType.asList() else {
                preconditionFailure("Attempt to push new unkeyed encoding container when already previously encoded at this path.")
            }
            
            topContainer = container
        }
        
        return _PythonListEncodingContainer(referencing: self, codingPath: self.codingPath, wrapping: topContainer)
    }
    
    public func singleValueContainer() -> SingleValueEncodingContainer {
        return self
    }
}

fileprivate class _PythonDict {
    var dict : Dictionary<String,PythonObject>
    init(_ fromDict: Dictionary<String,PythonObject>) {
        self.dict = fromDict  // pass-by-copy
    }
    
    init() {
        self.dict = Dictionary<String,PythonObject>()
    }
    
    subscript(key:String) -> PythonObject? {
        get {
            return dict[key]
        }
        set {
            self.dict[key] = newValue
        }
    }
    
    func toPythonObject() -> PythonObject {
        return PythonObject(dict)
    }
    
    var count : Int { get {
        return dict.count
    } }
}

fileprivate class _PythonList {
    var list : Array<PythonObject>
    
    init(_ fromArray: [PythonObject]) {
        self.list = fromArray  // pass-by-copy
    }
    
    init() {
        self.list = Array<PythonObject>()
    }
    
    func append(_ value: PythonObject) {
        self.list.append(value)
    }
    
    func insert(_ value: PythonObject, at: Int) {
        self.list.insert(value, at: at)
    }
    
    func toPythonObject() -> PythonObject {
        return PythonObject(list)
    }
    
    var count : Int { get {
        return list.count
    } }
}


// MARK: - Encoding Storage and Containers
fileprivate struct _PythonEncodingStorage {
    enum ContainerType {
        case keyed(_PythonDict)
        case unkeyed(_PythonList)
        case singleValue(PythonObject)
        
        func asPythonDict() -> _PythonDict? {
            if case let .keyed(dict) = self {
                return dict
            } else {
                return nil
            }
        }
        
        func asList() -> _PythonList? {
            if case let .unkeyed(list) = self {
                return list
            } else {
                return nil
            }
        }
        
        func asValue() -> PythonObject? {
            if case let .singleValue(value) = self {
                return value
            } else {
                return nil
            }
        }
    }
    
    // MARK: Properties
    /// The container stack.
    private(set) fileprivate var containers: [ContainerType] = []
    
    // MARK: - Initialization
    /// Initializes `self` with no containers.
    fileprivate init() {}
    
    // MARK: - Modifying the Stack
    fileprivate var count: Int {
        return self.containers.count
    }

    
    fileprivate mutating func pushKeyedContainer() -> _PythonDict {
        let dictionary = _PythonDict()
        self.containers.append(.keyed(dictionary))
        return dictionary
    }
    
    fileprivate mutating func pushUnkeyedContainer() -> _PythonList {
        let list = _PythonList()
        self.containers.append(.unkeyed(list))
        return list
    }
    
    fileprivate mutating func pushKeyed(container: _PythonDict) {
        self.containers.append(.keyed(container))
    }
    
    fileprivate mutating func pushUnkeyed(container: _PythonList) {
        self.containers.append(.unkeyed(container))
    }

    fileprivate mutating func push(value: PythonObject) {
        self.containers.append(.singleValue(value))
    }
    
    fileprivate mutating func popContainer() -> PythonObject {
        guard let topContainer = self.containers.popLast() else {fatalError("Empty container stack.")}
        switch topContainer {
        case .keyed(let dict):
            return dict.toPythonObject()
        case .unkeyed(let list):
            return list.toPythonObject()
        case .singleValue(let obj):
            return obj
        }
    }
}

// MARK: - Encoding Containers
fileprivate struct _PythonDictionaryEncodingContainer<K : CodingKey> : KeyedEncodingContainerProtocol {
    typealias Key = K
    
    // MARK: Properties
    /// A reference to the encoder we're writing to.
    private let encoder: _PythonEncoder
    
    /// A reference to the container we're writing to.
    private var container: _PythonDict
    
    /// The path of coding keys taken to get to this point in encoding.
    private(set) public var codingPath: [CodingKey]
    
    // MARK: - Initialization
    /// Initializes `self` with the given references.
    fileprivate init(referencing encoder: _PythonEncoder, codingPath: [CodingKey], wrapping container: _PythonDict) {
        self.encoder = encoder
        self.codingPath = codingPath
        self.container = container
    }
    
    // MARK: - Coding Path Operations
    private func _converted(_ key: CodingKey) -> CodingKey {
        switch encoder.options.keyEncodingStrategy {
        case .useDefaultKeys:
            return key
        case .convertToSnakeCase:
            let newKeyString = PythonEncoder.KeyEncodingStrategy._convertToSnakeCase(key.stringValue)
            return DictionaryCodingKey(stringValue: newKeyString, intValue: key.intValue)
        case .custom(let converter):
            return converter(codingPath + [key])
        }
    }
    
    // MARK: - KeyedEncodingContainerProtocol Methods
    public mutating func encodeNil(forKey key: Key) throws {
        self.container[_converted(key).stringValue] = Python.None
    }
    public mutating func encode(_ value: Bool, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: Int, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: Int8, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: Int16, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: Int32, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: Int64, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: UInt, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: UInt8, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: UInt16, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: UInt32, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: UInt64, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    public mutating func encode(_ value: String, forKey key: Key) throws {
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    
    public mutating func encode(_ value: Float, forKey key: Key) throws {
        // Since the float may be invalid and throw, the coding path needs to contain this key.
        self.encoder.codingPath.append(key)
        defer { self.encoder.codingPath.removeLast() }
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    
    public mutating func encode(_ value: Double, forKey key: Key) throws {
        // Since the double may be invalid and throw, the coding path needs to contain this key.
        self.encoder.codingPath.append(key)
        defer { self.encoder.codingPath.removeLast() }
        self.container[_converted(key).stringValue] = PythonObject(value)
    }
    
    public mutating func encode<T : Encodable>(_ value: T, forKey key: Key) throws {
        self.encoder.codingPath.append(key)
        defer { self.encoder.codingPath.removeLast() }
        self.container[_converted(key).stringValue] = try self.encoder.box(value)
    }
    
    public mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) -> KeyedEncodingContainer<NestedKey> {
        let dictionary = _PythonDict()
        self.container[_converted(key).stringValue] = dictionary.toPythonObject()

        self.codingPath.append(key)
        defer { self.codingPath.removeLast() }

        let container = _PythonDictionaryEncodingContainer<NestedKey>(referencing: self.encoder, codingPath: self.codingPath, wrapping: dictionary)
        return KeyedEncodingContainer(container)

    }
    
    public mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        let list = _PythonList()
        self.container[_converted(key).stringValue] = list.toPythonObject()

        self.codingPath.append(key)
        defer { self.codingPath.removeLast() }
        return _PythonListEncodingContainer(referencing: self.encoder, codingPath: self.codingPath, wrapping: list)
    }
    
    public mutating func superEncoder() -> Encoder {
        return _PythonReferencingEncoder(referencing: self.encoder, key: DictionaryCodingKey.super, convertedKey: _converted(DictionaryCodingKey.super), wrapping: self.container)
    }
    
    public mutating func superEncoder(forKey key: Key) -> Encoder {
        return _PythonReferencingEncoder(referencing: self.encoder, key: key, convertedKey: _converted(key), wrapping: self.container)
    }
}

fileprivate struct _PythonListEncodingContainer : UnkeyedEncodingContainer {
    // MARK: Properties
    /// A reference to the encoder we're writing to.
    private let encoder: _PythonEncoder
    
    /// A reference to the container we're writing to.
    private var container: _PythonList
    
    /// The path of coding keys taken to get to this point in encoding.
    private(set) public var codingPath: [CodingKey]
    
    /// The number of elements encoded into the container.
    public var count: Int {
        return self.container.count
    }
    
    // MARK: - Initialization
    /// Initializes `self` with the given references.
    fileprivate init(referencing encoder: _PythonEncoder, codingPath: [CodingKey], wrapping container: _PythonList) {
        self.encoder = encoder
        self.codingPath = codingPath
        self.container = container
    }
    
    // MARK: - UnkeyedEncodingContainer Methods
    public mutating func encodeNil()             throws { self.container.append(Python.None) }
    public mutating func encode(_ value: Bool)   throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: Int)    throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: Int8)   throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: Int16)  throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: Int32)  throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: Int64)  throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: UInt)   throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: UInt8)  throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: UInt16) throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: UInt32) throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: UInt64) throws { self.container.append(self.encoder.box(value)) }
    public mutating func encode(_ value: String) throws { self.container.append(self.encoder.box(value)) }
    
    public mutating func encode(_ value: Float)  throws {
        // Since the float may be invalid and throw, the coding path needs to contain this key.
        self.encoder.codingPath.append(DictionaryCodingKey(index: self.count))
        defer { self.encoder.codingPath.removeLast() }
        self.container.append(try self.encoder.box(value))
    }
    
    public mutating func encode(_ value: Double) throws {
        // Since the double may be invalid and throw, the coding path needs to contain this key.
        self.encoder.codingPath.append(DictionaryCodingKey(index: self.count))
        defer { self.encoder.codingPath.removeLast() }
        self.container.append(try self.encoder.box(value))
    }
    
    public mutating func encode<T : Encodable>(_ value: T) throws {
        self.encoder.codingPath.append(DictionaryCodingKey(index: self.count))
        defer { self.encoder.codingPath.removeLast() }
        self.container.append(try self.encoder.box(value))
    }
    
    public mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        self.codingPath.append(DictionaryCodingKey(index: self.count))
        defer { self.codingPath.removeLast() }
        
        let dictionary = _PythonDict()
        self.container.append(dictionary.toPythonObject() )
        
        let container = _PythonDictionaryEncodingContainer<NestedKey>(referencing: self.encoder, codingPath: self.codingPath, wrapping: dictionary)
        return KeyedEncodingContainer(container)
    }
    
    public mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        self.codingPath.append(DictionaryCodingKey(index: self.count))
        defer { self.codingPath.removeLast() }
        
        let list = _PythonList()
        self.container.append(list.toPythonObject())
        return _PythonListEncodingContainer(referencing: self.encoder, codingPath: self.codingPath, wrapping: list)
    }
    
    public mutating func superEncoder() -> Encoder {
//        return _DictionaryReferencingEncoder(referencing: self.encoder, at: self.container.count, wrapping: self.container)
        fatalError("Cannot encode superclass")

    }
}

extension _PythonEncoder : SingleValueEncodingContainer {
    // MARK: - SingleValueEncodingContainer Methods
    fileprivate func assertCanEncodeNewValue() {
        precondition(self.canEncodeNewValue, "Attempt to encode value through single value container when previously value already encoded.")
    }
    
    public func encodeNil() throws {
        assertCanEncodeNewValue()
        self.storage.push(value: Python.None)
    }
    
    public func encode(_ value: Bool) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: Int) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: Int8) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: Int16) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: Int32) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: Int64) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: UInt) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: UInt8) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: UInt16) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: UInt32) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: UInt64) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: String) throws {
        assertCanEncodeNewValue()
        self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: Float) throws {
        assertCanEncodeNewValue()
        try self.storage.push(value: self.box(value))
    }
    
    public func encode(_ value: Double) throws {
        assertCanEncodeNewValue()
        try self.storage.push(value: self.box(value))
    }
    
    public func encode<T : Encodable>(_ value: T) throws {
        assertCanEncodeNewValue()
        try self.storage.push(value: self.box(value))
    }
}

// MARK: - Concrete Value Representations
extension _PythonEncoder {
    /// Returns the given value boxed in a container appropriate for pushing onto the container stack.
    fileprivate func box(_ value: Bool)   -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: Int)    -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: Int8)   -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: Int16)  -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: Int32)  -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: Int64)  -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: UInt)   -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: UInt8)  -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: UInt16) -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: UInt32) -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: UInt64) -> PythonObject { return PythonObject(value) }
    fileprivate func box(_ value: String) -> PythonObject { return PythonObject(value) }
    
    fileprivate func box(_ float: Float) throws -> PythonObject {
        guard !float.isInfinite && !float.isNaN else {
            guard case let .convertToString(positiveInfinity: posInfString,
                                            negativeInfinity: negInfString,
                                            nan: nanString) = self.options.nonConformingFloatEncodingStrategy else {
                                                throw EncodingError.invalidValue(float, EncodingError.Context(codingPath: codingPath, debugDescription: "Could not convert floating point number to string"))
            }
            
            if float == Float.infinity {
                return PythonObject(posInfString)
            } else if float == -Float.infinity {
                return PythonObject(negInfString)
            } else {
                return PythonObject(nanString)
            }
        }
        
        return PythonObject(float)
    }
    
    fileprivate func box(_ double: Double) throws -> PythonObject {
        guard !double.isInfinite && !double.isNaN else {
            guard case let .convertToString(positiveInfinity: posInfString,
                                            negativeInfinity: negInfString,
                                            nan: nanString) = self.options.nonConformingFloatEncodingStrategy else {
                                                throw EncodingError.invalidValue(double, EncodingError.Context(codingPath: codingPath, debugDescription: "Could not convert double to string"))
            }
            
            if double == Double.infinity {
                return PythonObject(posInfString)
            } else if double == -Double.infinity {
                return PythonObject(negInfString)
            } else {
                return PythonObject(nanString)
            }
        }
        
        return PythonObject(double)
    }
    
    fileprivate func box(_ date: Date) throws -> PythonObject {
        switch self.options.dateEncodingStrategy {
        case .deferredToDate:
            // Must be called with a surrounding with(pushedKey:) call.
            // Dates encode as single-value objects; this can't both throw and push a container, so no need to catch the error.
//            try date.encode(to: self)
//            return self.storage.popContainer()
            fatalError("deferredToDate is unsupported")
            
        case .secondsSince1970:
            return PythonObject(date.timeIntervalSince1970)
            
        case .millisecondsSince1970:
            return PythonObject(1000.0 * date.timeIntervalSince1970)
            
        case .iso8601:
            if #available(OSX 10.12, *) {
                let formatter = ISO8601DateFormatter()
                return PythonObject( formatter.string(from: date))
            } else {
                fatalError("ISO8601DateFormatter is unsupported on this platform")
            }

            
        case .formatted(let formatter):
            return PythonObject(formatter.string(from: date))
            
        case .custom(let closure):
            let depth = self.storage.count
            do {
                try closure(date, self)
            } catch {
                // If the value pushed a container before throwing, pop it back off to restore state.
                if self.storage.count > depth {
                    let _ = self.storage.popContainer()
                }

                throw error
            }

            guard self.storage.count > depth else {
                // The closure didn't encode anything. Return the default keyed container.
                return _PythonDict().toPythonObject()
            }

            // We can pop because the closure encoded something.
            return self.storage.popContainer()
        }
    }
    
    fileprivate func box(_ data: Data) throws -> PythonObject {
        switch self.options.dataEncodingStrategy {
        case .deferredToData:
            // Must be called with a surrounding with(pushedKey:) call.
            let depth = self.storage.count
            do {
                try data.encode(to: self)
            } catch {
                // If the value pushed a container before throwing, pop it back off to restore state.
                // This shouldn't be possible for Data (which encodes as an array of bytes), but it can't hurt to catch a failure.
                if self.storage.count > depth {
                    let _ = self.storage.popContainer()
                }

                throw error
            }

            return self.storage.popContainer()
            
        case .base64:
            return PythonObject(data.base64EncodedString())
            
        case .custom(let closure):
            let depth = self.storage.count
            do {
                try closure(data, self)
            } catch {
                // If the value pushed a container before throwing, pop it back off to restore state.
                if self.storage.count > depth {
                    let _ = self.storage.popContainer()
                }

                throw error
            }

            guard self.storage.count > depth else {
                // The closure didn't encode anything. Return the default keyed container.
                return _PythonDict().toPythonObject()
            }

            // We can pop because the closure encoded something.
            return self.storage.popContainer()
        }
    }
    
    fileprivate func box<T : Encodable>(_ value: T) throws -> PythonObject {
        return try self.box_(value) ?? PythonObject([:])
    }
    
    // This method is called "box_" instead of "box" to disambiguate it from the overloads. Because the return type here is different from all of the "box" overloads (and is more general), any "box" calls in here would call back into "box" recursively instead of calling the appropriate overload, which is not what we want.
    fileprivate func box_<T : Encodable>(_ value: T) throws -> PythonObject? {
        if T.self == Date.self || T.self == NSDate.self {
            // Respect Date encoding strategy
            return try self.box((value as! Date))
        } else if T.self == Data.self || T.self == NSData.self {
            // Respect Data encoding strategy
            return try self.box((value as! Data))
        } else if T.self == URL.self || T.self == NSURL.self {
            // Encode URLs as single strings.
            return self.box((value as! URL).absoluteString)
        } else if T.self == Decimal.self || T.self == NSDecimalNumber.self {
            return PythonObject(Int(value as! Int))
        }
        
        // Other types of object should request a container from the _DictionaryEncoder.
        
        let depth = self.storage.count
        do {
            try value.encode(to: self)
        } catch {
            // If the value pushed a container before throwing, pop it back off to restore state.
            if self.storage.count > depth {
               let _ = self.storage.popContainer()
            }

            throw error
        }
        
        // The top container should be a new container.
        guard self.storage.count > depth else {
            return nil
        }
        
        return self.storage.popContainer()
 
    }
}


// MARK: - _PythonReferencingEncoder
/// _DictionaryReferencingEncoder is a special subclass of _DictionaryEncoder which has its own storage, but references the contents of a different encoder.
/// It's used in superEncoder(), which returns a new encoder for encoding a superclass -- the lifetime of the encoder should not escape the scope it's created in, but it doesn't necessarily know when it's done being used (to write to the original container).
fileprivate class _PythonReferencingEncoder : _PythonEncoder {
    // MARK: Reference types.
    /// The type of container we're referencing.
    private enum Reference {
        /// Referencing a specific index in an array container.
        case list(_PythonList, Int)
        
        /// Referencing a specific key in a dictionary container.
        case dictionary(_PythonDict, String)
    }
    
    // MARK: - Properties
    /// The encoder we're referencing.
    fileprivate let encoder: _PythonEncoder
    
    /// The container reference itself.
    private let reference: Reference
    
    // MARK: - Initialization
    /// Initializes `self` by referencing the given array container in the given encoder.
    fileprivate init(referencing encoder: _PythonEncoder, at index: Int, wrapping array: _PythonList) {
        self.encoder = encoder
        self.reference = .list(array, index)
        super.init(options: encoder.options, codingPath: encoder.codingPath)
        
        self.codingPath.append(DictionaryCodingKey(index: index))
    }
    
    /// Initializes `self` by referencing the given dictionary container in the given encoder.
    fileprivate init(referencing encoder: _PythonEncoder,
                     key: CodingKey, convertedKey: CodingKey, wrapping dictionary: _PythonDict) {
        self.encoder = encoder
        self.reference = .dictionary(dictionary, convertedKey.stringValue)
        super.init(options: encoder.options, codingPath: encoder.codingPath)
        
        self.codingPath.append(key)
    }
    
    // MARK: - Coding Path Operations
    fileprivate override var canEncodeNewValue: Bool {
        // With a regular encoder, the storage and coding path grow together.
        // A referencing encoder, however, inherits its parents coding path, as well as the key it was created for.
        // We have to take this into account.
        return self.storage.count == self.codingPath.count - self.encoder.codingPath.count - 1
    }
    
    // MARK: - Deinitialization
    // Finalizes `self` by writing the contents of our storage to the referenced encoder's storage.
    deinit {
        let value: PythonObject
        switch self.storage.count {
        case 0: value = _PythonDict().toPythonObject()
        case 1: value = self.storage.popContainer()
        default: fatalError("Referencing encoder deallocated with multiple containers on stack.")
        }
        
        switch self.reference {
        case .list(let list, let index):
            list.insert(value, at: index)
            
        case .dictionary(let dictionary, let key):
            dictionary[key] = value
        }
    }
}

