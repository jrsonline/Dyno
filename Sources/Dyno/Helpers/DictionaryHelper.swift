//
//  DictionaryHelper.swift
//  Dyno
//
//  Created by RedPanda on 26-Feb-19.
//
//
//import Foundation
//
//extension KeyValuePairs where Key: Hashable{
//    static func +(left:KeyValuePairs<Key,Value>, right:KeyValuePairs<Key,Value> ) -> KeyValuePairs<Key,Value> {
//        var combined : [(Key,Value)] = []
//        for l in left { combined += [l] }
//        for r in right { combined += [r] }
//        return KeyValuePairs<Key,Value>(dictionaryLiteral: Dictionary<Key,Value>(uniqueKeysWithValues:combined))
//    }
//}
