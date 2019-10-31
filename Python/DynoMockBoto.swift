//
//  DynoMockBoto.swift
//  Dyno
//
//  Created by strictlyswift on 11-Apr-19.
//

import Foundation
import PythonKit
import Dyno


/// Represents a mock Boto3-mediated connection to the AWS DynamoDB database
public class DynoMockBoto : DynoConnection {
    public struct MockConnectionQuality {
        let waitTime: Int
        let failsAfterWait: Bool
        let canReconnect: Bool
        
        public init(waitTime: Int, failsAfterWait: Bool, canReconnect: Bool) {
            self.waitTime = waitTime
            self.failsAfterWait = failsAfterWait
            self.canReconnect = canReconnect
        }
        
    }
    
    public typealias TableName = String
    public typealias KeyField = String
    public struct MockTableInfo {
        var tableData : [TableName: [KeyField:[DynoObject : DynoObject]]]
        var keyFields: [TableName: KeyField]
        
        public init(tableData : [TableName: [KeyField:[DynoObject : DynoObject]]], keyFields: [TableName: KeyField] ) {
            self.tableData = tableData
            self.keyFields = keyFields
        }
    }
    
    var connectionQuality : MockConnectionQuality
    var isCurrentlyValidConnection : Bool
    var tableInfo : MockTableInfo
    
    public init( connectionQuality: MockConnectionQuality,
                mockTableInfo: MockTableInfo,
                isValid: Bool,
                 _ options : DynoOptions? = nil) {
        self.connectionQuality = connectionQuality
        self.isCurrentlyValidConnection = isValid
        self.tableInfo = mockTableInfo
    }
    
    public func isValid() -> Bool {
        return self.isCurrentlyValidConnection
    }
    
    public func lastError() -> DynoError? {
        switch self.isCurrentlyValidConnection {
        case true: return nil
        case false: return DynoError("Connection to DynoMockBoto is invalid")
        }
    }
    
    public func tryReconnect() -> Bool {
        if self.connectionQuality.canReconnect {
            self.isCurrentlyValidConnection = true
        } else {
            self.isCurrentlyValidConnection = false
        }
        return self.isValid()
    }
    
    public func simulate<T>(_ after: () -> DynoResult<T>) -> DynoResult<T> {
        if !self.isValid() {
            return .failure(DynoError("No mock connection exists"))
        }
        
        // Wait to simulate a poor connection
        if self.connectionQuality.waitTime > 0 {
            let waitSemaphore = DispatchSemaphore(value: 0)
            _ = waitSemaphore.wait(timeout: .now() + Double(self.connectionQuality.waitTime))
            
            if self.connectionQuality.failsAfterWait {
                self.isCurrentlyValidConnection = false
                return .failure(DynoError("Connection failed after waiting \(self.connectionQuality.waitTime ) seconds"))
            }
        }
        
        return after()
    }
    
    public func getItem(from table: String, key: (String, DynoObject)) -> DynoResult<Dictionary<String, DynoObject>> {
        return simulate {
            let (keyField, keyValue) = key
            
            if let res = self.tableInfo.tableData[table]?[keyField]?[keyValue] {
                return DynoResult<Dictionary<String, DynoObject>> .success(res)
            } else {
                return .failure(DynoError("Couldn't find key \(keyField) with value \(keyValue) in mock table \(table)"))
            }
        }
    }
    
    public func scan(table: String, filter: DynoFilter?, scanLimit: Int) -> DynoResult<[Dictionary<String, DynoObject>]> {
    // TODO: Allow for multiple pages and filters
        return simulate {
            if let res = self.tableInfo.tableData[table] {
                let values = ["Items":Array(res.values)]
                return .success(DynoObject.map(values))
            } else {
                return .failure(DynoError("Couldn't find mock table \(table)"))
            }
        }
    }
    
    public func setItem(into table: String, withItemInfo args: Dictionary<String, DynoObject>) -> DynoResult<Dictionary<String, DynoObject>> {
        return simulate {
            guard let keyField = self.tableInfo.keyFields[table] else {
                return .failure(DynoError("Mock table \(table) does not have key field defined"))
            }
            
            // Convert args into dictionary
            guard let (item,pydict) = args.first else { return .failure(DynoError("Could not retrieve dictionary from argument"))}
            guard item == "Item" else { return .failure(DynoError("Arguments do not encode item"))}
            guard let dict = Dictionary<DynoObject, DynoObject>(pydict.pythonObject) else { return .failure(DynoError("Arguments do not encode object dictionary"))}
            guard let keyValue = dict[DynoObject(keyField)].flatMap (String.init) else { return .failure(DynoError("Dictionary does not contain value for table's key \(keyField)"))}
            
            self.tableInfo.tableData[table, default:[:]][keyValue] = dict
            return .success(DynoObject(dict))
        }
    }
}
