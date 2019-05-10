//
//  DynoBoto3.swift
//  Dyno
//
//  Created by strictlyswift on 7-Apr-19.
//

import Foundation
import PythonKit

/// Global connection to boto3 Python library  (https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
let BOTO3 = Python.import("boto3")
let PYLOGGING = Python.import("logging")

/// Represents a Boto3-mediated connection to the AWS DynamoDB database
public class DynoBoto3 : DynoConnection {
    var remoteConnection : DynoResult<PythonObject>
    let resource: String
    let accessKeyId: String?
    let secretAccessKey: String?
    let region: String?
    let options : DynoOptions
    
    public init( resource: String = "dynamodb",
                 accessKeyId: String? = nil,
                 secretAccessKey: String? = nil,
                 region: String? = nil,
                 _ options : DynoOptions) {
        
        self.resource = resource
        self.accessKeyId = accessKeyId
        self.secretAccessKey = secretAccessKey
        self.region = region
        self.options = options


        self.remoteConnection = DynoBoto3.getRawConnection(resource: resource, accessKeyId: accessKeyId, secretAccessKey: secretAccessKey, region: region, options: options)
    }

    public func isValid() -> Bool {
        switch remoteConnection {
            case let .success(conn): return DynoBoto3.confirmUsableConnection(conn)
            default: return false
        }
    }
    
    public func lastError() -> DynoError? {
        switch remoteConnection {
            case .success(_): return nil
            case let .failure(f): return f
        }
    }
    
    public func tryReconnect() -> Bool {
        self.remoteConnection = DynoBoto3.getRawConnection(resource: self.resource, accessKeyId: self.accessKeyId, secretAccessKey: self.secretAccessKey, region: self.region, options: self.options)
        
        return self.isValid()
    }

    internal static func getRawConnection( resource: String = "dynamodb", accessKeyId: String? = nil, secretAccessKey: String? = nil, region: String? = nil, options: DynoOptions) -> DynoResult<PythonObject> {
        do {
            let endpoint_url = options.dummyUrl ? "https://dynamodb.dummy.com/" : Python.None
            
            return try .success( BOTO3.resource.throwingCall(withKeywordArguments:
                ["":resource,
                 "aws_access_key_id":accessKeyId ?? Python.None,
                 "aws_secret_access_key":secretAccessKey ?? Python.None,
                 "region_name":region ?? Python.None,
                 "endpoint_url":endpoint_url
                ]))
        } catch {
            return .failure(DynoError("Connection error: \(error)"))
        }
    }
    
    
    
    /// Calls a dynamoDB function on remote db (via boto3) and ensures a) it doesn't throw an exception ;
    /// b) it returns a valid HTTP code
    ///
    /// - Parameters:
    ///   - callable: method to call
    ///   - args: named arguments to call on the method
    /// - Returns: .success<PythonObject> as the value returned from the call; or .failure
    internal func boto3Call(_ callable:PythonObject, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject> {
        do {
            // Important for next line to be a throwingCall if we have no actual connection, else we will
            // get a fatal error 30 seconds later when boto3 decides to terminate!
            let response = try callable.throwingCall(withKeywordArguments: args)
            if  response.get("ResponseMetadata") == Python.None ||
                response["ResponseMetadata"]["HTTPStatusCode"] < 200 ||
                response["ResponseMetadata"]["HTTPStatusCode"] > 299 {
                return .failure(DynoError("Invalid HTTP response"))
            }
            
            return .success(response)
            
        } catch {
            return .failure(DynoError("Boto3 error :\(error)"))
        }
    }
    
    
    
    /// Do we have a usable connection - checks if we have Table object
    internal static func confirmUsableConnection(_ conn: PythonObject) -> Bool {
        // a valid connection allows us to look at a Table
        return conn.checking.Table != nil
    }
    
    
    public func getItem(from table: String, key:[String: PythonObject]) -> DynoResult<PythonObject>  {
        assert(key.count == 1, "No key data provided for getItem")
        let (keyField, keyValue) = key.first!
        return self.remoteConnection.flatMap { rc in self.boto3Call( rc.Table(table).get_item, withArgs: ["Key":[keyField: keyValue ]]) }
    }
    
    
    public func scan(table: String, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject> {
        return self.remoteConnection.flatMap { rc in self.boto3Call(rc.Table(table).scan, withArgs: args) }
    }

    public func setItem(into table: String, withArgs args: KeyValuePairs<String,PythonConvertible>) -> DynoResult<PythonObject> {
        return self.remoteConnection.flatMap { rc in self.boto3Call(rc.Table(table).put_item, withArgs: args) }
    }
}
