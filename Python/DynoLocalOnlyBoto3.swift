//
//  DynoLocalOnlyBoto3.swift
//  Dyno
//
//  Created by strictlyswift on 26-Apr-19.
//


import Foundation
import PythonKit
import StrictlySwiftLib
@testable import Dyno

let PYLOGGING = Python.import("logging")


/// **DynoLocalOnlyBoto3** is a special class for test purposes only.  It subclasses
/// the proper Boto3 connection and replaces it with a "dummy" connection which cannot connect
/// to AWS.  It also captures the "send" traffic.  This is done for unit testing purposes.
///
///    - Important : This is not thread-safe as it switches the Python logger around. Ensure you only call from one thread at once.
public class DynoLocalOnlyBoto3 : DynoBoto3 {
    public let tempFilename : URL
    
    public init(source: String) {
        // create our own options
        let options = DynoOptions(log: true,
                                  dummyUrl: true)
        
        self.tempFilename = DynoLocalOnlyBoto3.uniqueTempFilename()

        // Call through to Python code to set the filename of the logger. We have to remove any other loggers first as this can be called multiple times.
        // Note this is not thread safe
        BOTO3.set_stream_logger(name:"botocore")
        
        let fileHandle = PYLOGGING.FileHandler(self.tempFilename.path, "a")
        let format = "[\(source)] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
        let formatter = PYLOGGING.Formatter(format)
        fileHandle.setFormatter(formatter)
        
        let boto3Logger = PYLOGGING.getLogger("botocore")
        boto3Logger.handlers = [fileHandle]
        
        
        super.init(options)
        
        NSLog("Creating DynoLocalOnlyBoto3 and logging to \(self.tempFilename.path)")
    }
    
    static private func uniqueTempFilename() -> URL
    {
        let fileName = "botocore_log_\(UUID())"
        return URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true).appendingPathComponent(fileName)
    }
    
    public static func priorOperationOutput(name: String, inLogFile logFile: URL) throws -> [String:Any] {
        // Find the last OperationModel entry in the log file
        guard let operationLine =
            (String.readLines(fromFile: logFile.path)?
            .filter{ $0.contains("OperationModel(name=\(name))") }
            .last)
            else {throw DynoError("Couldn't find Operation lines for \(name) in log file \(logFile)")}

        // Now, process this line. First, the bit we are interested in is _after_ the params log
        let params = operationLine.suffix(after: "params: ")
        
        // Now, we have to do quite a bit of work to turn the log entry into parseable JSON
        let swiftParams = String(params)
            .replacingOccurrences(of: "\"", with: "¬")
            .replacingOccurrences(of: "u'",    with: "\"")
            .replacingOccurrences(of: "'",     with: "\"")
            .replacingOccurrences(of: "<",     with: "\"<")
            .replacingOccurrences(of: ">",     with: ">\"")
            .replacingOccurrences(of: "None",  with: "false")
            .replacingOccurrences(of: "False", with: "false")
            .replacingOccurrences(of: "True",  with: "true")
        
        // next, let's turn this into a JSON structure for ease of comparison
        // Note, we don't want to use Codable/Decodable so we can deal with different types of result
        // ie this is 'stringly typed'
        do {
            if let data = swiftParams.data(using: .utf8),
               let wrapperJson = try JSONSerialization.jsonObject(with: data) as? [String: Any],
               let body = wrapperJson["body"] as? String,
               let innerData = body.replacingOccurrences(of: "¬", with: "\"").data(using: .utf8),
               let innerJson = try JSONSerialization.jsonObject(with: innerData) as? [String: Any] {
                    return innerJson
            } else {
                throw DynoError("Could not parse JSON in \(swiftParams)")
            }
        } catch let error {
            throw DynoError("JSON parsing error: \(error) when parsing \(swiftParams)")
        }
    }
    
    /// isExpectedFailure is a special testing function which returns true iff the Boto3 library call
    /// fails with an "expected" error (specifically, that it cannot reach the dummy URL)
    ///
    /// - Returns: a tuple (Bool, String?) which is true if the result was as expected; and if not
    /// the string contains the reason.
    static func isExpectedFailure<T>(outcome: DynoResult<T>, expectSuccess: Bool) -> (Bool,String?) {
        switch (outcome, expectSuccess) {
        case (.success(_), true): return (true,nil)
        case (.failure(let f), false):
            return (f.reason.hasPrefix("Boto3 error :Python exception: An error occurred (302) when calling"), f.reason)
        default: return (false, "Expected success:\(expectSuccess ? "yes" : "no") but result was actually \(outcome)")
        }
    }
}

