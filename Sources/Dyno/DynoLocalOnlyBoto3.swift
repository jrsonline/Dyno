//
//  DynoLocalOnlyBoto3.swift
//  Dyno
//
//  Created by strictlyswift on 26-Apr-19.
//


import Foundation
import PythonKit

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
        // Walk back through log file and find the previous OperationModel entry
        let logLines = try String(contentsOf: logFile, encoding: .utf8).split(separator: "\n")
        guard let operationLine =
            logLines
                .last(where: { $0.contains("OperationModel(name=\(name))") } )
                .map (String.init)
            else { return [:] }
        
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
                fatalError("Could not parse JSON in \(swiftParams)")
            }
        } catch let error {
            fatalError("JSON parsing error: \(error) when parsing \(swiftParams)")
        }
    }
}

extension String {
    /// Returns the portion of the string following the parameter.
    /// For example `"LOG=a,b,c".suffix(after:"LOG=")` is `"a,b,c"`.
    /// If the parameter is not found in the string, the substring is empty.
    ///
    /// - Parameter after: Suffix of the string _after_ this are returned
    /// - Returns: The substring with the initial string removed.
    func suffix<S:StringProtocol>(after str: S) -> Substring {
        if let strRange = self.range(of: str) {
            return self.suffix(from: strRange.upperBound)
        } else {
            return self.suffix(0)
        }
    }

}
