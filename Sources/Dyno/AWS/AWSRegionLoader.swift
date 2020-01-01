//
//  AWSRegionLoader.swift
//  
//
//  Created by RedPanda on 30-Oct-19.
//

import Foundation
import StrictlySwiftLib


struct AWSRegionLoader {
    /// If a region string is passed in, use that directly.  Else, look up the region in ~/.aws/config.
    /// The *first* `region=` value is used.
    /// If no region can be found, use 'us-east-1'
    static func retrieve(for region: String?, log: Bool = false) -> String {
        guard region == nil else {
            if (log) {NSLog("Using AWS region: \(region!)")}
            return region!
        }
        
        let configLocation : URL
        #if os(macOS)
            configLocation = URL(fileURLWithPath: ".aws/config", relativeTo: FileManager().homeDirectoryForCurrentUser)
        #else
            fatalError("For non-macOS platforms, region must be specified specifically (usually as part of Dyno initialization")
        #endif
        let fileName = configLocation.standardizedFileURL.path
        
        guard let lines = FileLinesSequence(fromFile: fileName, encoding: .utf8, delimiter: "\n")  else {
            if (log) {NSLog("Failed to read AWS region from \(configLocation.absoluteString)")}
            return "us-east-1"
        }
        
        let region = lines.first { line in line.hasPrefix("region=")  }?.suffix(after: "region=")
        
        guard let validRegion = region.map (String.init) else {
            if (log) {NSLog("Failed to read valid AWS region from \(region!)")};
            return "us-east-1"
        }
        
        if log {NSLog("Using AWS region from \(configLocation.absoluteString): \(validRegion)")}
        return validRegion
    }
}
