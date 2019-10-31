//
//  PythonActions.swift
//  
//
//  Created by RedPanda on 24-Sep-19.
//
import PythonKit
import Foundation
import PythonCodable


extension PythonDecoder {
    /// Helper function to convert a Decodable into a 'builder'
    ///
    internal static func toBuilder<T>(type: T.Type) -> ([String: DynoObject]) -> DynoResult<T>
    where T : Decodable {
        return { dict in
            do {
                
                
                return .success(try PythonDecoder().decode(T.self, from: PythonObject(dict.compactMapValues { $0.toPythonObject() })))
            }
            catch {
                return .failure(DynoError(error))
            }
        }
    }
}

extension PythonEncoder {
    /// Helper function to convert an Encodable into a 'writer'
    ///
    internal static func toWriter<T : Encodable>(obj: T) -> DynoResult<Dictionary<String,DynoObject>> {
        do {
            if let dict = Dictionary<String,PythonObject>(try PythonEncoder().encode(obj)) {
                return DynoResult.success( dict.compactMapValues { DynoObject.fromPythonObject(po: $0 )})
            } else {
                return DynoResult.failure(DynoError("Could not encode \(obj) into Dictionary"))
            }
        }
        catch {
            return .failure(DynoError(error))
        }
    }
}
