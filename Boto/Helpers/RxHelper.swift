//
//  RxHelper.swift
//  Dyno
//
//  Created by simplyswift on 12-Mar-19.
//

import Foundation
import RxSwift

public extension Observable {
    /// Simple helper function that boxes an DynoActivity on a single value into a DynoActivity on
    /// an array.  This allows for easier merging of Observable sequences of single and multiple results.
    ///
    /// - Returns: Any result is boxed as `[S]`
    func arrayBox<S>() -> Observable<DynoActivity<[S]>> where Element == DynoActivity<S> {
        return self.map {  dynoActivity in dynoActivity.map { [$0] } }
    }
}

//public extension SharedSequence {
//    /// Simple helper function that boxes an DynoActivity on a single value into a DynoActivity on
//    /// an array.  This allows for easier merging of Observable sequences of single and multiple results.
//    ///
//    /// - Returns: Any result is boxed as `[S]`
//    func arrayBox<S>() -> Observable<DynoActivity<[S]>> where Element == DynoActivity<S> {
//        return self.map {  dynoActivity in dynoActivity.map { [$0] } }
//    }
//}

