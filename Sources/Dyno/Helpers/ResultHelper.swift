//
//  ResultHelper.swift
//  Dyno
//
//  Created by strictlyswift on 19-Feb-19.
//

import Foundation


func zip5<A,B,C,D,E,Q,Z>( with f: @escaping (A,B,C,D,E) -> Z) -> (Result<A,Q>, Result<B,Q>, Result<C,Q>, Result<D,Q>, Result<E,Q>) -> Result<Z,Q> {
    return { ra, rb, rc, rd, re in
        switch (ra, rb, rc, rd, re) {
        case let (.success(sa), .success(sb), .success(sc), .success(sd), .success(se)): return .success( f(sa,sb,sc,sd,se) )
        case let (.failure(f), _, _, _, _): return Result<Z,Q>.failure(f)
        case let (_, .failure(f), _, _, _): return Result<Z,Q>.failure(f)
        case let (_, _, .failure(f), _, _): return Result<Z,Q>.failure(f)
        case let (_, _, _, .failure(f), _): return Result<Z,Q>.failure(f)
        case let (_, _, _, _, .failure(f)): return Result<Z,Q>.failure(f)
        }
    }
}

func zip4<A,B,C,D,Q,Z>( with f: @escaping (A,B,C,D) -> Z) -> (Result<A,Q>, Result<B,Q>, Result<C,Q>, Result<D,Q>) -> Result<Z,Q> {
    return { ra, rb, rc, rd in
        switch (ra, rb, rc, rd) {
        case let (.success(sa), .success(sb), .success(sc), .success(sd)): return .success( f(sa,sb,sc,sd) )
        case let (.failure(f), _, _, _): return Result<Z,Q>.failure(f)
        case let (_, .failure(f), _, _): return Result<Z,Q>.failure(f)
        case let (_, _, .failure(f), _): return Result<Z,Q>.failure(f)
        case let (_, _, _, .failure(f)): return Result<Z,Q>.failure(f)
        }
    }
}

func zip3<A,B,C,Q,Z>( with f: @escaping (A,B,C) -> Z) -> (Result<A,Q>, Result<B,Q>, Result<C,Q>) -> Result<Z,Q> {
    return { ra, rb, rc in
        switch (ra, rb, rc) {
        case let (.success(sa), .success(sb), .success(sc)): return .success( f(sa,sb,sc) )
        case let (.failure(f), _, _): return Result<Z,Q>.failure(f)
        case let (_, .failure(f), _): return Result<Z,Q>.failure(f)
        case let (_, _, .failure(f)): return Result<Z,Q>.failure(f)
        }
    }
}

func zip<A,B,Q,Z>( with f: @escaping (A,B) -> Z) -> (Result<A,Q>, Result<B,Q>) -> Result<Z,Q> {
    return { ra, rb in
        switch (ra, rb) {
        case let (.success(sa), .success(sb)): return .success( f(sa,sb) )
        case let (.failure(fa), _): return Result<Z,Q>.failure(fa)
        case let (_, .failure(fb)): return Result<Z,Q>.failure(fb)
        }
    }
}



extension Result {
    /// Convert a `Result< S?, Failure>` into a `Result<S,Failure>` so that .success(s) is returned if
    /// s is non-nil.  If s is nil, then .failure(reporting) is returned.
    ///
    /// - Parameter reporting: Value to return if  S? is nil.
    func nilError<S>(_ reporting: Failure) -> Result<S, Failure> where Success == Optional<S> {
        switch self {
        case .success(.some(let s)): return Result<S,Failure>.success(s)
        case .success(.none): return Result<S,Failure>.failure(reporting)
        case .failure(let f): return Result<S,Failure>.failure(f)
        }
    }
}

