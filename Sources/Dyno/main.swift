import Foundation
import PythonKit
import Dispatch
import RxCocoa
import RxSwift


enum AktState : String {
    case ready
    case complete
    case archived
    case new
    
    static func make(from str: String) -> Result<AktState, DynoError> {
        return AktState.init(rawValue: str).toResult(withNil: DynoError("Invalid state '\(str)'"))
    }
}

struct AktInstance {
    let auditId: String
    let name: String
    let state: AktState
    let id: String
    let ownerGroup: String
    
    static func builder(_ dict: Dictionary<String,PythonObject>) -> Result<AktInstance, DynoError> {
        return  zip5(with:AktInstance.init)(  Dyno.getStr(dict, "audit_id"),
                                              Dyno.getStr(dict, "name"),
                                              Dyno.getStr(dict, "state").flatMap (AktState.make),
                                              Dyno.getStr(dict, "instance_id"),
                                              Dyno.getStr(dict, "ownergroup"))
    }
}

let 🦕 = Dyno()
let disposeBag = DisposeBag()

let item2 = 🦕.getItem( fromTable: "InstanceTable",
                        keyField: "instance_id",
                        value: "i+2D4C89B8-1163-46E7-8AE7-69E434F5F071",
                        building: AktInstance.builder )

item2.log("Getter_Test2").subscribe().disposed(by: disposeBag)


let item3 = 🦕.getItem( fromTable: "InstanceTable",
                          keyField: "instance_id",
                          value: "i+1BD4C61B-4E91-4982-A8A2-C562D32EC481",
                          building: AktInstance.builder )
NSLog("Finished creation")

item3.log("Getter_Test3").subscribe().disposed(by: disposeBag)

NSLog("Finished subscriptions")
sleep(20)

NSLog("Done")
