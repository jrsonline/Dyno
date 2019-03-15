import Foundation
import PythonKit
import RxSwift

struct Dinosaur {
    let id: String
    let name: String
    let colour: String
    
    static func builder(_ dict: Dictionary<String,PythonObject>) -> DynoResult<Dinosaur> {
            return  zip3(with:Dinosaur.init)(
                Dyno.getStr(dict, "id"),
                Dyno.getStr(dict, "name"),
                Dyno.getStr(dict, "colour"))
    }
    
    static func writer(_ v: Dinosaur) -> DynoResult<Dictionary<String,PythonObject>> {
        return .success(
            ["id":PythonObject(v.id),
             "name":PythonObject(v.name),
             "colour":PythonObject(v.colour)]
        )
    }
}


let 🦕 = Dyno(DynoOptions(log:true))
let disposeBag = DisposeBag() // Get rid of our observables when we're done.


// By using observables, we can say: set 2 items in parallel, and only scan the table _after_ we
// have successfully stored both of them on the db.
// "merge" means "run in parallel" (merge the Observable streams)
// "concat" means "stream 2 waits for stream 1 to complete"
//   Note that 'arrayBox' just converts the single element results from the 'setItem' into an array
//   for easier concatenation.

Observable.merge(
    🦕.setItem( inTable: "Dinosaurs",
                  value: Dinosaur(id: "1", name: "Emojisaurus", colour: "blue"),
                  writing: Dinosaur.writer ),

    🦕.setItem( inTable: "Dinosaurs",
                  value: Dinosaur(id: "2", name: "Tyrannosaurus", colour: "green"),
                  writing: Dinosaur.writer )
)
.arrayBox()
.concat(
        🦕.scan(inTable: "Dinosaurs", building: Dinosaur.builder)
).subscribe().disposed(by: disposeBag)

// we wait for all the above to happen - note that all of it is non-blocking!
sleep(10)

NSLog("Done")
