/*import Foundation
import RxSwift

enum Colour : String, Codable {
    case red, blue, green, grey, pink, silver, black
}

/*  The old Dinosaur structure
struct Dinosaur : Codable {
    let id: String
    let name: String
    let colour: String
}
*/

/// Our new way of storing dinosaurs.  You might not want to change the structure name
struct MultiColouredDinosaur : Codable {
    let id: String
    let name: String
    let teeth: Int?
    let colours: [Colour]
    
    enum CodingKeys : String, CodingKey {
        case id, name, teeth, colour, colours
    }
    
    init(id: String, name: String, teeth: Int, colours: [Colour]) {
        self.id = id
        self.name = name
        self.teeth = teeth
        self.colours = colours
    }

    /// We use our own Decoder to demonstrate upgrading to a different data model
    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try values.decode(String.self, forKey: .id)
        self.name = try values.decode(String.self, forKey: .name)
        self.teeth = try? values.decode(Int.self, forKey: .teeth) // if we can't find "teeth", set nil
        
        // Convert the old 'colour' string into the 'colours' enum
        if let colours = try? values.decode(Array<Colour>.self, forKey: .colours) {
            self.colours = colours
        } else {
            if let colour = Colour(rawValue: try values.decode(String.self, forKey: .colour) ) {
                self.colours = [colour]
            } else {
                throw DynoError("Don't recognize dinosaur colour")
            }
        }
    }
    
    /// We use our own Encoder to demonstrate upgrading to a different data model
    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(name, forKey: .name)
        try container.encode(teeth, forKey: .teeth)
        try container.encode(colours, forKey: .colours)
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
                  value: MultiColouredDinosaur(id: "6", name: "Pinkisaur", teeth: 40, colours: [.pink])),

    🦕.setItem( inTable: "Dinosaurs",
                  value: MultiColouredDinosaur(id: "7", name: "Dottisaur", teeth: 50, colours: [.black, .blue]))
)
.arrayBox()
.concat(
        🦕.scan(inTable: "Dinosaurs" /*, filter: DynoFilter.between(DynoPathNonKey("teeth"), from: "35", to: "45") */, ofType: MultiColouredDinosaur.self)
).subscribe().disposed(by: disposeBag)

// we wait for all the above to happen - note that all of it is non-blocking!
sleep(2)

NSLog("Done")
*/
