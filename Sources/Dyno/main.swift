import Foundation
import PythonKit


let boto3 = Python.import("boto3")
let table = boto3.Table("Dinosaurs")
print( table.scan() )

table.put_item(
         Item:{
             "id" : "1",
             "name" : "Emojisaurus",
             "weight" : 30,
             "height" : 15,
             "colour" : "blue"
})


print( table.scan() )
